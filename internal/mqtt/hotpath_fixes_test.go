package mqtt

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"monstermq.io/edge/internal/mqtt/packets"
)

// Regression tests for dev/plans/plan-mqtt-perf-fixes.md. Each test drives a
// client through EstablishConnection over net.Pipe and checks what reaches the
// wire.

func newHotpathServer(t *testing.T, caps *Capabilities) (*Server, *aclRecHook) {
	t.Helper()
	s := New(&Options{
		InlineClient: true,
		Capabilities: caps,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	h := new(aclRecHook)
	if err := s.AddHook(h, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, h
}

type rawClient struct {
	t       *testing.T
	conn    net.Conn
	r       *bufio.Reader
	done    chan struct{} // closed when the server's connection handler returned
	aliases map[uint16]string
}

type rawConnect struct {
	id             string
	clean          bool
	sessionExpiry  uint32
	receiveMaximum uint16
	maxPacketSize  uint32
	aliasMaximum   uint16
}

// dialRaw connects an MQTT 5 client and returns it with the CONNACK.
func dialRaw(t *testing.T, s *Server, c rawConnect) (*rawClient, packets.Packet) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	rc := &rawClient{t: t, conn: clientSide, r: bufio.NewReader(clientSide), done: make(chan struct{}), aliases: map[uint16]string{}}
	go func() {
		defer close(rc.done)
		_ = s.EstablishConnection("t1", serverSide)
	}()
	t.Cleanup(func() {
		_ = clientSide.Close()
		_ = serverSide.Close()
	})
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Connect},
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			Clean:            c.clean,
			Keepalive:        60,
			ClientIdentifier: c.id,
		},
		Properties: packets.Properties{
			ReceiveMaximum:    c.receiveMaximum,
			MaximumPacketSize: c.maxPacketSize,
			TopicAliasMaximum: c.aliasMaximum,
		},
	}
	if c.sessionExpiry > 0 {
		pk.Properties.SessionExpiryInterval = c.sessionExpiry
		pk.Properties.SessionExpiryIntervalFlag = true
	}
	rc.send(pk)
	connack := rc.read(2 * time.Second)
	if connack.FixedHeader.Type != packets.Connack || connack.ReasonCode != packets.CodeSuccess.Code {
		t.Fatalf("expected successful CONNACK, got type=%v code=0x%02x", connack.FixedHeader.Type, connack.ReasonCode)
	}
	return rc, connack
}

func (c *rawClient) send(pk packets.Packet) {
	c.t.Helper()
	pk.ProtocolVersion = 5
	var buf bytes.Buffer
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(&buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(&buf)
	case packets.Publish:
		err = pk.PublishEncode(&buf)
	case packets.Puback:
		err = pk.PubackEncode(&buf)
	case packets.Pingreq:
		err = pk.PingreqEncode(&buf)
	default:
		c.t.Fatalf("unsupported client packet type %v", pk.FixedHeader.Type)
	}
	if err != nil {
		c.t.Fatalf("encode: %v", err)
	}
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

func (c *rawClient) subscribe(filter string, qos byte) {
	c.t.Helper()
	c.send(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		PacketID:    1,
		Filters:     packets.Subscriptions{{Filter: filter, Qos: qos}},
	})
	if pk := c.read(2 * time.Second); pk.FixedHeader.Type != packets.Suback {
		c.t.Fatalf("expected SUBACK, got %v", pk.FixedHeader.Type)
	}
}

// read returns the next packet from the server, failing the test on timeout.
func (c *rawClient) read(timeout time.Duration) packets.Packet {
	c.t.Helper()
	pk, err := c.tryRead(timeout)
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return pk
}

// tryRead returns the next packet. A PUBLISH has its topic alias resolved
// against this connection's aliases; an alias the client was never told about
// fails the test, as it would be a protocol error for a real client.
func (c *rawClient) tryRead(timeout time.Duration) (packets.Packet, error) {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	b, err := c.r.ReadByte()
	if err != nil {
		return packets.Packet{}, err
	}
	var fh packets.FixedHeader
	if err := fh.Decode(b); err != nil {
		return packets.Packet{}, err
	}
	rem, _, err := packets.DecodeLength(c.r)
	if err != nil {
		return packets.Packet{}, err
	}
	fh.Remaining = rem
	body := make([]byte, rem)
	if _, err := io.ReadFull(c.r, body); err != nil {
		return packets.Packet{}, err
	}
	pk := packets.Packet{FixedHeader: fh, ProtocolVersion: 5}
	switch fh.Type {
	case packets.Connack:
		err = pk.ConnackDecode(body)
	case packets.Suback:
		err = pk.SubackDecode(body)
	case packets.Publish:
		err = pk.PublishDecode(body)
	case packets.Puback:
		err = pk.PubackDecode(body)
	case packets.Pingresp:
		err = pk.PingrespDecode(body)
	case packets.Disconnect:
		err = pk.DisconnectDecode(body)
	default:
		c.t.Fatalf("unexpected server packet type %v", fh.Type)
	}
	if err != nil {
		return packets.Packet{}, err
	}
	if fh.Type == packets.Publish {
		pk.TopicName = strings.Clone(pk.TopicName)
		if pk.Properties.TopicAliasFlag && pk.Properties.TopicAlias > 0 {
			if pk.TopicName != "" {
				c.aliases[pk.Properties.TopicAlias] = pk.TopicName
			} else if topic, ok := c.aliases[pk.Properties.TopicAlias]; ok {
				pk.TopicName = topic
			} else {
				c.t.Fatalf("PUBLISH uses topic alias %d, which was never sent on this connection", pk.Properties.TopicAlias)
			}
		}
		if pk.TopicName == "" {
			c.t.Fatal("PUBLISH has neither a topic name nor a topic alias")
		}
	}
	return pk, nil
}

// readPublish skips packets until a PUBLISH arrives.
func (c *rawClient) readPublish(timeout time.Duration) packets.Packet {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		pk := c.read(time.Until(deadline))
		if pk.FixedHeader.Type == packets.Publish {
			return pk
		}
	}
}

func (c *rawClient) close() {
	_ = c.conn.Close()
	select {
	case <-c.done:
	case <-time.After(3 * time.Second):
		c.t.Fatal("server did not end the connection")
	}
}

// Item 2: only PUBLISH bodies are cut from the shared read slab, so the strings
// of a SUBSCRIBE (stored for the session) do not keep a slab alive.
func TestReadSlabOnlyForPublish(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	c1, c2 := net.Pipe()
	t.Cleanup(func() { _ = c1.Close(); _ = c2.Close() })
	cl := s.NewClient(c1, "t1", "slab", false)
	cl.Properties.ProtocolVersion = 5

	read := func(pk packets.Packet) packets.Packet {
		t.Helper()
		pk.ProtocolVersion = 5
		var buf bytes.Buffer
		var err error
		if pk.FixedHeader.Type == packets.Publish {
			err = pk.PublishEncode(&buf)
		} else {
			err = pk.SubscribeEncode(&buf)
		}
		if err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = c2.Write(buf.Bytes()) }()
		var fh packets.FixedHeader
		if err := cl.ReadFixedHeader(&fh); err != nil {
			t.Fatal(err)
		}
		out, err := cl.ReadPacket(&fh)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	read(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		PacketID:    1,
		Filters:     packets.Subscriptions{{Filter: "sensors/+/temperature", Qos: 1}},
	})
	if cl.Net.readSlab != nil {
		t.Fatal("SUBSCRIBE body was cut from the shared read slab")
	}

	pub := read(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "a/b", Payload: []byte("x")})
	if pub.TopicName != "a/b" || cl.Net.readSlab == nil {
		t.Fatal("PUBLISH body was not cut from the shared read slab")
	}
}

// Item 1: clients without a connection never run a writer and get no queues.
func TestClientWithoutConnectionHasNoWriteQueues(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	cl := s.NewClient(nil, LocalListener, "inline-x", true)
	if cl.State.outbound != nil || cl.State.control != nil {
		t.Fatal("client without a connection has write queues")
	}
}

// Item 1: a persistent session stays in Server.Clients after its connection
// ends, but its read buffers are released.
func TestReadBuffersReleasedWhenConnectionEnds(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	c, _ := dialRaw(t, s, rawConnect{id: "keep", sessionExpiry: 300})
	c.subscribe("x", 0)
	c.close()

	cl, ok := s.Clients.Get("keep")
	if !ok {
		t.Fatal("persistent session was removed")
	}
	if cl.Net.bconn != nil || cl.Net.readSlab != nil {
		t.Fatal("read buffers kept after the connection ended")
	}
}

// Item 3: acks written while live publishes are queued are not held back when
// those publishes are all dropped for exceeding the client's Maximum Packet Size.
func TestAckNotStalledByOversizePublishes(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	c, _ := dialRaw(t, s, rawConnect{id: "stall", clean: true, maxPacketSize: 128})
	c.subscribe("big", 0)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		big := bytes.Repeat([]byte("x"), 1024)
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.Publish("big", big, false, 0)
			}
		}
	}()
	defer func() { close(stop); wg.Wait() }()

	for id := uint16(1); id <= 200; id++ {
		c.send(packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID:    id,
			TopicName:   "in",
			Payload:     []byte("y"),
		})
		for {
			pk, err := c.tryRead(2 * time.Second)
			if err != nil {
				t.Fatalf("PUBACK %d did not arrive: %v", id, err)
			}
			if pk.FixedHeader.Type == packets.Puback && pk.PacketID == id {
				break
			}
		}
	}
}

// Item 4: an oversize QoS 1 message is discarded as if delivered: it leaves
// Inflight and returns its send quota, later messages are not deferred, and the
// session can be resumed.
func TestOversizeQoSMessageReleasesInflightAndQuota(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	conn := rawConnect{id: "over", sessionExpiry: 300, receiveMaximum: 2, maxPacketSize: 128}
	c, _ := dialRaw(t, s, conn)
	c.subscribe("big", 1)

	cl, _ := s.Clients.Get("over")
	big := bytes.Repeat([]byte("x"), 1024)
	for i := 0; i < 5; i++ {
		if err := s.Publish("big", big, false, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Publish("big", []byte("small"), false, 1); err != nil {
		t.Fatal(err)
	}

	pub := c.readPublish(2 * time.Second)
	if string(pub.Payload) != "small" {
		t.Fatalf("got payload %q, want small", pub.Payload)
	}
	if n := cl.State.Inflight.Len(); n != 1 {
		t.Fatalf("Inflight holds %d messages, want only the unacked small one", n)
	}
	c.send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: pub.PacketID})
	deadline := time.Now().Add(2 * time.Second)
	for cl.State.Inflight.Len() != 0 || cl.State.Inflight.SendQuota() != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("after PUBACK: Inflight=%d SendQuota=%d, want 0 and 2", cl.State.Inflight.Len(), cl.State.Inflight.SendQuota())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Leave an oversize message unacknowledged in the session, then resume it.
	if err := s.Publish("big", big, false, 1); err != nil {
		t.Fatal(err)
	}
	c.send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingreq}})
	if pk := c.read(2 * time.Second); pk.FixedHeader.Type != packets.Pingresp {
		t.Fatalf("expected PINGRESP, got %v", pk.FixedHeader.Type)
	}
	c.close()

	c2, connack := dialRaw(t, s, conn)
	if !connack.SessionPresent {
		t.Fatal("session was not resumed")
	}
	c2.send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingreq}})
	pk, err := c2.tryRead(2 * time.Second)
	if err != nil || pk.FixedHeader.Type != packets.Pingresp {
		t.Fatalf("resumed connection did not answer PINGREQ: type=%v err=%v", pk.FixedHeader.Type, err)
	}
}

// Item 5: the first packet the client receives for a topic carries the topic
// name, even when an earlier packet for that topic was deferred by the send quota.
func TestTopicAliasNotUsedBeforeDeferredPacketIsSent(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	c, _ := dialRaw(t, s, rawConnect{id: "alias-defer", clean: true, receiveMaximum: 1, aliasMaximum: 10})
	c.subscribe("a/#", 1)

	if err := s.Publish("a/0", []byte("p0"), false, 1); err != nil {
		t.Fatal(err)
	}
	first := c.readPublish(2 * time.Second) // keeps the only send quota unit
	if err := s.Publish("a/t", []byte("p1"), false, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Publish("a/t", []byte("p2"), false, 0); err != nil {
		t.Fatal(err)
	}
	if pk := c.readPublish(2 * time.Second); pk.TopicName != "a/t" || string(pk.Payload) != "p2" {
		t.Fatalf("got %s=%q, want a/t=p2", pk.TopicName, pk.Payload)
	}
	c.send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: first.PacketID})
	if pk := c.readPublish(2 * time.Second); pk.TopicName != "a/t" || string(pk.Payload) != "p1" {
		t.Fatalf("got %s=%q, want a/t=p1", pk.TopicName, pk.Payload)
	}
}

// Item 5: publishes dropped from a full outbound queue register no alias.
func TestTopicAliasNotRegisteredByDroppedPacket(t *testing.T) {
	caps := NewDefaultServerCapabilities()
	caps.MaximumClientWritesPending = 4
	s, _ := newHotpathServer(t, caps)
	c, _ := dialRaw(t, s, rawConnect{id: "alias-drop", clean: true, aliasMaximum: 100})
	c.subscribe("q/#", 0)

	topics := make([]string, 50)
	for i := range topics {
		topics[i] = "q/" + Int64toa(int64(i))
	}
	for _, topic := range topics { // the client isn't reading, so most of these are dropped
		_ = s.Publish(topic, []byte("first"), false, 0)
	}
	for {
		if _, err := c.tryRead(300 * time.Millisecond); err != nil {
			break
		}
	}
	for _, topic := range topics {
		if err := s.Publish(topic, []byte("second"), false, 0); err != nil {
			t.Fatal(err)
		}
		pk := c.readPublish(2 * time.Second)
		if pk.TopicName != topic || string(pk.Payload) != "second" {
			t.Fatalf("got %s=%q, want %s=second", pk.TopicName, pk.Payload, topic)
		}
	}
}

// Item 6: inflight messages keep their full topic. On a resumed session they
// are checked against the ACL with the real topic and resent without the old
// connection's aliases.
func TestResentInflightMessagesKeepTopic(t *testing.T) {
	s, h := newHotpathServer(t, nil)
	conn := rawConnect{id: "resume", sessionExpiry: 300, aliasMaximum: 10}
	c, _ := dialRaw(t, s, conn)
	c.subscribe("r", 1)
	for _, p := range []string{"one", "two"} {
		if err := s.Publish("r", []byte(p), false, 1); err != nil {
			t.Fatal(err)
		}
		if pk := c.readPublish(2 * time.Second); string(pk.Payload) != p { // not acknowledged
			t.Fatalf("got %q, want %q", pk.Payload, p)
		}
	}
	c.close()

	conn.aliasMaximum = 0 // the new connection accepts no aliases at all
	c2, _ := dialRaw(t, s, conn)
	for _, p := range []string{"one", "two"} {
		pk := c2.readPublish(2 * time.Second)
		if pk.TopicName != "r" || string(pk.Payload) != p || pk.Properties.TopicAliasFlag {
			t.Fatalf("resent %s=%q alias=%d, want r=%q without alias", pk.TopicName, pk.Payload, pk.Properties.TopicAlias, p)
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, topic := range h.topics {
		if topic == "" {
			t.Fatal("ACL was checked against an empty topic")
		}
	}
}

// Item 7: a live publish queued before the CONNACK is written after it.
func TestConnackWrittenBeforeQueuedPublish(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	c1, c2 := net.Pipe()
	t.Cleanup(func() { _ = c1.Close(); _ = c2.Close() })
	cl := s.NewClient(c1, "t1", "connack-first", false)
	cl.Properties.ProtocolVersion = 5
	go cl.WriteLoop()
	t.Cleanup(func() { cl.Stop(nil) })

	cl.State.outbound.push(&packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish},
		TopicName:   "early",
		Payload:     []byte("x"),
	})
	time.Sleep(50 * time.Millisecond)
	go func() {
		_ = cl.WritePacket(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connack}})
	}()

	rc := &rawClient{t: t, conn: c2, r: bufio.NewReader(c2), aliases: map[uint16]string{}}
	if pk := rc.read(2 * time.Second); pk.FixedHeader.Type != packets.Connack {
		t.Fatalf("first packet is %v, want CONNACK", packets.PacketNames[pk.FixedHeader.Type])
	}
	if pk := rc.read(2 * time.Second); pk.FixedHeader.Type != packets.Publish || pk.TopicName != "early" {
		t.Fatalf("second packet is %v %q, want the queued PUBLISH", packets.PacketNames[pk.FixedHeader.Type], pk.TopicName)
	}
}

// A DISCONNECT sent by the server reaches the client before the connection is
// closed, now that writes are asynchronous.
func TestDisconnectWrittenBeforeClose(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	c, _ := dialRaw(t, s, rawConnect{id: "bye", clean: true})
	cl, _ := s.Clients.Get("bye")
	go func() { _ = s.DisconnectClient(cl, packets.ErrServerShuttingDown) }()
	pk, err := c.tryRead(2 * time.Second)
	if err != nil {
		t.Fatalf("no DISCONNECT before close: %v", err)
	}
	if pk.FixedHeader.Type != packets.Disconnect || pk.ReasonCode != packets.ErrServerShuttingDown.Code {
		t.Fatalf("got %v code 0x%02x, want DISCONNECT 0x8B", packets.PacketNames[pk.FixedHeader.Type], pk.ReasonCode)
	}
	if _, err := c.tryRead(2 * time.Second); !errors.Is(err, io.EOF) {
		t.Fatalf("connection not closed after DISCONNECT: %v", err)
	}
}

// Item 1: the outbound queue keeps FIFO order across growth and wrap-around,
// refuses packets beyond its limit, and releases its ring once drained.
func TestOutboundQueueGrowsAndShrinks(t *testing.T) {
	q := newOutboundQueue(1000)
	if q.ring != nil {
		t.Fatal("new queue allocated a ring")
	}
	next, want := 0, 0
	push := func(n int) {
		for i := 0; i < n; i++ {
			if !q.push(&packets.Packet{PacketID: uint16(next)}) {
				t.Fatalf("push %d refused below the limit", next)
			}
			next++
		}
	}
	pop := func(n int) {
		for i := 0; i < n; i++ {
			pk, ok := q.pop()
			if !ok || int(pk.PacketID) != want {
				t.Fatalf("pop = %v %v, want %d", pk, ok, want)
			}
			want++
		}
	}
	push(10)
	pop(7)
	push(600) // grows while the ring has wrapped
	pop(300)
	push(697) // up to the limit
	if q.push(&packets.Packet{}) {
		t.Fatal("push beyond the limit was accepted")
	}
	pop(q.len())
	if _, ok := q.pop(); ok || q.ring != nil {
		t.Fatalf("drained queue: ring of %d entries kept", len(q.ring))
	}
}

// Item 3: a SUBACK queued before the retained messages of its subscription is
// written before them, although live publishes and control packets travel in
// separate queues.
func TestSubackBeforeRetainedMessages(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	for i := 0; i < 50; i++ {
		if err := s.Publish("ret/"+Int64toa(int64(i)), []byte("v"), true, 0); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; round < 100; round++ {
		c, _ := dialRaw(t, s, rawConnect{id: "sr" + Int64toa(int64(round)), clean: true})
		c.send(packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
			PacketID:    7,
			Filters:     packets.Subscriptions{{Filter: "ret/#", Qos: 0}},
		})
		if pk := c.read(2 * time.Second); pk.FixedHeader.Type != packets.Suback {
			t.Fatalf("round %d: first packet after SUBSCRIBE is %v, want SUBACK", round, packets.PacketNames[pk.FixedHeader.Type])
		}
		c.close()
	}
}

// Item 3: nothing follows a DISCONNECT sent by the server [MQTT-3.14.4-1], even
// while live publishes keep arriving for the client.
func TestNothingWrittenAfterServerDisconnect(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	for round := 0; round < 20; round++ {
		id := "flooded" + Int64toa(int64(round))
		c, _ := dialRaw(t, s, rawConnect{id: id, clean: true})
		c.subscribe("flood", 0)
		cl, _ := s.Clients.Get(id)

		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			p := bytes.Repeat([]byte("x"), 200)
			for {
				select {
				case <-stop:
					return
				default:
					_ = s.Publish("flood", p, false, 0)
				}
			}
		}()
		for i := 0; i < 100; i++ {
			c.read(2 * time.Second)
		}
		go func() { _ = s.DisconnectClient(cl, packets.ErrServerShuttingDown) }()
		sawDisconnect, after := false, 0
		for {
			pk, err := c.tryRead(2 * time.Second)
			if err != nil {
				break
			}
			if pk.FixedHeader.Type == packets.Disconnect {
				sawDisconnect = true
			} else if sawDisconnect {
				after++
			}
		}
		close(stop)
		<-done
		if !sawDisconnect || after > 0 {
			t.Fatalf("round %d: DISCONNECT received=%v, packets after it=%d", round, sawDisconnect, after)
		}
		c.close()
	}
}

// Item 4: only application messages are discarded for their size. A CONNACK
// larger than the client's Maximum Packet Size closes the connection, as before.
func TestOversizeConnackClosesConnection(t *testing.T) {
	s, _ := newHotpathServer(t, nil)
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { _ = clientSide.Close(); _ = serverSide.Close() })
	rc := &rawClient{t: t, conn: clientSide, r: bufio.NewReader(clientSide), done: make(chan struct{}), aliases: map[uint16]string{}}
	go func() { defer close(rc.done); _ = s.EstablishConnection("t1", serverSide) }()
	rc.send(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Connect},
		Connect:     packets.ConnectParams{ProtocolName: []byte("MQTT"), Clean: true, Keepalive: 60, ClientIdentifier: "tiny"},
		Properties:  packets.Properties{MaximumPacketSize: 6},
	})
	if pk, err := rc.tryRead(2 * time.Second); err == nil {
		t.Fatalf("received %v, want the connection closed", packets.PacketNames[pk.FixedHeader.Type])
	}
	select {
	case <-rc.done:
	case <-time.After(2 * time.Second):
		t.Fatal("connection with an undeliverable CONNACK was kept open")
	}
}
