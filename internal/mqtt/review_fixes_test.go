package mqtt

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"monstermq.io/edge/internal/mqtt/packets"
)

// Regression tests for dev/done/plan-mqtt-code-review-findings.md.

func TestInlineHashSubscriptionMatchesNestedTopic(t *testing.T) {
	x := NewTopicsIndex()
	x.InlineSubscribe(InlineSubscription{
		Subscription: packets.Subscription{Filter: "foo/#", Identifier: 1},
		Handler:      func(cl *Client, sub packets.Subscription, pk packets.Packet) {},
	})

	for _, topic := range []string{"foo", "foo/bar", "foo/bar/baz"} {
		if subs := x.Subscribers(topic); len(subs.InlineSubscriptions) != 1 {
			t.Errorf("%s: got %d inline subscriptions, want 1", topic, len(subs.InlineSubscriptions))
		}
	}
	if subs := x.Subscribers("other/bar"); len(subs.InlineSubscriptions) != 0 {
		t.Errorf("other/bar: got %d inline subscriptions, want 0", len(subs.InlineSubscriptions))
	}
}

func TestIsValidFilterRejectsPartialWildcardLevels(t *testing.T) {
	invalid := []string{"sport/tennis#", "foo+bar", "foo/+bar/baz", "foo/#/bar", "#/foo", "$share//foo", "$share/group/"}
	for _, f := range invalid {
		if IsValidFilter(f, false) {
			t.Errorf("IsValidFilter(%q) = true, want false", f)
		}
	}

	valid := []string{"#", "+", "foo/#", "foo/+/bar", "+/+", "/foo", "foo//bar", "$share/group/foo/#", "$share/group/+"}
	for _, f := range valid {
		if !IsValidFilter(f, false) {
			t.Errorf("IsValidFilter(%q) = false, want true", f)
		}
	}
}

func TestNextImmediateReturnsDeferredPacket(t *testing.T) {
	i := NewInflights()
	i.Set(packets.Packet{PacketID: 1, Expiry: 100})
	i.Set(packets.Packet{PacketID: 2, Expiry: -1})

	pk, ok := i.NextImmediate()
	if !ok || pk.PacketID != 2 {
		t.Fatalf("NextImmediate() = %d, %v; want 2, true", pk.PacketID, ok)
	}
}

func TestQuotaGetters(t *testing.T) {
	i := NewInflights()
	i.ResetReceiveQuota(3)
	i.ResetSendQuota(2)
	i.DecreaseReceiveQuota()
	i.DecreaseSendQuota()

	if got := i.ReceiveQuota(); got != 2 {
		t.Errorf("ReceiveQuota() = %d, want 2", got)
	}
	if got := i.SendQuota(); got != 1 {
		t.Errorf("SendQuota() = %d, want 1", got)
	}
	if got := i.MaximumSendQuota(); got != 2 {
		t.Errorf("MaximumSendQuota() = %d, want 2", got)
	}
}

type aclRecHook struct {
	HookBase
	mu     sync.Mutex
	topics []string
}

func (h *aclRecHook) ID() string { return "acl-rec" }

func (h *aclRecHook) Provides(b byte) bool {
	return b == OnACLCheck || b == OnConnectAuthenticate
}

func (h *aclRecHook) OnConnectAuthenticate(_ *Client, _ packets.Packet) bool {
	return true
}

func (h *aclRecHook) OnACLCheck(_ *Client, topic string, _ bool) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.topics = append(h.topics, topic)
	return true
}

func newAliasClient(t *testing.T) (*Server, *Client, *aclRecHook) {
	t.Helper()
	s := New(&Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	h := new(aclRecHook)
	if err := s.AddHook(h, nil); err != nil {
		t.Fatal(err)
	}
	c1, c2 := net.Pipe()
	go func() { _, _ = io.Copy(io.Discard, c2) }()
	t.Cleanup(func() { _ = c1.Close(); _ = c2.Close() })
	cl := s.NewClient(c1, "t1", "alias-client", false)
	cl.Properties.ProtocolVersion = 5
	return s, cl, h
}

func aliasPk(topic string, alias uint16) packets.Packet {
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: topic, Payload: []byte("x")}
	pk.Properties.TopicAlias = alias
	pk.Properties.TopicAliasFlag = true
	return pk
}

func TestTopicAliasResolvedBeforeACL(t *testing.T) {
	s, cl, h := newAliasClient(t)
	if err := s.processPublish(cl, aliasPk("a/b", 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.processPublish(cl, aliasPk("", 1)); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.topics) != 2 || h.topics[0] != "a/b" || h.topics[1] != "a/b" {
		t.Fatalf("ACL saw topics %q, want [a/b a/b]", h.topics)
	}
}

func TestUnknownTopicAliasDisconnects(t *testing.T) {
	s, cl, h := newAliasClient(t)
	err := s.processPublish(cl, aliasPk("", 7))
	if !errors.Is(err, packets.ErrTopicAliasInvalid) {
		t.Fatalf("err = %v, want ErrTopicAliasInvalid", err)
	}
	if len(h.topics) != 0 {
		t.Fatalf("ACL checked %q for an unknown alias", h.topics)
	}
	if got := cl.State.TopicAliases.Inbound.Set(7, ""); got != "" {
		t.Fatalf("unknown alias was stored as %q", got)
	}
}

func TestConnectTimeoutClosesSilentConnection(t *testing.T) {
	s := New(&Options{ConnectTimeout: 50 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	c1, c2 := net.Pipe()
	defer c2.Close()

	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("t1", c1) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("EstablishConnection returned nil for a connection that never sent CONNECT")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection without CONNECT was not closed")
	}
}

func writeClientPacket(t *testing.T, w io.Writer, pk packets.Packet) {
	t.Helper()
	pk.ProtocolVersion = 5
	var buf bytes.Buffer
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(&buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(&buf)
	case packets.Puback:
		err = pk.PubackEncode(&buf)
	case packets.Pingreq:
		err = pk.PingreqEncode(&buf)
	default:
		t.Fatalf("unsupported client packet type %v", pk.FixedHeader.Type)
	}
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		t.Fatalf("write packet: %v", err)
	}
}

func readServerPacket(t *testing.T, r *bufio.Reader) packets.Packet {
	t.Helper()
	b, err := r.ReadByte()
	if err != nil {
		t.Fatalf("read header byte: %v", err)
	}
	var fh packets.FixedHeader
	if err := fh.Decode(b); err != nil {
		t.Fatalf("decode header: %v", err)
	}
	rem, _, err := packets.DecodeLength(r)
	if err != nil {
		t.Fatalf("decode length: %v", err)
	}
	fh.Remaining = rem
	body := make([]byte, rem)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	pk := packets.Packet{FixedHeader: fh, ProtocolVersion: 5}
	switch fh.Type {
	case packets.Connack:
		err = pk.ConnackDecode(body)
	case packets.Suback:
		err = pk.SubackDecode(body)
	case packets.Publish:
		err = pk.PublishDecode(body)
	case packets.Pubrec:
		err = pk.PubrecDecode(body)
	case packets.Pubrel:
		err = pk.PubrelDecode(body)
	case packets.Pubcomp:
		err = pk.PubcompDecode(body)
	case packets.Pingresp:
		err = pk.PingrespDecode(body)
	default:
		t.Fatalf("unexpected server packet type %v", fh.Type)
	}
	if err != nil {
		t.Fatalf("decode server packet: %v", err)
	}
	return pk
}

func connectV5PipeClient(t *testing.T, s *Server, clientID string, recvMax uint16) (net.Conn, *bufio.Reader) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() {
		_ = clientSide.Close()
		_ = serverSide.Close()
	})
	go func() { _ = s.EstablishConnection("t1", serverSide) }()

	r := bufio.NewReader(clientSide)
	writeClientPacket(t, clientSide, packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			Clean:            true,
			Keepalive:        60,
			ClientIdentifier: clientID,
		},
		Properties: packets.Properties{
			ReceiveMaximum: recvMax,
		},
	})
	connack := readServerPacket(t, r)
	if connack.FixedHeader.Type != packets.Connack || connack.ReasonCode != packets.CodeSuccess.Code {
		t.Fatalf("expected successful CONNACK, got type=%v code=0x%02x", connack.FixedHeader.Type, connack.ReasonCode)
	}
	return clientSide, r
}

// Item 1: A client with Receive Maximum 1 is sent three or more QoS 1 messages.
// All of them are delivered, each is acked, and the send quota returns to 1.
func TestDeferredQoSSendKeepsAckState(t *testing.T) {
	s := New(&Options{
		InlineClient: true,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = s.Close() })
	if err := s.AddHook(new(aclRecHook), nil); err != nil {
		t.Fatal(err)
	}

	conn, r := connectV5PipeClient(t, s, "rm1-client", 1)

	writeClientPacket(t, conn, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		PacketID:    1,
		Filters:     packets.Subscriptions{{Filter: "test/rm1", Qos: 1}},
	})
	if suback := readServerPacket(t, r); suback.FixedHeader.Type != packets.Suback {
		t.Fatalf("expected SUBACK, got %v", suback.FixedHeader.Type)
	}

	cl, ok := s.Clients.Get("rm1-client")
	if !ok {
		t.Fatal("client rm1-client not found")
	}

	payloads := []string{"msg-1", "msg-2", "msg-3", "msg-4"}
	for _, p := range payloads {
		if err := s.Publish("test/rm1", []byte(p), false, 1); err != nil {
			t.Fatalf("Publish(%q): %v", p, err)
		}
	}

	for idx, want := range payloads {
		pub := readServerPacket(t, r)
		if pub.FixedHeader.Type != packets.Publish {
			t.Fatalf("msg %d: expected PUBLISH, got %v", idx, pub.FixedHeader.Type)
		}
		if string(pub.Payload) != want {
			t.Fatalf("msg %d: got payload %q, want %q", idx, pub.Payload, want)
		}
		// While unacked on the wire, the packet must remain in Inflight with non-negative Expiry
		// and SendQuota must be 0.
		stored, exists := cl.State.Inflight.Get(pub.PacketID)
		if !exists {
			t.Fatalf("msg %d (id=%d): missing from Inflight before PUBACK", idx, pub.PacketID)
		}
		if stored.Expiry < 0 {
			t.Fatalf("msg %d (id=%d): sent packet still has negative Expiry %d", idx, pub.PacketID, stored.Expiry)
		}
		if got := cl.State.Inflight.SendQuota(); got != 0 {
			t.Fatalf("msg %d: SendQuota while in-flight = %d, want 0", idx, got)
		}

		writeClientPacket(t, conn, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback},
			PacketID:    pub.PacketID,
		})
	}

	// Barrier with PINGREQ/PINGRESP so the final PUBACK has been processed on the server goroutine.
	writeClientPacket(t, conn, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pingreq},
	})
	if ping := readServerPacket(t, r); ping.FixedHeader.Type != packets.Pingresp {
		t.Fatalf("expected PINGRESP, got %v", ping.FixedHeader.Type)
	}

	if got := cl.State.Inflight.SendQuota(); got != 1 {
		t.Fatalf("final SendQuota = %d, want 1", got)
	}
	if got := cl.State.Inflight.Len(); got != 0 {
		t.Fatalf("final Inflight.Len() = %d, want 0", got)
	}
}

// Item 2: Valid QoS 2 transitions in both directions succeed. Invalid ones
// leave both quotas unchanged: PUBREC for a QoS 1 ID, PUBREL without PUBREC,
// and unsolicited PUBCOMP.
func TestQoS2AckStateValidationAndQuotas(t *testing.T) {
	s, cl, _ := newAliasClient(t)
	cl.State.Inflight.ResetReceiveQuota(2)
	cl.State.Inflight.ResetSendQuota(2)

	// 1. Valid inbound QoS 2 flow: PUBLISH -> PUBREC -> PUBREL -> PUBCOMP
	inPub := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2},
		PacketID:    10,
		TopicName:   "qos2/in",
		Payload:     []byte("hello"),
	}
	if err := s.processPublish(cl, inPub); err != nil {
		t.Fatalf("processPublish QoS 2: %v", err)
	}
	if got := cl.State.Inflight.ReceiveQuota(); got != 1 {
		t.Fatalf("after inbound QoS 2 PUBLISH: ReceiveQuota = %d, want 1", got)
	}
	if got := cl.State.Inflight.SendQuota(); got != 2 {
		t.Fatalf("after inbound QoS 2 PUBLISH: SendQuota = %d, want 2", got)
	}
	if err := s.processPubrel(cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1},
		PacketID:    10,
	}); err != nil {
		t.Fatalf("processPubrel: %v", err)
	}
	if got := cl.State.Inflight.ReceiveQuota(); got != 2 {
		t.Fatalf("after valid PUBREL: ReceiveQuota = %d, want 2", got)
	}
	if got := cl.State.Inflight.SendQuota(); got != 2 {
		t.Fatalf("after valid PUBREL: SendQuota = %d, want 2", got)
	}

	// 2. Valid outbound QoS 2 flow: publishToClient -> PUBREC -> PUBREL -> PUBCOMP
	sub := packets.Subscription{Filter: "qos2/out", Qos: 2}
	outPk, err := s.publishToClient(cl, sub, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2},
		TopicName:   "qos2/out",
		Payload:     []byte("world"),
	})
	if err != nil {
		t.Fatalf("publishToClient QoS 2: %v", err)
	}
	if got := cl.State.Inflight.SendQuota(); got != 1 {
		t.Fatalf("after outbound QoS 2 PUBLISH: SendQuota = %d, want 1", got)
	}
	if got := cl.State.Inflight.ReceiveQuota(); got != 2 {
		t.Fatalf("after outbound QoS 2 PUBLISH: ReceiveQuota = %d, want 2", got)
	}
	if err := s.processPubrec(cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubrec},
		PacketID:    outPk.PacketID,
	}); err != nil {
		t.Fatalf("processPubrec: %v", err)
	}
	if got := cl.State.Inflight.SendQuota(); got != 1 {
		t.Fatalf("after valid PUBREC: SendQuota = %d, want 1", got)
	}
	if got := cl.State.Inflight.ReceiveQuota(); got != 2 {
		t.Fatalf("after valid PUBREC: ReceiveQuota = %d, want 2", got)
	}
	if err := s.processPubcomp(cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubcomp},
		PacketID:    outPk.PacketID,
	}); err != nil {
		t.Fatalf("processPubcomp: %v", err)
	}
	if got := cl.State.Inflight.SendQuota(); got != 2 {
		t.Fatalf("after valid PUBCOMP: SendQuota = %d, want 2", got)
	}
	if got := cl.State.Inflight.ReceiveQuota(); got != 2 {
		t.Fatalf("after valid PUBCOMP: ReceiveQuota = %d, want 2", got)
	}

	// 3. Invalid transitions against an active QoS 1 outbound packet and unknown IDs
	qos1Pk, err := s.publishToClient(cl, packets.Subscription{Filter: "qos1/out", Qos: 1}, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   "qos1/out",
		Payload:     []byte("q1"),
	})
	if err != nil {
		t.Fatalf("publishToClient QoS 1: %v", err)
	}
	if cl.State.Inflight.SendQuota() != 1 || cl.State.Inflight.ReceiveQuota() != 2 {
		t.Fatalf("unexpected quotas before invalid transitions: send=%d recv=%d",
			cl.State.Inflight.SendQuota(), cl.State.Inflight.ReceiveQuota())
	}

	// 3a. PUBREC for a QoS 1 packet ID
	if err := s.processPubrec(cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubrec},
		PacketID:    qos1Pk.PacketID,
	}); err != nil {
		t.Fatalf("processPubrec for QoS 1 ID: %v", err)
	}
	if cl.State.Inflight.SendQuota() != 1 || cl.State.Inflight.ReceiveQuota() != 2 {
		t.Fatalf("PUBREC for QoS 1 ID changed quotas: send=%d recv=%d",
			cl.State.Inflight.SendQuota(), cl.State.Inflight.ReceiveQuota())
	}
	if stored, ok := cl.State.Inflight.Get(qos1Pk.PacketID); !ok || stored.FixedHeader.Type != packets.Publish || stored.FixedHeader.Qos != 1 {
		t.Fatalf("PUBREC for QoS 1 ID corrupted stored inflight entry: %+v, ok=%v", stored, ok)
	}

	// 3b. PUBREL without PUBREC (for QoS 1 ID and unknown ID)
	for _, id := range []uint16{qos1Pk.PacketID, 999} {
		if err := s.processPubrel(cl, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1},
			PacketID:    id,
		}); err != nil {
			t.Fatalf("processPubrel(id=%d): %v", id, err)
		}
		if cl.State.Inflight.SendQuota() != 1 || cl.State.Inflight.ReceiveQuota() != 2 {
			t.Fatalf("invalid PUBREL(id=%d) changed quotas: send=%d recv=%d",
				id, cl.State.Inflight.SendQuota(), cl.State.Inflight.ReceiveQuota())
		}
	}

	// 3c. Unsolicited PUBCOMP (for QoS 1 ID and unknown ID)
	for _, id := range []uint16{qos1Pk.PacketID, 999} {
		if err := s.processPubcomp(cl, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Pubcomp},
			PacketID:    id,
		}); err != nil {
			t.Fatalf("processPubcomp(id=%d): %v", id, err)
		}
		if cl.State.Inflight.SendQuota() != 1 || cl.State.Inflight.ReceiveQuota() != 2 {
			t.Fatalf("unsolicited PUBCOMP(id=%d) changed quotas: send=%d recv=%d",
				id, cl.State.Inflight.SendQuota(), cl.State.Inflight.ReceiveQuota())
		}
	}

	// Finally, valid PUBACK for the QoS 1 packet restores SendQuota to 2.
	if err := s.processPuback(cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Puback},
		PacketID:    qos1Pk.PacketID,
	}); err != nil {
		t.Fatalf("processPuback: %v", err)
	}
	if cl.State.Inflight.SendQuota() != 2 || cl.State.Inflight.ReceiveQuota() != 2 || cl.State.Inflight.Len() != 0 {
		t.Fatalf("after final PUBACK: send=%d recv=%d len=%d",
			cl.State.Inflight.SendQuota(), cl.State.Inflight.ReceiveQuota(), cl.State.Inflight.Len())
	}
}

type wireInflightHook struct {
	HookBase
	inFlight atomic.Int32
	maxSeen  atomic.Int32
}

func (h *wireInflightHook) ID() string { return "wire-inflight" }

func (h *wireInflightHook) Provides(b byte) bool {
	return b == OnPacketSent || b == OnQosComplete
}

func (h *wireInflightHook) OnPacketSent(_ *Client, pk packets.Packet, _ []byte) {
	if pk.FixedHeader.Type == packets.Publish && pk.FixedHeader.Qos > 0 {
		cur := h.inFlight.Add(1)
		for {
			prev := h.maxSeen.Load()
			if cur <= prev || h.maxSeen.CompareAndSwap(prev, cur) {
				break
			}
		}
	}
}

func (h *wireInflightHook) OnQosComplete(_ *Client, pk packets.Packet) {
	if pk.FixedHeader.Type == packets.Puback || pk.FixedHeader.Type == packets.Pubcomp {
		h.inFlight.Add(-1)
	}
}

// Item 3: Concurrent publishers to a single Receive Maximum 1 client while
// the client acks. The inflight count never exceeds 1, and the quota never goes
// negative or above its maximum.
func TestConcurrentPublishAndAckQuotaAtomic(t *testing.T) {
	s := New(&Options{
		InlineClient: true,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = s.Close() })
	if err := s.AddHook(new(aclRecHook), nil); err != nil {
		t.Fatal(err)
	}

	hook := new(wireInflightHook)
	if err := s.AddHook(hook, nil); err != nil {
		t.Fatal(err)
	}

	conn, r := connectV5PipeClient(t, s, "conc-rm1", 1)

	writeClientPacket(t, conn, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		PacketID:    1,
		Filters:     packets.Subscriptions{{Filter: "conc/test", Qos: 1}},
	})
	if suback := readServerPacket(t, r); suback.FixedHeader.Type != packets.Suback {
		t.Fatalf("expected SUBACK, got %v", suback.FixedHeader.Type)
	}

	cl, ok := s.Clients.Get("conc-rm1")
	if !ok {
		t.Fatal("client conc-rm1 not found")
	}

	const publishers = 8
	const msgsPerPublisher = 10
	const totalMsgs = publishers * msgsPerPublisher

	var pubWg sync.WaitGroup
	pubWg.Add(publishers)
	for p := 0; p < publishers; p++ {
		go func() {
			defer pubWg.Done()
			for m := 0; m < msgsPerPublisher; m++ {
				if err := s.Publish("conc/test", []byte("data"), false, 1); err != nil {
					t.Errorf("Publish error: %v", err)
				}
			}
		}()
	}

	for i := 0; i < totalMsgs; i++ {
		pub := readServerPacket(t, r)
		if pub.FixedHeader.Type != packets.Publish {
			t.Fatalf("msg %d: expected PUBLISH, got %v", i, pub.FixedHeader.Type)
		}
		if q := cl.State.Inflight.SendQuota(); q < 0 || q > 1 {
			t.Fatalf("msg %d: SendQuota out of bounds: %d", i, q)
		}
		writeClientPacket(t, conn, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback},
			PacketID:    pub.PacketID,
		})
	}

	pubWg.Wait()

	writeClientPacket(t, conn, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pingreq},
	})
	if ping := readServerPacket(t, r); ping.FixedHeader.Type != packets.Pingresp {
		t.Fatalf("expected PINGRESP, got %v", ping.FixedHeader.Type)
	}

	if got := hook.maxSeen.Load(); got > 1 {
		t.Fatalf("max concurrent unacknowledged QoS 1 packets on wire = %d, want <= 1", got)
	}
	if got := cl.State.Inflight.SendQuota(); got != 1 {
		t.Fatalf("final SendQuota = %d, want 1", got)
	}
	if got := cl.State.Inflight.Len(); got != 0 {
		t.Fatalf("final Inflight.Len() = %d, want 0", got)
	}
}

// Item 4: With MaximumClients reached by pending sockets, further
// sockets are rejected or bounded. Established sessions are unaffected.
func TestPendingConnectCountsAgainstMaximumClients(t *testing.T) {
	caps := NewDefaultServerCapabilities()
	caps.MaximumClients = 2
	s := New(&Options{
		Capabilities:   caps,
		ConnectTimeout: 2 * time.Second,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = s.Close() })
	if err := s.AddHook(new(aclRecHook), nil); err != nil {
		t.Fatal(err)
	}

	// 1. Establish one valid session (uses slot 1 of 2).
	estConn, estReader := connectV5PipeClient(t, s, "established-client", 10)

	// 2. Open a pending socket that never sends CONNECT (uses slot 2 of 2).
	pendingSrv1, pendingCli1 := net.Pipe()
	defer pendingCli1.Close()
	pendingDone1 := make(chan error, 1)
	go func() { pendingDone1 <- s.EstablishConnection("t1", pendingSrv1) }()

	// Wait until the pending socket has reserved its slot.
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt64(&s.Info.ClientsConnected) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := atomic.LoadInt64(&s.Info.ClientsConnected); got != 2 {
		t.Fatalf("ClientsConnected = %d, want 2 (1 established + 1 pending)", got)
	}

	// 3. A further socket must be rejected immediately because MaximumClients (2) is reached.
	extraSrv, extraCli := net.Pipe()
	defer extraCli.Close()
	err := s.EstablishConnection("t1", extraSrv)
	if !errors.Is(err, packets.ErrServerBusy) {
		t.Fatalf("EstablishConnection when full = %v, want ErrServerBusy", err)
	}

	// 4. The established session must remain unaffected and responsive.
	writeClientPacket(t, estConn, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pingreq},
	})
	if ping := readServerPacket(t, estReader); ping.FixedHeader.Type != packets.Pingresp {
		t.Fatalf("established client expected PINGRESP, got %v", ping.FixedHeader.Type)
	}

	// 5. Closing the pending socket frees its slot so a new client can connect.
	_ = pendingCli1.Close()
	select {
	case <-pendingDone1:
	case <-time.After(time.Second):
		t.Fatal("pending connection did not exit after close")
	}

	conn2, r2 := connectV5PipeClient(t, s, "second-client", 10)
	writeClientPacket(t, conn2, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pingreq},
	})
	if ping := readServerPacket(t, r2); ping.FixedHeader.Type != packets.Pingresp {
		t.Fatalf("second client expected PINGRESP, got %v", ping.FixedHeader.Type)
	}
}
