package mqttclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"monstermq.io/edge/internal/stores"
)

func TestOutboundFilterTreatsFixedLocalTopicAsPrefix(t *testing.T) {
	if got := outboundFilter("opcua/factory"); got != "opcua/factory/#" {
		t.Fatalf("outboundFilter = %q", got)
	}
	if got := outboundFilter("opcua/factory/#"); got != "opcua/factory/#" {
		t.Fatalf("outboundFilter wildcard = %q", got)
	}
}

func TestMapOutboundTopicFixedLocalPrefix(t *testing.T) {
	addr := Address{
		Mode:        "PUBLISH",
		LocalTopic:  "opcua/factory",
		RemoteTopic: "cloud/factory",
		RemovePath:  true,
	}
	if got := mapOutboundTopic(addr, "opcua/factory"); got != "cloud/factory" {
		t.Fatalf("exact topic mapped to %q", got)
	}
	if got := mapOutboundTopic(addr, "opcua/factory/line1/temp"); got != "cloud/factory/line1/temp" {
		t.Fatalf("child topic mapped to %q", got)
	}
}

func TestMapOutboundTopicWildcardLocalPrefix(t *testing.T) {
	addr := Address{
		Mode:        "PUBLISH",
		LocalTopic:  "opcua/factory/#",
		RemoteTopic: "cloud/factory",
		RemovePath:  true,
	}
	if got := mapOutboundTopic(addr, "opcua/factory/line1/temp"); got != "cloud/factory/line1/temp" {
		t.Fatalf("wildcard topic mapped to %q", got)
	}
}

func TestMapOutboundTopicRemoteWildcardUsesLiteralPrefix(t *testing.T) {
	addr := Address{
		Mode:        "PUBLISH",
		LocalTopic:  "opcua/factory/#",
		RemoteTopic: "cloud/factory/#",
		RemovePath:  true,
	}
	if got := mapOutboundTopic(addr, "opcua/factory/line1/temp"); got != "cloud/factory/line1/temp" {
		t.Fatalf("remote wildcard topic mapped to %q", got)
	}
}

func TestMapInboundTopicLocalWildcardUsesLiteralPrefix(t *testing.T) {
	addr := Address{
		Mode:        "SUBSCRIBE",
		RemoteTopic: "cloud/factory/#",
		LocalTopic:  "opcua/factory/#",
		RemovePath:  true,
	}
	if got := mapInboundTopic(addr, "cloud/factory/line1/temp"); got != "opcua/factory/line1/temp" {
		t.Fatalf("local wildcard topic mapped to %q", got)
	}
}

func TestMapInboundTopicWithoutRemovePathAppendsFullRemoteTopic(t *testing.T) {
	addr := Address{
		Mode:        "SUBSCRIBE",
		RemoteTopic: "cloud/factory/#",
		LocalTopic:  "opcua/factory",
		RemovePath:  false,
	}
	if got := mapInboundTopic(addr, "cloud/factory/line1/temp"); got != "opcua/factory/cloud/factory/line1/temp" {
		t.Fatalf("full remote topic mapped to %q", got)
	}
}

func TestMapInboundTopicExactMatch(t *testing.T) {
	addr := Address{
		Mode:        "SUBSCRIBE",
		RemoteTopic: "sensor/temp",
		LocalTopic:  "plant/temp",
	}
	if got := mapInboundTopic(addr, "sensor/temp"); got != "plant/temp" {
		t.Fatalf("exact inbound topic mapped to %q, want plant/temp", got)
	}
}

func TestConfigMillisOrSecondsSupportsOldSecondValues(t *testing.T) {
	if got := configMillisOrSeconds(10).Seconds(); got != 10 {
		t.Fatalf("duration for small value = %v seconds, want 10", got)
	}
	if got := configMillisOrSeconds(30000).Seconds(); got != 30 {
		t.Fatalf("duration for millisecond value = %v seconds, want 30", got)
	}
}

type fakePublish struct {
	topic  string
	msg    stores.BrokerMessage
	qos    byte
	retain bool
}

type fakeTransport struct {
	mu        sync.Mutex
	connected bool
	queued    bool // models the library queue: accepts while disconnected
	err       error
	published []fakePublish
	closed    bool
	closeGate chan struct{} // when set, Close waits for it
}

func (f *fakeTransport) Publish(_ context.Context, topic string, msg stores.BrokerMessage, qos byte, retain bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if !f.connected && !f.queued {
		return errTransportDown
	}
	f.published = append(f.published, fakePublish{topic: topic, msg: msg, qos: qos, retain: retain})
	return nil
}

func (f *fakeTransport) Connected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

func (f *fakeTransport) Close() {
	f.mu.Lock()
	gate := f.closeGate
	f.mu.Unlock()
	if gate != nil {
		<-gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakeTransport) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeTransport) sent() []fakePublish {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakePublish(nil), f.published...)
}

type fakeBus struct {
	ch     chan stores.BrokerMessage
	buffer int // what the connector asked for
}

func newFakeBus() *fakeBus { return &fakeBus{ch: make(chan stores.BrokerMessage, 16)} }

func (b *fakeBus) Subscribe(_ []string, buffer int) (int, <-chan stores.BrokerMessage) {
	b.buffer = buffer
	return 1, b.ch
}
func (b *fakeBus) Unsubscribe(int) {}

type localPublish struct {
	clientID string
	msg      stores.BrokerMessage
}

type recorder struct {
	mu  sync.Mutex
	got []localPublish
}

func (r *recorder) publish(clientID string, msg stores.BrokerMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, localPublish{clientID: clientID, msg: msg})
	return nil
}

func (r *recorder) all() []localPublish {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]localPublish(nil), r.got...)
}

var discard = slog.New(slog.DiscardHandler)

var (
	outAll = Address{Mode: "PUBLISH", LocalTopic: "data/#", RemoteTopic: "data/#", QoS: -1, RemovePath: true}
	inAll  = Address{Mode: "SUBSCRIBE", RemoteTopic: "data/#", LocalTopic: "data/#", QoS: 1, RemovePath: true}
)

// startWithFake starts a connector named "to-remote" on a fake transport and
// returns the transport options it was started with.
func startWithFake(t *testing.T, cfg Config, tr *fakeTransport, rec *recorder, bus *fakeBus) *transportOptions {
	t.Helper()
	if cfg.BrokerURL == "" {
		cfg.BrokerURL = "tcp://remote:1883"
	}
	c := NewConnector("to-remote", cfg, rec.publish, bus, discard)
	var opts transportOptions
	c.newTransport = func(_ context.Context, o transportOptions) (transport, error) {
		opts = o
		return tr, nil
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	return &opts
}

func waitSent(t *testing.T, tr *fakeTransport, n int) []fakePublish {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(tr.sent()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("got %d remote publishes, want %d", len(tr.sent()), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return tr.sent()
}

func TestOutboundSkipsMessagesThisBridgeBroughtIn(t *testing.T) {
	tr := &fakeTransport{connected: true}
	bus := newFakeBus()
	startWithFake(t, Config{Addresses: []Address{outAll}}, tr, &recorder{}, bus)

	// The bus is FIFO: once data/b is sent, data/a has already been handled.
	bus.ch <- stores.BrokerMessage{TopicName: "data/a", ClientID: "bridge:to-remote", Inline: true}
	bus.ch <- stores.BrokerMessage{TopicName: "data/b", ClientID: "pub-1"}

	sent := waitSent(t, tr, 1)
	if len(sent) != 1 || sent[0].topic != "data/b" {
		t.Fatalf("remote publishes = %+v, want only data/b", sent)
	}
}

func TestOutboundForwardsOtherBridgesAndLookalikeNetworkClients(t *testing.T) {
	tr := &fakeTransport{connected: true}
	bus := newFakeBus()
	startWithFake(t, Config{Addresses: []Address{outAll}}, tr, &recorder{}, bus)

	bus.ch <- stores.BrokerMessage{TopicName: "data/a", ClientID: "bridge:to-other", Inline: true}
	bus.ch <- stores.BrokerMessage{TopicName: "data/b", ClientID: "bridge:to-remote"} // network client, same id

	sent := waitSent(t, tr, 2)
	if sent[0].topic != "data/a" || sent[1].topic != "data/b" {
		t.Fatalf("remote publishes = %+v, want data/a and data/b", sent)
	}
}

func TestOutboundKeepsPropertiesAndRetainFlag(t *testing.T) {
	tr := &fakeTransport{connected: true}
	bus := newFakeBus()
	startWithFake(t, Config{Addresses: []Address{outAll}}, tr, &recorder{}, bus)

	expiry := uint32(60)
	format := byte(1)
	in := stores.BrokerMessage{
		TopicName: "data/a", ClientID: "pub-1", QoS: 1, IsRetain: true,
		ContentType: "text/plain", ResponseTopic: "data/r", CorrelationData: []byte("c"),
		MessageExpiryInterval: &expiry, PayloadFormatIndicator: &format,
		UserProperties: []stores.UserProperty{{Key: "k", Value: "1"}, {Key: "k", Value: "2"}},
	}
	bus.ch <- in

	got := waitSent(t, tr, 1)[0]
	if got.topic != "data/a" || got.qos != 1 || !got.retain {
		t.Errorf("publish = topic %q qos %d retain %v, want data/a 1 true", got.topic, got.qos, got.retain)
	}
	if got.msg.ContentType != "text/plain" || got.msg.ResponseTopic != "data/r" || string(got.msg.CorrelationData) != "c" {
		t.Errorf("properties = %+v", got.msg)
	}
	if !reflect.DeepEqual(got.msg.UserProperties, in.UserProperties) {
		t.Errorf("user properties = %+v, want %+v", got.msg.UserProperties, in.UserProperties)
	}
}

func TestInboundPublishesLocallyAsThisBridgeWithProperties(t *testing.T) {
	rec := &recorder{}
	opts := startWithFake(t, Config{Addresses: []Address{inAll}}, &fakeTransport{connected: true}, rec, newFakeBus())

	expiry := uint32(60)
	in := stores.BrokerMessage{
		TopicName: "data/cmd/x", QoS: 1, IsRetain: true, ContentType: "application/json",
		MessageExpiryInterval: &expiry,
		UserProperties:        []stores.UserProperty{{Key: "src", Value: "app"}},
	}
	opts.OnMessage("data/cmd/x", in)

	got := rec.all()
	if len(got) != 1 {
		t.Fatalf("local publishes = %d, want 1", len(got))
	}
	if got[0].clientID != "bridge:to-remote" {
		t.Errorf("local publisher = %q, want bridge:to-remote", got[0].clientID)
	}
	m := got[0].msg
	if m.TopicName != "data/cmd/x" || !m.IsRetain || m.ContentType != "application/json" || *m.MessageExpiryInterval != 60 {
		t.Errorf("local message = %+v", m)
	}
	if !reflect.DeepEqual(m.UserProperties, in.UserProperties) {
		t.Errorf("user properties = %+v, want %+v", m.UserProperties, in.UserProperties)
	}
}

func TestInboundRetainFlag(t *testing.T) {
	for _, tc := range []struct{ addrRetain, msgRetain, want bool }{
		{false, false, false},
		{false, true, true}, // Retain As Published
		{true, false, true}, // mapping override
	} {
		rec := &recorder{}
		addr := inAll
		addr.Retain = tc.addrRetain
		opts := startWithFake(t, Config{Addresses: []Address{addr}}, &fakeTransport{connected: true}, rec, newFakeBus())
		opts.OnMessage("data/x", stores.BrokerMessage{TopicName: "data/x", IsRetain: tc.msgRetain})
		if got := rec.all()[0].msg.IsRetain; got != tc.want {
			t.Errorf("addr.Retain=%v msg retain=%v → %v, want %v", tc.addrRetain, tc.msgRetain, got, tc.want)
		}
	}
}

func TestInboundFansOutToEveryMatchingSubscription(t *testing.T) {
	rec := &recorder{}
	cfg := Config{Addresses: []Address{
		{Mode: "SUBSCRIBE", RemoteTopic: "data/#", LocalTopic: "all", RemovePath: true},
		{Mode: "SUBSCRIBE", RemoteTopic: "data/cmd/#", LocalTopic: "cmds", RemovePath: true},
	}}
	opts := startWithFake(t, cfg, &fakeTransport{connected: true}, rec, newFakeBus())
	opts.OnMessage("data/cmd/x", stores.BrokerMessage{TopicName: "data/cmd/x"})

	var topics []string
	for _, p := range rec.all() {
		topics = append(topics, p.msg.TopicName)
	}
	if want := []string{"all/cmd/x", "cmds/x"}; !reflect.DeepEqual(topics, want) {
		t.Errorf("local topics = %v, want %v", topics, want)
	}
}

func TestRemoteOptionsFromConfig(t *testing.T) {
	cfg := Config{
		BrokerURL: "tcp://remote:1883", ClientID: "local-bridge", Username: "u", Password: "p",
		KeepAlive: 15, ConnectionTimeout: 5, ReconnectDelay: 2,
		Addresses: []Address{outAll, inAll, {Mode: "SUBSCRIBE", RemoteTopic: "other/#", LocalTopic: "other", QoS: -1}},
	}
	o := startWithFake(t, cfg, &fakeTransport{connected: true}, &recorder{}, newFakeBus())

	if o.URL != "tcp://remote:1883" || o.ClientID != "local-bridge" || o.Username != "u" || o.Password != "p" {
		t.Errorf("identity = %+v", o)
	}
	if o.CleanSession || o.KeepAlive != 15*time.Second || o.ConnectTimeout != 5*time.Second || o.ReconnectDelay != 2*time.Second {
		t.Errorf("session/timing = clean %v keepalive %v connect %v reconnect %v", o.CleanSession, o.KeepAlive, o.ConnectTimeout, o.ReconnectDelay)
	}
	want := []transportSubscription{{Topic: "data/#", QoS: 1}, {Topic: "other/#", QoS: 2}}
	if !reflect.DeepEqual(o.Subscriptions, want) {
		t.Errorf("subscriptions = %+v, want %+v", o.Subscriptions, want)
	}
	if o.LibraryQueue != "" {
		t.Errorf("library queue = %q, want none for the default buffer", o.LibraryQueue)
	}
}

func TestRemoteOptionsUseDefaultsWhenUnset(t *testing.T) {
	o := startWithFake(t, Config{}, &fakeTransport{connected: true}, &recorder{}, newFakeBus())
	if o.KeepAlive != 30*time.Second || o.ConnectTimeout != 30*time.Second || o.ReconnectDelay != 30*time.Second {
		t.Errorf("defaults = keepalive %v connect %v reconnect %v, want 30s each", o.KeepAlive, o.ConnectTimeout, o.ReconnectDelay)
	}
}

func TestRemoteOptionsLibraryQueueForPahoBuffer(t *testing.T) {
	o := startWithFake(t, Config{BufferEnabled: true, BufferImplementation: "PAHO"}, &fakeTransport{connected: true}, &recorder{}, newFakeBus())
	if o.LibraryQueue != memoryQueue {
		t.Errorf("library queue = %q, want %q", o.LibraryQueue, memoryQueue)
	}
	o = startWithFake(t, Config{BufferEnabled: true, BufferImplementation: "PAHO", PersistBuffer: true}, &fakeTransport{connected: true}, &recorder{}, newFakeBus())
	if o.LibraryQueue != pahoBufferDir("to-remote") {
		t.Errorf("library queue = %q, want %q", o.LibraryQueue, pahoBufferDir("to-remote"))
	}
}

var exactAddr = Address{Mode: "PUBLISH", LocalTopic: "a", RemoteTopic: "b", QoS: 1}

func TestPublishLocalMessageUsesConfiguredAddressQoS(t *testing.T) {
	tr := &fakeTransport{connected: true}
	c := NewConnector("test", Config{}, nil, nil, discard)
	c.tr = tr
	if !c.publishLocalMessage(exactAddr, stores.BrokerMessage{TopicName: "a", QoS: 0}, false) {
		t.Fatal("publish reported failure")
	}
	if got := tr.sent()[0]; got.topic != "b" || got.qos != 1 {
		t.Errorf("publish = topic %q qos %d, want b 1", got.topic, got.qos)
	}
}

func TestPublishLocalMessageKeepsOriginQoS(t *testing.T) {
	tr := &fakeTransport{connected: true}
	c := NewConnector("test", Config{}, nil, nil, discard)
	c.tr = tr
	addr := exactAddr
	addr.QoS = -1
	c.publishLocalMessage(addr, stores.BrokerMessage{TopicName: "a", QoS: 2}, false)
	if got := tr.sent()[0].qos; got != 2 {
		t.Errorf("qos = %d, want the publisher's 2", got)
	}
}

func TestPublishLocalMessageQueuesWhenRemoteIsDown(t *testing.T) {
	tr := &fakeTransport{connected: false}
	c := NewConnector("test", Config{BufferEnabled: true}, nil, nil, discard)
	if err := c.initQueue(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.queue.Close() })
	c.tr = tr
	if c.publishLocalMessage(exactAddr, stores.BrokerMessage{TopicName: "a"}, false) {
		t.Error("publish must report not sent while the remote is down")
	}
	if c.queue.Size() != 1 || len(tr.sent()) != 0 {
		t.Errorf("queued=%d sent=%d, want 1 and 0", c.queue.Size(), len(tr.sent()))
	}
}

func TestPublishLocalMessageUsesLibraryQueueWhenPahoBuffer(t *testing.T) {
	tr := &fakeTransport{connected: false, queued: true}
	c := NewConnector("test", Config{BufferEnabled: true, BufferImplementation: "PAHO"}, nil, nil, discard)
	c.tr = tr
	if !c.publishLocalMessage(exactAddr, stores.BrokerMessage{TopicName: "a"}, false) {
		t.Error("with the library queue the transport takes the message while disconnected")
	}
	if len(tr.sent()) != 1 {
		t.Errorf("sent = %d, want 1", len(tr.sent()))
	}
}

func TestOversizedMessageIsDroppedNotRequeued(t *testing.T) {
	tr := &fakeTransport{connected: true, err: errTooLarge}
	c := NewConnector("test", Config{BufferEnabled: true}, nil, nil, discard)
	if err := c.initQueue(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.queue.Close() })
	c.tr = tr
	if !c.publishLocalMessage(exactAddr, stores.BrokerMessage{TopicName: "a"}, false) {
		t.Error("an oversized message must count as handled so the buffer keeps flowing")
	}
	if c.queue.Size() != 0 {
		t.Errorf("queued = %d, want 0 (dropped, not retried forever)", c.queue.Size())
	}
}

func TestRejectedMessageIsDroppedNotRequeued(t *testing.T) {
	tr := &fakeTransport{connected: true, err: errors.New("publish rejected: reason code 0x87")}
	c := NewConnector("test", Config{BufferEnabled: true}, nil, nil, discard)
	if err := c.initQueue(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.queue.Close() })
	c.tr = tr
	if !c.publishLocalMessage(exactAddr, stores.BrokerMessage{TopicName: "a"}, true) {
		t.Error("a message the remote rejects must count as handled so the buffer keeps flowing")
	}
	if c.queue.Size() != 0 {
		t.Errorf("queued = %d, want 0 (dropped, not retried forever)", c.queue.Size())
	}
}

// The old client logged "bridge connection lost"; the connector must still hear about it.
func TestConnectorListensForConnectionDown(t *testing.T) {
	opts := startWithFake(t, Config{Addresses: []Address{outAll}}, &fakeTransport{connected: true}, &recorder{}, newFakeBus())
	if opts.OnConnectionDown == nil {
		t.Fatal("OnConnectionDown not set")
	}
}

// The bus drops silently when a subscriber falls behind; the old adapter had
// room for about 512 messages.
func TestOutboundBusSubscriptionHasRoomForBursts(t *testing.T) {
	bus := newFakeBus()
	startWithFake(t, Config{Addresses: []Address{outAll}}, &fakeTransport{connected: true}, &recorder{}, bus)
	if bus.buffer < 1024 {
		t.Errorf("bus buffer = %d, want at least 1024", bus.buffer)
	}
}

// transportFactory hands out fake transports and records the options each
// was started with. With v3OnFirst, the first transport reports a 3.1.1
// CONNACK while it is being created, as autopaho can.
type transportFactory struct {
	mu        sync.Mutex
	v3OnFirst bool
	opts      []transportOptions
	trs       []*fakeTransport
}

func (f *transportFactory) new(_ context.Context, o transportOptions) (transport, error) {
	f.mu.Lock()
	tr := &fakeTransport{connected: true}
	f.opts = append(f.opts, o)
	f.trs = append(f.trs, tr)
	first := len(f.opts) == 1
	f.mu.Unlock()
	if first && f.v3OnFirst {
		o.OnConnectError(fmt.Errorf("%w: EOF", errRemoteNotMQTT5))
	}
	return tr, nil
}

func (f *transportFactory) started() ([]transportOptions, []*fakeTransport) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]transportOptions(nil), f.opts...), append([]*fakeTransport(nil), f.trs...)
}

func waitStarted(t *testing.T, f *transportFactory, n int) ([]transportOptions, []*fakeTransport) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		opts, trs := f.started()
		if len(opts) >= n {
			return opts, trs
		}
		if time.Now().After(deadline) {
			t.Fatalf("started %d transports, want %d", len(opts), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// lockedBuffer is a log sink safe for concurrent writers.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func startWithFactory(t *testing.T, cfg Config, f *transportFactory, logger *slog.Logger) *Connector {
	t.Helper()
	if cfg.BrokerURL == "" {
		cfg.BrokerURL = "tcp://remote:1883"
	}
	if logger == nil {
		logger = discard
	}
	c := NewConnector("to-remote", cfg, (&recorder{}).publish, newFakeBus(), logger)
	c.newTransport = f.new
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	return c
}

func currentTransport(c *Connector) transport {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tr
}

var errV3Connack = fmt.Errorf("%w: EOF", errRemoteNotMQTT5)

func TestProtocolVersionSelectsTransport(t *testing.T) {
	for _, tc := range []struct{ stored, want int }{{0, 5}, {5, 5}, {4, 4}, {3, 3}} {
		f := &transportFactory{}
		startWithFactory(t, Config{ProtocolVersion: tc.stored}, f, nil)
		opts, _ := f.started()
		if len(opts) != 1 {
			t.Fatalf("protocolVersion %d: started %d transports, want 1", tc.stored, len(opts))
		}
		if opts[0].ProtocolVersion != tc.want {
			t.Errorf("protocolVersion %d: transport version %d, want %d", tc.stored, opts[0].ProtocolVersion, tc.want)
		}
	}
}

func TestUnsupportedProtocolVersionFailsStart(t *testing.T) {
	c := NewConnector("to-remote", Config{BrokerURL: "tcp://remote:1883", ProtocolVersion: 6}, (&recorder{}).publish, newFakeBus(), discard)
	c.newTransport = (&transportFactory{}).new
	err := c.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsupported protocolVersion 6") {
		t.Fatalf("Start = %v, want unsupported protocolVersion 6", err)
	}
}

func TestRemoteNotMQTT5FallsBackOnce(t *testing.T) {
	f := &transportFactory{}
	c := startWithFactory(t, Config{}, f, nil)
	opts, _ := f.started()
	opts[0].OnConnectError(errV3Connack)

	opts, trs := waitStarted(t, f, 2)
	if opts[1].ProtocolVersion != 4 {
		t.Errorf("fallback transport version = %d, want 4", opts[1].ProtocolVersion)
	}
	if !trs[0].isClosed() {
		t.Error("the MQTT 5 transport was not closed")
	}
	if currentTransport(c) != transport(trs[1]) {
		t.Error("the connector does not use the 3.1.1 transport")
	}

	opts[0].OnConnectError(errV3Connack) // a late second report changes nothing
	time.Sleep(200 * time.Millisecond)
	if opts, _ := f.started(); len(opts) != 2 {
		t.Errorf("started %d transports, want 2", len(opts))
	}
}

func TestOtherConnectErrorsDoNotFallBack(t *testing.T) {
	f := &transportFactory{}
	startWithFactory(t, Config{}, f, nil)
	opts, _ := f.started()
	opts[0].OnConnectError(errors.New("connection refused"))
	time.Sleep(200 * time.Millisecond)
	if opts, _ := f.started(); len(opts) != 1 {
		t.Errorf("started %d transports, want 1", len(opts))
	}
}

func TestFallbackTriggeredDuringStartStillSwaps(t *testing.T) {
	f := &transportFactory{v3OnFirst: true}
	c := startWithFactory(t, Config{}, f, nil)
	_, trs := waitStarted(t, f, 2)
	deadline := time.Now().Add(2 * time.Second)
	for currentTransport(c) != transport(trs[1]) {
		if time.Now().After(deadline) {
			t.Fatal("the connector never switched to the 3.1.1 transport")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestStopAfterFallbackClosesMQTT311Transport(t *testing.T) {
	f := &transportFactory{}
	c := startWithFactory(t, Config{}, f, nil)
	opts, _ := f.started()
	opts[0].OnConnectError(errV3Connack)
	_, trs := waitStarted(t, f, 2)
	deadline := time.Now().Add(2 * time.Second)
	for currentTransport(c) != transport(trs[1]) {
		if time.Now().After(deadline) {
			t.Fatal("the connector never switched to the 3.1.1 transport")
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.Stop()
	if !trs[1].isClosed() {
		t.Error("Stop left the 3.1.1 transport open")
	}
}

func TestFallbackAfterStopStartsNothing(t *testing.T) {
	f := &transportFactory{}
	c := startWithFactory(t, Config{}, f, nil)
	opts, _ := f.started()
	c.Stop()
	opts[0].OnConnectError(errV3Connack)
	time.Sleep(200 * time.Millisecond)
	if opts, _ := f.started(); len(opts) != 1 {
		t.Errorf("started %d transports after Stop, want 1", len(opts))
	}
}

func TestFallbackWarnsWhatIsLost(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cfg      Config
		wantPaho bool
	}{
		{"monster buffer", Config{}, false},
		{"paho buffer", Config{BufferEnabled: true, BufferImplementation: "PAHO"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &lockedBuffer{}
			f := &transportFactory{}
			startWithFactory(t, tc.cfg, f, slog.New(slog.NewTextHandler(logs, nil)))
			opts, _ := f.started()
			opts[0].OnConnectError(errV3Connack)
			waitStarted(t, f, 2)
			deadline := time.Now().Add(2 * time.Second)
			for !strings.Contains(logs.String(), "using MQTT 3.1.1") {
				if time.Now().After(deadline) {
					t.Fatalf("no fallback warning in logs:\n%s", logs)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !strings.Contains(logs.String(), "inbound retain flag and No Local are not available") {
				t.Errorf("warning does not say what is lost:\n%s", logs)
			}
			if got := strings.Contains(logs.String(), "PAHO buffer"); got != tc.wantPaho {
				t.Errorf("PAHO buffer note present = %v, want %v:\n%s", got, tc.wantPaho, logs)
			}
		})
	}
}

// A shared subscription delivers messages under the plain topic, without the
// $share/<group>/ or $queue/ prefix of the filter.
func TestInboundMatchesSharedSubscriptions(t *testing.T) {
	rec := &recorder{}
	cfg := Config{Addresses: []Address{
		{Mode: "SUBSCRIBE", RemoteTopic: "$share/g1/data/#", LocalTopic: "shared", RemovePath: true},
		{Mode: "SUBSCRIBE", RemoteTopic: "$queue/cmd/+", LocalTopic: "queued", RemovePath: true},
	}}
	opts := startWithFake(t, cfg, &fakeTransport{connected: true}, rec, newFakeBus())
	opts.OnMessage("data/a/b", stores.BrokerMessage{TopicName: "data/a/b"})
	opts.OnMessage("cmd/x", stores.BrokerMessage{TopicName: "cmd/x"})

	var topics []string
	for _, p := range rec.all() {
		topics = append(topics, p.msg.TopicName)
	}
	if want := []string{"shared/a/b", "queued/x"}; !reflect.DeepEqual(topics, want) {
		t.Errorf("local topics = %v, want %v", topics, want)
	}
}

func TestStopWaitsForFallbackToCloseMQTT5Transport(t *testing.T) {
	f := &transportFactory{}
	c := startWithFactory(t, Config{}, f, nil)
	opts, trs := f.started()
	gate := make(chan struct{})
	trs[0].mu.Lock()
	trs[0].closeGate = gate
	trs[0].mu.Unlock()
	opts[0].OnConnectError(errV3Connack)
	waitStarted(t, f, 2)

	stopped := make(chan struct{})
	go func() { c.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned while the fallback was still closing the MQTT 5 transport")
	case <-time.After(200 * time.Millisecond):
	}
	close(gate)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop never returned")
	}
}

func TestFailedStartClosesTheBuffer(t *testing.T) {
	c := NewConnector("to-remote", Config{BrokerURL: "tcp://remote:1883", BufferEnabled: true}, (&recorder{}).publish, newFakeBus(), discard)
	c.newTransport = func(context.Context, transportOptions) (transport, error) { return nil, errors.New("boom") }
	if err := c.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded, want the transport error")
	}
	c.mu.Lock()
	q := c.queue
	c.mu.Unlock()
	if q != nil {
		t.Error("the bridge buffer is still open after a failed Start")
	}
}
