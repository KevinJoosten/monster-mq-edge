package mqttclient

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	engine "monstermq.io/edge/internal/mqtt"
	"monstermq.io/edge/internal/mqtt/listeners"
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/stores"
)

// denyHook lets everyone in and refuses topics starting with deny.
type denyHook struct {
	engine.HookBase
	deny string
}

func (h *denyHook) ID() string { return "deny-prefix" }
func (h *denyHook) Provides(b byte) bool {
	return b == engine.OnConnectAuthenticate || b == engine.OnACLCheck
}
func (h *denyHook) OnConnectAuthenticate(*engine.Client, packets.Packet) bool { return true }
func (h *denyHook) OnACLCheck(_ *engine.Client, topic string, _ bool) bool {
	return h.deny == "" || !strings.HasPrefix(topic, h.deny)
}

// freeAddr returns a localhost address nothing listens on.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// startEngineAt runs an in-process broker on addr. Topics starting with
// denyPrefix are refused ("" refuses nothing).
func startEngineAt(t *testing.T, addr, denyPrefix string) *engine.Server {
	t.Helper()
	srv := engine.New(&engine.Options{InlineClient: true, Logger: slog.New(slog.DiscardHandler)})
	if err := srv.AddHook(&denyHook{deny: denyPrefix}, nil); err != nil {
		t.Fatal(err)
	}
	if err := srv.AddListener(listeners.NewTCP(listeners.Config{ID: "t", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })
	time.Sleep(50 * time.Millisecond)
	return srv
}

func newTestMQTT311(t *testing.T, o transportOptions) *mqtt311Transport {
	t.Helper()
	tr, err := newMQTT311Transport(withTestDefaults(o))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	return tr
}

func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the transport")
	}
}

func nextMessage(t *testing.T, ch <-chan stores.BrokerMessage) stores.BrokerMessage {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a message")
		return stores.BrokerMessage{}
	}
}

func signal() (chan struct{}, func()) {
	ch := make(chan struct{}, 8)
	return ch, func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func TestMQTT311TransportDeliversBothWays(t *testing.T) {
	addr := freeAddr(t)
	srv := startEngineAt(t, addr, "")
	if err := srv.Publish("in/state", []byte("r"), true, 1); err != nil {
		t.Fatal(err)
	}
	got := make(chan stores.BrokerMessage, 8)
	up, onUp := signal()
	tr := newTestMQTT311(t, transportOptions{
		URL: "tcp://" + addr, ClientID: "bridge-311", CleanSession: true,
		Subscriptions: []transportSubscription{{Topic: "in/#", QoS: 1}},
		OnMessage:     func(_ string, m stores.BrokerMessage) { got <- m },
		OnConnected:   onUp,
	})
	waitSignal(t, up)

	// Inbound: the retained replay keeps its flag, a live message has none.
	if m := nextMessage(t, got); m.TopicName != "in/state" || string(m.Payload) != "r" || !m.IsRetain {
		t.Errorf("retained replay = %+v, want down/state r retained", m)
	}
	if err := srv.Publish("in/live", []byte("l"), false, 1); err != nil {
		t.Fatal(err)
	}
	if m := nextMessage(t, got); m.TopicName != "in/live" || m.IsRetain || m.QoS != 1 {
		t.Errorf("live message = %+v, want down/live, not retained, QoS 1", m)
	}

	// Outbound: payload and retain arrive; MQTT 5 properties cannot.
	if !tr.Connected() {
		t.Fatal("Connected() = false after OnConnected")
	}
	err := tr.Publish(context.Background(), "out/x", stores.BrokerMessage{
		Payload: []byte("v"), UserProperties: []stores.UserProperty{{Key: "k", Value: "v"}},
	}, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(srv.Topics.Messages("out/x")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("out/x never retained at the remote")
		}
		time.Sleep(20 * time.Millisecond)
	}
	pk := srv.Topics.Messages("out/x")[0]
	if string(pk.Payload) != "v" || len(pk.Properties.User) != 0 {
		t.Errorf("remote got payload %q with user properties %+v, want v and none", pk.Payload, pk.Properties.User)
	}
}

func TestMQTT311PublishWhileDisconnectedReturnsErrTransportDown(t *testing.T) {
	tr := newTestMQTT311(t, transportOptions{URL: "tcp://" + freeAddr(t), ClientID: "down-311"})
	err := tr.Publish(context.Background(), "out/x", stores.BrokerMessage{Payload: []byte("v")}, 1, false)
	if !errors.Is(err, errTransportDown) {
		t.Fatalf("err = %v, want errTransportDown", err)
	}
}

func TestMQTT311RejectedSubscriptionKeepsOthers(t *testing.T) {
	addr := freeAddr(t)
	srv := startEngineAt(t, addr, "secret")
	got := make(chan stores.BrokerMessage, 8)
	up, onUp := signal()
	newTestMQTT311(t, transportOptions{
		URL: "tcp://" + addr, ClientID: "acl-311", CleanSession: true,
		// The refused topic comes first, so a refusal that stopped the rest would show.
		Subscriptions: []transportSubscription{{Topic: "secret/#", QoS: 1}, {Topic: "data/#", QoS: 1}},
		OnMessage:     func(_ string, m stores.BrokerMessage) { got <- m },
		OnConnected:   onUp,
	})
	waitSignal(t, up)
	if err := srv.Publish("data/x", []byte("1"), false, 1); err != nil {
		t.Fatal(err)
	}
	if m := nextMessage(t, got); m.TopicName != "data/x" {
		t.Errorf("got %s, want data/x", m.TopicName)
	}
}

func TestMQTT311PersistentSessionKeepsQueuedMessages(t *testing.T) {
	addr := freeAddr(t)
	srv := startEngineAt(t, addr, "")
	opts := transportOptions{
		URL: "tcp://" + addr, ClientID: "persist-311", CleanSession: false,
		Subscriptions: []transportSubscription{{Topic: "q/#", QoS: 1}},
	}
	up, onUp := signal()
	first := opts
	first.OnConnected = onUp
	tr, err := newMQTT311Transport(withTestDefaults(first))
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, up)
	tr.Close()

	if err := srv.Publish("q/x", []byte("while-away"), false, 1); err != nil {
		t.Fatal(err)
	}
	got := make(chan stores.BrokerMessage, 8)
	second := opts
	second.OnMessage = func(_ string, m stores.BrokerMessage) { got <- m }
	newTestMQTT311(t, second)
	if m := nextMessage(t, got); m.TopicName != "q/x" || string(m.Payload) != "while-away" {
		t.Errorf("got %s %q, want q/x while-away from the kept session", m.TopicName, m.Payload)
	}
}

func TestMQTT311ReportsConnectErrorsAndConnectionLoss(t *testing.T) {
	t.Run("connect error", func(t *testing.T) {
		failed, onFail := signal()
		newTestMQTT311(t, transportOptions{
			URL: "tcp://" + freeAddr(t), ClientID: "err-311",
			OnConnectError: func(error) { onFail() },
		})
		waitSignal(t, failed)
	})
	t.Run("connection lost", func(t *testing.T) {
		addr := freeAddr(t)
		srv := startEngineAt(t, addr, "")
		up, onUp := signal()
		lost, onLost := signal()
		newTestMQTT311(t, transportOptions{
			URL: "tcp://" + addr, ClientID: "lost-311",
			OnConnected: onUp, OnConnectionDown: func(error) { onLost() },
		})
		waitSignal(t, up)
		_ = srv.Close()
		waitSignal(t, lost)
	})
}

// With the PAHO buffer, a message published before the remote is reachable is
// kept by the library and sent once it connects.
func TestMQTT311LibraryQueueSendsAfterConnect(t *testing.T) {
	for _, tc := range []struct{ name, queue string }{
		{"memory", memoryQueue},
		{"file", filepath.Join(t.TempDir(), "paho-x")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := freeAddr(t)
			tr := newTestMQTT311(t, transportOptions{
				URL: "tcp://" + addr, ClientID: "queue-311-" + tc.name, LibraryQueue: tc.queue,
			})
			if err := tr.Publish(context.Background(), "out/q", stores.BrokerMessage{Payload: []byte("queued")}, 1, true); err != nil {
				t.Fatalf("queued publish = %v, want accepted", err)
			}
			srv := startEngineAt(t, addr, "")
			deadline := time.Now().Add(10 * time.Second)
			for len(srv.Topics.Messages("out/q")) == 0 {
				if time.Now().After(deadline) {
					t.Fatal("queued message never reached the remote")
				}
				time.Sleep(50 * time.Millisecond)
			}
		})
	}
}

func TestMQTT311AcceptsBridgeURLForms(t *testing.T) {
	for _, raw := range []string{"tcp://h:1", "mqtt://h:1", "ssl://h:1", "tls://h:1", "mqtts://h:1", "mqtt+ssl://h:1", "tcps://h:1", "ws://h:1/mqtt", "wss://h:1/mqtt", "h:1"} {
		tr, err := newMQTT311Transport(withTestDefaults(transportOptions{URL: raw, ClientID: "url-311"}))
		if err != nil {
			t.Errorf("%s: %v", raw, err)
			continue
		}
		tr.Close()
	}
}

func withTestDefaults(o transportOptions) transportOptions {
	if o.KeepAlive == 0 {
		o.KeepAlive = 30 * time.Second
	}
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 5 * time.Second
	}
	if o.ReconnectDelay == 0 {
		o.ReconnectDelay = time.Second
	}
	if o.Logger == nil {
		o.Logger = discard
	}
	return o
}

// paho drops a QoS 0 message while the remote is down, so queued mode must
// report it instead of claiming it was kept.
func TestMQTT311QueuedQoS0WhileDownReturnsErrTransportDown(t *testing.T) {
	tr := newTestMQTT311(t, transportOptions{
		URL: "tcp://" + freeAddr(t), ClientID: "q0-311", LibraryQueue: memoryQueue,
	})
	err := tr.Publish(context.Background(), "out/q", stores.BrokerMessage{Payload: []byte("x")}, 0, false)
	if !errors.Is(err, errTransportDown) {
		t.Fatalf("err = %v, want errTransportDown", err)
	}
}

func TestMQTT311QueuedPublishAfterCloseReturnsErrTransportDown(t *testing.T) {
	tr, err := newMQTT311Transport(withTestDefaults(transportOptions{
		URL: "tcp://" + freeAddr(t), ClientID: "closed-311", LibraryQueue: memoryQueue,
	}))
	if err != nil {
		t.Fatal(err)
	}
	tr.Close()
	err = tr.Publish(context.Background(), "out/q", stores.BrokerMessage{Payload: []byte("x")}, 1, false)
	if !errors.Is(err, errTransportDown) {
		t.Fatalf("err = %v, want errTransportDown", err)
	}
}

func TestMQTT311OverlappingSubscriptionsDeliverOnce(t *testing.T) {
	addr := freeAddr(t)
	srv := startEngineAt(t, addr, "")
	got := make(chan stores.BrokerMessage, 8)
	up, onUp := signal()
	newTestMQTT311(t, transportOptions{
		URL: "tcp://" + addr, ClientID: "overlap-311", CleanSession: true,
		Subscriptions: []transportSubscription{{Topic: "a/#", QoS: 1}, {Topic: "a/b/#", QoS: 1}},
		OnMessage:     func(_ string, m stores.BrokerMessage) { got <- m },
		OnConnected:   onUp,
	})
	waitSignal(t, up)
	if err := srv.Publish("a/b/c", []byte("1"), false, 1); err != nil {
		t.Fatal(err)
	}
	nextMessage(t, got)
	select {
	case m := <-got:
		t.Errorf("second OnMessage call for one PUBLISH: %+v", m)
	case <-time.After(500 * time.Millisecond):
	}
}
