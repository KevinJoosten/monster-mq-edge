package broker

import (
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	mqtt "monstermq.io/edge/internal/mqtt"
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/stores"
)

type captured struct {
	clientID string
	inline   bool
	pk       packets.Packet
}

// captureHook records every publish the engine accepts.
type captureHook struct {
	mqtt.HookBase
	mu  sync.Mutex
	got []captured
}

func (h *captureHook) ID() string           { return "capture" }
func (h *captureHook) Provides(b byte) bool { return b == mqtt.OnPublished }
func (h *captureHook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.got = append(h.got, captured{clientID: cl.ID, inline: cl.Net.Inline, pk: pk})
}

func (h *captureHook) wait(t *testing.T, n int) []captured {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		h.mu.Lock()
		got := append([]captured(nil), h.got...)
		h.mu.Unlock()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("engine accepted %d publishes, want %d", len(got), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func newTestEngine(t *testing.T) (*mqtt.Server, *captureHook) {
	t.Helper()
	srv := mqtt.New(&mqtt.Options{InlineClient: true, Logger: slog.New(slog.DiscardHandler)})
	h := &captureHook{}
	if err := srv.AddHook(h, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv, h
}

func TestInlinePublisherPublishesAsNamedClientWithProperties(t *testing.T) {
	srv, h := newTestEngine(t)
	expiry := uint32(120)
	format := byte(1)
	msg := stores.BrokerMessage{
		TopicName:              "data/cmd/x",
		Payload:                []byte("go"),
		QoS:                    1,
		IsRetain:               true,
		ContentType:            "text/plain",
		ResponseTopic:          "data/cmd/x/reply",
		CorrelationData:        []byte("c-9"),
		MessageExpiryInterval:  &expiry,
		PayloadFormatIndicator: &format,
		UserProperties: []stores.UserProperty{
			{Key: "src", Value: "app"},
			{Key: "src", Value: "operator"},
		},
	}

	if err := NewInlinePublisher(srv).Publish("bridge:to-remote", msg); err != nil {
		t.Fatal(err)
	}

	got := h.wait(t, 1)[0]
	if got.clientID != "bridge:to-remote" || !got.inline {
		t.Errorf("publisher = %q inline=%v, want bridge:to-remote inline", got.clientID, got.inline)
	}
	pk := got.pk
	if pk.TopicName != "data/cmd/x" || string(pk.Payload) != "go" || pk.FixedHeader.Qos != 1 || !pk.FixedHeader.Retain {
		t.Errorf("packet = topic %q payload %q qos %d retain %v", pk.TopicName, pk.Payload, pk.FixedHeader.Qos, pk.FixedHeader.Retain)
	}
	p := pk.Properties
	if p.ContentType != "text/plain" || p.ResponseTopic != "data/cmd/x/reply" || string(p.CorrelationData) != "c-9" {
		t.Errorf("properties = %+v", p)
	}
	if p.MessageExpiryInterval != 120 || p.PayloadFormat != 1 || !p.PayloadFormatFlag {
		t.Errorf("expiry=%d format=%d flag=%v", p.MessageExpiryInterval, p.PayloadFormat, p.PayloadFormatFlag)
	}
	wantUser := []packets.UserProperty{{Key: "src", Val: "app"}, {Key: "src", Val: "operator"}}
	if !reflect.DeepEqual(p.User, wantUser) {
		t.Errorf("user properties = %+v, want %+v", p.User, wantUser)
	}
}

func TestInlinePublisherReusesOneClientPerID(t *testing.T) {
	srv, h := newTestEngine(t)
	p := NewInlinePublisher(srv)
	for _, id := range []string{"bridge:a", "bridge:a", "bridge:b"} {
		if err := p.Publish(id, stores.BrokerMessage{TopicName: "t", Payload: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	h.wait(t, 3)
	if len(p.clients) != 2 {
		t.Errorf("inline clients = %d, want 2 (one per id)", len(p.clients))
	}
}
