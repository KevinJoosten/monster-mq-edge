package broker

import (
	"testing"
	"time"

	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/winccoanative"
)

func TestInlinePublisherRelease(t *testing.T) {
	srv, h := newTestEngine(t)
	p := NewInlinePublisher(srv)

	if err := p.Publish("nats-1", stores.BrokerMessage{TopicName: "a/b", Payload: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	h.wait(t, 1)
	if len(p.clients) != 1 {
		t.Fatalf("clients = %d, want 1", len(p.clients))
	}

	p.Release("nats-1")
	if len(p.clients) != 0 {
		t.Fatalf("clients after Release = %d, want 0", len(p.clients))
	}
}

func TestProtocolPublishCarriesIdentityPropertiesAndTime(t *testing.T) {
	srv, h := newTestEngine(t)
	pub := NewInlinePublisher(srv)
	protoPub := NewProtocolPublish(nil, nil, pub)

	ts := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	msg := stores.BrokerMessage{
		MessageUUID:     "uuid-1",
		TopicName:       "sensors/temp",
		Payload:         []byte("21.5"),
		QoS:             0,
		IsRetain:        true,
		Time:            ts,
		ResponseTopic:   "_INBOX/reply",
		CorrelationData: []byte("corr-1"),
		UserProperties: []stores.UserProperty{
			{Key: "hdr", Value: "val"},
		},
	}

	if err := protoPub("nats-abc", msg); err != nil {
		t.Fatal(err)
	}

	got := h.wait(t, 1)[0]
	if got.clientID != "nats-abc" || !got.inline {
		t.Errorf("clientID = %q inline = %v, want nats-abc inline", got.clientID, got.inline)
	}
	if got.pk.TopicName != "sensors/temp" || string(got.pk.Payload) != "21.5" || !got.pk.FixedHeader.Retain {
		t.Errorf("packet = %+v", got.pk)
	}
	if got.pk.Properties.ResponseTopic != "_INBOX/reply" || string(got.pk.Properties.CorrelationData) != "corr-1" {
		t.Errorf("properties = %+v", got.pk.Properties)
	}
	if len(got.pk.Properties.User) != 1 || got.pk.Properties.User[0].Key != "hdr" || got.pk.Properties.User[0].Val != "val" {
		t.Errorf("user properties = %+v", got.pk.Properties.User)
	}
	if got.pk.Source == nil || !got.pk.Source.Time.Equal(ts) || got.pk.Source.MessageUUID != "uuid-1" {
		t.Errorf("source = %+v, want time %v uuid uuid-1", got.pk.Source, ts)
	}
}

func TestNativeValueWrite(t *testing.T) {
	names := winccoanative.DefaultNames
	cases := []struct {
		topic   string
		want    string
		wantOK  bool
	}{
		{"winccoa/tags/Pump1/speed", "winccoa/tags/Pump1/speed/set", true},
		{"winccoa/systems/Sys1/tags/Pump1/speed", "winccoa/systems/Sys1/tags/Pump1/speed/set", true},
		{"winccoa/types/PumpType/Pump1/speed", "winccoa/types/PumpType/Pump1/speed/set", true},
		{"winccoa/tags/Pump1/speed/set", "", false},
		{"winccoa/tags/Pump1/speed/_online.._stime", "", false},
		{"winccoa/tags/Pump1/speed/_online.._value", "", false},
		{"winccoa/tags/_Users/x", "", false},
		{"winccoa/tags/MMQConfigs_1/x", "", false},
		{"winccoa/tags/MMQSessions_1/x", "", false},
		{"winccoa/topics/a/b", "", false},
		{"winccoa/cns/a/b", "", false},
		{"winccoa", "", false},
		{"winccoa/systems/Sys1", "", false},
		{"winccoa/tags/+/speed", "", false},
		{"sensors/temp", "", false},
	}

	for _, tc := range cases {
		got, ok := NativeValueWrite(names, tc.topic)
		if ok != tc.wantOK || got != tc.want {
			t.Errorf("NativeValueWrite(%q) = (%q, %v), want (%q, %v)", tc.topic, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestProtocolSessions(t *testing.T) {
	ps := NewProtocolSessions()
	closed := 0
	ps.Register("nats-1", func() { closed++ })

	if !ps.Close("nats-1") {
		t.Fatal("expected Close(nats-1) to return true")
	}
	if closed != 1 {
		t.Fatalf("closed = %d, want 1", closed)
	}
	if ps.Close("nats-1") {
		t.Fatal("expected second Close(nats-1) to return false")
	}

	ps.Register("redis-1", func() { closed++ })
	ps.Unregister("redis-1")
	if ps.Close("redis-1") {
		t.Fatal("expected Close(redis-1) after Unregister to return false")
	}
	if closed != 1 {
		t.Fatalf("closed = %d, want 1", closed)
	}
}
