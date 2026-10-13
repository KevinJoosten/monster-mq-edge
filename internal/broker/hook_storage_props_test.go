package broker

import (
	"reflect"
	"testing"
	"time"

	mqtt "monstermq.io/edge/internal/mqtt"
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/pubsub"
	"monstermq.io/edge/internal/stores"
)

// publishThroughHook runs one packet through StorageHook.OnPublished and
// returns what landed on the internal bus.
func publishThroughHook(t *testing.T, cl *mqtt.Client, pk packets.Packet) stores.BrokerMessage {
	t.Helper()
	bus := pubsub.NewBus()
	_, ch := bus.Subscribe([]string{"#"}, 1)
	h := &StorageHook{bus: bus}
	h.OnPublished(cl, pk)
	select {
	case msg := <-ch:
		return msg
	case <-time.After(time.Second):
		t.Fatal("no message on the bus")
		return stores.BrokerMessage{}
	}
}

func TestOnPublishedCopiesAllMQTT5Properties(t *testing.T) {
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1, Retain: true},
		TopicName:   "data/temp",
		Payload:     []byte("21.5"),
		Properties: packets.Properties{
			PayloadFormat:         1,
			PayloadFormatFlag:     true,
			MessageExpiryInterval: 60,
			ContentType:           "text/plain",
			ResponseTopic:         "data/temp/reply",
			CorrelationData:       []byte("corr-1"),
			User: []packets.UserProperty{
				{Key: "unit", Val: "C"},
				{Key: "src", Val: "dev-7"},
				{Key: "unit", Val: "celsius"},
			},
		},
	}
	msg := publishThroughHook(t, &mqtt.Client{ID: "pub-1"}, pk)

	if msg.PayloadFormatIndicator == nil || *msg.PayloadFormatIndicator != 1 {
		t.Errorf("payload format = %v, want 1", msg.PayloadFormatIndicator)
	}
	if msg.MessageExpiryInterval == nil || *msg.MessageExpiryInterval != 60 {
		t.Errorf("message expiry = %v, want 60", msg.MessageExpiryInterval)
	}
	if msg.ContentType != "text/plain" {
		t.Errorf("content type = %q", msg.ContentType)
	}
	if msg.ResponseTopic != "data/temp/reply" {
		t.Errorf("response topic = %q", msg.ResponseTopic)
	}
	if string(msg.CorrelationData) != "corr-1" {
		t.Errorf("correlation data = %q", msg.CorrelationData)
	}
	want := []stores.UserProperty{
		{Key: "unit", Value: "C"},
		{Key: "src", Value: "dev-7"},
		{Key: "unit", Value: "celsius"},
	}
	if !reflect.DeepEqual(msg.UserProperties, want) {
		t.Errorf("user properties = %+v, want %+v (order and repeated keys kept)", msg.UserProperties, want)
	}
}

func TestOnPublishedLeavesUnsetPropertiesEmpty(t *testing.T) {
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish},
		TopicName:   "data/a",
		Payload:     []byte("x"),
	}
	msg := publishThroughHook(t, &mqtt.Client{ID: "pub-1"}, pk)
	if msg.PayloadFormatIndicator != nil || msg.MessageExpiryInterval != nil {
		t.Errorf("unset properties must stay nil: format=%v expiry=%v", msg.PayloadFormatIndicator, msg.MessageExpiryInterval)
	}
	if msg.UserProperties != nil || msg.CorrelationData != nil {
		t.Errorf("unset properties must stay nil: user=%v corr=%v", msg.UserProperties, msg.CorrelationData)
	}
}

func TestOnPublishedMarksInlinePublishers(t *testing.T) {
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "data/a"}

	inline := &mqtt.Client{ID: "bridge:to-remote"}
	inline.Net.Inline = true
	if msg := publishThroughHook(t, inline, pk); !msg.Inline || msg.ClientID != "bridge:to-remote" {
		t.Errorf("inline publisher: Inline=%v ClientID=%q, want true and bridge:to-remote", msg.Inline, msg.ClientID)
	}

	network := &mqtt.Client{ID: "bridge:to-remote"} // a network client that chose the same id
	if msg := publishThroughHook(t, network, pk); msg.Inline {
		t.Error("a network client must not be marked inline")
	}
}
