package memory

import (
	"context"
	"testing"
	"unsafe"

	"monstermq.io/edge/internal/stores"
)

// PutOne detaches the stored message from the caller's buffers: the topic and
// payload of a publish may point into a connection read buffer that would
// otherwise stay alive as long as the stored value.
func TestPutOneDetachesTopicAndPayload(t *testing.T) {
	s := NewMessageStore("t")
	buf := []byte("plant/line1/temp21.5")
	topic := unsafe.String(&buf[0], 16) // "plant/line1/temp", sharing buf like a zero-copy decode
	payload := buf[16:]

	for round := 0; round < 2; round++ { // insert, then update
		s.PutOne(stores.BrokerMessage{TopicName: topic, Payload: payload})
		var key string
		_ = s.FindMatchingTopics(context.Background(), "#", func(k string) bool { key = k; return false })
		if key != "plant/line1/temp" {
			t.Fatalf("round %d: stored key %q", round, key)
		}
		if unsafe.StringData(key) == unsafe.StringData(topic) {
			t.Fatalf("round %d: stored key shares memory with the publish buffer", round)
		}
		m, _ := s.Get(context.Background(), "plant/line1/temp")
		if unsafe.StringData(m.TopicName) == unsafe.StringData(topic) {
			t.Fatalf("round %d: stored topic shares memory with the publish buffer", round)
		}
		payload[0] = '9'
		if string(m.Payload) != "21.5" {
			t.Fatalf("round %d: stored payload changed with the publish buffer: %q", round, m.Payload)
		}
		payload[0] = '2'
	}
}
