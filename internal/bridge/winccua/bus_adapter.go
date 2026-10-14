package winccua

import (
	"monstermq.io/edge/internal/pubsub"
)

// LocalMessage represents an MQTT message received from the local broker bus.
type LocalMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

// LocalSubscriber is the interface used to subscribe to local MQTT topics for
// TAG_WRITE addresses.
type LocalSubscriber interface {
	Subscribe(filters []string, buffer int) (id int, ch <-chan LocalMessage)
	Unsubscribe(id int)
}

// BusAdapter wraps the in-process pubsub.Bus to satisfy LocalSubscriber.
type BusAdapter struct {
	Bus *pubsub.Bus
}

func (a *BusAdapter) Subscribe(filters []string, buffer int) (int, <-chan LocalMessage) {
	if a == nil || a.Bus == nil {
		ch := make(chan LocalMessage)
		close(ch)
		return 0, ch
	}
	id, raw := a.Bus.Subscribe(filters, buffer)
	out := make(chan LocalMessage, buffer)
	go func() {
		defer close(out)
		for m := range raw {
			out <- LocalMessage{
				Topic:   m.TopicName,
				Payload: m.Payload,
				QoS:     m.QoS,
				Retain:  m.IsRetain,
			}
		}
	}()
	return id, out
}

func (a *BusAdapter) Unsubscribe(id int) {
	if a != nil && a.Bus != nil {
		a.Bus.Unsubscribe(id)
	}
}
