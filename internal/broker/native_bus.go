package broker

import (
	"errors"
	"strconv"
	"strings"
	"time"

	mqtt "monstermq.io/edge/internal/mqtt"
	"monstermq.io/edge/internal/pubsub"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/winccoanative"
)

// busClientPrefix marks the native interests of in-process bus subscribers
// (GraphQL subscriptions, SSE, scripts, bridges). U+0000 is not allowed in
// MQTT strings, so no MQTT client id can collide.
const busClientPrefix = "\x00bus/"

func busClientID(id int) string { return busClientPrefix + strconv.Itoa(id) }

func busSubscriberID(clientID string) (int, bool) {
	rest, ok := strings.CutPrefix(clientID, busClientPrefix)
	if !ok {
		return 0, false
	}
	id, err := strconv.Atoi(rest)
	return id, err == nil
}

// nativeBusInterest registers native WinCC OA interest for bus subscribers,
// so they get datapoint values like an MQTT client subscribing the same
// filters.
type nativeBusInterest struct {
	svc *winccoanative.Service
}

func (n nativeBusInterest) Validate(filters []string) error {
	for _, f := range filters {
		switch v, why := n.svc.Validate(f); v {
		case winccoanative.Accept, winccoanative.NotNative:
		default:
			msg := "subscription rejected for " + f + ": " + v.String()
			if why != "" {
				msg += ": " + why
			}
			return errors.New(msg)
		}
	}
	return nil
}

func (n nativeBusInterest) Subscribed(id int, filters []string) {
	cid := busClientID(id)
	for _, f := range filters {
		if v, _ := n.svc.Validate(f); v == winccoanative.Accept {
			n.svc.Subscribed(cid, f, false)
		}
	}
}

func (n nativeBusInterest) Unsubscribed(id int, filters []string) {
	n.svc.Unsubscribed(busClientID(id), filters)
}

// nativeBroker routes current values for bus subscribers to the bus; MQTT
// clients are served by the MQTT server.
type nativeBroker struct {
	*mqtt.Server
	bus *pubsub.Bus
}

func (b nativeBroker) PublishCurrentValue(clientID, filter, topic string, payload []byte, existed bool) error {
	id, ok := busSubscriberID(clientID)
	if !ok {
		return b.Server.PublishCurrentValue(clientID, filter, topic, payload, existed)
	}
	b.bus.Deliver(id, stores.BrokerMessage{
		TopicName: topic,
		Payload:   append([]byte(nil), payload...),
		QoS:       1,
		IsRetain:  true,
		Time:      time.Now().UTC(),
	})
	return nil
}

// busSessionExists reports a live bus subscriber for a bus client id.
func busSessionExists(bus *pubsub.Bus, clientID string) (exists, isBus bool) {
	id, ok := busSubscriberID(clientID)
	if !ok {
		return false, false
	}
	return bus.Active(id), true
}
