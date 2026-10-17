package broker

import (
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/winccoanative"
)

// ProtocolPublish publishes msg on behalf of a protocol session. The caller
// has already checked the ACL (inline clients bypass it) and sets msg.Time.
type ProtocolPublish func(clientID string, msg stores.BrokerMessage) error

// NewProtocolPublish builds the protocol publish path over the UNS Modeler
// hook, the WinCC OA native hook and the inline publisher. Nil hooks are
// skipped; the first hook that handles the message wins.
func NewProtocolPublish(unsHook *UnsModelerHook, nativeHook *WinCCOaNativeHook, pub *InlinePublisher) ProtocolPublish {
	return func(clientID string, msg stores.BrokerMessage) error {
		if unsHook != nil {
			if handled, err := unsHook.InlinePublish(msg.TopicName, msg.Payload); handled {
				return err
			}
		}
		if nativeHook != nil {
			if handled, err := nativeHook.InlinePublishMessage(clientID, msg); handled {
				return err
			}
		}
		return pub.PublishMessage(clientID, msg)
	}
}

// NativeValueWrite maps a write to a WinCC OA tag value topic onto its command
// topic. ok is false when topic is not a writable native value topic.
func NativeValueWrite(n winccoanative.Names, topic string) (setTopic string, ok bool) {
	n = n.WithDefaults()
	if n.Classify(topic) != winccoanative.KindNative {
		return "", false
	}
	t, err := n.Parse(topic)
	if err != nil || t.Command || t.Explicit || winccoanative.Protected(t.DP) {
		return "", false
	}
	return topic + "/" + winccoanative.SegSet, true
}
