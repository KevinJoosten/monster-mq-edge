package broker

import (
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/stores"
)

// userPropsFromPacket keeps order and repeated keys.
func userPropsFromPacket(up []packets.UserProperty) []stores.UserProperty {
	if len(up) == 0 {
		return nil
	}
	out := make([]stores.UserProperty, len(up))
	for i, p := range up {
		out[i] = stores.UserProperty{Key: p.Key, Value: p.Val}
	}
	return out
}

// propsToPacket builds engine packet properties from a bus message.
func propsToPacket(m stores.BrokerMessage) packets.Properties {
	p := packets.Properties{
		ContentType:     m.ContentType,
		ResponseTopic:   m.ResponseTopic,
		CorrelationData: m.CorrelationData,
	}
	if m.MessageExpiryInterval != nil {
		p.MessageExpiryInterval = *m.MessageExpiryInterval
	}
	if m.PayloadFormatIndicator != nil {
		p.PayloadFormat = *m.PayloadFormatIndicator
		p.PayloadFormatFlag = true
	}
	for _, u := range m.UserProperties {
		p.User = append(p.User, packets.UserProperty{Key: u.Key, Val: u.Value})
	}
	return p
}
