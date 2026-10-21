package broker

import (
	"strings"

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

// setV5Props copies the MQTT 5 publish properties of a packet (except the
// message expiry, which callers derive from the packet's timestamps) to a
// message for storage, cloning what may point into a connection read buffer.
func setV5Props(m *stores.BrokerMessage, p *packets.Properties) {
	if p.PayloadFormatFlag {
		v := p.PayloadFormat
		m.PayloadFormatIndicator = &v
	}
	m.ContentType = strings.Clone(p.ContentType)
	m.ResponseTopic = strings.Clone(p.ResponseTopic)
	if len(p.CorrelationData) > 0 {
		m.CorrelationData = append([]byte(nil), p.CorrelationData...)
	}
	if len(p.User) > 0 {
		m.UserProperties = make([]stores.UserProperty, len(p.User))
		for i, u := range p.User {
			m.UserProperties[i] = stores.UserProperty{Key: strings.Clone(u.Key), Value: strings.Clone(u.Val)}
		}
	}
}
