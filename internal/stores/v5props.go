package stores

import "encoding/json"

// v5Props is the stored form of the MQTT 5 publish properties of a message
// that have no column of their own (Message Expiry Interval has one).
type v5Props struct {
	PayloadFormat   *byte          `json:"pfi,omitempty"`
	ContentType     string         `json:"ct,omitempty"`
	ResponseTopic   string         `json:"rt,omitempty"`
	CorrelationData []byte         `json:"cd,omitempty"`
	UserProperties  []UserProperty `json:"up,omitempty"`
}

// EncodeV5Props returns the MQTT 5 properties of m (Payload Format Indicator,
// Content Type, Response Topic, Correlation Data, User Properties) as a
// compact JSON document for a store column, or nil when m has none, so rows
// of messages without properties stay NULL.
func EncodeV5Props(m BrokerMessage) any {
	if m.PayloadFormatIndicator == nil && m.ContentType == "" && m.ResponseTopic == "" &&
		len(m.CorrelationData) == 0 && len(m.UserProperties) == 0 {
		return nil
	}
	b, err := json.Marshal(v5Props{
		PayloadFormat:   m.PayloadFormatIndicator,
		ContentType:     m.ContentType,
		ResponseTopic:   m.ResponseTopic,
		CorrelationData: m.CorrelationData,
		UserProperties:  m.UserProperties,
	})
	if err != nil {
		return nil
	}
	return string(b)
}

// DecodeV5Props sets the MQTT 5 properties stored by EncodeV5Props on m. An
// empty or invalid document leaves m unchanged.
func DecodeV5Props(s string, m *BrokerMessage) {
	if s == "" {
		return
	}
	var p v5Props
	if json.Unmarshal([]byte(s), &p) != nil {
		return
	}
	m.PayloadFormatIndicator = p.PayloadFormat
	m.ContentType = p.ContentType
	m.ResponseTopic = p.ResponseTopic
	m.CorrelationData = p.CorrelationData
	m.UserProperties = p.UserProperties
}
