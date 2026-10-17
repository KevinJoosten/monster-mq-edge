package kafkaclient

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"

	"monstermq.io/edge/internal/stores"
)

// trailerMagic starts the versioned trailer of Core's BrokerMessageCodec
// ('P', 'L', 0x00, 0x01).
const trailerMagic = 0x504C0001

// PeerForward is Core's PeerForward block of the codec trailer.
type PeerForward struct {
	SourceNode string
	ClientID   string
	Username   *string
	TimeNs     int64
	Epoch      int64
	Offset     int64
	Dup        bool
	Will       bool
	Snapshot   bool
}

// WireMessage is every field Core's BrokerMessageCodec carries. Edge's
// stores.BrokerMessage has no slot for the cluster-only fields (sender,
// origin node, peer), so they live here and are dropped on conversion.
type WireMessage struct {
	MessageUUID string
	MessageID   int32
	TopicName   string
	Payload     []byte
	QoS         int
	IsRetain    bool
	IsDup       bool
	IsQueued    bool
	ClientID    string
	Time        time.Time
	OriginNode  *string

	SenderID               *string
	MessageExpiryInterval  *int64
	PayloadFormatIndicator *int32
	ContentType            *string
	ResponseTopic          *string
	CorrelationData        []byte // nil = absent
	UserProperties         []stores.UserProperty
	HasUserProperties      bool
	IsWill                 bool
	Username               *string
	PeerSource             *string
	Peer                   *PeerForward
}

// EncodeWire writes m in Core's BrokerMessageCodec.encodeToWire layout. The
// trailer is always written, as Core does.
func EncodeWire(m WireMessage) []byte {
	var w wireWriter
	w.str(m.MessageUUID)
	w.i32(m.MessageID)
	status := byte((m.QoS&0x03)<<3) | boolBit(m.IsDup)<<2 | boolBit(m.IsRetain)<<1 | boolBit(m.IsQueued)
	w.byte(status)
	w.str(m.TopicName)
	w.str(m.ClientID)
	w.i64(m.Time.UnixMilli())
	w.bytes(m.Payload)
	w.nstr(m.OriginNode)

	w.i32(trailerMagic)
	w.nstr(m.SenderID)
	if m.MessageExpiryInterval != nil {
		w.i64(*m.MessageExpiryInterval)
	} else {
		w.i64(-1)
	}
	if m.PayloadFormatIndicator != nil {
		w.i32(*m.PayloadFormatIndicator)
	} else {
		w.i32(-1)
	}
	w.nstr(m.ContentType)
	w.nstr(m.ResponseTopic)
	if m.CorrelationData == nil {
		w.i32(-1)
	} else {
		w.bytes(m.CorrelationData)
	}
	if !m.HasUserProperties && m.UserProperties == nil {
		w.i32(-1)
	} else {
		w.i32(int32(len(m.UserProperties)))
		for _, p := range m.UserProperties {
			w.str(p.Key)
			w.str(p.Value)
		}
	}
	w.byte(boolBit(m.IsWill))
	w.nstr(m.Username)
	w.nstr(m.PeerSource)
	if m.Peer == nil {
		w.byte(0)
	} else {
		p := m.Peer
		w.byte(1)
		w.str(p.SourceNode)
		w.str(p.ClientID)
		w.nstr(p.Username)
		w.i64(p.TimeNs)
		w.i64(p.Epoch)
		w.i64(p.Offset)
		w.byte(boolBit(p.Dup)<<2 | boolBit(p.Will)<<1 | boolBit(p.Snapshot))
	}
	return w.buf
}

// DecodeWire parses Core's BrokerMessageCodec layout. A message without the
// trailer (older Core versions) decodes with the trailer fields unset.
func DecodeWire(b []byte) (WireMessage, error) {
	r := wireReader{buf: b}
	var m WireMessage
	m.MessageUUID = r.str()
	m.MessageID = r.i32()
	status := r.byte()
	m.QoS = int(status>>3) & 0x03
	m.IsDup = status>>2&1 == 1
	m.IsRetain = status>>1&1 == 1
	m.IsQueued = status&1 == 1
	m.TopicName = r.str()
	m.ClientID = r.str()
	m.Time = time.UnixMilli(r.i64()).UTC()
	m.Payload = r.bytes()
	m.OriginNode = r.nstr()
	if r.err != nil {
		return WireMessage{}, r.err
	}

	if r.remaining() < 4 || binary.BigEndian.Uint32(r.buf[r.pos:]) != trailerMagic {
		return m, nil
	}
	r.pos += 4
	m.SenderID = r.nstr()
	if v := r.i64(); v != -1 {
		m.MessageExpiryInterval = &v
	}
	if v := r.i32(); v != -1 {
		m.PayloadFormatIndicator = &v
	}
	m.ContentType = r.nstr()
	m.ResponseTopic = r.nstr()
	if n := r.i32(); n != -1 {
		m.CorrelationData = r.raw(int(n))
	}
	if n := r.i32(); n != -1 {
		if n < 0 || int(n) > r.remaining() {
			return WireMessage{}, fmt.Errorf("codec: invalid user property count %d", n)
		}
		m.HasUserProperties = true
		m.UserProperties = make([]stores.UserProperty, 0, n)
		for i := int32(0); i < n && r.err == nil; i++ {
			k := r.str()
			v := r.str()
			m.UserProperties = append(m.UserProperties, stores.UserProperty{Key: k, Value: v})
		}
	}
	m.IsWill = r.byte() == 1
	m.Username = r.nstr()
	m.PeerSource = r.nstr()
	if r.byte() == 1 {
		p := &PeerForward{}
		p.SourceNode = r.str()
		p.ClientID = r.str()
		p.Username = r.nstr()
		p.TimeNs = r.i64()
		p.Epoch = r.i64()
		p.Offset = r.i64()
		flags := r.byte()
		p.Dup = flags&0x04 != 0
		p.Will = flags&0x02 != 0
		p.Snapshot = flags&0x01 != 0
		m.Peer = p
	}
	if r.err != nil {
		return WireMessage{}, r.err
	}
	return m, nil
}

// WireFromBroker converts an edge message for outbound DEFAULT encoding the
// way Core encodes a message it received from an MQTT client: the sender is
// the publishing client and user properties are a map (a repeated key keeps
// its first position and its last value).
func WireFromBroker(msg stores.BrokerMessage) WireMessage {
	w := WireMessage{
		MessageUUID:     msg.MessageUUID,
		MessageID:       int32(msg.MessageID),
		TopicName:       msg.TopicName,
		Payload:         msg.Payload,
		QoS:             int(msg.QoS),
		IsRetain:        msg.IsRetain,
		IsDup:           msg.IsDup,
		IsQueued:        msg.IsQueued,
		ClientID:        msg.ClientID,
		Time:            msg.Time,
		CorrelationData: msg.CorrelationData,
		UserProperties:  userPropertyMap(msg.UserProperties),
		SenderID:        &msg.ClientID,
	}
	if w.MessageUUID == "" {
		w.MessageUUID = uuid.NewString()
	}
	if w.Time.IsZero() {
		w.Time = time.Now()
	}
	if msg.MessageExpiryInterval != nil {
		v := int64(*msg.MessageExpiryInterval)
		w.MessageExpiryInterval = &v
	}
	if msg.PayloadFormatIndicator != nil {
		v := int32(*msg.PayloadFormatIndicator)
		w.PayloadFormatIndicator = &v
	}
	if msg.ContentType != "" {
		w.ContentType = &msg.ContentType
	}
	if msg.ResponseTopic != "" {
		w.ResponseTopic = &msg.ResponseTopic
	}
	if msg.Username != "" {
		w.Username = &msg.Username
	}
	return w
}

// ToBroker converts a decoded wire message into an edge message, keeping the
// MQTT v5 properties (edge-only extension E2). Values outside the MQTT range
// are dropped rather than wrapped.
func (w WireMessage) ToBroker() stores.BrokerMessage {
	msg := stores.BrokerMessage{
		MessageUUID:     w.MessageUUID,
		TopicName:       w.TopicName,
		Payload:         w.Payload,
		QoS:             clampQoS(w.QoS),
		IsRetain:        w.IsRetain,
		IsDup:           w.IsDup,
		IsQueued:        w.IsQueued,
		ClientID:        w.ClientID,
		Time:            w.Time,
		CorrelationData: w.CorrelationData,
		UserProperties:  w.UserProperties,
	}
	if w.MessageID >= 0 && w.MessageID <= math.MaxUint16 {
		msg.MessageID = uint16(w.MessageID)
	}
	if v := w.MessageExpiryInterval; v != nil && *v >= 0 && *v <= math.MaxUint32 {
		e := uint32(*v)
		msg.MessageExpiryInterval = &e
	}
	if v := w.PayloadFormatIndicator; v != nil && *v >= 0 && *v <= math.MaxUint8 {
		p := byte(*v)
		msg.PayloadFormatIndicator = &p
	}
	if w.ContentType != nil {
		msg.ContentType = *w.ContentType
	}
	if w.ResponseTopic != nil {
		msg.ResponseTopic = *w.ResponseTopic
	}
	return msg
}

func clampQoS(q int) byte {
	if q < 0 || q > 2 {
		return 0
	}
	return byte(q)
}

// jsonEnvelopeOut is the object Core's outbound JSON format writes. Note
// "topic", not "topicName".
type jsonEnvelopeOut struct {
	MessageUUID   string `json:"messageUuid"`
	MessageID     int    `json:"messageId"`
	Topic         string `json:"topic"`
	PayloadBase64 string `json:"payloadBase64"`
	QoSLevel      int    `json:"qosLevel"`
	IsRetain      bool   `json:"isRetain"`
	IsDup         bool   `json:"isDup"`
	IsQueued      bool   `json:"isQueued"`
	ClientID      string `json:"clientId"`
	Time          string `json:"time"`
}

// EncodeJSON writes msg as Core's outbound JSON envelope.
func EncodeJSON(msg stores.BrokerMessage) []byte {
	t := msg.Time
	if t.IsZero() {
		t = time.Now()
	}
	id := msg.MessageUUID
	if id == "" {
		id = uuid.NewString()
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // Core writes <, > and & literally
	_ = enc.Encode(jsonEnvelopeOut{
		MessageUUID:   id,
		MessageID:     int(msg.MessageID),
		Topic:         msg.TopicName,
		PayloadBase64: base64.StdEncoding.EncodeToString(msg.Payload),
		QoSLevel:      int(msg.QoS),
		IsRetain:      msg.IsRetain,
		IsDup:         msg.IsDup,
		IsQueued:      msg.IsQueued,
		ClientID:      msg.ClientID,
		Time:          isoInstantString(t),
	})
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

// errNoTopic means a JSON envelope had neither topicName nor topic; Core drops
// such records without counting an error.
var errNoTopic = errors.New("json envelope has no topicName or topic")

// DecodeJSON parses Core's inbound JSON envelope. defaultClientID is used when
// the envelope has no clientId.
func DecodeJSON(b []byte, defaultClientID string) (stores.BrokerMessage, error) {
	obj, err := parseObject(b)
	if err != nil {
		return stores.BrokerMessage{}, err
	}
	topic, ok := obj.str("topicName")
	if !ok {
		topic, ok = obj.str("topic")
	}
	if !ok {
		return stores.BrokerMessage{}, errNoTopic
	}
	payload := []byte{}
	if s, ok := obj.str("payloadBase64"); ok {
		if payload, err = decodeBase64(s); err != nil {
			return stores.BrokerMessage{}, fmt.Errorf("payloadBase64: %w", err)
		}
	} else if s, ok := obj.str("payload"); ok {
		payload = []byte(s)
	}
	msg := stores.BrokerMessage{TopicName: topic, Payload: payload, ClientID: defaultClientID, Time: time.Now().UTC()}
	if s, ok := obj.str("messageUuid"); ok {
		msg.MessageUUID = s
	} else {
		msg.MessageUUID = uuid.NewString()
	}
	id, err := obj.integerDef("messageId", 0)
	if err != nil {
		return stores.BrokerMessage{}, err
	}
	if id >= 0 && id <= math.MaxUint16 {
		msg.MessageID = uint16(id)
	}
	qos, err := obj.integerDef("qosLevel", 0)
	if err != nil {
		return stores.BrokerMessage{}, err
	}
	msg.QoS = clampQoS(int(qos))
	for key, dst := range map[string]*bool{"isRetain": &msg.IsRetain, "isDup": &msg.IsDup, "isQueued": &msg.IsQueued} {
		if *dst, err = obj.booleanDef(key, false); err != nil {
			return stores.BrokerMessage{}, err
		}
	}
	if s, ok := obj.str("clientId"); ok {
		msg.ClientID = s
	}
	if s, ok := obj.str("time"); ok {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			msg.Time = t.UTC()
		}
	}
	return msg, nil
}

// isoInstantString formats t the way Core writes the JSON "time" field: UTC, 'Z',
// and the fraction trimmed to 0, 3, 6 or 9 digits.
func isoInstantString(t time.Time) string {
	t = t.UTC()
	ns := t.Nanosecond()
	switch {
	case ns == 0:
		return t.Format("2006-01-02T15:04:05Z")
	case ns%1_000_000 == 0:
		return t.Format("2006-01-02T15:04:05.000Z")
	case ns%1_000 == 0:
		return t.Format("2006-01-02T15:04:05.000000Z")
	default:
		return t.Format("2006-01-02T15:04:05.000000000Z")
	}
}

func boolBit(b bool) byte {
	if b {
		return 1
	}
	return 0
}

type wireWriter struct{ buf []byte }

func (w *wireWriter) byte(b byte) { w.buf = append(w.buf, b) }
func (w *wireWriter) i32(v int32) { w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v)) }
func (w *wireWriter) i64(v int64) { w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(v)) }
func (w *wireWriter) bytes(b []byte) {
	w.i32(int32(len(b)))
	w.buf = append(w.buf, b...)
}
func (w *wireWriter) str(s string) { w.bytes([]byte(s)) }
func (w *wireWriter) nstr(s *string) {
	if s == nil {
		w.i32(-1)
		return
	}
	w.str(*s)
}

// wireReader keeps the first error and returns zero values afterwards, so the
// decoder reads straight through and checks once.
type wireReader struct {
	buf []byte
	pos int
	err error
}

func (r *wireReader) remaining() int { return len(r.buf) - r.pos }

func (r *wireReader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if n < 0 || r.remaining() < n {
		r.err = fmt.Errorf("codec: truncated message at offset %d", r.pos)
		return false
	}
	return true
}

func (r *wireReader) byte() byte {
	if !r.need(1) {
		return 0
	}
	b := r.buf[r.pos]
	r.pos++
	return b
}

func (r *wireReader) i32() int32 {
	if !r.need(4) {
		return 0
	}
	v := int32(binary.BigEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	return v
}

func (r *wireReader) i64() int64 {
	if !r.need(8) {
		return 0
	}
	v := int64(binary.BigEndian.Uint64(r.buf[r.pos:]))
	r.pos += 8
	return v
}

func (r *wireReader) raw(n int) []byte {
	if !r.need(n) {
		return nil
	}
	b := make([]byte, n)
	copy(b, r.buf[r.pos:r.pos+n])
	r.pos += n
	return b
}

func (r *wireReader) bytes() []byte { return r.raw(int(r.i32())) }

func (r *wireReader) str() string { return string(r.bytes()) }

func (r *wireReader) nstr() *string {
	n := r.i32()
	if n == -1 || r.err != nil {
		return nil
	}
	s := string(r.raw(int(n)))
	return &s
}

// userPropertyMap applies Core's map semantics to MQTT user properties.
func userPropertyMap(props []stores.UserProperty) []stores.UserProperty {
	if len(props) < 2 {
		return props
	}
	index := make(map[string]int, len(props))
	out := make([]stores.UserProperty, 0, len(props))
	for _, p := range props {
		if i, ok := index[p.Key]; ok {
			out[i].Value = p.Value
			continue
		}
		index[p.Key] = len(out)
		out = append(out, p)
	}
	return out
}
