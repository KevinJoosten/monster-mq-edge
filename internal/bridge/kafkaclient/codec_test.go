package kafkaclient

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"monstermq.io/edge/internal/stores"
)

func sp(s string) *string { return &s }

// goldenNoTrailer is a BrokerMessageCodec header without the 0x504C0001
// trailer (pre-trailer Core): uuid "u1", messageId 7, qos 1 + retain, topic
// "a/b", client "c", time 1000 ms, payload "hi", originNodeId null.
const goldenNoTrailer = "00000002" + "7531" + // uuid
	"00000007" + // messageId
	"0a" + // status: qos 1 <<3 | retain <<1
	"00000003" + "612f62" + // topic
	"00000001" + "63" + // clientId
	"00000000000003e8" + // time
	"00000002" + "6869" + // payload
	"ffffffff" // originNodeId null

// goldenTrailer is the same message as Core's current encoder writes it, with
// an empty trailer: every nullable field null, no peer.
const goldenTrailer = goldenNoTrailer +
	"504c0001" + // magic
	"ffffffff" + // senderId
	"ffffffffffffffff" + // messageExpiryInterval
	"ffffffff" + // payloadFormatIndicator
	"ffffffff" + // contentType
	"ffffffff" + // responseTopic
	"ffffffff" + // correlationData
	"ffffffff" + // userProperties
	"00" + // isWill
	"ffffffff" + // username
	"ffffffff" + // peerSource
	"00" // hasPeer

func goldenMessage() WireMessage {
	return WireMessage{
		MessageUUID: "u1", MessageID: 7, TopicName: "a/b", Payload: []byte("hi"),
		QoS: 1, IsRetain: true, ClientID: "c", Time: time.UnixMilli(1000).UTC(),
	}
}

func TestEncodeWireGolden(t *testing.T) {
	got := hex.EncodeToString(EncodeWire(goldenMessage()))
	if got != goldenTrailer {
		t.Fatalf("encoded\n got %s\nwant %s", got, goldenTrailer)
	}
}

func TestDecodeWireWithoutTrailer(t *testing.T) {
	b, _ := hex.DecodeString(goldenNoTrailer)
	m, err := DecodeWire(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m, goldenMessage()) {
		t.Fatalf("decoded %+v, want %+v", m, goldenMessage())
	}
}

func TestWireRoundTripFull(t *testing.T) {
	exp, pfi := int64(60), int32(1)
	in := goldenMessage()
	in.IsDup, in.IsQueued = true, true
	in.OriginNode = sp("node-a")
	in.SenderID = sp("node-a")
	in.MessageExpiryInterval = &exp
	in.PayloadFormatIndicator = &pfi
	in.ContentType = sp("application/json")
	in.ResponseTopic = sp("reply/x")
	in.CorrelationData = []byte{1, 2, 3}
	in.UserProperties = []stores.UserProperty{{Key: "k", Value: "v"}, {Key: "k2", Value: "v2"}}
	in.HasUserProperties = true
	in.IsWill = true
	in.Username = sp("alice")
	in.PeerSource = sp("peer")
	in.Peer = &PeerForward{SourceNode: "n1", ClientID: "c1", Username: sp("bob"), TimeNs: 5, Epoch: 6, Offset: 7, Dup: true, Snapshot: true}

	enc := EncodeWire(in)
	// Peer flags byte is the last byte: dup<<2 | will<<1 | snapshot.
	if enc[len(enc)-1] != 0x05 {
		t.Errorf("peer flags = %#x, want 0x05", enc[len(enc)-1])
	}
	out, err := DecodeWire(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestDecodeWireTruncated(t *testing.T) {
	b, _ := hex.DecodeString(goldenTrailer)
	for _, n := range []int{0, 3, 10, len(b) - 1} {
		if _, err := DecodeWire(b[:n]); err == nil {
			t.Errorf("truncated to %d bytes: no error", n)
		}
	}
	if _, err := DecodeWire([]byte(`{"topic":"x"}`)); err == nil {
		t.Error("JSON accepted as codec bytes")
	}
}

func TestWireToBrokerKeepsV5Properties(t *testing.T) {
	exp, pfi := int64(30), int32(1)
	w := goldenMessage()
	w.MessageExpiryInterval = &exp
	w.PayloadFormatIndicator = &pfi
	w.ContentType = sp("text/plain")
	w.ResponseTopic = sp("r")
	w.CorrelationData = []byte{9}
	w.UserProperties = []stores.UserProperty{{Key: "a", Value: "1"}}
	m := w.ToBroker()
	if m.MessageExpiryInterval == nil || *m.MessageExpiryInterval != 30 || m.PayloadFormatIndicator == nil || *m.PayloadFormatIndicator != 1 ||
		m.ContentType != "text/plain" || m.ResponseTopic != "r" || !bytes.Equal(m.CorrelationData, []byte{9}) || len(m.UserProperties) != 1 {
		t.Fatalf("v5 properties lost: %+v", m)
	}
	big := int64(1) << 40
	w.MessageExpiryInterval = &big
	if w.ToBroker().MessageExpiryInterval != nil {
		t.Error("out-of-range expiry kept")
	}
}

func TestJSONEnvelope(t *testing.T) {
	msg := stores.BrokerMessage{
		MessageUUID: "id", MessageID: 3, TopicName: "t/1", Payload: []byte{0, 1},
		QoS: 1, IsRetain: true, ClientID: "c", Time: time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC),
	}
	var obj map[string]any
	if err := json.Unmarshal(EncodeJSON(msg), &obj); err != nil {
		t.Fatal(err)
	}
	if obj["topic"] != "t/1" || obj["payloadBase64"] != "AAE=" || obj["time"] != "2026-01-02T03:04:05.006Z" {
		t.Fatalf("outbound JSON = %v", obj)
	}
	if _, ok := obj["topicName"]; ok {
		t.Error("outbound JSON has topicName; Core writes topic")
	}

	back, err := DecodeJSON(EncodeJSON(msg), "def")
	if err != nil {
		t.Fatal(err)
	}
	if back.TopicName != "t/1" || !bytes.Equal(back.Payload, []byte{0, 1}) || back.QoS != 1 || !back.IsRetain || back.ClientID != "c" || !back.Time.Equal(msg.Time) {
		t.Fatalf("decoded %+v", back)
	}

	m, err := DecodeJSON([]byte(`{"topicName":"x","payload":"text"}`), "def")
	if err != nil || m.TopicName != "x" || string(m.Payload) != "text" || m.ClientID != "def" || m.MessageUUID == "" {
		t.Fatalf("defaults: %+v %v", m, err)
	}
	if _, err := DecodeJSON([]byte(`{"payload":"x"}`), "def"); err != errNoTopic {
		t.Fatalf("missing topic err = %v", err)
	}
}

func TestISOInstantString(t *testing.T) {
	for ns, want := range map[int]string{
		0:           "2026-01-02T03:04:05Z",
		5_000_000:   "2026-01-02T03:04:05.005Z",
		5_000:       "2026-01-02T03:04:05.000005Z",
		123_456_789: "2026-01-02T03:04:05.123456789Z",
	} {
		if got := isoInstantString(time.Date(2026, 1, 2, 3, 4, 5, ns, time.UTC)); got != want {
			t.Errorf("ns=%d: %s, want %s", ns, got, want)
		}
	}
}

// JSON envelope values follow Core's rules: a wrong type or an explicit null
// drops the record; a non-string topic is used as its string form.
func TestDecodeJSONTypeRulesMatchCore(t *testing.T) {
	for _, bad := range []string{
		`{"topic":"t","qosLevel":"1"}`,
		`{"topic":"t","qosLevel":null}`,
		`{"topic":"t","isRetain":"true"}`,
		`{"topic":"t","messageId":null}`,
		`{"topic":"t","payloadBase64":"AA\nAA"}`,
	} {
		if _, err := DecodeJSON([]byte(bad), "d"); err == nil || err == errNoTopic {
			t.Errorf("%s: err = %v, want a decode error", bad, err)
		}
	}
	m, err := DecodeJSON([]byte(`{"topicName":123,"payloadBase64":"aGk","qosLevel":1.9}`), "d")
	if err != nil {
		t.Fatal(err)
	}
	if m.TopicName != "123" || string(m.Payload) != "hi" || m.QoS != 1 {
		t.Fatalf("decoded %+v", m)
	}
}

// Outbound DEFAULT carries the publisher as sender and user properties with
// Core's map semantics; JSON leaves <, > and & unescaped.
func TestOutboundEncodingMatchesCore(t *testing.T) {
	msg := stores.BrokerMessage{
		TopicName: "a<b>&c", ClientID: "pub", Payload: []byte("x"), Time: time.Unix(1, 0),
		UserProperties: []stores.UserProperty{{Key: "k", Value: "1"}, {Key: "j", Value: "2"}, {Key: "k", Value: "3"}},
	}
	w := WireFromBroker(msg)
	if w.SenderID == nil || *w.SenderID != "pub" {
		t.Errorf("senderId = %v", w.SenderID)
	}
	want := []stores.UserProperty{{Key: "k", Value: "3"}, {Key: "j", Value: "2"}}
	if !reflect.DeepEqual(w.UserProperties, want) {
		t.Errorf("user properties = %v, want %v", w.UserProperties, want)
	}
	if js := string(EncodeJSON(msg)); !strings.Contains(js, `"topic":"a<b>&c"`) || strings.HasSuffix(js, "\n") {
		t.Errorf("JSON = %s", js)
	}
}

func TestUTF8String(t *testing.T) {
	if got := string(utf8String([]byte{'a', 0xff, 0xfe, 'b'})); got != "a��b" {
		t.Errorf("utf8String = %q", got)
	}
	ok := []byte("grüß")
	if got := utf8String(ok); &got[0] != &ok[0] {
		t.Error("valid input copied")
	}
}
