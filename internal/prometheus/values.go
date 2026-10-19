package prometheus

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"

	"monstermq.io/edge/internal/metrics"
)

var brokerMetricLabels = []string{
	"messages_in", "messages_out",
	"sessions", "subscriptions", "queue_depth",
	"bus_in", "bus_out",
	"winccoa_in", "winccoa_out",
	"mqtt_client_in", "mqtt_client_out",
	"kafka_client_in", "kafka_client_out",
	"winccoa_client_in", "winccua_client_in",
	"nats_client_in", "nats_client_out",
	"redis_client_in", "redis_client_out",
}

var scrapeBrokerMetricLabels = []string{
	"messages_in",
	"messages_out",
	"sessions",
	"subscriptions",
	"queue_depth",
	"bus_in",
	"bus_out",
	"winccoa_in",
	"winccoa_out",
}

func instantBrokerMetricValue(snap metrics.BrokerSnapshot, label string) (float64, bool) {
	switch label {
	case "messages_in":
		return snap.MessagesIn, true
	case "messages_out":
		return snap.MessagesOut, true
	case "sessions":
		return float64(snap.NodeSessionCount), true
	case "subscriptions":
		return float64(snap.SubscriptionCount), true
	case "queue_depth":
		return float64(snap.QueuedMessages), true
	case "bus_in":
		return snap.MessageBusIn, true
	case "bus_out":
		return snap.MessageBusOut, true
	case "winccoa_in":
		return snap.WinCCOaIn, true
	case "winccoa_out":
		return snap.WinCCOaOut, true
	case "winccoa_client_in":
		return snap.WinCCOaClientIn, true
	case "mqtt_client_in":
		return snap.MqttClientIn, true
	case "mqtt_client_out":
		return snap.MqttClientOut, true
	case "kafka_client_in":
		return snap.KafkaClientIn, true
	case "kafka_client_out":
		return snap.KafkaClientOut, true
	default:
		return 0, false
	}
}

var historyBrokerMetricKey = map[string]string{
	"messages_in":       "messagesIn",
	"messages_out":      "messagesOut",
	"sessions":          "nodeSessionCount",
	"subscriptions":     "subscriptionCount",
	"queue_depth":       "queuedMessagesCount",
	"bus_in":            "messageBusIn",
	"bus_out":           "messageBusOut",
	"winccoa_in":        "winCCOaIn",
	"winccoa_out":       "winCCOaOut",
	"mqtt_client_in":    "mqttClientIn",
	"mqtt_client_out":   "mqttClientOut",
	"kafka_client_in":   "kafkaClientIn",
	"kafka_client_out":  "kafkaClientOut",
	"winccoa_client_in": "winCCOaClientIn",
	"winccua_client_in": "winCCUaClientIn",
	"nats_client_in":    "natsClientIn",
	"nats_client_out":   "natsClientOut",
	"redis_client_in":   "redisClientIn",
	"redis_client_out":  "redisClientOut",
}

func extractHistoryBrokerMetricValue(payloadJSON, label string) (float64, bool) {
	key, ok := historyBrokerMetricKey[label]
	if !ok {
		return 0, false
	}
	var m map[string]float64
	if err := json.Unmarshal([]byte(payloadJSON), &m); err != nil {
		return 0, false
	}
	return m[key], true
}

func trimJavaSpace(s string) string {
	start := 0
	for start < len(s) && s[start] <= 0x20 {
		start++
	}
	end := len(s)
	for end > start && s[end-1] <= 0x20 {
		end--
	}
	return s[start:end]
}

// parseJavaDouble matches Kotlin String.toDoubleOrNull() / Java Double.valueOf grammar.
func parseJavaDouble(s string) (float64, bool) {
	s = trimJavaSpace(s)
	if s == "" {
		return 0, false
	}
	if strings.ContainsRune(s, '_') {
		return 0, false
	}
	switch s {
	case "NaN", "+NaN", "-NaN":
		return math.NaN(), true
	case "Infinity", "+Infinity":
		return math.Inf(1), true
	case "-Infinity":
		return math.Inf(-1), true
	}
	lower := strings.ToLower(s)
	if strings.Contains(lower, "inf") || strings.Contains(lower, "nan") {
		return 0, false
	}
	isHex := strings.HasPrefix(lower, "0x") || strings.HasPrefix(lower, "+0x") || strings.HasPrefix(lower, "-0x")
	if isHex && !strings.ContainsRune(lower, 'p') {
		return 0, false
	}
	if last := s[len(s)-1]; last == 'd' || last == 'D' || last == 'f' || last == 'F' {
		s = s[:len(s)-1]
	}
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return 0, false
	}
	return v, true
}

// javaDouble formats f with Java Double.toString (JDK 21) rules.
func javaDouble(f float64) string {
	if math.IsNaN(f) {
		return "NaN"
	}
	if math.IsInf(f, 1) {
		return "Infinity"
	}
	if math.IsInf(f, -1) {
		return "-Infinity"
	}
	if f == 0 {
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}

	s := strconv.FormatFloat(f, 'e', -1, 64)
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = s[1:]
	}
	mantissa, expStr, _ := strings.Cut(s, "e")
	exp, _ := strconv.Atoi(expStr)
	digits := strings.ReplaceAll(mantissa, ".", "")

	prefix := ""
	if neg {
		prefix = "-"
	}

	if exp >= -3 && exp < 7 {
		if exp >= 0 {
			intLen := exp + 1
			if len(digits) <= intLen {
				return prefix + digits + strings.Repeat("0", intLen-len(digits)) + ".0"
			}
			return prefix + digits[:intLen] + "." + digits[intLen:]
		}
		zeros := -exp - 1
		return prefix + "0." + strings.Repeat("0", zeros) + digits
	}

	var m string
	if len(digits) == 1 {
		m = digits + ".0"
	} else {
		m = digits[:1] + "." + digits[1:]
	}
	return prefix + m + "E" + strconv.Itoa(exp)
}

func extractNumericValue(payload, field string) (float64, bool) {
	if field == "" {
		if v, ok := parseJavaDouble(payload); ok {
			return v, true
		}
		var wrapper struct {
			V *json.Number `json:"v"`
		}
		dec := json.NewDecoder(strings.NewReader(`{"v":` + payload + `}`))
		dec.UseNumber()
		if err := dec.Decode(&wrapper); err != nil || wrapper.V == nil {
			return 0, false
		}
		if _, err := dec.Token(); err != io.EOF {
			return 0, false
		}
		v, err := wrapper.V.Float64()
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return extractJSONField(payload, field)
}

func extractJSONField(jsonStr, fieldPath string) (float64, bool) {
	dec := json.NewDecoder(strings.NewReader(jsonStr))
	dec.UseNumber()
	var root map[string]any
	if err := dec.Decode(&root); err != nil || root == nil {
		return 0, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return 0, false
	}
	parts := strings.Split(fieldPath, ".")
	var cur any = root
	for _, part := range parts {
		obj, ok := cur.(map[string]any)
		if !ok {
			return 0, false
		}
		cur, ok = obj[part]
		if !ok {
			return 0, false
		}
	}
	switch v := cur.(type) {
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case string:
		return parseJavaDouble(v)
	default:
		return 0, false
	}
}

type fieldValue struct {
	Key   string
	Value float64
}

// numericFields walks top-level entries of a JSON object in payload order,
// returning those whose value is a JSON number or a numeric string. If a key
// appears more than once in the object, the last value wins at the first key
// position (matching Vert.x JsonObject / LinkedHashMap).
func numericFields(payload string) ([]fieldValue, bool) {
	dec := json.NewDecoder(bytes.NewReader([]byte(payload)))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, false
	}

	type rawEntry struct {
		key string
		val any
	}
	var order []string
	seen := map[string]int{}
	var entries []rawEntry

	for dec.More() {
		kTok, err := dec.Token()
		if err != nil {
			return nil, false
		}
		key, ok := kTok.(string)
		if !ok {
			return nil, false
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, false
		}
		var val any
		vDec := json.NewDecoder(bytes.NewReader(raw))
		vDec.UseNumber()
		if err := vDec.Decode(&val); err != nil {
			return nil, false
		}
		if idx, exists := seen[key]; exists {
			entries[idx].val = val
		} else {
			seen[key] = len(entries)
			order = append(order, key)
			entries = append(entries, rawEntry{key: key, val: val})
		}
	}
	endTok, err := dec.Token()
	if err != nil || endTok != json.Delim('}') {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, false
	}

	var out []fieldValue
	for _, e := range entries {
		switch v := e.val.(type) {
		case json.Number:
			if f, err := v.Float64(); err == nil {
				out = append(out, fieldValue{Key: e.key, Value: f})
			}
		case string:
			if f, ok := parseJavaDouble(v); ok {
				out = append(out, fieldValue{Key: e.key, Value: f})
			}
		}
	}
	return out, true
}
