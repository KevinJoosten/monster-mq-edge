package winccua

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LocalPublisher is the function the bridge calls to inject an MQTT message
// into the local broker. *mqtt.Server.Publish satisfies this shape.
type LocalPublisher func(topic string, payload []byte, retain bool, qos byte) error

// publisher holds a topic-transformation cache shared between transports for
// one device. Tag → MQTT topic resolution is done once per tag and reused.
type publisher struct {
	namespace       string
	transformConfig TransformConfig
	messageFormat   string

	mu       sync.Mutex
	topicMap map[string]string
}

func newPublisher(namespace string, tc TransformConfig, format string) *publisher {
	return &publisher{
		namespace:       namespace,
		transformConfig: tc,
		messageFormat:   format,
		topicMap:        make(map[string]string),
	}
}

// resolveTagTopic returns "<namespace>/<addressTopic>/<transformed-tag-name>".
// Cached per tagName.
func (p *publisher) resolveTagTopic(addressTopic, tagName string) string {
	p.mu.Lock()
	if cached, ok := p.topicMap[tagName]; ok {
		p.mu.Unlock()
		return joinTopic(p.namespace, addressTopic, cached)
	}
	transformed := p.transformConfig.TransformTagName(tagName)
	p.topicMap[tagName] = transformed
	p.mu.Unlock()
	return joinTopic(p.namespace, addressTopic, transformed)
}

func (p *publisher) resolveAlarmTopic(addressTopic, alarmName string) string {
	return joinTopic(p.namespace, addressTopic, alarmName)
}

// formatTagPayload builds the published MQTT payload for one tag value.
//
//   FORMAT_JSON_ISO  → {"value":<v>,"time":"<ISO>",[ "quality":<q>, "qualityCode":<qc> ]}
//   FORMAT_JSON_MS   → {"value":<v>,"time":<ms>,[ "quality":<q>, "qualityCode":<qc> ]}
//   FORMAT_RAW_VALUE → string(value)
//   FORMAT_RAW_JSON  → raw tag object from WinCC (1:1)
func (p *publisher) formatTagPayload(value any, timestamp string, quality map[string]any, rawTag map[string]any) []byte {
	switch p.messageFormat {
	case FormatRawJSON:
		if rawTag != nil {
			out, _ := json.Marshal(rawTag)
			return out
		}
		obj := map[string]any{"value": value}
		if timestamp != "" {
			obj["time"] = timestamp
		}
		for k, v := range quality {
			obj[k] = v
		}
		out, _ := json.Marshal(obj)
		return out
	case FormatJSONMS:
		ms := time.Now().UnixMilli()
		if timestamp != "" {
			if t, err := parseTimestamp(timestamp); err == nil {
				ms = t.UnixMilli()
			}
		}
		obj := map[string]any{"value": value, "time": ms}
		for k, v := range quality {
			obj[k] = v
		}
		out, _ := json.Marshal(obj)
		return out
	case FormatRawValue:
		return []byte(fmt.Sprintf("%v", value))
	default:
		// FORMAT_JSON_ISO and any unknown value
		obj := map[string]any{"value": value}
		if timestamp != "" {
			obj["time"] = timestamp
		}
		for k, v := range quality {
			obj[k] = v
		}
		out, _ := json.Marshal(obj)
		return out
	}
}

// resolveAlarmName extracts a stable identifier from an alarm payload, trying
// the GraphQL "name", the OpenPipe "Name", and finally "path"/"Path".
func resolveAlarmName(alarm map[string]any) string {
	for _, k := range []string{"name", "Name", "path", "Path"} {
		if v, ok := alarm[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return "unknown"
}

func joinTopic(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.Trim(p, "/")
		if p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, "/")
}

// parseTimestamp parses an ISO 8601 or WinCC format timestamp.
func parseTimestamp(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05.999999999", s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unknown timestamp format: %s", s)
}

// extractWriteData extracts the tag value and optional ISO 8601 UTC timestamp
// from an incoming MQTT payload according to the TAG_WRITE Address config.
// When addr.JsonPath is blank, the raw payload is used 1:1 (coerced to a
// native scalar when forGraphQL is true for GraphQL Variant inputs).
func extractWriteData(payload []byte, addr Address, forGraphQL bool) (any, string, error) {
	jsonPath := strings.TrimSpace(addr.JsonPath)
	if jsonPath == "" {
		raw := string(payload)
		if forGraphQL {
			return parseScalarValue(raw), "", nil
		}
		return raw, "", nil
	}

	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var root any
	if err := dec.Decode(&root); err != nil {
		return nil, "", fmt.Errorf("decode json payload: %w", err)
	}

	val, ok := navigateJSONPath(root, jsonPath)
	if !ok {
		return nil, "", fmt.Errorf("jsonPath %q not found or null", jsonPath)
	}
	switch val.(type) {
	case map[string]any, []any:
		return nil, "", fmt.Errorf("jsonPath %q resolved to non-scalar value", jsonPath)
	}

	tsPath := strings.TrimSpace(addr.TimestampJsonPath)
	if !forGraphQL || tsPath == "" {
		return val, "", nil
	}
	rawTS, ok := navigateJSONPath(root, tsPath)
	if !ok {
		return nil, "", fmt.Errorf("timestampJsonPath %q not found or null", tsPath)
	}
	tsISO, err := parseWriteTimestamp(rawTS, addr.TimestampFormat)
	if err != nil {
		return nil, "", fmt.Errorf("timestampJsonPath %q: %w", tsPath, err)
	}
	return val, tsISO, nil
}

func navigateJSONPath(root any, path string) (any, bool) {
	cur := root
	for _, seg := range strings.Split(strings.TrimSpace(path), ".") {
		if seg == "" {
			continue
		}
		switch m := cur.(type) {
		case map[string]any:
			next, ok := m[seg]
			if !ok {
				return nil, false
			}
			cur = next
		case []any:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(m) {
				return nil, false
			}
			cur = m[idx]
		default:
			return nil, false
		}
	}
	if cur == nil {
		return nil, false
	}
	return cur, true
}

func parseWriteTimestamp(raw any, format string) (string, error) {
	if format == "" {
		format = TimestampFormatISO
	}
	switch format {
	case TimestampFormatMS:
		ms, err := toEpochMillis(raw)
		if err != nil {
			return "", err
		}
		return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z"), nil
	case TimestampFormatISO:
		s, ok := raw.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return "", fmt.Errorf("expected ISO timestamp string, got %T", raw)
		}
		t, err := parseTimestamp(strings.TrimSpace(s))
		if err != nil {
			return "", err
		}
		return t.UTC().Format(time.RFC3339Nano), nil
	default:
		return "", fmt.Errorf("unsupported timestamp format %q", format)
	}
}

func toEpochMillis(raw any) (int64, error) {
	switch v := raw.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i, nil
		}
		if f, err := v.Float64(); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return int64(f), nil
		}
		return 0, fmt.Errorf("invalid millisecond timestamp %q", v.String())
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("invalid millisecond timestamp %v", v)
		}
		return int64(v), nil
	case string:
		s := strings.TrimSpace(v)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i, nil
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return int64(f), nil
		}
		return 0, fmt.Errorf("invalid millisecond timestamp %q", v)
	default:
		return 0, fmt.Errorf("expected millisecond number, got %T", raw)
	}
}

func parseScalarValue(raw string) any {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return raw
	}
	if strings.EqualFold(trimmed, "true") {
		return true
	}
	if strings.EqualFold(trimmed, "false") {
		return false
	}
	if i, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
		return f
	}
	return raw
}

func formatPipeValue(val any) string {
	switch v := val.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func startWriteSubscriber(ctx context.Context, subBus LocalSubscriber, addresses []Address, wg *sync.WaitGroup, handleWrite func(ctx context.Context, addr Address, payload []byte)) {
	if subBus == nil {
		return
	}
	byTopic := map[string][]Address{}
	filters := make([]string, 0)
	for _, a := range addresses {
		if a.Type != AddressTypeTagWrite {
			continue
		}
		topic := strings.TrimSpace(a.Topic)
		if topic == "" {
			continue
		}
		if _, exists := byTopic[topic]; !exists {
			filters = append(filters, topic)
		}
		byTopic[topic] = append(byTopic[topic], a)
	}
	if len(filters) == 0 {
		return
	}
	subID, ch := subBus.Subscribe(filters, 256)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer subBus.Unsubscribe(subID)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				for _, addr := range byTopic[msg.Topic] {
					handleWrite(ctx, addr, msg.Payload)
				}
			}
		}
	}()
}

