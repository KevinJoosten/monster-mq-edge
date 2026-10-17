package nats

import (
	"strings"

	"monstermq.io/edge/internal/stores"
)

// parseHeaders parses a NATS header block (starts with "NATS/1.0[ status]", then "Key: Value" lines).
// First value of a key wins (NC:472-482).
func parseHeaders(block string) []stores.UserProperty {
	lines := strings.Split(block, "\r\n")
	if len(lines) <= 1 {
		return nil
	}
	seen := make(map[string]bool)
	var props []stores.UserProperty
	for _, line := range lines[1:] {
		idx := strings.IndexByte(line, ':')
		if idx > 0 {
			key := strings.TrimSpace(line[:idx])
			val := strings.TrimSpace(line[idx+1:])
			if !seen[key] {
				seen[key] = true
				props = append(props, stores.UserProperty{Key: key, Value: val})
			}
		}
	}
	return props
}

// formatHeaders formats headers as a NATS/1.0 header block.
// Status is e.g. "100 Idle Heartbeat" or nil.
// Duplicate user-property keys are collapsed to the last value, as main's map does.
func formatHeaders(status string, props []stores.UserProperty, extra map[string]string) string {
	var sb strings.Builder
	sb.WriteString("NATS/1.0")
	if status != "" {
		sb.WriteByte(' ')
		sb.WriteString(status)
	}
	sb.WriteString("\r\n")

	clean := func(s string) string {
		return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
	}

	// Collapse user properties: last value wins, preserving order of first appearance or last?
	// In main, userProperties is a Map<String, String>. When converted from MQTT user properties,
	// later keys overwrite earlier ones in the map.
	merged := make(map[string]string)
	var order []string
	for _, p := range props {
		if _, exists := merged[p.Key]; !exists {
			order = append(order, p.Key)
		}
		merged[p.Key] = p.Value
	}
	for k, v := range extra {
		if _, exists := merged[k]; !exists {
			order = append(order, k)
		}
		merged[k] = v
	}

	for _, k := range order {
		v := merged[k]
		kClean := strings.ReplaceAll(clean(k), ":", "_")
		vClean := clean(v)
		sb.WriteString(kClean)
		sb.WriteString(": ")
		sb.WriteString(vClean)
		sb.WriteString("\r\n")
	}

	sb.WriteString("\r\n")
	return sb.String()
}

// formatHeadersMap formats a map of headers with an optional status.
func formatHeadersMap(status string, headers map[string]string) string {
	var sb strings.Builder
	sb.WriteString("NATS/1.0")
	if status != "" {
		sb.WriteByte(' ')
		sb.WriteString(status)
	}
	sb.WriteString("\r\n")

	clean := func(s string) string {
		return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
	}

	for k, v := range headers {
		kClean := strings.ReplaceAll(clean(k), ":", "_")
		vClean := clean(v)
		sb.WriteString(kClean)
		sb.WriteString(": ")
		sb.WriteString(vClean)
		sb.WriteString("\r\n")
	}

	sb.WriteString("\r\n")
	return sb.String()
}
