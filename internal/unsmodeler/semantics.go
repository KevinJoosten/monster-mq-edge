package unsmodeler

import (
	"fmt"
	"slices"
	"strings"
)

// Semantics are the effective topic semantics and payload fields of one
// published topic.
type Semantics struct {
	Retain      bool
	Qos         byte
	PublishMode string
	Deadband    float64
	IntervalMs  int64
	Timestamp   string
	Encoding    string
	Payload     []PayloadField
}

// BuiltinSemantics are the built-in defaults (§3.3, §4.4).
func BuiltinSemantics() Semantics {
	return Semantics{
		PublishMode: ModeOnChange,
		Timestamp:   TimestampSource,
		Encoding:    EncodingJSON,
		Payload:     []PayloadField{{Field: FieldValue}, {Field: FieldTime}},
	}
}

// Apply returns s with the overrides of one level: retain, qos, timestamp
// and encoding one by one; publishMode with deadband and intervalMs as one
// unit; payload replaces the list.
func (s Semantics) Apply(o *Overrides) Semantics {
	if o == nil {
		return s
	}
	if o.Retain != nil {
		s.Retain = *o.Retain
	}
	if o.Qos != nil {
		s.Qos = byte(*o.Qos)
	}
	if o.Timestamp != nil {
		s.Timestamp = *o.Timestamp
	}
	if o.Encoding != nil {
		s.Encoding = *o.Encoding
	}
	if o.PublishMode != nil {
		s.PublishMode = *o.PublishMode
		s.Deadband, s.IntervalMs = 0, 0
		if o.Deadband != nil {
			s.Deadband = *o.Deadband
		}
		if o.IntervalMs != nil {
			s.IntervalMs = int64(*o.IntervalMs)
		}
	}
	if o.Payload != nil {
		s.Payload = slices.Clone(o.Payload)
	}
	return s
}

// DeviceSemantics are the built-in defaults with the device's topicDefaults
// and payloadDefaults applied.
func DeviceSemantics(topicDefaults *Overrides, payloadDefaults []PayloadField) Semantics {
	s := BuiltinSemantics().Apply(topicDefaults)
	if payloadDefaults != nil {
		s.Payload = slices.Clone(payloadDefaults)
	}
	return s
}

// Equal reports whether two semantics publish identically.
func (s Semantics) Equal(o Semantics) bool {
	return s.Retain == o.Retain && s.Qos == o.Qos && s.PublishMode == o.PublishMode && s.Deadband == o.Deadband &&
		s.IntervalMs == o.IntervalMs && s.Timestamp == o.Timestamp && s.Encoding == o.Encoding && slices.Equal(s.Payload, o.Payload)
}

// Config attribute allowlist (§4.4). Attributes of _online (except _value)
// and _lock are dynamic and connected together with the value; the others
// are static and read on apply, start and reconnect.
var staticAlertAttrs = map[string]bool{
	"_type": true, "_l_limit": true, "_u_limit": true, "_l_incl": true, "_u_incl": true, "_class": true,
	"_text": true, "_text0": true, "_text1": true, "_num_ranges": true, "_min_prio": true, "_panel": true,
	"_help": true, "_active": true, "_orig_hdl": true, "_ok_range": true, "_hyst_type": true,
	"_l_hyst_limit": true, "_u_hyst_limit": true, "_discrete_states": true, "_match": true,
}

// attributeConfig returns the config part of "_config.<detail>._attr".
func attributeConfig(attr string) (config, detail, name string) {
	parts := strings.SplitN(attr, ".", 3)
	if len(parts) != 3 {
		return attr, "", ""
	}
	return parts[0], parts[1], parts[2]
}

// attributeAllowed returns why a payload config attribute is rejected, or
// "" when it is on the allowlist.
func attributeAllowed(attr string) string {
	config, _, name := attributeConfig(attr)
	switch config {
	case "_online":
		if name == "_value" {
			return "_online.._value is the value field; use \"value\""
		}
		return ""
	case "_lock":
		return ""
	case "_pv_range", "_archive", "_default", "_smooth":
		return ""
	case "_alert_hdl":
		if staticAlertAttrs[name] {
			return ""
		}
		return fmt.Sprintf("alert state attribute %s cannot be published; only limits, classes and texts of _alert_hdl", name)
	case "_original", "_address", "_distrib":
		return fmt.Sprintf("config %s cannot be published", config)
	}
	return fmt.Sprintf("config %s is not on the allowlist (_online, _lock, _pv_range, _alert_hdl, _archive, _default, _smooth)", config)
}

// AttributeDynamic reports whether an allowed attribute changes with the
// value (connected with it) rather than being static metadata.
func AttributeDynamic(attr string) bool {
	config, _, _ := attributeConfig(attr)
	return config == "_online" || config == "_lock"
}

// numericKind reports whether a deadband can apply to an element kind.
func numericKind(kind uint32) bool {
	switch kind {
	case 2, 3, 4: // int, uint, float
		return true
	}
	return false
}
