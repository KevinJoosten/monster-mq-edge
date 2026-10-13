package unsmodeler

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// DeviceType is the DeviceConfig type of UNS Modeler devices.
const DeviceType = "UNS_MODELER"

// Approval modes and redundancy modes of a device.
const (
	ApprovalReview = "review"
	ApprovalAuto   = "auto"

	RedundancyHotStandby  = "HOT_STANDBY"
	RedundancyColdStandby = "COLD_STANDBY"
	RedundancyAlways      = "ALWAYS"
)

// Defaults of the AI run limits.
const (
	DefaultAiMaxSteps    = 30
	DefaultAiTimeoutSec  = 600
	DefaultAiTokenBudget = 500000
)

// Settings is the device configuration of a UNS Modeler device, stored as
// DeviceConfig.Config. The mapping documents are stored separately.
type Settings struct {
	Direction     string `json:"direction"`
	AiProvider    string `json:"aiProvider,omitempty"`
	ApprovalMode  string `json:"approvalMode,omitempty"`
	SystemPrompt  string `json:"systemPrompt,omitempty"`
	AiMaxSteps    int    `json:"aiMaxSteps,omitempty"`
	AiTimeoutSec  int    `json:"aiTimeoutSec,omitempty"`
	AiTokenBudget int    `json:"aiTokenBudget,omitempty"`
	Redundancy    string `json:"redundancy,omitempty"`

	// Direction A
	UnsRoot         string          `json:"unsRoot,omitempty"`
	AllowWrite      bool            `json:"allowWrite,omitempty"`
	TopicDefaults   json.RawMessage `json:"topicDefaults,omitempty"`
	PayloadDefaults json.RawMessage `json:"payloadDefaults,omitempty"`

	// Direction B
	OaPrefix    string `json:"oaPrefix,omitempty"`
	FlowEnabled *bool  `json:"flowEnabled,omitempty"`
}

// ParseSettings decodes a device configuration and fills the defaults.
func ParseSettings(raw string) (*Settings, error) {
	var s Settings
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &s); err != nil {
			return nil, fmt.Errorf("device configuration: %w", err)
		}
	}
	s.applyDefaults()
	return &s, nil
}

func (s *Settings) applyDefaults() {
	if s.ApprovalMode == "" {
		s.ApprovalMode = ApprovalReview
	}
	if s.AiMaxSteps <= 0 {
		s.AiMaxSteps = DefaultAiMaxSteps
	}
	if s.AiTimeoutSec <= 0 {
		s.AiTimeoutSec = DefaultAiTimeoutSec
	}
	if s.AiTokenBudget <= 0 {
		s.AiTokenBudget = DefaultAiTokenBudget
	}
	if s.Redundancy == "" {
		s.Redundancy = RedundancyHotStandby
	}
	if s.Direction == DirectionMqttToWinccoa && s.FlowEnabled == nil {
		t := true
		s.FlowEnabled = &t
	}
}

// Flow reports whether Direction B writes values after create.
func (s *Settings) Flow() bool { return s.FlowEnabled == nil || *s.FlowEnabled }

// Encode serializes the settings.
func (s *Settings) Encode() string {
	b, _ := marshalNoEscape(s)
	return string(b)
}

// Defaults returns the decoded topic and payload defaults of the device.
// The settings must have passed Validate.
func (s *Settings) Defaults() (*Overrides, []PayloadField) {
	var td Overrides
	if len(s.TopicDefaults) > 0 && string(s.TopicDefaults) != "null" {
		_ = json.Unmarshal(s.TopicDefaults, &td)
	}
	var pd []PayloadField
	if len(s.PayloadDefaults) > 0 && string(s.PayloadDefaults) != "null" {
		_ = json.Unmarshal(s.PayloadDefaults, &pd)
	}
	return &td, pd
}

// Issue is one validation finding with its severity.
type Issue struct {
	Severity string `json:"severity"`
	Path     string `json:"path"`
	Message  string `json:"message"`
}

// Issue severities: errors block apply, warnings never do.
const (
	SeverityError   = "error"
	SeverityWarning = "warning"
	SeverityInfo    = "info"
)

// HasErrors reports whether any issue is an error.
func HasErrors(issues []Issue) bool {
	for _, is := range issues {
		if is.Severity == SeverityError {
			return true
		}
	}
	return false
}

var segmentRe = regexp.MustCompile(`^[^ /+#\x00-\x1f\x7f-\x{a0}\x{1680}\x{2000}-\x{200f}\x{2028}-\x{202f}\x{205f}-\x{2064}\x{3000}\x{feff}\x{fffe}\x{ffff}]([^/+#\x00-\x1f\x7f-\x9f\x{200b}-\x{200f}\x{2028}-\x{202e}\x{2060}-\x{2064}\x{feff}\x{fffe}\x{ffff}]*[^ /+#\x00-\x1f\x7f-\x{a0}\x{1680}\x{2000}-\x{200f}\x{2028}-\x{202f}\x{205f}-\x{2064}\x{3000}\x{feff}\x{fffe}\x{ffff}])?$`)

// ValidSegment reports whether s is one valid UNS topic level (the schema's
// Segment).
func ValidSegment(s string) bool {
	return s != "..set" && len(s) <= 256 && segmentRe.MatchString(s)
}

// TopicRoots are the reserved branches no effective topic may enter.
type TopicRoots struct {
	Native string // WinCCOaNative.TopicRoot, may have several levels
}

// UnderRoot reports whether topic is root or lies below it.
func UnderRoot(topic, root string) bool {
	return root != "" && (topic == root || strings.HasPrefix(topic, root+"/"))
}

// reservedTopic reports why a topic is not allowed for publishing.
func reservedTopic(topic string, roots TopicRoots) string {
	if UnderRoot(topic, "$SYS") || strings.HasPrefix(topic, "$") {
		return "lies in $SYS"
	}
	if UnderRoot(topic, roots.Native) {
		return fmt.Sprintf("lies inside the native WinCC OA branch %q", roots.Native)
	}
	return ""
}

// ValidateSettings checks a device configuration when the device is
// saved: direction, unsRoot, topicDefaults and payloadDefaults (errors
// reject the save) plus the warnings of the combination.
func ValidateSettings(s *Settings, roots TopicRoots) []Issue {
	var out []Issue
	add := func(sev, path, msg string) { out = append(out, Issue{Severity: sev, Path: path, Message: msg}) }
	switch s.Direction {
	case DirectionWinccoaToUns, DirectionMqttToWinccoa:
	default:
		add(SeverityError, "/direction", fmt.Sprintf("must be %s or %s", DirectionWinccoaToUns, DirectionMqttToWinccoa))
		return out
	}
	if s.ApprovalMode != ApprovalReview && s.ApprovalMode != ApprovalAuto {
		add(SeverityError, "/approvalMode", "must be review or auto")
	}
	switch s.Redundancy {
	case RedundancyHotStandby, RedundancyColdStandby, RedundancyAlways:
	default:
		add(SeverityError, "/redundancy", "must be HOT_STANDBY, COLD_STANDBY or ALWAYS")
	}
	if s.Direction == DirectionMqttToWinccoa {
		if s.OaPrefix == "" {
			add(SeverityError, "/oaPrefix", "is required for Direction B")
		} else if !oaNameRe.MatchString(s.OaPrefix) || strings.HasPrefix(s.OaPrefix, "_") {
			add(SeverityError, "/oaPrefix", "must be a WinCC OA name not starting with '_'")
		}
		return out
	}
	out = append(out, ValidateUnsRoot(s.UnsRoot, roots)...)
	var td Overrides
	if len(s.TopicDefaults) > 0 && string(s.TopicDefaults) != "null" {
		issues, err := ValidateDef("TopicSemantics", s.TopicDefaults)
		if err != nil {
			add(SeverityError, "/topicDefaults", err.Error())
		}
		for _, is := range issues {
			add(SeverityError, "/topicDefaults"+is.Path, is.Message)
		}
		if len(issues) == 0 {
			_ = json.Unmarshal(s.TopicDefaults, &td)
		}
	}
	var pd []PayloadField
	pdSet := false
	if len(s.PayloadDefaults) > 0 && string(s.PayloadDefaults) != "null" {
		issues, err := ValidateDef("PayloadFields", s.PayloadDefaults)
		if err != nil {
			add(SeverityError, "/payloadDefaults", err.Error())
		}
		for _, is := range issues {
			add(SeverityError, "/payloadDefaults"+is.Path, is.Message)
		}
		if len(issues) == 0 {
			_ = json.Unmarshal(s.PayloadDefaults, &pd)
			pdSet = true
			for i, f := range pd {
				if f.Field == "" {
					if why := attributeAllowed(f.Attribute); why != "" {
						add(SeverityError, fmt.Sprintf("/payloadDefaults/%d/attribute", i), why)
					}
				}
			}
		}
	}
	if td.PublishMode != nil && *td.PublishMode == ModeCyclic && td.IntervalMs != nil && *td.IntervalMs < 1000 {
		add(SeverityWarning, "/topicDefaults/intervalMs", "cyclic below 1000 ms publishes every DPE at that rate whether it changed or not")
	}
	if td.Encoding != nil && *td.Encoding == EncodingRaw && pdSet && !isDefaultPayload(pd) {
		add(SeverityWarning, "/payloadDefaults", "encoding raw ignores the payload fields")
	}
	return out
}

// ValidateUnsRoot checks a unsRoot (§3.2): one or more Segment levels, not
// starting with '$', not inside the native branch.
func ValidateUnsRoot(root string, roots TopicRoots) []Issue {
	if root == "" {
		return []Issue{{Severity: SeverityError, Path: "/unsRoot", Message: "is required for Direction A"}}
	}
	if strings.HasPrefix(root, "$") {
		return []Issue{{Severity: SeverityError, Path: "/unsRoot", Message: "must not start with '$'"}}
	}
	for _, lvl := range strings.Split(root, "/") {
		if !ValidSegment(lvl) {
			return []Issue{{Severity: SeverityError, Path: "/unsRoot", Message: fmt.Sprintf("level %s is not a valid topic level (no empty levels, wildcards, control or invisible characters, '..set')", quoteJSON(lvl))}}
		}
	}
	if why := reservedTopic(root, roots); why != "" {
		return []Issue{{Severity: SeverityError, Path: "/unsRoot", Message: why}}
	}
	return nil
}

var oaNameRe = regexp.MustCompile(`^[^\x00-\x20\x7f-\x9f$*,.:;?@\[\]{}]+$`)

func isDefaultPayload(p []PayloadField) bool {
	return len(p) == 2 && p[0].Field == FieldValue && p[1].Field == FieldTime
}
