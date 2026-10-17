// Package kafkaclient is the MQTT <-> Kafka bridge, a port of MonsterMQ Core's
// KAFKA-Client device (devices/kafkaclient/KafkaClientConnector.kt).
package kafkaclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// DeviceTypeKafkaClient is Core's DeviceConfig.DEVICE_TYPE_KAFKA_CLIENT, stored verbatim.
const DeviceTypeKafkaClient = "KAFKA-Client"

// Payload formats, as Core's PayloadFormat object.
const (
	FormatDefault = "DEFAULT" // BrokerMessageCodec binary envelope
	FormatJSON    = "JSON"    // BrokerMessage serialised as JSON text
	FormatBinary  = "BINARY"  // raw record value; MQTT topic = record key
	FormatText    = "TEXT"    // UTF-8 record value; MQTT topic = record key
)

const (
	DefaultBootstrapServers = "localhost:9092"
	DefaultGroupID          = "monstermq-subscriber"
	DefaultReconnectDelayMs = int64(5000)
	minReconnectDelayMs     = int64(500)
)

// Config is the persisted JSON config (DeviceConfig.Config) of one Kafka
// client, field for field Core's KafkaClientConfig. The JSON keys match what
// Core writes with JsonObject.mapFrom, so configs move between both brokers.
type Config struct {
	BootstrapServers            string            `json:"bootstrapServers"`
	GroupID                     string            `json:"groupId"`
	PayloadFormat               string            `json:"payloadFormat"`
	ExtraConsumerConfig         map[string]string `json:"extraConsumerConfig"`
	ReconnectDelayMs            int64             `json:"reconnectDelayMs"`
	DestinationTopicPrefix      *string           `json:"destinationTopicPrefix"`
	TopicKeyRegex               *string           `json:"topicKeyRegex"`
	TopicKeyReplacement         *string           `json:"topicKeyReplacement"`
	OutboundEnabled             bool              `json:"outboundEnabled"`
	OutboundTopicFilters        []string          `json:"outboundTopicFilters"`
	OutboundKafkaTopic          *string           `json:"outboundKafkaTopic"`
	ExtraProducerConfig         map[string]string `json:"extraProducerConfig"`
	InboundEnabled              bool              `json:"inboundEnabled"`
	OutboundTopicKeyRegex       *string           `json:"outboundTopicKeyRegex"`
	OutboundTopicKeyReplacement *string           `json:"outboundTopicKeyReplacement"`
}

// DefaultConfig returns Core's KafkaClientConfig() defaults.
func DefaultConfig() Config {
	return Config{
		BootstrapServers:     DefaultBootstrapServers,
		GroupID:              DefaultGroupID,
		PayloadFormat:        FormatDefault,
		ExtraConsumerConfig:  map[string]string{},
		ReconnectDelayMs:     DefaultReconnectDelayMs,
		OutboundTopicFilters: []string{},
		ExtraProducerConfig:  map[string]string{},
		InboundEnabled:       true,
	}
}

// ParseConfig parses a stored device config the way Core's connector does:
// an optional outer "kafka" object is unwrapped, then KafkaClientConfig.fromJson
// rules apply.
func ParseConfig(raw string) (Config, error) {
	var cfg Config
	if strings.TrimSpace(raw) == "" {
		return DefaultConfig(), nil
	}
	err := json.Unmarshal([]byte(raw), &cfg)
	return cfg, err
}

// UnmarshalJSON mirrors KafkaClientConfig.fromJson, including the outer
// "kafka" unwrapping that Core's connector does before calling it, and Core's
// value rules: a value of the wrong type fails the config, as it fails the
// device in Core.
func (c *Config) UnmarshalJSON(data []byte) error {
	obj, err := parseObject(data)
	if err != nil {
		return err
	}
	if nested, ok, err := obj.object("kafka"); err != nil {
		return err
	} else if ok {
		obj = nested
	}
	cfg := DefaultConfig()

	// getString/getLong/getJsonObject with a default: an explicit null fails.
	for _, k := range []string{"bootstrapServers", "groupId", "reconnectDelayMs", "extraConsumerConfig"} {
		if obj.isExplicitNull(k) {
			return fmt.Errorf("%s: null", k)
		}
	}
	if s, ok := obj.str("bootstrapServers"); ok {
		cfg.BootstrapServers = s
	}
	if s, ok := obj.str("groupId"); ok {
		cfg.GroupID = s
	}
	if s, ok := obj.str("payloadFormat"); ok {
		cfg.PayloadFormat = s
	}
	if m, ok, err := obj.stringMap("extraConsumerConfig"); err != nil {
		return err
	} else if ok {
		cfg.ExtraConsumerConfig = m
	}
	if cfg.ReconnectDelayMs, err = obj.longDef("reconnectDelayMs", DefaultReconnectDelayMs); err != nil {
		return err
	}

	rawPrefix := obj.strPtr("destinationTopicPrefix")
	if rawPrefix == nil {
		rawPrefix = obj.strPtr("keyTopicPrefix")
	}
	if rawPrefix != nil {
		if cfg.DestinationTopicPrefix, err = NormalizePrefix(*rawPrefix); err != nil {
			return err
		}
	}

	// Core reads the PascalCase key first for the inbound key mapping and the
	// camelCase key first for everything else.
	cfg.TopicKeyRegex = nonBlank(firstStr(obj, "TopicKeyRegex", "topicKeyRegex"))
	cfg.TopicKeyReplacement = firstStr(obj, "TopicKeyReplacement", "topicKeyReplacement")
	cfg.OutboundTopicKeyRegex = nonBlank(firstStr(obj, "outboundTopicKeyRegex", "OutboundTopicKeyRegex"))
	cfg.OutboundTopicKeyReplacement = firstStr(obj, "outboundTopicKeyReplacement", "OutboundTopicKeyReplacement")
	if b, ok, err := firstBool(obj, "inboundEnabled", "InboundEnabled"); err != nil {
		return err
	} else if ok {
		cfg.InboundEnabled = b
	}
	if b, ok, err := firstBool(obj, "outboundEnabled", "OutboundEnabled"); err != nil {
		return err
	} else if ok {
		cfg.OutboundEnabled = b
	}
	for _, k := range []string{"outboundTopicFilters", "OutboundTopicFilters"} {
		items, ok, err := obj.array(k)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		cfg.OutboundTopicFilters = make([]string, 0, len(items))
		for _, it := range items {
			cfg.OutboundTopicFilters = append(cfg.OutboundTopicFilters, valueString(it))
		}
		break
	}
	cfg.OutboundKafkaTopic = nonBlank(firstStr(obj, "outboundKafkaTopic", "OutboundKafkaTopic"))
	for _, k := range []string{"extraProducerConfig", "ExtraProducerConfig"} {
		m, ok, err := obj.stringMap(k)
		if err != nil {
			return err
		}
		if ok {
			cfg.ExtraProducerConfig = m
			break
		}
	}

	*c = cfg
	return nil
}

func firstStr(obj jsonObject, keys ...string) *string {
	for _, k := range keys {
		if s := obj.strPtr(k); s != nil {
			return s
		}
	}
	return nil
}

// firstBool is Core's getBoolean(a) ?: getBoolean(b): each key is read, so a
// wrongly typed first key fails even when the second is set.
func firstBool(obj jsonObject, keys ...string) (bool, bool, error) {
	for _, k := range keys {
		b, ok, err := obj.boolean(k)
		if err != nil {
			return false, false, err
		}
		if ok {
			return b, true, nil
		}
	}
	return false, false, nil
}

// NormalizePrefix applies Core's destinationTopicPrefix rules: trimmed, blank
// disables it, a trailing '/' is appended, and MQTT wildcards are rejected.
func NormalizePrefix(raw string) (*string, error) {
	p := strings.TrimSpace(raw)
	if p == "" {
		return nil, nil
	}
	if strings.ContainsAny(p, "+#") {
		return nil, errors.New("destinationTopicPrefix must not contain MQTT wildcards + or #")
	}
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return &p, nil
}

// Validate returns Core's KafkaClientConfig.validate() errors.
func (c Config) Validate() []string {
	var errs []string
	if c.ReconnectDelayMs < minReconnectDelayMs {
		errs = append(errs, "reconnectDelayMs should be >=500")
	}
	if c.DestinationTopicPrefix != nil && strings.ContainsAny(*c.DestinationTopicPrefix, "+#") {
		errs = append(errs, "destinationTopicPrefix must not contain MQTT wildcards + or #")
	}
	if c.OutboundEnabled && len(c.OutboundTopicFilters) == 0 {
		errs = append(errs, "outboundTopicFilters must not be empty when outbound is enabled")
	}
	return errs
}

// ValidateStrict adds the edge-only checks (plan E5) that Core does not do:
// payload format, key regexes and outbound topic filters. Kafka client
// properties are never refused, as in Core (see IgnoredProperties).
func (c Config) ValidateStrict() []string {
	errs := c.Validate()
	switch c.PayloadFormat {
	case FormatDefault, FormatJSON, FormatBinary, FormatText:
	default:
		errs = append(errs, fmt.Sprintf("payloadFormat %q is not one of DEFAULT, JSON, BINARY, TEXT", c.PayloadFormat))
	}
	errs = append(errs, checkRewrite("topicKey", c.TopicKeyRegex, c.TopicKeyReplacement)...)
	errs = append(errs, checkRewrite("outboundTopicKey", c.OutboundTopicKeyRegex, c.OutboundTopicKeyReplacement)...)
	if c.OutboundEnabled {
		for _, f := range c.OutboundTopicFilters {
			if err := validateFilter(f); err != nil {
				errs = append(errs, fmt.Sprintf("outboundTopicFilters %q: %v", f, err))
			}
		}
	}
	return errs
}

// validateFilter checks MQTT topic filter syntax. Filters that need broker
// state (WinCC OA native namespaces) are checked by the bus at subscribe time.
func validateFilter(f string) error {
	if f == "" {
		return errors.New("empty filter")
	}
	levels := strings.Split(f, "/")
	for i, l := range levels {
		switch {
		case l == "#":
			if i != len(levels)-1 {
				return errors.New("'#' must be the last level")
			}
		case l == "+":
		case strings.ContainsAny(l, "+#"):
			return errors.New("wildcards must occupy a whole level")
		}
	}
	return nil
}

func nonBlank(s *string) *string {
	if s == nil || strings.TrimSpace(*s) == "" {
		return nil
	}
	return s
}

// checkRewrite validates a key regex and, when both are set (the only case
// Core applies them), its replacement.
func checkRewrite(prefix string, pattern, replacement *string) []string {
	if pattern == nil {
		return nil
	}
	re, err := regexp.Compile(*pattern)
	if err != nil {
		return []string{fmt.Sprintf("%sRegex: %v", prefix, err)}
	}
	if replacement == nil {
		return nil
	}
	if _, err := parseReplacement(re, *replacement); err != nil {
		return []string{fmt.Sprintf("%sReplacement: %v", prefix, err)}
	}
	return nil
}

// rewrite returns the compiled key rewrite, or nil when Core would not apply
// one (regex or replacement missing).
func rewrite(pattern, replacement *string) (*keyRewrite, error) {
	if pattern == nil || replacement == nil {
		return nil, nil
	}
	return newKeyRewrite(*pattern, *replacement)
}
