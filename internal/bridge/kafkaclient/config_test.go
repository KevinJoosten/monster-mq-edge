package kafkaclient

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := ParseConfig(`{}`)
	if err != nil {
		t.Fatal(err)
	}
	want := DefaultConfig()
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("defaults = %+v, want %+v", cfg, want)
	}
	if cfg.BootstrapServers != "localhost:9092" || cfg.GroupID != "monstermq-subscriber" || cfg.PayloadFormat != "DEFAULT" ||
		cfg.ReconnectDelayMs != 5000 || !cfg.InboundEnabled || cfg.OutboundEnabled {
		t.Fatalf("Core defaults not applied: %+v", cfg)
	}
}

func TestParseConfigUnwrapsKafkaKey(t *testing.T) {
	cfg, err := ParseConfig(`{"kafka":{"bootstrapServers":"k1:9092,k2:9092","payloadFormat":"JSON"}}`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BootstrapServers != "k1:9092,k2:9092" || cfg.PayloadFormat != "JSON" {
		t.Fatalf("nested config not unwrapped: %+v", cfg)
	}
}

func TestPrefixNormalisation(t *testing.T) {
	for raw, want := range map[string]string{
		`{"destinationTopicPrefix":"plant"}`:      "plant/",
		`{"destinationTopicPrefix":" plant/a/ "}`: "plant/a/",
		`{"keyTopicPrefix":"legacy"}`:             "legacy/",
		`{"destinationTopicPrefix":"   "}`:        "",
		`{"destinationTopicPrefix":null}`:         "",
	} {
		cfg, err := ParseConfig(raw)
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		got := ""
		if cfg.DestinationTopicPrefix != nil {
			got = *cfg.DestinationTopicPrefix
		}
		if got != want {
			t.Errorf("%s: prefix = %q, want %q", raw, got, want)
		}
	}
	if _, err := ParseConfig(`{"destinationTopicPrefix":"a/+/b"}`); err == nil {
		t.Error("wildcard prefix accepted")
	}
}

// Core accepts PascalCase only for some keys, with PascalCase winning for the
// inbound key mapping and camelCase winning elsewhere.
func TestPascalCaseAliasesMatchCore(t *testing.T) {
	cfg, err := ParseConfig(`{
		"TopicKeyRegex":"P","topicKeyRegex":"c",
		"outboundTopicKeyRegex":"c","OutboundTopicKeyRegex":"P",
		"InboundEnabled":false,
		"OutboundEnabled":true,"OutboundTopicFilters":["a/#"],
		"OutboundKafkaTopic":"out",
		"ExtraProducerConfig":{"acks":1},
		"BootstrapServers":"ignored:1"
	}`)
	if err != nil {
		t.Fatal(err)
	}
	if *cfg.TopicKeyRegex != "P" {
		t.Errorf("TopicKeyRegex = %q, want PascalCase to win", *cfg.TopicKeyRegex)
	}
	if *cfg.OutboundTopicKeyRegex != "c" {
		t.Errorf("OutboundTopicKeyRegex = %q, want camelCase to win", *cfg.OutboundTopicKeyRegex)
	}
	if cfg.InboundEnabled || !cfg.OutboundEnabled || len(cfg.OutboundTopicFilters) != 1 || *cfg.OutboundKafkaTopic != "out" {
		t.Errorf("PascalCase aliases not read: %+v", cfg)
	}
	if cfg.ExtraProducerConfig["acks"] != "1" {
		t.Errorf("ExtraProducerConfig = %v, want acks=1 as a string", cfg.ExtraProducerConfig)
	}
	if cfg.BootstrapServers != DefaultBootstrapServers {
		t.Errorf("BootstrapServers alias accepted (%q); Core reads camelCase only", cfg.BootstrapServers)
	}
}

func TestValidate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReconnectDelayMs = 100
	cfg.OutboundEnabled = true
	errs := cfg.Validate()
	if len(errs) != 2 {
		t.Fatalf("errs = %v, want reconnect + filters", errs)
	}

	cfg = DefaultConfig()
	bad := "("
	cfg.TopicKeyRegex = &bad
	cfg.PayloadFormat = "AVRO"
	cfg.OutboundEnabled = true
	cfg.OutboundTopicFilters = []string{"a/#/b"}
	errs = cfg.ValidateStrict()
	joined := strings.Join(errs, "|")
	for _, want := range []string{"payloadFormat", "topicKeyRegex", "outboundTopicFilters"} {
		if !strings.Contains(joined, want) {
			t.Errorf("strict errors %v miss %s", errs, want)
		}
	}
}

// Configs written by edge read back unchanged, and use Core's JSON keys.
func TestConfigRoundTrip(t *testing.T) {
	cfg := DefaultConfig()
	p := "pre/"
	cfg.DestinationTopicPrefix = &p
	cfg.OutboundEnabled = true
	cfg.OutboundTopicFilters = []string{"plant/#"}
	cfg.ExtraConsumerConfig = map[string]string{"auto.offset.reset": "earliest"}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"bootstrapServers"`, `"destinationTopicPrefix"`, `"outboundTopicFilters"`, `"extraConsumerConfig"`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("marshalled config %s misses %s", raw, key)
		}
	}
	back, err := ParseConfig(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, cfg) {
		t.Fatalf("round trip = %+v, want %+v", back, cfg)
	}
}

// The replacement syntax and its errors follow Core's Regex.replace.
func TestKeyRewriteMatchesCore(t *testing.T) {
	for _, c := range []struct{ pattern, repl, in, want string }{
		{`\.`, "/", "a.b.c", "a/b/c"},
		{`(\w+)\.(\w+)`, "$2/$1", "line.temp", "temp/line"},
		{`(a)`, "$12", "a", "a2"}, // group 1, then a literal 2
		{`(?P<x>a)`, "${x}!", "a", "a!"},
		{`a`, `\$1`, "a", "$1"},
		{`(a)|(b)`, "[$2]", "a", "[]"}, // unmatched group adds nothing
	} {
		k, err := newKeyRewrite(c.pattern, c.repl)
		if err != nil {
			t.Fatalf("%s / %s: %v", c.pattern, c.repl, err)
		}
		if got := k.apply(c.in); got != c.want {
			t.Errorf("%s / %s on %q = %q, want %q", c.pattern, c.repl, c.in, got, c.want)
		}
	}
	for _, c := range []struct{ pattern, repl string }{
		{`(a)`, "$2"},      // no group 2
		{`a`, "cost$"},     // trailing $
		{`a`, "$x"},        // $ without a group
		{`a`, `x\`},        // trailing escape
		{`(a)`, "${1}"},    // names cannot start with a digit
		{`(a)`, "${nope}"}, // unknown name
		{`(a)`, "${open"},  // missing }
	} {
		if _, err := newKeyRewrite(c.pattern, c.repl); err == nil {
			t.Errorf("%s / %q accepted", c.pattern, c.repl)
		}
	}
}

// A value of the wrong type fails the config, as Core's fromJson fails.
func TestConfigTypeRulesMatchCore(t *testing.T) {
	for _, bad := range []string{
		`{"inboundEnabled":"false"}`,
		`{"reconnectDelayMs":"5000"}`,
		`{"outboundTopicFilters":"a/#"}`,
		`{"extraConsumerConfig":[1]}`,
		`{"kafka":"x"}`,
		`{"bootstrapServers":null}`,
		`{"extraProducerConfig":{"acks":null}}`,
	} {
		if _, err := ParseConfig(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	cfg, err := ParseConfig(`{"bootstrapServers":123,"topicKeyReplacement":1,"payloadFormat":null,
		"extraProducerConfig":{"linger.ms":5,"ratio":1e3,"half":0.5,"nested":{"a":1},"flag":true},
		"outboundTopicFilters":["a/#",7]}`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BootstrapServers != "123" || *cfg.TopicKeyReplacement != "1" || cfg.PayloadFormat != FormatDefault {
		t.Errorf("string conversion: %+v", cfg)
	}
	want := map[string]string{"linger.ms": "5", "ratio": "1000.0", "half": "0.5", "nested": `{"a":1}`, "flag": "true"}
	if !reflect.DeepEqual(cfg.ExtraProducerConfig, want) {
		t.Errorf("extraProducerConfig = %v, want %v", cfg.ExtraProducerConfig, want)
	}
	if !reflect.DeepEqual(cfg.OutboundTopicFilters, []string{"a/#", "7"}) {
		t.Errorf("filters = %v", cfg.OutboundTopicFilters)
	}
}

func TestDoubleString(t *testing.T) {
	for f, want := range map[float64]string{
		1: "1.0", 1000: "1000.0", 0.5: "0.5", 0.001: "0.001", 1e7: "1.0E7", 1.25e-4: "1.25E-4", -3.5: "-3.5", 12345678.9: "1.23456789E7",
	} {
		if got := DoubleString(f); got != want {
			t.Errorf("DoubleString(%v) = %s, want %s", f, got, want)
		}
	}
}
