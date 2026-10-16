package mqttclient

import (
	"reflect"
	"strings"
	"testing"
)

func TestValidateConfigAcceptsExistingForms(t *testing.T) {
	for _, url := range []string{
		"tcp://site:1883", "mqtt://site:1883", "ssl://site:8883", "tls://site:8883",
		"mqtts://site:8883", "ws://site:80/mqtt", "wss://site:443/mqtt", "unix:///run/mqtt.sock",
	} {
		if errs := ValidateConfig(Config{BrokerURL: url, ClientID: "c"}); len(errs) != 0 {
			t.Errorf("%s: %v", url, errs)
		}
	}
	ok := Config{
		BrokerURL: "ssl://site:8883", ClientID: "c", TlsCaCertPath: "/ca.pem",
		TlsClientCertPath: "/c.pem", TlsClientKeyPath: "/c.key", TlsClientKeyFormat: "pem",
		TlsAlpnProtocols: []string{"x-amzn-mqtt-ca"}, TlsServerName: "site",
	}
	if errs := ValidateConfig(ok); len(errs) != 0 {
		t.Errorf("full TLS config: %v", errs)
	}
	p12 := Config{BrokerURL: "wss://site/mqtt", ClientID: "c", TlsClientCertPath: "/c.p12", TlsClientKeyFormat: "PKCS12", TlsClientKeyPassword: "x"}
	if errs := ValidateConfig(p12); len(errs) != 0 {
		t.Errorf("PKCS12 config: %v", errs)
	}
}

func TestValidateConfigRules(t *testing.T) {
	base := func(mod func(*Config)) Config {
		c := Config{BrokerURL: "ssl://site:8883", ClientID: "c"}
		mod(&c)
		return c
	}
	for name, c := range map[string]struct {
		cfg  Config
		want string
	}{
		"blank url":       {base(func(c *Config) { c.BrokerURL = "" }), "brokerUrl cannot be blank"},
		"no protocol":     {base(func(c *Config) { c.BrokerURL = "//site:1883" }), "brokerUrl must include a protocol (tcp://, ssl://, ws://, or wss://)"},
		"bad protocol":    {base(func(c *Config) { c.BrokerURL = "http://site" }), "protocol must be one of: tcp, mqtt, ssl, tls, mqtts, ws, wss, unix"},
		"no host":         {base(func(c *Config) { c.BrokerURL = "tcp://:1883" }), "brokerUrl must include a hostname"},
		"blank client id": {base(func(c *Config) { c.ClientID = " " }), "clientId cannot be blank"},
		"tls on tcp":      {base(func(c *Config) { c.BrokerURL, c.TlsCaCertPath = "tcp://site:1883", "/ca.pem" }), "TLS options (tls*) require an ssl:// or wss:// brokerUrl"},
		"alpn on ws":      {base(func(c *Config) { c.BrokerURL, c.TlsAlpnProtocols = "ws://site", []string{"a"} }), "TLS options (tls*) require an ssl:// or wss:// brokerUrl"},
		"cert no key":     {base(func(c *Config) { c.TlsClientCertPath = "/c.pem" }), "tlsClientKeyPath is required when tlsClientCertPath is set"},
		"key no cert":     {base(func(c *Config) { c.TlsClientKeyPath = "/c.key" }), "tlsClientCertPath is required when tlsClientKeyPath is set"},
		"p12 no bundle":   {base(func(c *Config) { c.TlsClientKeyFormat = "PKCS12" }), "tlsClientCertPath (PKCS12 bundle) is required when tlsClientKeyFormat is PKCS12"},
		"p12 with key":    {base(func(c *Config) { c.TlsClientKeyFormat, c.TlsClientCertPath, c.TlsClientKeyPath = "PKCS12", "/c.p12", "/c.key" }), "tlsClientKeyPath is not used with tlsClientKeyFormat PKCS12, the key is part of the bundle"},
		"bad format":      {base(func(c *Config) { c.TlsClientKeyFormat = "DER" }), "tlsClientKeyFormat must be 'PEM' or 'PKCS12'"},
		"blank alpn":      {base(func(c *Config) { c.TlsAlpnProtocols = []string{" "} }), "invalid tlsAlpnProtocols entry ' ': must be 1-255 printable ASCII characters"},
		"long alpn":       {base(func(c *Config) { c.TlsAlpnProtocols = []string{strings.Repeat("a", 256)} }), "invalid tlsAlpnProtocols entry '" + strings.Repeat("a", 256) + "': must be 1-255 printable ASCII characters"},
		"non-ascii alpn":  {base(func(c *Config) { c.TlsAlpnProtocols = []string{"mqtté"} }), "invalid tlsAlpnProtocols entry 'mqtté': must be 1-255 printable ASCII characters"},
	} {
		if errs := ValidateConfig(c.cfg); !reflect.DeepEqual(errs, []string{c.want}) {
			t.Errorf("%s: errors = %q, want [%q]", name, errs, c.want)
		}
	}
}

// Connector start checks only the tls* rules: stored configs that ran before
// must keep starting.
func TestTLSErrorsIgnoreURLAndClientID(t *testing.T) {
	if errs := (Config{BrokerURL: "site:1883"}).tlsErrors(); len(errs) != 0 {
		t.Errorf("legacy config without TLS options: %v", errs)
	}
	if errs := (Config{BrokerURL: "tcp://site", TlsServerName: "x"}).tlsErrors(); len(errs) != 1 {
		t.Errorf("TLS option on tcp:// = %v, want one error", errs)
	}
}

// Blank paths count as not set, as in Core.
func TestBlankTLSPathsAreNotSet(t *testing.T) {
	if errs := (Config{BrokerURL: "tcp://h:1", TlsCaCertPath: " ", TlsServerName: "  "}).tlsErrors(); len(errs) != 0 {
		t.Errorf("blank tls* fields on tcp:// = %v, want none", errs)
	}
}
