package mqttclient

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
)

var (
	brokerSchemes = []string{"tcp", "mqtt", "ssl", "tls", "mqtts", "ws", "wss", "unix"}
	tlsSchemes    = []string{"ssl", "tls", "mqtts", "wss"}
)

// ValidateConfig returns MonsterMQ Core's validation messages for a bridge
// config coming in through GraphQL. Only the tls* rules are repeated at start.
func ValidateConfig(c Config) []string {
	var errs []string
	if strings.TrimSpace(c.BrokerURL) == "" {
		errs = append(errs, "brokerUrl cannot be blank")
	} else if u, err := url.Parse(c.BrokerURL); err != nil {
		errs = append(errs, "invalid brokerUrl format: "+err.Error())
	} else {
		switch {
		case u.Scheme == "":
			errs = append(errs, "brokerUrl must include a protocol (tcp://, ssl://, ws://, or wss://)")
		case !slices.Contains(brokerSchemes, strings.ToLower(u.Scheme)):
			errs = append(errs, "protocol must be one of: "+strings.Join(brokerSchemes, ", "))
		}
		if u.Hostname() == "" && !strings.EqualFold(u.Scheme, "unix") {
			errs = append(errs, "brokerUrl must include a hostname")
		}
	}
	if strings.TrimSpace(c.ClientID) == "" {
		errs = append(errs, "clientId cannot be blank")
	}
	return append(errs, c.tlsErrors()...)
}

// trimTLS treats blank tls* values as not set, as Core does.
func (c Config) trimTLS() Config {
	for _, f := range []*string{&c.TlsCaCertPath, &c.TlsClientCertPath, &c.TlsClientKeyPath, &c.TlsServerName} {
		*f = strings.TrimSpace(*f)
	}
	return c
}

func (c Config) tlsErrors() []string {
	c = c.trimTLS()
	var errs []string
	if c.hasTLSOptions() && !c.isTLSURL() {
		errs = append(errs, "TLS options (tls*) require an ssl:// or wss:// brokerUrl")
	}
	switch strings.ToUpper(c.TlsClientKeyFormat) {
	case "", "PEM":
		if c.TlsClientCertPath != "" && c.TlsClientKeyPath == "" {
			errs = append(errs, "tlsClientKeyPath is required when tlsClientCertPath is set")
		}
		if c.TlsClientKeyPath != "" && c.TlsClientCertPath == "" {
			errs = append(errs, "tlsClientCertPath is required when tlsClientKeyPath is set")
		}
	case "PKCS12":
		if c.TlsClientCertPath == "" {
			errs = append(errs, "tlsClientCertPath (PKCS12 bundle) is required when tlsClientKeyFormat is PKCS12")
		}
		if c.TlsClientKeyPath != "" {
			errs = append(errs, "tlsClientKeyPath is not used with tlsClientKeyFormat PKCS12, the key is part of the bundle")
		}
	default:
		errs = append(errs, "tlsClientKeyFormat must be 'PEM' or 'PKCS12'")
	}
	for _, p := range c.TlsAlpnProtocols {
		if !validALPN(p) {
			errs = append(errs, fmt.Sprintf("invalid tlsAlpnProtocols entry '%s': must be 1-255 printable ASCII characters", p))
		}
	}
	return errs
}

// hasTLSOptions leaves out the password, as Core does.
func (c Config) hasTLSOptions() bool {
	return c.TlsCaCertPath != "" || c.TlsClientCertPath != "" || c.TlsClientKeyPath != "" ||
		len(c.TlsAlpnProtocols) > 0 || c.TlsServerName != ""
}

func (c Config) isTLSURL() bool {
	u, err := url.Parse(c.BrokerURL)
	return err == nil && slices.Contains(tlsSchemes, strings.ToLower(u.Scheme))
}

func validALPN(p string) bool {
	if strings.TrimSpace(p) == "" || len(p) > 255 {
		return false
	}
	for i := 0; i < len(p); i++ {
		if p[i] < ' ' || p[i] > '~' {
			return false
		}
	}
	return true
}
