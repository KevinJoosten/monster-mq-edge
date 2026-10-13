package resolvers

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"monstermq.io/edge/internal/graphql/generated"
)

func sp(s string) *string { return &s }

func storedTLS() string {
	b, _ := json.Marshal(map[string]any{
		"brokerUrl": "ssl://remote:8883", "tlsCaCertPath": "/ca.pem", "tlsClientCertPath": "/c.pem",
		"tlsClientKeyPath": "/c.key", "tlsClientKeyPassword": "old", "tlsClientKeyFormat": "PEM",
		"tlsAlpnProtocols": []string{"mqtt"}, "tlsServerName": "remote",
	})
	return string(b)
}

func TestMqttClientCreateStoresTLSFields(t *testing.T) {
	m := mqttClientConfigInputToMap(&generated.MqttClientConnectionConfigInput{
		BrokerURL: "ssl://remote:8883", TLSCaCertPath: sp("/ca.pem"), TLSClientCertPath: sp("/c.p12"),
		TLSClientKeyPassword: sp("pw"), TLSClientKeyFormat: sp("PKCS12"),
		TLSAlpnProtocols: []string{"x-amzn-mqtt-ca"}, TLSServerName: sp("remote"),
	})
	want := map[string]any{
		"tlsCaCertPath": "/ca.pem", "tlsClientCertPath": "/c.p12", "tlsClientKeyPassword": "pw",
		"tlsClientKeyFormat": "PKCS12", "tlsAlpnProtocols": []string{"x-amzn-mqtt-ca"}, "tlsServerName": "remote",
	}
	for k, v := range want {
		if !reflect.DeepEqual(m[k], v) {
			t.Errorf("%s = %#v, want %#v", k, m[k], v)
		}
	}
	if _, ok := m["tlsClientKeyPath"]; ok {
		t.Error("unset tlsClientKeyPath stored")
	}
}

func TestMqttClientOutputTLSFieldsWithoutPassword(t *testing.T) {
	var m map[string]any
	_ = json.Unmarshal([]byte(storedTLS()), &m)
	c := mapToConnectionConfig(m)
	if c.TLSCaCertPath == nil || *c.TLSCaCertPath != "/ca.pem" || c.TLSClientKeyPath == nil || *c.TLSClientKeyPath != "/c.key" ||
		c.TLSServerName == nil || *c.TLSServerName != "remote" || !reflect.DeepEqual(c.TLSAlpnProtocols, []string{"mqtt"}) {
		t.Errorf("TLS outputs = %+v", c)
	}
	if !c.TLSClientKeyPasswordSet || c.TLSClientKeyFormat != "PEM" {
		t.Errorf("passwordSet %v format %q, want true and PEM", c.TLSClientKeyPasswordSet, c.TLSClientKeyFormat)
	}
	empty := mapToConnectionConfig(map[string]any{"brokerUrl": "tcp://x"})
	if empty.TLSClientKeyPasswordSet || empty.TLSClientKeyFormat != "PEM" || empty.TLSCaCertPath != nil {
		t.Errorf("no TLS stored: %+v", empty)
	}
}

func TestMqttClientUpdateMergesTLSFields(t *testing.T) {
	pem := sp("PEM")
	for name, c := range map[string]struct {
		in   generated.MqttClientConnectionConfigInput
		want map[string]any // nil value: the key must be absent
	}{
		"null keeps everything": {
			generated.MqttClientConnectionConfigInput{BrokerURL: "ssl://remote:8883", TLSClientKeyFormat: pem},
			map[string]any{"tlsCaCertPath": "/ca.pem", "tlsClientCertPath": "/c.pem", "tlsClientKeyPath": "/c.key",
				"tlsClientKeyPassword": "old", "tlsAlpnProtocols": []any{"mqtt"}, "tlsServerName": "remote"},
		},
		"empty clears": {
			generated.MqttClientConnectionConfigInput{BrokerURL: "ssl://remote:8883", TLSClientKeyFormat: pem,
				TLSCaCertPath: sp(""), TLSServerName: sp(""), TLSAlpnProtocols: []string{}, TLSClientKeyPassword: sp("")},
			map[string]any{"tlsCaCertPath": nil, "tlsServerName": nil, "tlsAlpnProtocols": nil, "tlsClientKeyPassword": nil,
				"tlsClientCertPath": "/c.pem"},
		},
		"new values replace": {
			generated.MqttClientConnectionConfigInput{BrokerURL: "ssl://remote:8883", TLSClientKeyFormat: pem,
				TLSClientKeyPassword: sp("new"), TLSAlpnProtocols: []string{"a", "b"}},
			map[string]any{"tlsClientKeyPassword": "new", "tlsAlpnProtocols": []string{"a", "b"}},
		},
		"password dropped without certificate": {
			generated.MqttClientConnectionConfigInput{BrokerURL: "ssl://remote:8883", TLSClientKeyFormat: pem,
				TLSClientCertPath: sp(""), TLSClientKeyPath: sp("")},
			map[string]any{"tlsClientCertPath": nil, "tlsClientKeyPassword": nil},
		},
	} {
		got := mqttClientConfigInputToMergedMap(&c.in, storedTLS())
		for k, v := range c.want {
			if gv, ok := got[k]; v == nil && ok {
				t.Errorf("%s: %s = %#v, want it removed", name, k, gv)
			} else if v != nil && !reflect.DeepEqual(gv, v) {
				t.Errorf("%s: %s = %#v, want %#v", name, k, gv, v)
			}
		}
	}
}

// An older dashboard sends the default PEM and no paths: a stored PKCS12
// format stays.
func TestMqttClientUpdateKeepsPKCS12Format(t *testing.T) {
	b, _ := json.Marshal(map[string]any{"brokerUrl": "ssl://r:8883", "tlsClientCertPath": "/c.p12", "tlsClientKeyFormat": "PKCS12"})
	got := mqttClientConfigInputToMergedMap(&generated.MqttClientConnectionConfigInput{BrokerURL: "ssl://r:8883", TLSClientKeyFormat: sp("PEM")}, string(b))
	if got["tlsClientKeyFormat"] != "PKCS12" {
		t.Errorf("format = %v, want PKCS12 kept", got["tlsClientKeyFormat"])
	}
}

func TestMqttClientCreateAndUpdateValidate(t *testing.T) {
	r := &mqttClientMutationsResolver{&Resolver{}}
	in := generated.MqttClientInput{Name: "b", Namespace: "n", NodeID: "x", Config: &generated.MqttClientConnectionConfigInput{
		BrokerURL: "tcp://remote:1883", ClientID: sp("c"), TLSServerName: sp("remote"), TLSClientKeyFormat: sp("PEM"),
	}}
	res, err := r.Create(context.Background(), nil, in)
	if err != nil || res.Success || !reflect.DeepEqual(res.Errors, []string{"TLS options (tls*) require an ssl:// or wss:// brokerUrl"}) {
		t.Errorf("create = %+v, %v", res, err)
	}
}

func TestMqttClientBlankTLSValuesAreNotStored(t *testing.T) {
	m := mqttClientConfigInputToMap(&generated.MqttClientConnectionConfigInput{BrokerURL: "ssl://r:8883", TLSCaCertPath: sp("  "), TLSServerName: sp(" ")})
	if _, ok := m["tlsCaCertPath"]; ok {
		t.Error("blank tlsCaCertPath stored")
	}
	if _, ok := m["tlsServerName"]; ok {
		t.Error("blank tlsServerName stored")
	}
}
