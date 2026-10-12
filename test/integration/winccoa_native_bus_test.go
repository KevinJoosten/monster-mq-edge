package integration

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/stores"
)

func busValue(t *testing.T, ch <-chan stores.BrokerMessage, topic string, timeout time.Duration) (stores.BrokerMessage, bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return m, false
			}
			if m.TopicName == topic {
				return m, true
			}
		case <-deadline:
			return stores.BrokerMessage{}, false
		}
	}
}

func busPayloadValue(t *testing.T, m stores.BrokerMessage) any {
	t.Helper()
	var v struct {
		Value any `json:"value"`
	}
	if err := json.Unmarshal(m.Payload, &v); err != nil {
		t.Fatalf("payload %s: %v", m.Payload, err)
	}
	return v.Value
}

// Bus subscribers (GraphQL topicUpdates, SSE, scripts) register native
// interest like an MQTT SUBSCRIBE, without any MQTT client.
func TestNativeBusSubscriber(t *testing.T) {
	sim, client := newSim(0)
	defer sim.Close()
	env := startNative(t, 27199, filepath.Join(t.TempDir(), "n.db"), sim, client, nil, broker.Options{})
	defer env.srv.Close()
	bus := env.srv.Bus()

	if err := sim.Set("System1:Pump101.speed", oahost.Value{Kind: oahost.KindFloat, Float: 42.5}); err != nil {
		t.Fatal(err)
	}
	tags := "winccoa/systems/System1/tags/Pump101/speed"
	if err := bus.Validate([]string{tags}); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if err := bus.Validate([]string{"winccoa/systems/System1/tags/Missing/x"}); err == nil {
		t.Fatal("unknown datapoint must be rejected")
	}

	id, ch := bus.Subscribe([]string{tags}, 16)
	m, ok := busValue(t, ch, tags, 3*time.Second)
	if !ok {
		t.Fatal("no initial value for a bus subscriber")
	}
	if !m.IsRetain || busPayloadValue(t, m) != 42.5 {
		t.Fatalf("initial value retain=%v payload=%s", m.IsRetain, m.Payload)
	}
	if n := sim.Connections(); n != 1 {
		t.Fatalf("OA connections = %d, want 1", n)
	}

	if err := sim.Set("System1:Pump101.speed", oahost.Value{Kind: oahost.KindFloat, Float: 7}); err != nil {
		t.Fatal(err)
	}
	if m, ok := busValue(t, ch, tags, 3*time.Second); !ok || m.IsRetain || busPayloadValue(t, m) != float64(7) {
		t.Fatalf("live value: ok=%v retain=%v %s", ok, m.IsRetain, m.Payload)
	}

	// Reconcile must keep the interest of a live bus subscriber.
	time.Sleep(500 * time.Millisecond)
	if st := env.srv.Native().Stats(); st.Interests != 1 || st.DPEs != 1 {
		t.Fatalf("stats after reconcile %+v", st)
	}

	bus.Unsubscribe(id)
	time.Sleep(300 * time.Millisecond)
	if n := sim.Connections(); n != 0 {
		t.Fatalf("unsubscribe must disconnect; connections=%d", n)
	}
}

// The GraphQL publish mutation follows the native publish rules like an
// MQTT client: commands are written to WinCC OA, value topics are refused.
func TestNativeGraphQLPublish(t *testing.T) {
	sim, client := newSim(0)
	defer sim.Close()
	gqlPort := 28199
	env := startNative(t, 27198, filepath.Join(t.TempDir(), "n.db"), sim, client, func(c *config.Config) {
		c.GraphQL.Enabled = true
		c.GraphQL.Port = gqlPort
		c.GraphQL.TLSPort = gqlPort + 1000
	}, broker.Options{})
	defer env.srv.Close()
	url := fmt.Sprintf("http://localhost:%d/graphql", gqlPort)

	publish := func(topic, payload string) map[string]any {
		t.Helper()
		data := gqlQuery(t, url, `mutation P($t: String!, $p: String!) { publish(input: { topic: $t, payload: $p, qos: 1 }) { success error } }`,
			map[string]any{"t": topic, "p": payload})
		return data["publish"].(map[string]any)
	}

	base := "winccoa/systems/System1/tags/Pump1/speed"
	if res := publish(base+"/set", `{"value":99.5}`); res["success"] != true {
		t.Fatalf("command: %v", res)
	}
	time.Sleep(200 * time.Millisecond)
	if v, err := sim.Get("System1:Pump1.speed:_original.._value"); err != nil || v.Float != 99.5 {
		t.Fatalf("command not written to OA: %+v err %v", v, err)
	}

	if res := publish(base+"/set", `{"value":"x"}`); res["success"] != false {
		t.Fatalf("bad command payload accepted: %v", res)
	}
	if res := publish(base, `{"value":1}`); res["success"] != false {
		t.Fatalf("publish to broker-owned value topic accepted: %v", res)
	}
	if res := publish("plain/topic", `1`); res["success"] != true {
		t.Fatalf("plain publish: %v", res)
	}
}
