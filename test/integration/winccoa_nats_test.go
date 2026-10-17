package integration

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/oahost"
)

// W1: SUB winccoa.tags.Pump101.speed gets the current value, then a live value after sim.Set.
// sim.Connections() is 1 while subscribed and 0 after UNSUB.
func TestWinCCOaNatsW1(t *testing.T) {
	sim, client := newSim(128)
	defer sim.Close()

	natsPort := getFreePort(t)
	mqttPort := getFreePort(t)

	env := startNative(t, mqttPort, filepath.Join(t.TempDir(), "w1.db"), sim, client, func(c *config.Config) {
		c.NATS = natsPort
	}, broker.Options{})
	defer env.srv.Close()

	if err := sim.Set("System1:Pump101.speed", oahost.Value{Kind: oahost.KindFloat, Float: 42.5}); err != nil {
		t.Fatal(err)
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	msgChan := make(chan *nats.Msg, 5)
	sub, err := nc.Subscribe("winccoa.tags.Pump101.speed", func(m *nats.Msg) {
		msgChan <- m
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1. Initial value
	select {
	case m := <-msgChan:
		var data map[string]any
		if err := json.Unmarshal(m.Data, &data); err != nil {
			t.Fatalf("unmarshal initial value: %v", err)
		}
		if data["value"].(float64) != 42.5 {
			t.Fatalf("expected initial value 42.5, got %v", data["value"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for initial native value")
	}

	time.Sleep(100 * time.Millisecond)
	if n := sim.Connections(); n != 1 {
		t.Fatalf("expected 1 OA connection while subscribed, got %d", n)
	}

	// 2. Live value update
	if err := sim.Set("System1:Pump101.speed", oahost.Value{Kind: oahost.KindFloat, Float: 88.0}); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-msgChan:
		var data map[string]any
		if err := json.Unmarshal(m.Data, &data); err != nil {
			t.Fatalf("unmarshal live value: %v", err)
		}
		if data["value"].(float64) != 88.0 {
			t.Fatalf("expected live value 88.0, got %v", data["value"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for live native value")
	}

	// 3. Unsubscribe -> sim.Connections() becomes 0
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if n := sim.Connections(); n != 0 {
		t.Fatalf("expected 0 OA connections after unsub, got %d", n)
	}
}

// W2: SUB winccoa.tags.Missing.x -> -ERR 'Invalid Subject' (E2).
// SUB winccoa.tags.Pump101.speed._online.._stime (raw) -> -ERR 'Invalid Subject', the documented consequence.
func TestWinCCOaNatsW2(t *testing.T) {
	sim, client := newSim(128)
	defer sim.Close()

	natsPort := getFreePort(t)
	mqttPort := getFreePort(t)

	env := startNative(t, mqttPort, filepath.Join(t.TempDir(), "w2.db"), sim, client, func(c *config.Config) {
		c.NATS = natsPort
	}, broker.Options{})
	defer env.srv.Close()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n') // INFO

	// Missing DP -> Invalid Subject
	_, _ = conn.Write([]byte("SUB winccoa.tags.Missing.x 1\r\n"))
	line1, err := r.ReadString('\n')
	if err != nil || line1 != "-ERR 'Invalid Subject'\r\n" {
		t.Fatalf("expected -ERR 'Invalid Subject', got line=%q err=%v", line1, err)
	}

	// Multiple consecutive dots in subject -> Invalid Subject
	_, _ = conn.Write([]byte("SUB winccoa.tags.Pump101.speed._online.._stime 2\r\n"))
	line2, err := r.ReadString('\n')
	if err != nil || line2 != "-ERR 'Invalid Subject'\r\n" {
		t.Fatalf("expected -ERR 'Invalid Subject', got line=%q err=%v", line2, err)
	}
}

// W3: PUB winccoa.tags.Pump101.speed.set with {"value":99.5} updates the DPE.
func TestWinCCOaNatsW3(t *testing.T) {
	sim, client := newSim(128)
	defer sim.Close()

	natsPort := getFreePort(t)
	mqttPort := getFreePort(t)

	env := startNative(t, mqttPort, filepath.Join(t.TempDir(), "w3.db"), sim, client, func(c *config.Config) {
		c.NATS = natsPort
	}, broker.Options{})
	defer env.srv.Close()

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	if err := nc.Publish("winccoa.tags.Pump101.speed.set", []byte(`{"value":99.5}`)); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()

	time.Sleep(200 * time.Millisecond)
	v, err := sim.Get("System1:Pump101.speed")
	if err != nil {
		t.Fatalf("sim.Get failed: %v", err)
	}
	if v.Float != 99.5 {
		t.Fatalf("expected speed 99.5, got %v", v.Float)
	}
}

// W4: nc.Request on the .set subject receives the native command reply (S1 carries the response topic).
func TestWinCCOaNatsW4(t *testing.T) {
	sim, client := newSim(128)
	defer sim.Close()

	natsPort := getFreePort(t)
	mqttPort := getFreePort(t)

	env := startNative(t, mqttPort, filepath.Join(t.TempDir(), "w4.db"), sim, client, func(c *config.Config) {
		c.NATS = natsPort
	}, broker.Options{})
	defer env.srv.Close()

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	rep, err := nc.Request("winccoa.tags.Pump101.speed.set", []byte(`{"value":99.5,"id":"req-1"}`), 2*time.Second)
	if err != nil {
		t.Fatalf("nc.Request error: %v", err)
	}
	if !strings.Contains(string(rep.Data), `"confirmed"`) {
		t.Fatalf("expected confirmed in reply, got %s", string(rep.Data))
	}
}

// W5: PUB on the value topic winccoa.tags.Pump101.speed -> -ERR 'Permissions Violation for Publish to "winccoa.tags.Pump101.speed"' (E4).
func TestWinCCOaNatsW5(t *testing.T) {
	sim, client := newSim(128)
	defer sim.Close()

	natsPort := getFreePort(t)
	mqttPort := getFreePort(t)

	env := startNative(t, mqttPort, filepath.Join(t.TempDir(), "w5.db"), sim, client, func(c *config.Config) {
		c.NATS = natsPort
	}, broker.Options{})
	defer env.srv.Close()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n') // INFO

	// PUB on value topic -> Permissions Violation (E4)
	payload := "test"
	cmd := fmt.Sprintf("PUB winccoa.tags.Pump101.speed %d\r\n%s\r\nPING\r\n", len(payload), payload)
	_, _ = conn.Write([]byte(cmd))

	errLine, err := r.ReadString('\n')
	if err != nil || errLine != "-ERR 'Permissions Violation for Publish to \"winccoa.tags.Pump101.speed\"'\r\n" {
		t.Fatalf("expected permissions violation error, got line=%q err=%v", errLine, err)
	}

	pongLine, err := r.ReadString('\n')
	if err != nil || pongLine != "PONG\r\n" {
		t.Fatalf("expected PONG after payload consumed, got line=%q err=%v", pongLine, err)
	}
}
