package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	engine "monstermq.io/edge/internal/mqtt"
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/stores/sqlite"
)

// seedBridges stores bridge configs for node in a fresh SQLite file.
func seedBridges(t *testing.T, dbPath, node string, bridges map[string]map[string]any) {
	t.Helper()
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dcs := sqlite.NewDeviceConfigStore(db)
	if err := dcs.EnsureTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	for name, cfg := range bridges {
		raw, _ := json.Marshal(cfg)
		if err := dcs.Save(context.Background(), stores.DeviceConfig{
			Name: name, Namespace: "bridge", NodeID: node, Type: "MQTT_CLIENT", Enabled: true, Config: string(raw),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// startNode starts the broker for node on port from dbPath.
func startNode(t *testing.T, node string, port int, dbPath string) *broker.Server {
	t.Helper()
	cfg := config.Default()
	cfg.NodeID = node
	cfg.TCP.Port = port
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = dbPath
	cfg.Features.MqttClient = true
	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	time.Sleep(100 * time.Millisecond)
	return srv
}

// chainBridge links a broker to the next broker in the chain with outbound
// and inbound mappings that overlap on purpose: the worst case for loops.
func chainBridge(remotePort int, clientID string) map[string]any {
	return map[string]any{
		"brokerUrl":      fmt.Sprintf("tcp://localhost:%d", remotePort),
		"clientId":       clientID,
		"cleanSession":   true,
		"keepAlive":      10,
		"reconnectDelay": 1,
		"addresses": []map[string]any{
			{"mode": "PUBLISH", "localTopic": "data/#", "remoteTopic": "data/#", "qos": -1, "removePath": true},
			{"mode": "SUBSCRIBE", "remoteTopic": "data/#", "localTopic": "data/#", "qos": 1, "removePath": true},
		},
	}
}

// observer records every message a broker delivers on a filter, through an
// inline subscription.
type observer struct {
	mu   sync.Mutex
	msgs []packets.Packet
}

func observe(t *testing.T, srv *broker.Server, filter string) *observer {
	t.Helper()
	o := &observer{}
	err := srv.MQTT().Subscribe(filter, 1, func(_ *engine.Client, _ packets.Subscription, pk packets.Packet) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.msgs = append(o.msgs, pk)
	})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func (o *observer) on(topic string) []packets.Packet {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []packets.Packet
	for _, pk := range o.msgs {
		if pk.TopicName == topic {
			out = append(out, pk)
		}
	}
	return out
}

// waitFor waits until topic has n messages, then settle longer, so a late
// duplicate is caught, and returns everything that arrived.
func (o *observer) waitFor(t *testing.T, topic string, n int, settle time.Duration) []packets.Packet {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for len(o.on(topic)) < n {
		if time.Now().After(deadline) {
			t.Fatalf("%s: got %d messages, want %d", topic, len(o.on(topic)), n)
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(settle)
	return o.on(topic)
}

func publishAt(t *testing.T, srv *broker.Server, clientID string, msg stores.BrokerMessage) {
	t.Helper()
	if err := broker.NewInlinePublisher(srv.MQTT()).Publish(clientID, msg); err != nil {
		t.Fatal(err)
	}
}

// waitBridged publishes probes at from until dst has seen one: every bridge
// on the path is connected and subscribed.
func waitBridged(t *testing.T, from *broker.Server, dst *observer) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for len(dst.on("data/_probe")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("bridges never came up")
		}
		publishAt(t, from, "probe", stores.BrokerMessage{TopicName: "data/_probe", Payload: []byte("p")})
		time.Sleep(250 * time.Millisecond)
	}
}

// startChain starts brokers A, B and C on base, base+1 and base+2, chained
// A <- B <- C: C bridges to B and B bridges to A.
func startChain(t *testing.T, base int) (a, b, c *broker.Server) {
	t.Helper()
	dir := t.TempDir()
	a = startNode(t, "a", base, filepath.Join(dir, "a.db"))
	t.Cleanup(func() { _ = a.Close() })

	bDB := filepath.Join(dir, "b.db")
	seedBridges(t, bDB, "b", map[string]map[string]any{"to-a": chainBridge(base, "b-bridge")})
	b = startNode(t, "b", base+1, bDB)
	t.Cleanup(func() { _ = b.Close() })

	cDB := filepath.Join(dir, "c.db")
	seedBridges(t, cDB, "c", map[string]map[string]any{"to-b": chainBridge(base+1, "c-bridge")})
	c = startNode(t, "c", base+2, cDB)
	t.Cleanup(func() { _ = c.Close() })
	return a, b, c
}

func TestBridgeChainKeepsRetainAndPropertiesExactlyOnce(t *testing.T) {
	a, _, c := startChain(t, 27801)
	obsA := observe(t, a, "data/#")
	waitBridged(t, c, obsA)

	expiry := uint32(300)
	format := byte(1)
	publishAt(t, c, "pub-1", stores.BrokerMessage{
		TopicName: "data/c/temp", Payload: []byte("21.5"), QoS: 1, IsRetain: true,
		ContentType: "text/plain", MessageExpiryInterval: &expiry, PayloadFormatIndicator: &format,
		UserProperties: []stores.UserProperty{{Key: "unit", Value: "C"}, {Key: "unit", Value: "celsius"}},
	})

	got := obsA.waitFor(t, "data/c/temp", 1, time.Second)
	if len(got) != 1 {
		t.Fatalf("A got %d copies, want exactly 1", len(got))
	}
	p := got[0].Properties
	if p.ContentType != "text/plain" || p.PayloadFormat != 1 || !p.PayloadFormatFlag {
		t.Errorf("properties lost over the chain: %+v", p)
	}
	if p.MessageExpiryInterval == 0 || p.MessageExpiryInterval > 300 {
		t.Errorf("message expiry = %d, want 1..300", p.MessageExpiryInterval)
	}
	wantUser := []packets.UserProperty{{Key: "unit", Val: "C"}, {Key: "unit", Val: "celsius"}}
	if !reflect.DeepEqual(p.User, wantUser) {
		t.Errorf("user properties = %+v, want %+v", p.User, wantUser)
	}
	// retained state lives in the storage hook, not the engine topic index
	retained := 0
	_ = a.Storage().Retained.FindMatchingMessages(context.Background(), "data/c/temp", func(stores.BrokerMessage) bool { retained++; return true })
	if retained != 1 {
		t.Errorf("A holds %d retained messages for the topic, want 1", retained)
	}
}

func TestBridgeChainInboundArrivesOnceWithoutEcho(t *testing.T) {
	a, b, c := startChain(t, 27811)
	obsA := observe(t, a, "data/#")
	obsB := observe(t, b, "data/#")
	obsC := observe(t, c, "data/#")
	waitBridged(t, a, obsC)

	publishAt(t, a, "pub-a", stores.BrokerMessage{TopicName: "data/cmd/start", Payload: []byte("1"), QoS: 1})

	if got := obsC.waitFor(t, "data/cmd/start", 1, time.Second); len(got) != 1 {
		t.Errorf("C got %d copies, want exactly 1", len(got))
	}
	if got := obsB.on("data/cmd/start"); len(got) != 1 {
		t.Errorf("B got %d copies, want exactly 1", len(got))
	}
	if got := obsA.on("data/cmd/start"); len(got) != 1 {
		t.Errorf("A saw %d copies, want 1 (anything more is an echo)", len(got))
	}
}

// Two brokers C1 and C2 bridged into the same broker B.
func TestBridgeTwoBrokersIntoOneDoNotBounce(t *testing.T) {
	dir := t.TempDir()
	b := startNode(t, "b", 27821, filepath.Join(dir, "b.db"))
	t.Cleanup(func() { _ = b.Close() })
	cs := make([]*broker.Server, 2)
	for i := range cs {
		db := filepath.Join(dir, fmt.Sprintf("c%d.db", i+1))
		seedBridges(t, db, fmt.Sprintf("c%d", i+1), map[string]map[string]any{
			"to-b": chainBridge(27821, fmt.Sprintf("c%d-bridge", i+1)),
		})
		cs[i] = startNode(t, fmt.Sprintf("c%d", i+1), 27822+i, db)
		t.Cleanup(func() { _ = cs[i].Close() })
	}
	obsB := observe(t, b, "data/#")
	obsC1 := observe(t, cs[0], "data/#")
	obsC2 := observe(t, cs[1], "data/#")
	waitBridged(t, cs[0], obsC2)

	publishAt(t, cs[0], "pub-1", stores.BrokerMessage{TopicName: "data/c1/state", Payload: []byte("run"), QoS: 1})

	if got := obsC2.waitFor(t, "data/c1/state", 1, time.Second); len(got) != 1 {
		t.Errorf("C2 got %d copies, want exactly 1", len(got))
	}
	if got := obsB.on("data/c1/state"); len(got) != 1 {
		t.Errorf("B got %d copies, want exactly 1", len(got))
	}
	if got := obsC1.on("data/c1/state"); len(got) != 1 {
		t.Errorf("C1 saw %d copies, want 1 (anything more bounced back)", len(got))
	}
}

func TestBridgeChainOutageIsBuffered(t *testing.T) {
	dir := t.TempDir()
	a := startNode(t, "a", 27826, filepath.Join(dir, "a.db"))
	t.Cleanup(func() { _ = a.Close() })

	bDB := filepath.Join(dir, "b.db")
	seedBridges(t, bDB, "b", map[string]map[string]any{"to-a": chainBridge(27826, "b-bridge")})
	b := startNode(t, "b", 27827, bDB)

	cBridge := chainBridge(27827, "c-bridge")
	cBridge["bufferEnabled"] = true
	cBridge["bufferImplementation"] = "MONSTER"
	cDB := filepath.Join(dir, "c.db")
	seedBridges(t, cDB, "c", map[string]map[string]any{"to-b": cBridge})
	c := startNode(t, "c", 27828, cDB)
	t.Cleanup(func() { _ = c.Close() })

	obsA := observe(t, a, "data/#")
	waitBridged(t, c, obsA)

	_ = b.Close()
	time.Sleep(300 * time.Millisecond)
	publishAt(t, c, "pub-1", stores.BrokerMessage{TopicName: "data/c/count", Payload: []byte("42"), QoS: 1})

	b = startNode(t, "b", 27827, bDB)
	t.Cleanup(func() { _ = b.Close() })

	if got := obsA.waitFor(t, "data/c/count", 1, 500*time.Millisecond); len(got) != 1 {
		t.Errorf("A got %d copies after the outage, want 1", len(got))
	}
}

// A bridge that resubscribes after an outage must not get its own retained
// message replayed: the remote's copy is older than the local one.
func TestBridgeRetainedReplayDoesNotOverwriteNewerLocalState(t *testing.T) {
	dir := t.TempDir()
	bDB := filepath.Join(dir, "b.db")
	b := startNode(t, "b", 27833, bDB)

	cBridge := chainBridge(27833, "c-bridge")
	cBridge["bufferEnabled"] = true
	cBridge["bufferImplementation"] = "MONSTER"
	cDB := filepath.Join(dir, "c.db")
	seedBridges(t, cDB, "c", map[string]map[string]any{"to-b": cBridge})
	c := startNode(t, "c", 27834, cDB)
	t.Cleanup(func() { _ = c.Close() })

	obsB := observe(t, b, "data/#")
	obsC := observe(t, c, "data/#")
	waitBridged(t, c, obsB)

	const topic = "data/c/setpoint"
	publishAt(t, c, "pub-1", stores.BrokerMessage{TopicName: topic, Payload: []byte("v1"), QoS: 1, IsRetain: true})
	obsB.waitFor(t, topic, 1, 300*time.Millisecond)

	_ = b.Close()
	time.Sleep(300 * time.Millisecond)
	publishAt(t, c, "pub-1", stores.BrokerMessage{TopicName: topic, Payload: []byte("v2"), QoS: 1, IsRetain: true})
	seen := len(obsC.on(topic))

	b = startNode(t, "b", 27833, bDB)
	t.Cleanup(func() { _ = b.Close() })
	obsB = observe(t, b, "data/#")
	obsB.waitFor(t, topic, 1, 2*time.Second)

	for _, pk := range obsC.on(topic)[seen:] {
		t.Errorf("C received %q again after the restart", pk.Payload)
	}
	for name, srv := range map[string]*broker.Server{"C": c, "B": b} {
		var got []string
		_ = srv.Storage().Retained.FindMatchingMessages(context.Background(), topic, func(m stores.BrokerMessage) bool {
			got = append(got, string(m.Payload))
			return true
		})
		if !reflect.DeepEqual(got, []string{"v2"}) {
			t.Errorf("%s retains %v, want [v2]", name, got)
		}
	}
}

// An inbound mapping may subscribe with a shared subscription on the remote;
// messages arrive under the plain topic.
func TestBridgeInboundSharedSubscription(t *testing.T) {
	for i, pv := range []int{5, 4} {
		t.Run(fmt.Sprintf("protocolVersion %d", pv), func(t *testing.T) {
			remotePort, localPort := 27851+2*i, 27852+2*i
			dir := t.TempDir()
			remote := startNode(t, "remote", remotePort, filepath.Join(dir, "remote.db"))
			t.Cleanup(func() { _ = remote.Close() })

			localDB := filepath.Join(dir, "local.db")
			seedBridges(t, localDB, "local", map[string]map[string]any{"to-remote": {
				"brokerUrl":       fmt.Sprintf("tcp://localhost:%d", remotePort),
				"clientId":        "shared-bridge",
				"cleanSession":    true,
				"reconnectDelay":  1,
				"protocolVersion": pv,
				"addresses": []map[string]any{
					{"mode": "SUBSCRIBE", "remoteTopic": "$share/g/data/#", "localTopic": "data/#", "qos": 1, "removePath": true},
				},
			}})
			local := startNode(t, "local", localPort, localDB)
			t.Cleanup(func() { _ = local.Close() })

			obs := observe(t, local, "data/#")
			waitBridged(t, remote, obs)
			publishAt(t, remote, "pub-a", stores.BrokerMessage{TopicName: "data/x", Payload: []byte("1"), QoS: 1})
			if got := obs.waitFor(t, "data/x", 1, 300*time.Millisecond); len(got) != 1 {
				t.Errorf("local got %d copies of data/x, want 1", len(got))
			}
		})
	}
}
