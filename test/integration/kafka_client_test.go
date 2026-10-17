package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"monstermq.io/edge/internal/bridge/kafkaclient"
	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/stores/sqlite"
)

// The live Kafka tests need a cluster and are configured by environment:
//
//	KAFKA_BOOTSTRAP    bootstrap servers, e.g. scada:9092
//	KAFKA_READ_TOPIC   a topic with live traffic the tests only ever consume
//	KAFKA_WRITE_TOPIC  a topic the tests may produce to
//
// Each test skips when the variables it needs are missing.

func kafkaEnv(t *testing.T, names ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, n := range names {
		v := os.Getenv(n)
		if v == "" {
			t.Skipf("%s not set; skipping live Kafka test", n)
		}
		out[n] = v
	}
	return out
}

// assertNeverWrites fails the test before the broker starts when a device
// config could produce to the read-only topic.
func assertNeverWrites(t *testing.T, namespace string, cfg map[string]any, readOnly string) {
	t.Helper()
	if readOnly == "" || cfg["outboundEnabled"] != true {
		return
	}
	dest, _ := cfg["outboundKafkaTopic"].(string)
	if strings.TrimSpace(dest) == "" {
		dest = namespace
	}
	if dest == readOnly {
		t.Fatalf("test config would write to read-only topic %q", readOnly)
	}
}

func seedKafkaClient(t *testing.T, dbPath, node, name, namespace string, cfg map[string]any) {
	t.Helper()
	assertNeverWrites(t, namespace, cfg, os.Getenv("KAFKA_READ_TOPIC"))
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dcs := sqlite.NewDeviceConfigStore(db)
	if err := dcs.EnsureTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(cfg)
	if err := dcs.Save(context.Background(), stores.DeviceConfig{
		Name: name, Namespace: namespace, NodeID: node, Type: kafkaclient.DeviceTypeKafkaClient, Enabled: true, Config: string(raw),
	}); err != nil {
		t.Fatal(err)
	}
}

func startKafkaNode(t *testing.T, port int, dbPath string) *broker.Server {
	t.Helper()
	cfg := config.Default()
	cfg.NodeID = "k"
	cfg.TCP.Port = port
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = dbPath
	cfg.Features.Kafka = true
	logger := slog.New(slog.DiscardHandler)
	if testing.Verbose() {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	srv, err := broker.New(cfg, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// Live traffic on the read-only topic arrives in MQTT through the TEXT format,
// with the record key rewritten to a topic and the destination prefix applied.
func TestKafkaClientInboundReadOnly(t *testing.T) {
	env := kafkaEnv(t, "KAFKA_BOOTSTRAP", "KAFKA_READ_TOPIC")
	dbPath := filepath.Join(t.TempDir(), "kafka.db")
	seedKafkaClient(t, dbPath, "k", "reader", env["KAFKA_READ_TOPIC"], map[string]any{
		"bootstrapServers":       env["KAFKA_BOOTSTRAP"],
		"groupId":                fmt.Sprintf("edge-it-read-%d", time.Now().UnixNano()),
		"payloadFormat":          "TEXT",
		"topicKeyRegex":          `\.`,
		"topicKeyReplacement":    "/",
		"destinationTopicPrefix": "kafka/in",
		"inboundEnabled":         true,
		"outboundEnabled":        false,
	})
	srv := startKafkaNode(t, 29470, dbPath)
	obs := observe(t, srv, "kafka/in/#")

	deadline := time.Now().Add(45 * time.Second)
	for {
		obs.mu.Lock()
		n := len(obs.msgs)
		obs.mu.Unlock()
		if n >= 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("received %d messages from %s, want >= 5", n, env["KAFKA_READ_TOPIC"])
		}
		time.Sleep(200 * time.Millisecond)
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	for _, pk := range obs.msgs[:5] {
		if !strings.HasPrefix(pk.TopicName, "kafka/in/") || strings.Contains(pk.TopicName, ".") || len(pk.Payload) == 0 {
			t.Errorf("unexpected message %q payload %q", pk.TopicName, pk.Payload)
		}
	}
	c := srv.KafkaClients().Get("reader")
	if c == nil {
		t.Fatal("connector not running")
	}
	if in, out, _, _ := c.Totals(); in < 5 || out != 0 {
		t.Errorf("totals in=%d out=%d", in, out)
	}
}

// One device produces local "out/<run>/#" to the write topic and consumes the
// same topic back under the prefix "in": each format must round-trip through
// the cluster exactly once (outbound skips what inbound brought in).
func TestKafkaClientRoundTrip(t *testing.T) {
	env := kafkaEnv(t, "KAFKA_BOOTSTRAP", "KAFKA_WRITE_TOPIC")
	for i, format := range []string{kafkaclient.FormatDefault, kafkaclient.FormatJSON, kafkaclient.FormatBinary, kafkaclient.FormatText} {
		t.Run(format, func(t *testing.T) {
			run := fmt.Sprintf("%d%s", time.Now().UnixNano(), strings.ToLower(format))
			dbPath := filepath.Join(t.TempDir(), "kafka.db")
			seedKafkaClient(t, dbPath, "k", "rt", env["KAFKA_WRITE_TOPIC"], map[string]any{
				"bootstrapServers":       env["KAFKA_BOOTSTRAP"],
				"groupId":                "edge-it-" + run,
				"payloadFormat":          format,
				"destinationTopicPrefix": "in",
				"outboundEnabled":        true,
				"outboundTopicFilters":   []string{"out/" + run + "/#"},
				"extraProducerConfig":    map[string]any{"acks": "all", "linger.ms": "5"},
			})
			srv := startKafkaNode(t, 29471+i, dbPath)

			probe := "in/out/" + run + "/probe"
			back := observe(t, srv, "in/out/"+run+"/#")
			deadline := time.Now().Add(45 * time.Second)
			for len(back.on(probe)) == 0 {
				if time.Now().After(deadline) {
					t.Fatal("record never came back from Kafka")
				}
				publishAt(t, srv, "sensor", stores.BrokerMessage{TopicName: "out/" + run + "/probe", Payload: []byte("p")})
				time.Sleep(time.Second)
			}

			payload := []byte("once-" + format)
			publishAt(t, srv, "sensor", stores.BrokerMessage{TopicName: "out/" + run + "/once", Payload: payload, QoS: 1, IsRetain: true})
			got := back.waitFor(t, "in/out/"+run+"/once", 1, 3*time.Second)
			if len(got) != 1 || string(got[0].Payload) != string(payload) {
				t.Fatalf("once: %d messages, want exactly 1 with payload %q", len(got), payload)
			}
			// As in Core, the outbound envelope carries QoS 0 and no retain
			// flag (the bridge's local subscription is QoS 0), and BINARY/TEXT
			// publish at QoS 0: every format comes back at QoS 0.
			if got[0].FixedHeader.Qos != 0 || got[0].FixedHeader.Retain {
				t.Errorf("QoS = %d retain = %v, want 0/false", got[0].FixedHeader.Qos, got[0].FixedHeader.Retain)
			}
		})
	}
}

// The GraphQL surface matches Core's: feature reporting, CRUD, Core-shaped
// configs, any Kafka client property accepted, Core exports importable.
func TestKafkaClientGraphQL(t *testing.T) {
	srv, url := startWithGraphQL(t, 29478, 29488, func(c *config.Config) {
		c.Features.Kafka = true
		c.Features.DeviceImportExport = true
	})
	defer srv.Close()

	features := gqlQuery(t, url, `{ broker { enabledFeatures } }`, nil)["broker"].(map[string]any)["enabledFeatures"].([]any)
	found := false
	for _, f := range features {
		found = found || f == "Kafka"
	}
	if !found {
		t.Fatalf("enabledFeatures = %v, want Kafka", features)
	}

	create := `mutation C($in: KafkaClientInput!) { kafkaClient { create(input: $in) { success errors client {
		name namespace nodeId enabled isOnCurrentNode
		config { bootstrapServers groupId payloadFormat destinationTopicPrefix outboundEnabled outboundTopicFilters extraConsumerConfig reconnectDelayMs }
	} } } }`
	in := map[string]any{
		"name": "k1", "namespace": "orders", "nodeId": "g-29488",
		"config": map[string]any{
			"bootstrapServers":       "127.0.0.1:1",
			"payloadFormat":          "TEXT",
			"destinationTopicPrefix": "plant",
			"extraConsumerConfig":    map[string]any{"sasl.kerberos.service.name": "kafka", "check.crcs": "true"},
		},
	}
	res := gqlQuery(t, url, create, map[string]any{"in": in})["kafkaClient"].(map[string]any)["create"].(map[string]any)
	if res["success"] != true {
		t.Fatalf("create: %v", res)
	}
	cfg := res["client"].(map[string]any)["config"].(map[string]any)
	if cfg["destinationTopicPrefix"] != "plant/" || cfg["groupId"] != "monstermq-subscriber" || cfg["payloadFormat"] != "TEXT" ||
		cfg["reconnectDelayMs"] != float64(5000) || cfg["extraConsumerConfig"].(map[string]any)["check.crcs"] != "true" {
		t.Fatalf("created config = %v", cfg)
	}

	res = gqlQuery(t, url, create, map[string]any{"in": in})["kafkaClient"].(map[string]any)["create"].(map[string]any)
	if res["success"] != false || !strings.Contains(fmt.Sprint(res["errors"]), "already exists") {
		t.Fatalf("duplicate create: %v", res)
	}

	bad := map[string]any{"name": "bad name", "namespace": "x", "nodeId": "g-29488",
		"config": map[string]any{"reconnectDelayMs": 10, "outboundEnabled": true}}
	res = gqlQuery(t, url, create, map[string]any{"in": bad})["kafkaClient"].(map[string]any)["create"].(map[string]any)
	errs := fmt.Sprint(res["errors"])
	for _, want := range []string{"letters, numbers", "reconnectDelayMs should be >=500", "outboundTopicFilters must not be empty"} {
		if !strings.Contains(errs, want) {
			t.Errorf("validation errors %s miss %q", errs, want)
		}
	}

	toggle := gqlQuery(t, url, `mutation { kafkaClient { toggle(name: "k1", enabled: false) { success client { enabled } } } }`, nil)
	if toggle["kafkaClient"].(map[string]any)["toggle"].(map[string]any)["client"].(map[string]any)["enabled"] != false {
		t.Fatalf("toggle: %v", toggle)
	}

	// A config exported from Core: nested under "kafka", legacy keyTopicPrefix.
	imp := gqlQuery(t, url, `mutation I($c: [DeviceInput!]!) { importDevices(configs: $c) { success imported errors } }`, map[string]any{
		"c": []any{map[string]any{
			"name": "from-core", "namespace": "events", "nodeId": "g-29488", "type": "KAFKA-Client", "enabled": true,
			"config": map[string]any{"kafka": map[string]any{
				"bootstrapServers": "127.0.0.1:1", "groupId": "g1", "payloadFormat": "JSON", "keyTopicPrefix": "core",
				"extraProducerConfig": map[string]any{"acks": 1, "transactional.id": "tx"},
			}},
		}},
	})
	if imp["importDevices"].(map[string]any)["success"] != true {
		t.Fatalf("import: %v", imp)
	}
	q := gqlQuery(t, url, `{ kafkaClients(name: "from-core") { name namespace config { groupId payloadFormat destinationTopicPrefix extraProducerConfig } metrics { messagesIn messagesOut timestamp } } }`, nil)
	clients := q["kafkaClients"].([]any)
	if len(clients) != 1 {
		t.Fatalf("kafkaClients = %v", clients)
	}
	ic := clients[0].(map[string]any)["config"].(map[string]any)
	if ic["groupId"] != "g1" || ic["payloadFormat"] != "JSON" || ic["destinationTopicPrefix"] != "core/" || ic["extraProducerConfig"].(map[string]any)["acks"] != "1" {
		t.Fatalf("imported config = %v", ic)
	}

	re := gqlQuery(t, url, `mutation { kafkaClient { reassign(name: "k1", nodeId: "nowhere") { success errors } } }`, nil)
	if errs := fmt.Sprint(re["kafkaClient"].(map[string]any)["reassign"].(map[string]any)["errors"]); !strings.Contains(errs, "Cluster node 'nowhere' not found. Available nodes: g-29488") {
		t.Fatalf("reassign to unknown node: %s", errs)
	}
	re = gqlQuery(t, url, `mutation { kafkaClient { reassign(name: "k1", nodeId: "g-29488") { success } } }`, nil)
	if re["kafkaClient"].(map[string]any)["reassign"].(map[string]any)["success"] != true {
		t.Fatalf("reassign to own node: %v", re)
	}

	// As in Core: history needs a time range; a node filter includes "*" devices.
	h := gqlQuery(t, url, `{ kafkaClients(name: "k1") { metricsHistory { messagesIn } recent: metricsHistory(lastMinutes: 5) { messagesIn } } }`, nil)
	if n := len(h["kafkaClients"].([]any)[0].(map[string]any)["metricsHistory"].([]any)); n != 0 {
		t.Fatalf("metricsHistory without range = %d rows, want 0", n)
	}
	star := map[string]any{"name": "everywhere", "namespace": "all", "nodeId": "*", "config": map[string]any{"bootstrapServers": "127.0.0.1:1", "inboundEnabled": false}}
	if res := gqlQuery(t, url, create, map[string]any{"in": star})["kafkaClient"].(map[string]any)["create"].(map[string]any); res["success"] != true {
		t.Fatalf("create *: %v", res)
	}
	names := []string{}
	for _, c := range gqlQuery(t, url, `{ kafkaClients(node: "g-29488") { name } }`, nil)["kafkaClients"].([]any) {
		names = append(names, c.(map[string]any)["name"].(string))
	}
	if !slices.Contains(names, "everywhere") || !slices.Contains(names, "k1") {
		t.Fatalf("kafkaClients(node) = %v, want k1 and the * device", names)
	}

	del := gqlQuery(t, url, `mutation { kafkaClient { delete(name: "k1") } }`, nil)
	if del["kafkaClient"].(map[string]any)["delete"] != true {
		t.Fatalf("delete: %v", del)
	}
	if n := len(gqlQuery(t, url, `{ kafkaClients(name: "k1") { name } }`, nil)["kafkaClients"].([]any)); n != 0 {
		t.Fatalf("k1 still listed after delete")
	}
}

func TestKafkaClientGraphQLDisabled(t *testing.T) {
	srv, url := startWithGraphQL(t, 29479, 29489)
	defer srv.Close()
	if n := len(gqlQuery(t, url, `{ kafkaClients { name } }`, nil)["kafkaClients"].([]any)); n != 0 {
		t.Fatalf("kafkaClients = %d entries with the feature off", n)
	}
	res := gqlQuery(t, url, `mutation { kafkaClient { create(input: {name: "x", namespace: "x", nodeId: "n", config: {}}) { success errors } } }`, nil)
	errs := fmt.Sprint(res["kafkaClient"].(map[string]any)["create"].(map[string]any)["errors"])
	if !strings.Contains(errs, "Kafka feature is not enabled on this node") {
		t.Fatalf("create with feature off: %s", errs)
	}
}

// The cluster is unreachable when the device starts (its bootstrap address is
// a local proxy that is not listening yet). Messages published meanwhile wait
// in the outbound buffer and reach Kafka, exactly once, when it comes up.
func TestKafkaClientClusterComesUpLater(t *testing.T) {
	env := kafkaEnv(t, "KAFKA_BOOTSTRAP", "KAFKA_WRITE_TOPIC")
	upstream := strings.Split(env["KAFKA_BOOTSTRAP"], ",")[0]
	const proxyAddr = "127.0.0.1:29491"
	run := fmt.Sprintf("%dlate", time.Now().UnixNano())
	dbPath := filepath.Join(t.TempDir(), "kafka.db")
	seedKafkaClient(t, dbPath, "k", "late", env["KAFKA_WRITE_TOPIC"], map[string]any{
		"bootstrapServers":       proxyAddr,
		"groupId":                "edge-it-" + run,
		"payloadFormat":          "TEXT",
		"destinationTopicPrefix": "in",
		"reconnectDelayMs":       500,
		"outboundEnabled":        true,
		"outboundTopicFilters":   []string{"out/" + run + "/#"},
		"extraConsumerConfig":    map[string]any{"auto.offset.reset": "earliest"},
	})
	srv := startKafkaNode(t, 29490, dbPath)
	back := observe(t, srv, "in/out/"+run+"/#")

	// The manager starts inside Serve; a publish before the connector has
	// subscribed would not reach it.
	var c *kafkaclient.Connector
	for deadline := time.Now().Add(10 * time.Second); c == nil; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("connector not running")
		}
		if m := srv.KafkaClients(); m != nil {
			c = m.Get("late")
		}
	}

	const n = 5
	for i := 0; i < n; i++ {
		publishAt(t, srv, "sensor", stores.BrokerMessage{TopicName: fmt.Sprintf("out/%s/m%d", run, i), Payload: []byte(fmt.Sprint(i))})
	}
	time.Sleep(3 * time.Second)
	if _, out, _, _ := c.Totals(); out != 0 {
		t.Fatalf("messagesOut = %d while the cluster is unreachable", out)
	}

	stop := startTCPProxy(t, proxyAddr, upstream)
	defer stop()
	for i := 0; i < n; i++ {
		topic := fmt.Sprintf("in/out/%s/m%d", run, i)
		got := back.waitFor(t, topic, 1, 0)
		if len(got) != 1 || string(got[0].Payload) != fmt.Sprint(i) {
			t.Fatalf("%s: %d messages", topic, len(got))
		}
	}
	time.Sleep(2 * time.Second)
	for i := 0; i < n; i++ {
		if got := back.on(fmt.Sprintf("in/out/%s/m%d", run, i)); len(got) != 1 {
			t.Fatalf("m%d arrived %d times, want once", i, len(got))
		}
	}
	// Earliest offsets also read older records of the topic, so in >= n.
	if in, out, dropped, _ := c.Totals(); out != n || in < n || dropped != 0 {
		t.Errorf("totals in=%d out=%d dropped=%d, want in>=%d out=%d dropped=0", in, out, dropped, n, n)
	}
}

// startTCPProxy forwards connections on addr to upstream until stop is called.
func startTCPProxy(t *testing.T, addr, upstream string) (stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				up, err := net.DialTimeout("tcp", upstream, 5*time.Second)
				if err != nil {
					return
				}
				defer up.Close()
				go func() { _, _ = io.Copy(up, conn) }()
				_, _ = io.Copy(conn, up)
			}()
		}
	}()
	return func() { _ = ln.Close() }
}
