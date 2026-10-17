package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats.go"
	"golang.org/x/crypto/bcrypt"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
)

func setupKvTestGroups(t *testing.T, srv *broker.Server) {
	t.Helper()
	ctx := context.Background()

	_ = srv.Storage().ArchiveConfig.Save(ctx, stores.ArchiveGroupConfig{
		Name:         "Default",
		Enabled:      true,
		TopicFilters: []string{"default/#"},
		LastValType:  stores.MessageStoreMemory,
		ArchiveType:  stores.ArchiveNone,
	})
	_ = srv.Storage().ArchiveConfig.Save(ctx, stores.ArchiveGroupConfig{
		Name:         "Plant",
		Enabled:      true,
		TopicFilters: []string{"plant/#"},
		LastValType:  stores.MessageStoreMemory,
		ArchiveType:  stores.ArchiveNone,
	})
	_ = srv.Storage().ArchiveConfig.Save(ctx, stores.ArchiveGroupConfig{
		Name:            "RO",
		Enabled:         true,
		TopicFilters:    []string{"ro/#"},
		LastValType:     stores.MessageStoreMemory,
		ArchiveType:     stores.ArchiveNone,
		LastValReadOnly: true,
	})
	_ = srv.Storage().ArchiveConfig.Save(ctx, stores.ArchiveGroupConfig{
		Name:         "Ret",
		Enabled:      true,
		TopicFilters: []string{"ret/#"},
		RetainedOnly: true,
		LastValType:  stores.MessageStoreMemory,
	})

	if err := srv.Archives().Reload(ctx); err != nil {
		t.Fatalf("Reload failed: %v", err)
	}
}

// 1. Buckets and info
func TestNatsKVBucketsAndInfo(t *testing.T) {
	srv, _, natsPort := startWithNats(t)
	defer srv.Close()
	setupKvTestGroups(t, srv)

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	// KeyValueStoreNames -> Default, Plant, RO, Ret
	namesChan := js.KeyValueStoreNames()
	var names []string
	for name := range namesChan {
		names = append(names, name)
	}
	sort.Strings(names)
	expectedNames := []string{"Default", "Plant", "RO", "Ret"}
	if fmt.Sprintf("%v", names) != fmt.Sprintf("%v", expectedNames) {
		t.Fatalf("expected names %v, got %v", expectedNames, names)
	}

	// Raw STREAM.INFO.KV_Plant
	rep, err := nc.Request("$JS.API.STREAM.INFO.KV_Plant", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("STREAM.INFO.KV_Plant request failed: %v", err)
	}
	var streamInfo map[string]any
	if err := json.Unmarshal(rep.Data, &streamInfo); err != nil {
		t.Fatalf("unmarshal stream info: %v", err)
	}
	cfg, ok := streamInfo["config"].(map[string]any)
	if !ok {
		t.Fatalf("expected config in stream info, got: %+v", streamInfo)
	}
	if cfg["allow_direct"] != false {
		t.Fatalf("expected allow_direct: false, got: %v", cfg["allow_direct"])
	}
	if cfg["max_msgs_per_subject"].(float64) != 1 {
		t.Fatalf("expected max_msgs_per_subject: 1, got: %v", cfg["max_msgs_per_subject"])
	}
	if cfg["deny_delete"] != true {
		t.Fatalf("expected deny_delete: true, got: %v", cfg["deny_delete"])
	}
	if cfg["sealed"] != false {
		t.Fatalf("expected sealed: false on Plant, got: %v", cfg["sealed"])
	}

	// Raw STREAM.INFO.KV_RO has sealed: true
	repRO, err := nc.Request("$JS.API.STREAM.INFO.KV_RO", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("STREAM.INFO.KV_RO request failed: %v", err)
	}
	var streamInfoRO map[string]any
	_ = json.Unmarshal(repRO.Data, &streamInfoRO)
	cfgRO := streamInfoRO["config"].(map[string]any)
	if cfgRO["sealed"] != true {
		t.Fatalf("expected sealed: true on RO, got: %v", cfgRO["sealed"])
	}

	// kv.Status().Values() counts non-empty keys
	kv, err := js.KeyValue("Plant")
	if err != nil {
		t.Fatal(err)
	}
	st, err := kv.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.Values() != 0 {
		t.Fatalf("expected 0 values initially, got %d", st.Values())
	}

	_, err = kv.Put("plant.line1.temp", []byte("20"))
	if err != nil {
		t.Fatal(err)
	}
	st2, _ := kv.Status()
	if st2.Values() != 1 {
		t.Fatalf("expected 1 value after put, got %d", st2.Values())
	}
}

// 2. Put and get
func TestNatsKVPutAndGet(t *testing.T) {
	srv, mqttPort, natsPort := startWithNats(t)
	defer srv.Close()
	setupKvTestGroups(t, srv)

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	kv, err := js.KeyValue("Plant")
	if err != nil {
		t.Fatal(err)
	}

	// kv.Put("plant.line1.temp", "21.5") returns revision r
	rev, err := kv.Put("plant.line1.temp", []byte("21.5"))
	if err != nil {
		t.Fatalf("kv.Put failed: %v", err)
	}
	if rev == 0 {
		t.Fatal("expected non-zero revision")
	}

	// An MQTT subscriber on plant/# gets it as retained
	mqttMsgs := make(chan paho.Message, 2)
	opts := paho.NewClientOptions().AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", mqttPort)).SetClientID("kv-mqtt-sub")
	mc := paho.NewClient(opts)
	_ = mc.Connect().Wait()
	defer mc.Disconnect(0)

	mc.Subscribe("plant/#", 0, func(_ paho.Client, m paho.Message) {
		mqttMsgs <- m
	})

	select {
	case m := <-mqttMsgs:
		if m.Topic() != "plant/line1/temp" || string(m.Payload()) != "21.5" || !m.Retained() {
			t.Fatalf("unexpected mqtt msg: topic=%s payload=%s retained=%v", m.Topic(), string(m.Payload()), m.Retained())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for retained mqtt message")
	}

	// kv.Get returns 21.5 with revision r
	entry, err := kv.Get("plant.line1.temp")
	if err != nil {
		t.Fatalf("kv.Get failed: %v", err)
	}
	if string(entry.Value()) != "21.5" || entry.Revision() != rev {
		t.Fatalf("expected 21.5 rev %d, got %s rev %d", rev, string(entry.Value()), entry.Revision())
	}

	// An MQTT publish to plant/line1/hum is visible to kv.Get
	mc.Publish("plant/line1/hum", 0, false, []byte("55%")).Wait()
	time.Sleep(100 * time.Millisecond)

	entryHum, err := kv.Get("plant.line1.hum")
	if err != nil {
		t.Fatalf("kv.Get hum failed: %v", err)
	}
	if string(entryHum.Value()) != "55%" {
		t.Fatalf("expected 55%%, got %s", string(entryHum.Value()))
	}
}

// 3. Delete
func TestNatsKVDelete(t *testing.T) {
	srv, mqttPort, natsPort := startWithNats(t)
	defer srv.Close()
	setupKvTestGroups(t, srv)

	nc, _ := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	defer nc.Close()
	js, _ := nc.JetStream()
	kv, _ := js.KeyValue("Plant")

	_, _ = kv.Put("plant.line1.temp", []byte("25"))

	// Delete clears retained message, and kv.Get returns ErrKeyNotFound
	err := kv.Delete("plant.line1.temp")
	if err != nil {
		t.Fatalf("kv.Delete failed: %v", err)
	}

	_, err = kv.Get("plant.line1.temp")
	if err != nats.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// Verify MQTT subscriber on plant/# does not receive it as retained
	retainedChan := make(chan paho.Message, 1)
	opts := paho.NewClientOptions().AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", mqttPort)).SetClientID("kv-del-sub")
	mc := paho.NewClient(opts)
	_ = mc.Connect().Wait()
	defer mc.Disconnect(0)
	mc.Subscribe("plant/line1/temp", 0, func(_ paho.Client, m paho.Message) {
		retainedChan <- m
	})
	select {
	case m := <-retainedChan:
		t.Fatalf("did not expect retained message after delete, got: %s", string(m.Payload()))
	case <-time.After(300 * time.Millisecond):
		// OK
	}
}

// 4. Create and update
func TestNatsKVCreateAndUpdate(t *testing.T) {
	srv, _, natsPort := startWithNats(t)
	defer srv.Close()
	setupKvTestGroups(t, srv)

	nc, _ := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	defer nc.Close()
	js, _ := nc.JetStream()
	kv, _ := js.KeyValue("Plant")

	// kv.Create on new key succeeds
	rev1, err := kv.Create("plant.line1.speed", []byte("100"))
	if err != nil {
		t.Fatalf("kv.Create failed: %v", err)
	}

	// kv.Create on existing key fails (wrong last sequence: <r>, 10071)
	_, err = kv.Create("plant.line1.speed", []byte("105"))
	if err == nil {
		t.Fatal("expected create on existing key to fail")
	}
	if !strings.Contains(err.Error(), "wrong last sequence") {
		t.Fatalf("expected wrong last sequence error, got: %v", err)
	}

	// kv.Update with matching revision succeeds
	time.Sleep(2 * time.Millisecond)
	rev2, err := kv.Update("plant.line1.speed", []byte("101"), rev1)
	if err != nil {
		t.Fatalf("kv.Update failed: %v", err)
	}
	if rev2 <= rev1 {
		t.Fatalf("expected rev2 > rev1, got rev1=%d rev2=%d", rev1, rev2)
	}

	// kv.Update with stale revision fails
	_, err = kv.Update("plant.line1.speed", []byte("102"), rev1)
	if err == nil {
		t.Fatal("expected stale update to fail")
	}
	if !strings.Contains(err.Error(), "wrong last sequence") {
		t.Fatalf("expected wrong last sequence error on stale update, got: %v", err)
	}
}

// 5. Keys and watch
func TestNatsKVKeysAndWatch(t *testing.T) {
	srv, mqttPort, natsPort := startWithNats(t)
	defer srv.Close()
	setupKvTestGroups(t, srv)

	nc, _ := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	defer nc.Close()
	js, _ := nc.JetStream()
	kv, _ := js.KeyValue("Plant")

	_, _ = kv.Put("plant.line1.temp", []byte("21.5"))
	_, _ = kv.Put("plant.line1.press", []byte("1013"))
	_, _ = kv.Put("plant.line2.temp", []byte("22.0"))

	// kv.Keys lists every key
	keys, err := kv.Keys()
	if err != nil {
		t.Fatalf("kv.Keys failed: %v", err)
	}
	sort.Strings(keys)
	expectedKeys := []string{"plant.line1.press", "plant.line1.temp", "plant.line2.temp"}
	if fmt.Sprintf("%v", keys) != fmt.Sprintf("%v", expectedKeys) {
		t.Fatalf("expected keys %v, got %v", expectedKeys, keys)
	}

	// kv.Watch delivers snapshot, nil marker, then live MQTT update
	w, err := kv.Watch("plant.line1.>")
	if err != nil {
		t.Fatalf("kv.Watch failed: %v", err)
	}
	defer w.Stop()

	var snapshotKeys []string
	sawNil := false
	for entry := range w.Updates() {
		if entry == nil {
			sawNil = true
			break
		}
		snapshotKeys = append(snapshotKeys, entry.Key())
	}
	if !sawNil {
		t.Fatal("expected nil marker at end of snapshot")
	}
	sort.Strings(snapshotKeys)
	expectedSnap := []string{"plant.line1.press", "plant.line1.temp"}
	if fmt.Sprintf("%v", snapshotKeys) != fmt.Sprintf("%v", expectedSnap) {
		t.Fatalf("expected snapshot %v, got %v", expectedSnap, snapshotKeys)
	}

	// Live MQTT update
	opts := paho.NewClientOptions().AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", mqttPort)).SetClientID("kv-watch-pub")
	mc := paho.NewClient(opts)
	_ = mc.Connect().Wait()
	defer mc.Disconnect(0)

	mc.Publish("plant/line1/temp", 0, false, []byte("23.5")).Wait()

	select {
	case entry := <-w.Updates():
		if entry == nil || entry.Key() != "plant.line1.temp" || string(entry.Value()) != "23.5" {
			t.Fatalf("unexpected live update: %+v", entry)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for live update in watch")
	}

	// A raw consumer with idle_heartbeat 100 ms receives HMSG heartbeats NATS/1.0 100 Idle Heartbeat
	connRaw, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer connRaw.Close()
	rRaw := bufio.NewReader(connRaw)
	_, _ = rRaw.ReadString('\n') // INFO
	_, _ = connRaw.Write([]byte("CONNECT {\"headers\":true}\r\n"))

	deliverSubj := "hb.deliver"
	_, _ = connRaw.Write([]byte(fmt.Sprintf("SUB %s 1\r\n", deliverSubj)))

	reqBody, _ := json.Marshal(map[string]any{
		"stream_name": "KV_Plant",
		"config": map[string]any{
			"deliver_subject": deliverSubj,
			"deliver_policy":  "last",
			"idle_heartbeat":  100000000, // 100ms in nanoseconds
		},
	})
	inbox := "hb.reply"
	_, _ = connRaw.Write([]byte(fmt.Sprintf("SUB %s 2\r\n", inbox)))
	_, _ = connRaw.Write([]byte(fmt.Sprintf("PUB $JS.API.CONSUMER.CREATE.KV_Plant %s %d\r\n%s\r\n", inbox, len(reqBody), string(reqBody))))

	// Wait for heartbeat on deliverSubj
	_ = connRaw.SetReadDeadline(time.Now().Add(2 * time.Second))
	foundHb := false
	for {
		line, err := rRaw.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "100 Idle Heartbeat") {
			foundHb = true
			break
		}
	}
	if !foundHb {
		t.Fatal("expected 100 Idle Heartbeat in raw consumer stream")
	}
}

// 6. Consumer rules (raw)
func TestNatsKVConsumerRulesRaw(t *testing.T) {
	srv, mqttPort, natsPort := startWithNats(t)
	defer srv.Close()
	setupKvTestGroups(t, srv)

	nc, _ := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	defer nc.Close()
	js, _ := nc.JetStream()
	kv, _ := js.KeyValue("Plant")

	r1, _ := kv.Put("plant.line1.a", []byte("val_a"))
	_, _ = kv.Put("plant.line1.b", []byte("val_b"))

	connRaw, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	defer connRaw.Close()
	rRaw := bufio.NewReader(connRaw)
	_, _ = rRaw.ReadString('\n')

	// deliver_policy: "new" sends no snapshot
	delivNew := "rules.new"
	_, _ = connRaw.Write([]byte(fmt.Sprintf("SUB %s 10\r\n", delivNew)))
	inboxNew := "reply.new"
	_, _ = connRaw.Write([]byte(fmt.Sprintf("SUB %s 11\r\n", inboxNew)))

	reqNew, _ := json.Marshal(map[string]any{
		"stream_name": "KV_Plant",
		"config": map[string]any{
			"deliver_subject": delivNew,
			"deliver_policy":  "new",
		},
	})
	_, _ = connRaw.Write([]byte(fmt.Sprintf("PUB $JS.API.CONSUMER.CREATE.KV_Plant %s %d\r\n%s\r\n", inboxNew, len(reqNew), string(reqNew))))

	// Read reply for consumer create
	_, _ = rRaw.ReadString('\n') // MSG reply.new ...
	_, _ = rRaw.ReadString('\n') // json payload

	// Drain any immediate messages: should be none for "new" policy
	_ = connRaw.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	line, err := rRaw.ReadString('\n')
	if err == nil && strings.HasPrefix(line, "MSG "+delivNew) {
		t.Fatalf("expected no snapshot on deliver_policy: new, got: %s", line)
	}

	// opt_start_seq = r+1 skips older entries
	delivSeq := "rules.seq"
	_, _ = connRaw.Write([]byte(fmt.Sprintf("SUB %s 20\r\n", delivSeq)))
	inboxSeq := "reply.seq"
	_, _ = connRaw.Write([]byte(fmt.Sprintf("SUB %s 21\r\n", inboxSeq)))

	reqSeq, _ := json.Marshal(map[string]any{
		"stream_name": "KV_Plant",
		"config": map[string]any{
			"deliver_subject": delivSeq,
			"deliver_policy":  "by_start_sequence",
			"opt_start_seq":   r1 + 1,
		},
	})
	_, _ = connRaw.Write([]byte(fmt.Sprintf("PUB $JS.API.CONSUMER.CREATE.KV_Plant %s %d\r\n%s\r\n", inboxSeq, len(reqSeq), string(reqSeq))))

	// by_start_sequence still delivers live updates
	opts := paho.NewClientOptions().AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", mqttPort)).SetClientID("kv-rules-pub")
	mc := paho.NewClient(opts)
	_ = mc.Connect().Wait()
	defer mc.Disconnect(0)
	mc.Publish("plant/line1/c", 0, false, []byte("val_c")).Wait()

	_ = connRaw.SetReadDeadline(time.Now().Add(2 * time.Second))
	foundLive := false
	for {
		l, err := rRaw.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(l, "val_c") {
			foundLive = true
			break
		}
	}
	if !foundLive {
		t.Fatal("expected live update on by_start_sequence consumer")
	}

	// Consumer created before SUB on deliver_subject delivers nothing until SUB
	delivLate := "rules.late"
	inboxLate := "reply.late"
	_, _ = connRaw.Write([]byte(fmt.Sprintf("SUB %s 31\r\n", inboxLate)))
	reqLate, _ := json.Marshal(map[string]any{
		"stream_name": "KV_Plant",
		"config": map[string]any{
			"name":            "late_consumer",
			"deliver_subject": delivLate,
			"deliver_policy":  "last",
		},
	})
	_, _ = connRaw.Write([]byte(fmt.Sprintf("PUB $JS.API.CONSUMER.CREATE.KV_Plant %s %d\r\n%s\r\n", inboxLate, len(reqLate), string(reqLate))))
	time.Sleep(100 * time.Millisecond)

	// Now send SUB for delivLate and verify entries arrive
	_, _ = connRaw.Write([]byte(fmt.Sprintf("SUB %s 30\r\n", delivLate)))
	_ = connRaw.SetReadDeadline(time.Now().Add(2 * time.Second))
	foundLateMsg := false
	for {
		l, err := rRaw.ReadString('\n')
		if err != nil {
			break
		}
		if strings.HasPrefix(l, "MSG $KV.Plant.") && strings.Contains(l, " 30 ") {
			foundLateMsg = true
			break
		}
	}
	if !foundLateMsg {
		t.Fatal("expected snapshot messages after delayed SUB")
	}

	// UNSUB of deliver_subject removes the consumer: CONSUMER.INFO then gives 404/10014
	_, _ = connRaw.Write([]byte("UNSUB 30\r\n"))
	time.Sleep(100 * time.Millisecond)

	// Raw CONSUMER.INFO for removed consumer returns 404
	repC, err := nc.Request("$JS.API.CONSUMER.INFO.KV_Plant.late_consumer", nil, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var errResp map[string]any
	_ = json.Unmarshal(repC.Data, &errResp)
	errObj := errResp["error"].(map[string]any)
	if errObj["code"].(float64) != 404 || errObj["err_code"].(float64) != 10014 {
		t.Fatalf("expected 404/10014, got %+v", errObj)
	}
}

// 7. Exact errors (raw JSON)
func TestNatsKVExactErrors(t *testing.T) {
	srv, _, natsPort := startWithNats(t, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = false
	})
	defer srv.Close()
	setupKvTestGroups(t, srv)

	hash, _ := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	_ = srv.Storage().Users.CreateUser(context.Background(), stores.User{
		Username:     "alice",
		PasswordHash: string(hash),
		Enabled:      true,
		CanPublish:   true,
		CanSubscribe: true,
	})
	_ = srv.Storage().Users.CreateAclRule(context.Background(), stores.AclRule{
		Username:     "alice",
		TopicPattern: "plant/#",
		CanPublish:   true,
		CanSubscribe: true,
		Priority:     1,
	})
	_ = srv.Storage().Users.CreateAclRule(context.Background(), stores.AclRule{
		Username:     "alice",
		TopicPattern: "_INBOX/#",
		CanPublish:   true,
		CanSubscribe: true,
		Priority:     1,
	})
	_ = srv.Storage().Users.CreateAclRule(context.Background(), stores.AclRule{
		Username:     "alice",
		TopicPattern: "plant/denied/#",
		CanPublish:   false,
		CanSubscribe: false,
		Priority:     10,
	})
	_ = srv.AuthCache().Refresh(context.Background())

	nc, err := nats.Connect(fmt.Sprintf("nats://alice:secret@127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// STREAM.INFO.KV_Missing -> 404/10059 stream not found
	rep1, _ := nc.Request("$JS.API.STREAM.INFO.KV_Missing", nil, 2*time.Second)
	var e1 map[string]any
	_ = json.Unmarshal(rep1.Data, &e1)
	err1 := e1["error"].(map[string]any)
	if err1["code"].(float64) != 404 || err1["err_code"].(float64) != 10059 || !strings.Contains(err1["description"].(string), "stream not found") {
		t.Fatalf("expected 404/10059 stream not found, got %+v", err1)
	}

	// STREAM.CREATE.X and DIRECT.GET.KV_Plant -> 400/10025 with main's text
	rep2, _ := nc.Request("$JS.API.STREAM.CREATE.X", nil, 2*time.Second)
	var e2 map[string]any
	_ = json.Unmarshal(rep2.Data, &e2)
	err2 := e2["error"].(map[string]any)
	if err2["code"].(float64) != 400 || err2["err_code"].(float64) != 10025 {
		t.Fatalf("expected 400/10025, got %+v", err2)
	}

	rep3, _ := nc.Request("$JS.API.DIRECT.GET.KV_Plant", nil, 2*time.Second)
	var e3 map[string]any
	_ = json.Unmarshal(rep3.Data, &e3)
	err3 := e3["error"].(map[string]any)
	if err3["code"].(float64) != 400 || err3["err_code"].(float64) != 10025 {
		t.Fatalf("expected 400/10025 on DIRECT.GET, got %+v", err3)
	}

	// A put to other.x in Plant -> 400/10052 key [other/x] does not match the topic filter of archive group [Plant]
	rep4, _ := nc.Request("$KV.Plant.other.x", []byte("val"), 2*time.Second)
	var e4 map[string]any
	_ = json.Unmarshal(rep4.Data, &e4)
	err4 := e4["error"].(map[string]any)
	if err4["code"].(float64) != 400 || err4["err_code"].(float64) != 10052 || !strings.Contains(err4["description"].(string), "does not match the topic filter") {
		t.Fatalf("expected 400/10052 topic filter mismatch, got %+v", err4)
	}

	// A put in RO -> 400/10039
	rep5, _ := nc.Request("$KV.RO.ro.test", []byte("val"), 2*time.Second)
	var e5 map[string]any
	_ = json.Unmarshal(rep5.Data, &e5)
	err5 := e5["error"].(map[string]any)
	if err5["code"].(float64) != 400 || err5["err_code"].(float64) != 10039 {
		t.Fatalf("expected 400/10039 on read-only group, got %+v", err5)
	}

	// A put on a denied topic -> 403/10101
	rep6, _ := nc.Request("$KV.Plant.plant.denied.key", []byte("val"), 2*time.Second)
	var e6 map[string]any
	_ = json.Unmarshal(rep6.Data, &e6)
	err6 := e6["error"].(map[string]any)
	if err6["code"].(float64) != 403 || err6["err_code"].(float64) != 10101 {
		t.Fatalf("expected 403/10101 on ACL denied topic, got %+v", err6)
	}

	// Key a..b -> 400/10052 invalid key
	rep7, _ := nc.Request("$KV.Plant.plant..b", []byte("val"), 2*time.Second)
	var e7 map[string]any
	_ = json.Unmarshal(rep7.Data, &e7)
	err7 := e7["error"].(map[string]any)
	if err7["code"].(float64) != 400 || err7["err_code"].(float64) != 10052 || !strings.Contains(err7["description"].(string), "invalid key") {
		t.Fatalf("expected 400/10052 invalid key, got %+v", err7)
	}

	// A pull consumer -> 400/10025 pull consumers are not supported by MonsterMQ, use a push consumer
	pullReq, _ := json.Marshal(map[string]any{
		"stream_name": "KV_Plant",
		"config": map[string]any{
			// deliver_subject omitted -> pull consumer
		},
	})
	rep8, _ := nc.Request("$JS.API.CONSUMER.CREATE.KV_Plant", pullReq, 2*time.Second)
	var e8 map[string]any
	_ = json.Unmarshal(rep8.Data, &e8)
	err8 := e8["error"].(map[string]any)
	if err8["code"].(float64) != 400 || err8["err_code"].(float64) != 10025 || !strings.Contains(err8["description"].(string), "pull consumers are not supported") {
		t.Fatalf("expected 400/10025 pull consumer error, got %+v", err8)
	}
}

// 8. Retained-only group (S4): a put in Ret is accepted and stored
func TestNatsKVRetainedOnly(t *testing.T) {
	srv, _, natsPort := startWithNats(t)
	defer srv.Close()
	setupKvTestGroups(t, srv)

	nc, _ := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort))
	defer nc.Close()
	js, _ := nc.JetStream()
	kv, err := js.KeyValue("Ret")
	if err != nil {
		t.Fatalf("KeyValue Ret failed: %v", err)
	}

	rev, err := kv.Put("ret.param1", []byte("val1"))
	if err != nil {
		t.Fatalf("put in Ret failed: %v", err)
	}
	if rev == 0 {
		t.Fatal("expected non-zero revision")
	}

	entry, err := kv.Get("ret.param1")
	if err != nil {
		t.Fatalf("get in Ret failed: %v", err)
	}
	if string(entry.Value()) != "val1" {
		t.Fatalf("expected val1, got %s", string(entry.Value()))
	}
}

// 9. Delivery-time ACL (F6): a watch on allowed.> does not show allowed/secret/... entries
func TestNatsKVDeliveryAcl(t *testing.T) {
	srv, mqttPort, natsPort := startWithNats(t, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = false
	})
	defer srv.Close()

	ctx := context.Background()
	_ = srv.Storage().ArchiveConfig.Save(ctx, stores.ArchiveGroupConfig{
		Name:         "Allowed",
		Enabled:      true,
		TopicFilters: []string{"allowed/#"},
		LastValType:  stores.MessageStoreMemory,
		ArchiveType:  stores.ArchiveNone,
	})
	_ = srv.Archives().Reload(ctx)

	hash, _ := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	_ = srv.Storage().Users.CreateUser(ctx, stores.User{
		Username:     "carol",
		PasswordHash: string(hash),
		Enabled:      true,
		CanPublish:   true,
		CanSubscribe: true,
	})
	_ = srv.Storage().Users.CreateAclRule(ctx, stores.AclRule{
		Username:     "carol",
		TopicPattern: "allowed/#",
		CanPublish:   true,
		CanSubscribe: true,
		Priority:     1,
	})
	_ = srv.Storage().Users.CreateAclRule(ctx, stores.AclRule{
		Username:     "carol",
		TopicPattern: "_INBOX/#",
		CanPublish:   true,
		CanSubscribe: true,
		Priority:     1,
	})
	_ = srv.Storage().Users.CreateAclRule(ctx, stores.AclRule{
		Username:     "carol",
		TopicPattern: "allowed/secret/#",
		CanPublish:   false,
		CanSubscribe: false,
		Priority:     10,
	})

	_ = srv.Storage().Users.CreateUser(ctx, stores.User{
		Username:     "admin",
		PasswordHash: string(hash),
		Enabled:      true,
		IsAdmin:      true,
		CanPublish:   true,
		CanSubscribe: true,
	})
	_ = srv.AuthCache().Refresh(ctx)

	// Publish an allowed key and a secret key into the group via admin MQTT client
	mqttOpts := paho.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", mqttPort)).
		SetClientID("admin-kv-pub").
		SetUsername("admin").
		SetPassword("secret").
		SetConnectTimeout(2 * time.Second)
	mc := paho.NewClient(mqttOpts)
	if tok := mc.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("mqtt connect failed: %v", tok.Error())
	}
	defer mc.Disconnect(0)

	mc.Publish("allowed/public/key", 0, true, []byte("pub_val")).Wait()
	mc.Publish("allowed/secret/key", 0, true, []byte("secret_val")).Wait()

	// Carol connects via NATS and watches allowed.>
	nc, err := nats.Connect(fmt.Sprintf("nats://carol:secret@127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	kv, err := js.KeyValue("Allowed")
	if err != nil {
		t.Fatal(err)
	}

	w, err := kv.Watch("allowed.>")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	var snapshotKeys []string
	for entry := range w.Updates() {
		if entry == nil {
			break
		}
		if entry.Key() == "allowed.secret.key" {
			t.Fatalf("F6 violated: secret key delivered in snapshot: %s", entry.Key())
		}
		snapshotKeys = append(snapshotKeys, entry.Key())
	}
	if len(snapshotKeys) != 1 || snapshotKeys[0] != "allowed.public.key" {
		t.Fatalf("expected only allowed.public.key, got: %v", snapshotKeys)
	}

	// Live publish to allowed/secret/key2 should not be delivered
	mc.Publish("allowed/secret/key2", 0, false, []byte("secret2")).Wait()
	mc.Publish("allowed/public/key2", 0, false, []byte("pub2")).Wait()

	select {
	case entry := <-w.Updates():
		if entry == nil {
			t.Fatal("unexpected nil entry")
		}
		if entry.Key() == "allowed.secret.key2" {
			t.Fatalf("F6 violated: secret key delivered in live update: %s", entry.Key())
		}
		if entry.Key() != "allowed.public.key2" {
			t.Fatalf("expected allowed.public.key2, got %s", entry.Key())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for allowed.public.key2 live update")
	}
}
