package unsmodeler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/stores/oastore"
)

// A draft and an applied document of MaxDocumentBytes each fit into one
// WinCC OA store record, the escaping of the Config string included.
func TestConfigDocumentsFitRecord(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"schemaVersion":1,"direction":"winccoa_to_uns","tree":[`)
	for i := 0; ; i++ {
		node := fmt.Sprintf(`{"id":"n%011d","name":"pump-%d","description":"Pump \"%d\" in hall 2","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P%d","element":"value.flow"}}`, i, i, i, i)
		if b.Len()+len(node)+3 > MaxDocumentBytes {
			break
		}
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(node)
	}
	b.WriteString(`]}`)
	doc := []byte(b.String())
	s := settingsA("acme/plant")
	s.SystemPrompt = strings.Repeat("Model the plant along ISA-95. ", 100)
	cfg, err := WithConfigDocument(s.Encode(), SlotDraft, doc)
	if err == nil {
		cfg, err = WithConfigDocument(cfg, SlotApplied, doc)
	}
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := json.Marshal(stores.DeviceConfig{Name: "uns-device", Namespace: "uns-modeler", NodeID: "node-1", Type: DeviceType, Enabled: true, Config: cfg})
	if len(rec)+256 > oastore.MaxEnvelope {
		t.Fatalf("record of %d bytes does not fit into %d", len(rec), oastore.MaxEnvelope)
	}
}

func TestConfigDocuments(t *testing.T) {
	cfg, err := WithConfigDocument(`{"direction":"WINCCOA_TO_UNS","unsRoot":"u"}`, SlotDraft, []byte(`{ "z": 1, "a": [ "<&>" ] }`))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(ConfigDocument(cfg, SlotDraft)); got != `{"z":1,"a":["<&>"]}` {
		t.Fatalf("draft %s", got)
	}
	if ConfigDocument(cfg, SlotApplied) != nil {
		t.Fatal("applied without save")
	}
	if s, err := ParseSettings(cfg); err != nil || s.UnsRoot != "u" {
		t.Fatalf("settings %v %v", s, err)
	}
	if _, err := WithConfigDocument(cfg, SlotApplied, []byte(`{"broken"`)); err == nil {
		t.Fatal("broken document stored")
	}
	// New settings keep the stored documents and drop documents of the input.
	next, err := WithConfigDocuments(`{"direction":"WINCCOA_TO_UNS","unsRoot":"v","applied":{"x":1}}`, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if string(ConfigDocument(next, SlotDraft)) != `{"z":1,"a":["<&>"]}` || ConfigDocument(next, SlotApplied) != nil {
		t.Fatalf("merged %s", next)
	}
	if settingsKey(cfg) == settingsKey(next) || settingsKey(cfg) != settingsKey(`{"unsRoot":"u","direction":"WINCCOA_TO_UNS"}`) {
		t.Fatal("settings key")
	}
}

// The documents live in the device record: a draft save neither restarts
// the device nor is lost by a settings update.
func TestManagerDocumentsInDeviceRecord(t *testing.T) {
	m, devs, _, _ := newTestManager(t, AlwaysActive{})
	ctx := context.Background()
	_ = devs.Save(ctx, stores.DeviceConfig{Name: "d", Type: DeviceType, Enabled: true, NodeID: "n1", Config: settingsA("u").Encode()})
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.SaveDraft(ctx, "d", docA(`[{"name":"a","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}}]`)); err != nil {
		t.Fatal(err)
	}
	if issues, err := m.Apply(ctx, "d"); err != nil || HasErrors(issues) {
		t.Fatalf("apply %v %v", err, issues)
	}
	m.mu.Lock()
	before := m.devices["d"]
	m.mu.Unlock()
	if _, _, err := m.SaveDraft(ctx, "d", docA(`[]`)); err != nil {
		t.Fatal(err)
	}
	if err := m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	after := m.devices["d"]
	m.mu.Unlock()
	if before != after || m.Status("d").Topics != 1 {
		t.Fatal("draft save restarted the device")
	}

	d, _ := devs.Get(ctx, "d")
	s := settingsA("u")
	s.AllowWrite = true
	d.Config = s.Encode()
	if err := m.SaveDevice(ctx, *d); err != nil {
		t.Fatal(err)
	}
	stored, _ := devs.Get(ctx, "d")
	if ConfigDocument(stored.Config, SlotDraft) == nil || ConfigDocument(stored.Config, SlotApplied) == nil {
		t.Fatalf("documents lost by a settings update: %s", stored.Config)
	}
	if got, _ := ParseSettings(stored.Config); !got.AllowWrite {
		t.Fatal("settings not updated")
	}
	if err := m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if st := m.Status("d"); !st.Running || st.Topics != 1 {
		t.Fatalf("not resumed after settings change: %+v", st)
	}
}
