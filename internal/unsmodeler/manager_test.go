package unsmodeler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"monstermq.io/edge/internal/stores"
)

type memDevices struct {
	mu sync.Mutex
	m  map[string]stores.DeviceConfig
}

func (s *memDevices) GetAll(context.Context) ([]stores.DeviceConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []stores.DeviceConfig
	for _, d := range s.m {
		out = append(out, d)
	}
	return out, nil
}
func (s *memDevices) GetByType(ctx context.Context, t string) ([]stores.DeviceConfig, error) {
	all, _ := s.GetAll(ctx)
	var out []stores.DeviceConfig
	for _, d := range all {
		if d.Type == t {
			out = append(out, d)
		}
	}
	return out, nil
}
func (s *memDevices) GetByNode(ctx context.Context, _ string) ([]stores.DeviceConfig, error) {
	return s.GetAll(ctx)
}
func (s *memDevices) GetEnabledByNode(ctx context.Context, _ string) ([]stores.DeviceConfig, error) {
	return s.GetAll(ctx)
}
func (s *memDevices) Get(_ context.Context, name string) (*stores.DeviceConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.m[name]; ok {
		return &d, nil
	}
	return nil, nil
}
func (s *memDevices) Save(_ context.Context, d stores.DeviceConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[d.Name] = d
	return nil
}
func (s *memDevices) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, name)
	return nil
}
func (s *memDevices) Toggle(context.Context, string, bool) (*stores.DeviceConfig, error) {
	return nil, nil
}
func (s *memDevices) Reassign(context.Context, string, string) (*stores.DeviceConfig, error) {
	return nil, nil
}
func (s *memDevices) Close() error { return nil }

type memDocs struct {
	mu sync.Mutex
	m  map[string]json.RawMessage
}

func (d *memDocs) Get(_ context.Context, device, slot string) (json.RawMessage, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.m[device+"/"+slot], nil
}
func (d *memDocs) Save(_ context.Context, device, slot string, doc json.RawMessage) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.m[device+"/"+slot] = doc
	return nil
}
func (d *memDocs) Delete(_ context.Context, device string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.m, device+"/draft")
	delete(d.m, device+"/applied")
	return nil
}
func (d *memDocs) Close() error { return nil }

func newTestManager(t *testing.T, gate RoleGate) (*Manager, *memDevices, *memDocs, *capture) {
	t.Helper()
	oa, _ := newSimOA(t)
	devs := &memDevices{m: map[string]stores.DeviceConfig{}}
	docs := &memDocs{m: map[string]json.RawMessage{}}
	c := newCapture()
	m := NewManager(devs, docs, oa, c.pub, gate, TopicRoots{Native: "winccoa"}, "n1", slog.New(slog.DiscardHandler))
	t.Cleanup(m.Stop)
	return m, devs, docs, c
}

func TestManagerApplyAndStandby(t *testing.T) {
	g := &SwitchGate{}
	m, devs, docs, _ := newTestManager(t, g)
	ctx := context.Background()
	s := settingsA("u")
	_ = devs.Save(ctx, stores.DeviceConfig{Name: "d", Type: DeviceType, Enabled: true, NodeID: "n1", Config: s.Encode()})
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	doc, issues, err := m.SaveDraft(ctx, "d", docA(`[{"name":"a","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}}]`))
	if err != nil || HasErrors(issues) {
		t.Fatalf("save %v %v", err, issues)
	}
	parsed, _ := ParseDocument(doc)
	if parsed.Tree[0].ID == "" {
		t.Fatal("no id assigned")
	}
	g.Set(false)
	if _, err := m.Apply(ctx, "d"); !errors.Is(err, ErrStandby) {
		t.Fatalf("apply on standby: %v", err)
	}
	if a, _ := docs.Get(ctx, "d", stores.UnsSlotApplied); a != nil {
		t.Fatal("applied on standby")
	}
	g.Set(true)
	if issues, err := m.Apply(ctx, "d"); err != nil || HasErrors(issues) {
		t.Fatalf("apply: %v %v", err, issues)
	}
	if st := m.Status("d"); !st.Running || st.Topics != 1 {
		t.Fatalf("status %+v", st)
	}
	// Oversized and broken documents are not stored.
	if _, _, err := m.SaveDraft(ctx, "d", []byte(`{"tree":[`)); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("broken JSON: %v", err)
	}
	big := make([]byte, MaxDocumentBytes+1)
	for i := range big {
		big[i] = ' '
	}
	if _, _, err := m.SaveDraft(ctx, "d", big); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("oversized: %v", err)
	}
}

func TestAssignNodeIDs(t *testing.T) {
	out, n, ok := AssignNodeIDs([]byte(`{"schemaVersion":1,"tree":[{"name":"a","children":[{"id":"keep","name":"b"},{"name":"c","id":5}]}],"x":1.0}`))
	if !ok || n != 2 {
		t.Fatalf("assigned %d %v", n, ok)
	}
	var doc struct {
		Tree []struct {
			ID       string `json:"id"`
			Children []struct {
				ID any `json:"id"`
			} `json:"children"`
		} `json:"tree"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Tree[0].ID == "" || doc.Tree[0].Children[0].ID != "keep" || doc.Tree[0].Children[1].ID == 5.0 {
		t.Fatalf("ids %s", out)
	}
	if string(out[len(out)-8:]) != `"x":1.0}` {
		t.Fatalf("number spelling or order changed: %s", out)
	}
}
