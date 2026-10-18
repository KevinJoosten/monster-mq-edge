package winccoabrowse_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/oahost/simhost"
	"monstermq.io/edge/internal/winccoabrowse"
)

func newBrowse(t *testing.T) (*winccoabrowse.Service, *simhost.Host) {
	t.Helper()
	sim := simhost.New("System1", 256)
	client := oahost.NewClient(sim, oahost.Limits{MaxPending: 256, EventQueue: 1024, DefaultTimeout: 2 * time.Second})
	sim.Attach(client)
	t.Cleanup(func() { client.Close(); sim.Close() })
	f := uint32(oahost.KindFloat)
	sim.AddType(simhost.Type{Name: "PumpType", Elements: map[string]uint32{
		"":            simhost.ElemStruct,
		"flow":        f,
		"motor":       simhost.ElemStruct,
		"motor.speed": f,
		"motor.on":    uint32(oahost.KindBool),
		"count":       oahost.ElemUnsupported,
	}, Order: []string{"flow", "motor", "motor.speed", "motor.on", "count"}, OaTypes: map[string]uint32{"count": 54}})
	sim.AddType(simhost.Type{Name: "Scalar", Elements: map[string]uint32{"": f}})
	sim.AddType(simhost.Type{Name: "MMQConfigs", Elements: map[string]uint32{"": simhost.ElemStruct, "config": uint32(oahost.KindString)}})
	sim.AddType(simhost.Type{Name: "_Internal", Elements: map[string]uint32{"": f}})
	sim.AddType(simhost.Type{Name: "_NGA_Group", Elements: map[string]uint32{
		"":            simhost.ElemStruct,
		"isAlert":     uint32(oahost.KindBool),
		"displayName": uint32(oahost.KindString),
	}})
	sim.AddType(simhost.Type{Name: "_ValueArchive", Elements: map[string]uint32{
		"":               simhost.ElemStruct,
		"state":          uint32(oahost.KindInt),
		"general":        simhost.ElemStruct,
		"general.arName": uint32(oahost.KindString),
	}})
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 25; i++ {
		must(sim.CreateDP("System1", fmt.Sprintf("P%02d", i), "PumpType"))
	}
	must(sim.CreateDP("System1", "Tag1", "Scalar"))
	must(sim.CreateDP("System1", "_Hidden", "Scalar"))
	must(sim.CreateDP("System1", "MMQConfigs_k1", "MMQConfigs"))
	must(sim.CreateDP("System1", "_NGA_G_EVENT", "_NGA_Group"))
	must(sim.Set("System1:_NGA_G_EVENT.isAlert", oahost.Value{Kind: oahost.KindBool, Bool: false}))
	must(sim.Set("System1:_NGA_G_EVENT.displayName", oahost.Value{Kind: oahost.KindString, Str: "0=Event Archive"}))
	must(sim.CreateDP("System1", "_NGA_G_ALERT", "_NGA_Group"))
	must(sim.Set("System1:_NGA_G_ALERT.isAlert", oahost.Value{Kind: oahost.KindBool, Bool: true}))
	must(sim.CreateDP("System1", "_NGA_G_RAW", "_NGA_Group")) // uninitialized attributes
	must(sim.CreateDP("System1", "_NGA_G_EVENT_2", "_NGA_Group"))
	must(sim.CreateDP("System1", "_ValueArchive_0", "_ValueArchive"))
	must(sim.Set("System1:_ValueArchive_0.state", oahost.Value{Kind: oahost.KindInt, Int: 1}))
	must(sim.Set("System1:_ValueArchive_0.general.arName", oahost.Value{Kind: oahost.KindString, Str: "0=ValueArchive_0"}))
	must(sim.CreateDP("System1", "_ValueArchive_1", "_ValueArchive"))
	must(sim.Set("System1:_ValueArchive_1.state", oahost.Value{Kind: oahost.KindInt, Int: 3})) // deleted
	must(sim.CreateDP("System1", "_ValueArchive_0_2", "_ValueArchive"))
	sim.AddSystem("Remote", true)
	must(sim.CreateDP("Remote", "R1", "Scalar"))
	sim.AddSystem("Down", false)
	sim.AnnounceSystems()
	must(sim.SetMeta("System1:P00.flow", "m³/h", "%.1f", "Outlet flow"))
	must(sim.Set("System1:P00.flow:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 42.5}))
	// The snapshot event is delivered asynchronously.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if m, known := client.Systems(); known && len(m) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("systems snapshot not delivered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return winccoabrowse.New(client), sim
}

func TestSystems(t *testing.T) {
	s, _ := newBrowse(t)
	got, err := s.Systems(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != (winccoabrowse.System{Name: "System1", Local: true, Connected: true}) || got[1].Name != "Remote" || !got[1].Connected {
		t.Fatalf("systems = %+v", got)
	}
	if _, _, err := s.System(context.Background(), "Down"); !errors.Is(err, winccoabrowse.ErrUnavailable) {
		t.Fatalf("unknown system: %v", err)
	}
}

func TestTypes(t *testing.T) {
	s, _ := newBrowse(t)
	got, err := s.Types(context.Background(), "", "*")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[PumpType Scalar]" {
		t.Fatalf("types = %v", got)
	}
	got, _ = s.Types(context.Background(), "", "Pump*")
	if fmt.Sprint(got) != "[PumpType]" {
		t.Fatalf("pattern types = %v", got)
	}
}

func TestTypeStructure(t *testing.T) {
	s, _ := newBrowse(t)
	ts, err := s.TypeStructure(context.Background(), "", "PumpType")
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, e := range ts.Elements {
		paths = append(paths, fmt.Sprintf("%s:%s:%d", e.Path, e.ElementType, e.Depth))
	}
	want := "[:struct:0 flow:float:1 motor:struct:1 motor.speed:float:2 motor.on:bool:2 count:long:1]"
	if fmt.Sprint(paths) != want {
		t.Fatalf("structure = %v", paths)
	}
	if ts.Elements[0].Children != 3 || ts.Elements[3].Parent != "motor" || ts.Elements[3].Name != "speed" {
		t.Fatalf("tree fields: %+v", ts.Elements)
	}
	if _, err := s.TypeStructure(context.Background(), "", "Nope"); !errors.Is(err, winccoabrowse.ErrNotFound) {
		t.Fatalf("unknown type: %v", err)
	}
	if _, err := s.TypeStructure(context.Background(), "", "MMQConfigs"); !errors.Is(err, winccoabrowse.ErrNotFound) {
		t.Fatalf("store type: %v", err)
	}
}

func TestDatapointsPaging(t *testing.T) {
	s, _ := newBrowse(t)
	ctx := context.Background()
	seen := map[string]bool{}
	for off := 0; ; off += 10 {
		p, err := s.Datapoints(ctx, "", "*", "", 10, off)
		if err != nil {
			t.Fatal(err)
		}
		if p.Total != 26 {
			t.Fatalf("total = %d", p.Total)
		}
		for _, d := range p.Items {
			if seen[d.Name] {
				t.Fatalf("duplicate %s", d.Name)
			}
			seen[d.Name] = true
			if d.Type == "" || d.System != "System1" {
				t.Fatalf("item %+v", d)
			}
		}
		if len(p.Items) < 10 {
			break
		}
	}
	if len(seen) != 26 || seen["_Hidden"] || seen["MMQConfigs_k1"] {
		t.Fatalf("seen %v", seen)
	}
	p, _ := s.Datapoints(ctx, "", "*", "", 5000, 0)
	if p.Limit != winccoabrowse.MaxLimit {
		t.Fatalf("limit not capped: %d", p.Limit)
	}
	p, _ = s.Datapoints(ctx, "", "P1*", "PumpType", 0, 0)
	if p.Total != 10 {
		t.Fatalf("typed pattern total = %d", p.Total)
	}
	p, err := s.Datapoints(ctx, "Remote", "*", "", 0, 0)
	if err != nil || p.Total != 1 || p.Items[0].Name != "R1" || p.Items[0].System != "Remote" {
		t.Fatalf("remote = %+v %v", p, err)
	}
	if _, err := s.Datapoints(ctx, "Down", "*", "", 0, 0); !errors.Is(err, winccoabrowse.ErrUnavailable) {
		t.Fatalf("down system: %v", err)
	}
	if _, err := s.Datapoints(ctx, "Unknown", "*", "", 0, 0); !errors.Is(err, winccoabrowse.ErrUnavailable) {
		t.Fatalf("unknown system: %v", err)
	}
}

func TestElementsAndInfo(t *testing.T) {
	s, _ := newBrowse(t)
	ctx := context.Background()
	els, err := s.Elements(ctx, "P00")
	if err != nil {
		t.Fatal(err)
	}
	if len(els) != 6 || els[0].DPE != "System1:P00." || els[0].Name != "P00" || els[3].DPE != "System1:P00.motor.speed" {
		t.Fatalf("elements = %+v", els)
	}
	infos, err := s.ElementInfo(ctx, []string{"P00.flow", "System1:P00.motor", "P00.nope", "Tag1", "Down:X.a"})
	if err != nil {
		t.Fatal(err)
	}
	f := infos[0]
	if !f.Exists || f.Unit != "m³/h" || f.Format != "%.1f" || f.Description != "Outlet flow" || f.Value != 42.5 || f.ElementType != "float" || f.Type != "PumpType" || f.Status == 0 {
		t.Fatalf("flow = %+v", f)
	}
	if !infos[1].Exists || infos[1].ValueElement || infos[1].HasValue {
		t.Fatalf("struct = %+v", infos[1])
	}
	if infos[2].Exists || infos[2].Error == "" {
		t.Fatalf("missing = %+v", infos[2])
	}
	if !infos[3].Exists || infos[3].DPE != "System1:Tag1." || !infos[3].HasValue {
		t.Fatalf("scalar = %+v", infos[3])
	}
	if infos[4].Exists || infos[4].Error == "" {
		t.Fatalf("down = %+v", infos[4])
	}
	many := make([]string, winccoabrowse.MaxElementInfoDPEs+1)
	for i := range many {
		many[i] = "Tag1"
	}
	if _, err := s.ElementInfo(ctx, many); !errors.Is(err, winccoabrowse.ErrInvalid) {
		t.Fatalf("too many: %v", err)
	}
}

func TestArchiveGroups(t *testing.T) {
	s, _ := newBrowse(t)
	ctx := context.Background()
	groups, err := s.ArchiveGroups(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 3 {
		t.Fatalf("expected 3 archive groups, got %d: %+v", len(groups), groups)
	}
	// Groups sorted by Kind then Name: "HDB" then "NGA"
	if groups[0].Kind != "HDB" || groups[0].Name != "ValueArchive_0" || groups[0].DpName != "_ValueArchive_0" {
		t.Fatalf("groups[0] = %+v", groups[0])
	}
	if groups[1].Kind != "NGA" || groups[1].Name != "EVENT" || groups[1].DpName != "_NGA_G_EVENT" || groups[1].DisplayName != "Event Archive" {
		t.Fatalf("groups[1] = %+v", groups[1])
	}
	if groups[2].Kind != "NGA" || groups[2].Name != "RAW" || groups[2].DpName != "_NGA_G_RAW" || groups[2].DisplayName != "RAW" {
		t.Fatalf("groups[2] = %+v", groups[2])
	}

	tests := []struct {
		ref  string
		want string
	}{
		{"EVENT", "_NGA_G_EVENT"},
		{"event", "_NGA_G_EVENT"},
		{"_NGA_G_EVENT", "_NGA_G_EVENT"},
		{"Event Archive", "_NGA_G_EVENT"},
		{"event archive", "_NGA_G_EVENT"},
		{"System1:_NGA_G_EVENT.", "_NGA_G_EVENT"},
		{"ValueArchive_0", "_ValueArchive_0"},
		{"_ValueArchive_0", "_ValueArchive_0"},
		{"NONEXISTENT", ""},
		{"", ""},
	}
	for _, tc := range tests {
		got := winccoabrowse.ResolveArchiveGroup(groups, tc.ref)
		if got != tc.want {
			t.Errorf("ResolveArchiveGroup(%q) = %q, want %q", tc.ref, got, tc.want)
		}
	}
}

