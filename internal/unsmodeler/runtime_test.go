package unsmodeler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/oahost/simhost"
	"monstermq.io/edge/internal/winccoabrowse"
)

type published struct {
	topic   string
	payload string
	retain  bool
	qos     byte
}

type capture struct {
	mu   sync.Mutex
	msgs []published
	ch   chan published
}

func newCapture() *capture { return &capture{ch: make(chan published, 1024)} }

func (c *capture) pub(topic string, payload []byte, retain bool, qos byte) error {
	p := published{topic, string(payload), retain, qos}
	c.mu.Lock()
	c.msgs = append(c.msgs, p)
	c.mu.Unlock()
	c.ch <- p
	return nil
}

// wait returns the next publication on topic within d.
func (c *capture) wait(t *testing.T, topic string, d time.Duration) published {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case p := <-c.ch:
			if p.topic == topic {
				return p
			}
		case <-deadline:
			t.Fatalf("no publication on %s", topic)
		}
	}
}

// waitValue returns the next publication on topic whose value is v.
func (c *capture) waitValue(t *testing.T, topic string, v any, d time.Duration) published {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case p := <-c.ch:
			if p.topic != topic {
				continue
			}
			var m map[string]any
			if json.Unmarshal([]byte(p.payload), &m) == nil && m["value"] == v {
				return p
			}
		case <-deadline:
			t.Fatalf("no publication of %v on %s", v, topic)
		}
	}
}

// none checks that nothing is published on topic within d.
func (c *capture) none(t *testing.T, topic string, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case p := <-c.ch:
			if p.topic == topic {
				t.Fatalf("unexpected publication %+v", p)
			}
		case <-deadline:
			return
		}
	}
}

func (c *capture) drain() {
	for {
		select {
		case <-c.ch:
		default:
			return
		}
	}
}

type rtEnv struct {
	oa   *winccoabrowse.Service
	sim  *simhost.Host
	cap  *capture
	rt   *RuntimeA
	gate *SwitchGate
	cnt  *Counters
}

func newRuntime(t *testing.T) *rtEnv {
	t.Helper()
	oa, sim := newSimOA(t)
	c := newCapture()
	g := &SwitchGate{}
	cnt := &Counters{}
	rt := NewRuntimeA("dev1", oa.API(), c.pub, g, cnt, slog.New(slog.DiscardHandler))
	t.Cleanup(rt.Stop)
	return &rtEnv{oa: oa, sim: sim, cap: c, rt: rt, gate: g, cnt: cnt}
}

func (e *rtEnv) apply(t *testing.T, s *Settings, tree string) *Expansion {
	t.Helper()
	res := runValidate(t, e.oa, s, docA(tree))
	if HasErrors(res.Issues) {
		t.Fatalf("issues: %v", res.Issues)
	}
	if err := e.rt.Apply(context.Background(), res.Expansion, s.AllowWrite); err != nil {
		t.Fatal(err)
	}
	return res.Expansion
}

func (e *rtEnv) set(t *testing.T, dpe string, v oahost.Value) {
	t.Helper()
	if err := e.sim.Set(dpe+":_original.._value", v); err != nil {
		t.Fatal(err)
	}
}

func fl(f float64) oahost.Value { return oahost.Value{Kind: oahost.KindFloat, Float: f} }

func payloadMap(t *testing.T, p published) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(p.payload), &m); err != nil {
		t.Fatalf("payload %q: %v", p.payload, err)
	}
	return m
}

func TestRuntimeBindings(t *testing.T) {
	e := newRuntime(t)
	e.set(t, "System1:Hall2_P101.flow", fl(1.5))
	e.apply(t, settingsA("acme"), `[{"name":"l","children":[
		{"name":"outlet","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}},
		{"name":"pump","binding":{"kind":"datapoint","type":"PumpType","datapoint":"Hall2_P101","elements":[{"path":"motor.speed"},{"path":"set"}]}},
		{"name":"pumps","binding":{"kind":"pattern","type":"PumpType","pattern":"Hall2_L2_P*","elements":[{"path":"flow"}]}}]}]`)
	// Initial values are published on apply.
	p := e.cap.wait(t, "acme/l/outlet", time.Second)
	if m := payloadMap(t, p); m["value"] != 1.5 || m["time"] == nil || len(m) != 2 {
		t.Fatalf("outlet %v", p.payload)
	}
	e.cap.drain()
	e.set(t, "System1:Hall2_P101.motor.speed", fl(1450))
	e.cap.waitValue(t, "acme/l/pump/motor/speed", 1450.0, time.Second)
	e.set(t, "System1:Hall2_P101.set", fl(7))
	e.cap.waitValue(t, "acme/l/pump/set", 7.0, time.Second)
	e.set(t, "System1:Hall2_L2_P2.flow", fl(3))
	e.cap.waitValue(t, "acme/l/pumps/Hall2_L2_P2/flow", 3.0, time.Second)
	// A datapoint created later is picked up by the running query.
	if err := e.sim.CreateDP("System1", "Hall2_L2_P7", "PumpType"); err != nil {
		t.Fatal(err)
	}
	e.set(t, "System1:Hall2_L2_P7.flow", fl(9))
	e.cap.waitValue(t, "acme/l/pumps/Hall2_L2_P7/flow", 9.0, time.Second)
}

func TestRuntimePayloadFields(t *testing.T) {
	e := newRuntime(t)
	if err := e.sim.SetMeta("System1:Hall2_P101.flow", "m³/h", "%.1f", "Outlet flow"); err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(e.sim.SetAttr("System1:Hall2_P101.flow:_pv_range.._type", oahost.Value{Kind: oahost.KindInt, Int: 7}))
	must(e.sim.SetAttr("System1:Hall2_P101.flow:_pv_range.._min", fl(0)))
	must(e.sim.SetAttr("System1:Hall2_P101.flow:_pv_range.._max", fl(200)))
	must(e.sim.SetAttr("System1:Hall2_P101.flow:_alert_hdl.2._u_limit", fl(180)))
	must(e.sim.SetAttr("System1:Hall2_P101.flow:_online.._invalid", oahost.Value{Kind: oahost.KindBool, Bool: true}))
	e.apply(t, settingsA("u"), `[{"name":"f","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow",
		"overrides":{"payload":["value","time","unit","description","format","pvRange","status",{"name":"inv","attribute":"_online.._invalid"},{"name":"hi","attribute":"_alert_hdl.2._u_limit"},{"name":"none","attribute":"_alert_hdl.3._u_limit"}]}}},
		{"name":"g","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P102","element":"flow","overrides":{"payload":["value","pvRange"]}}},
		{"name":"r","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"pressure","overrides":{"encoding":"raw"}}}]`)
	e.cap.drain()
	e.set(t, "System1:Hall2_P101.flow", fl(42.7))
	p := e.cap.waitValue(t, "u/f", 42.7, time.Second)
	want := `{"value":42.7,"time":`
	if !strings.HasPrefix(p.payload, want) {
		t.Fatalf("order %s", p.payload)
	}
	m := payloadMap(t, p)
	pv, _ := m["pvRange"].(map[string]any)
	if m["unit"] != "m³/h" || m["description"] != "Outlet flow" || m["format"] != "%.1f" || pv["min"] != 0.0 || pv["max"] != 200.0 ||
		m["status"] != float64(0x101) || m["inv"] != true || m["hi"] != 180.0 {
		t.Fatalf("payload %s", p.payload)
	}
	if _, ok := m["none"]; ok {
		t.Fatalf("unconfigured attribute published: %s", p.payload)
	}
	e.set(t, "System1:Hall2_P102.flow", fl(1))
	if m := payloadMap(t, e.cap.waitValue(t, "u/g", 1.0, time.Second)); m["pvRange"] != nil {
		t.Fatalf("pvRange without config: %v", m)
	}
	e.set(t, "System1:Hall2_P101.pressure", fl(math.NaN()))
	if p := e.cap.wait(t, "u/r", time.Second); p.payload != "null" {
		t.Fatalf("raw NaN: %s", p.payload)
	}
}

func TestRuntimeSemantics(t *testing.T) {
	e := newRuntime(t)
	e.apply(t, settingsA("u"), `[
		{"name":"rq","overrides":{"retain":true,"qos":1,"timestamp":"publish"},"binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}},
		{"name":"db","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"pressure","overrides":{"publishMode":"deadband","deadband":1}}},
		{"name":"mi","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P102","element":"flow","overrides":{"publishMode":"minInterval","intervalMs":300}}},
		{"name":"cy","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P102","element":"pressure","overrides":{"publishMode":"cyclic","intervalMs":100}}}]`)
	time.Sleep(100 * time.Millisecond)
	e.cap.drain()
	// retain, qos, timestamp publish
	before := time.Now().Add(-time.Millisecond)
	if err := e.sim.SetTimed("System1:Hall2_P101.flow:_original.._value", fl(5), time.Unix(1000, 0)); err != nil {
		t.Fatal(err)
	}
	p := e.cap.wait(t, "u/rq", time.Second)
	ts, _ := time.Parse(time.RFC3339, payloadMap(t, p)["time"].(string))
	if !p.retain || p.qos != 1 || ts.Before(before.Truncate(time.Millisecond)) {
		t.Fatalf("rq %+v", p)
	}
	// deadband: 0.5 is suppressed, 1.2 published
	e.set(t, "System1:Hall2_P101.pressure", fl(10))
	e.cap.wait(t, "u/db", time.Second)
	e.set(t, "System1:Hall2_P101.pressure", fl(10.5))
	e.cap.none(t, "u/db", 150*time.Millisecond)
	e.set(t, "System1:Hall2_P101.pressure", fl(11.2))
	e.cap.waitValue(t, "u/db", 11.2, time.Second)
	// minInterval: the second change within the interval is delayed and
	// only the latest is published.
	e.set(t, "System1:Hall2_P102.flow", fl(1))
	e.cap.wait(t, "u/mi", time.Second)
	start := time.Now()
	e.set(t, "System1:Hall2_P102.flow", fl(2))
	e.set(t, "System1:Hall2_P102.flow", fl(3))
	p = e.cap.wait(t, "u/mi", time.Second)
	if m := payloadMap(t, p); m["value"] != 3.0 || time.Since(start) < 150*time.Millisecond {
		t.Fatalf("minInterval %v after %v", p.payload, time.Since(start))
	}
	// cyclic: publishes the current value without change
	e.cap.wait(t, "u/cy", time.Second)
	e.cap.wait(t, "u/cy", time.Second)
}

func TestRuntimeWriteThrough(t *testing.T) {
	e := newRuntime(t)
	s := settingsA("u")
	e.apply(t, s, `[{"name":"f","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}}]`)
	if h, err := e.rt.Write("u/f", []byte(`1`)); !h || !errors.Is(err, ErrWriteDisabled) {
		t.Fatalf("disabled: %v %v", h, err)
	}
	if h, _ := e.rt.Write("u/other", []byte(`1`)); h {
		t.Fatal("foreign topic handled")
	}
	s.AllowWrite = true
	e.apply(t, s, `[{"name":"f","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}}]`)
	if h, err := e.rt.Write("u/f", []byte(`{"value": 12.5}`)); !h || err != nil {
		t.Fatalf("write: %v %v", h, err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		v, _ := e.sim.Get("System1:Hall2_P101.flow:_online.._value")
		if v.Float == 12.5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not written: %+v", v)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if h, err := e.rt.Write("u/f", []byte(`"text"`)); !h || err == nil {
		t.Fatalf("type mismatch accepted: %v", err)
	}
	e.gate.Set(false)
	if _, err := e.rt.Write("u/f", []byte(`1`)); !errors.Is(err, ErrStandby) {
		t.Fatalf("standby write: %v", err)
	}
}

func TestRuntimeReapply(t *testing.T) {
	e := newRuntime(t)
	s := settingsA("u")
	e.apply(t, s, `[{"name":"a","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}},
		{"name":"b","overrides":{"retain":true},"binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P102","element":"flow"}},
		{"name":"p","binding":{"kind":"pattern","type":"PumpType","pattern":"Hall2_L2_P*","elements":[{"path":"flow"}]}}]`)
	e.cap.wait(t, "u/b", time.Second)
	conns, queries := e.sim.Connections(), e.sim.Queries()
	if conns != 1 || queries != 1 {
		t.Fatalf("connections %d queries %d", conns, queries)
	}
	e.cap.drain()
	calls := e.sim.Calls(oahost.OpDpConnect)
	// b removed, c added, a and the pattern unchanged.
	e.apply(t, s, `[{"name":"a","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}},
		{"name":"c","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"pressure"}},
		{"name":"p","binding":{"kind":"pattern","type":"PumpType","pattern":"Hall2_L2_P*","elements":[{"path":"flow"}]}}]`)
	clear := e.cap.wait(t, "u/b", time.Second)
	if clear.payload != "" || !clear.retain {
		t.Fatalf("retained not cleared: %+v", clear)
	}
	if e.sim.Calls(oahost.OpQueryConnect) != 1 {
		t.Fatal("unchanged pattern re-registered")
	}
	if e.sim.Calls(oahost.OpDpConnect)-calls < 1 {
		t.Fatal("added DPE not connected")
	}
	e.cap.drain()
	e.set(t, "System1:Hall2_P101.flow", fl(4))
	e.cap.wait(t, "u/a", time.Second)
	e.set(t, "System1:Hall2_P102.flow", fl(4))
	e.cap.none(t, "u/b", 150*time.Millisecond)
	e.rt.Stop()
	if e.sim.Connections() != 0 || e.sim.Queries() != 0 {
		t.Fatalf("not released: %d %d", e.sim.Connections(), e.sim.Queries())
	}
	if e.cnt.DPEs.Load() != 0 {
		t.Fatal("dpes counter")
	}
}

func TestRuntimeRoleGate(t *testing.T) {
	e := newRuntime(t)
	e.apply(t, settingsA("u"), `[{"name":"a","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}}]`)
	e.cap.wait(t, "u/a", time.Second)
	e.gate.Set(false)
	e.set(t, "System1:Hall2_P101.flow", fl(8))
	e.cap.none(t, "u/a", 150*time.Millisecond)
	if e.sim.Connections() != 1 {
		t.Fatal("connections dropped on standby")
	}
	e.gate.Set(true)
	e.cap.waitValue(t, "u/a", 8.0, time.Second)
	e.set(t, "System1:Hall2_P101.flow", fl(9))
	e.cap.wait(t, "u/a", time.Second)
}

func TestRuntimeCollision(t *testing.T) {
	e := newRuntime(t)
	e.apply(t, settingsA("u"), `[{"name":"p","binding":{"kind":"pattern","type":"PumpType","pattern":"Hall2_L2_P*","elements":[{"path":"flow"}]},
		"children":[{"name":"Hall2_L2_P5","children":[{"name":"flow","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}}]}]}]`)
	e.cap.drain()
	before := e.cnt.Errors.Load()
	if err := e.sim.CreateDP("System1", "Hall2_L2_P5", "PumpType"); err != nil {
		t.Fatal(err)
	}
	e.set(t, "System1:Hall2_L2_P5.flow", fl(1))
	time.Sleep(150 * time.Millisecond)
	if e.cnt.Errors.Load() != before+1 {
		t.Fatalf("collision not counted")
	}
	e.set(t, "System1:Hall2_P101.flow", fl(2))
	e.cap.waitValue(t, "u/p/Hall2_L2_P5/flow", 2.0, time.Second)
}

func TestPatternQuery(t *testing.T) {
	spec := &PatternSpec{Type: "PumpType", Pattern: "Hall2_{A,B}*", System: "System2", Elements: []PatternElement{
		{Path: "flow", Sem: BuiltinSemantics()},
		{Path: "motor.speed", Sem: BuiltinSemantics().Apply(&Overrides{Payload: []PayloadField{{Field: FieldValue}, {Field: FieldStatus}}})},
	}}
	got := patternQuery(spec)
	want := `SELECT '_online.._value', '_online.._stime', '_online.._status' FROM '{Hall2_A*.flow,Hall2_A*.motor.speed,Hall2_B*.flow,Hall2_B*.motor.speed}' REMOTE 'System2' WHERE _DPT = "PumpType"`
	if got != want {
		t.Fatalf("query\n got %s\nwant %s", got, want)
	}
	spec = &PatternSpec{Type: "Scalar", Pattern: "Tag*", Elements: []PatternElement{{Path: "", Sem: BuiltinSemantics()}}}
	if got := patternQuery(spec); got != `SELECT '_online.._value', '_online.._stime' FROM 'Tag*.' WHERE _DPT = "Scalar"` {
		t.Fatal(got)
	}
}
