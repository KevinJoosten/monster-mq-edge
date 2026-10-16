package unsmodeler

import (
	"context"
	"strings"
	"testing"
	"time"

	"monstermq.io/edge/internal/oahost"
)

func TestRuntimeObject(t *testing.T) {
	e := newRuntime(t)
	e.set(t, "System1:Hall2_P101.flow", fl(1.5))
	e.set(t, "System1:Hall2_P101.motor.speed", fl(1450))
	s := settingsA("acme")
	s.AllowWrite = true
	e.apply(t, s, `[{"name":"pump","binding":{"kind":"datapoint","type":"PumpType","datapoint":"Hall2_P101","layout":"object",
		"elements":[{"path":"flow"},{"path":"motor.speed"},{"path":"set","overrides":{"encoding":"raw"}}]}}]`)
	// One list dpConnect for the whole datapoint.
	if n := e.sim.Connections(); n != 1 {
		t.Fatalf("connections %d, want 1", n)
	}
	p := e.cap.wait(t, "acme/pump", time.Second)
	if !strings.HasPrefix(p.payload, `{"flow":{"value":1.5,"time":`) || !strings.Contains(p.payload, `"motor":{"speed":{"value":1450,"time":`) || !strings.HasSuffix(p.payload, `"set":0}`) {
		t.Fatalf("object payload %s", p.payload)
	}
	e.cap.none(t, "acme/pump", 100*time.Millisecond)

	// A set of several elements publishes the object once.
	if err := e.oa.API().DpSet(context.Background(), []string{"System1:Hall2_P101.flow:_original.._value", "System1:Hall2_P101.motor.speed:_original.._value"},
		[]oahost.Value{fl(2), fl(1500)}, time.Second); err != nil {
		t.Fatal(err)
	}
	m := payloadMap(t, e.cap.wait(t, "acme/pump", time.Second))
	if m["flow"].(map[string]any)["value"] != 2.0 || m["motor"].(map[string]any)["speed"].(map[string]any)["value"] != 1500.0 {
		t.Fatalf("after set %v", m)
	}
	e.cap.none(t, "acme/pump", 100*time.Millisecond)

	// The live monitor shows one row with the element values.
	var st *TopicStatus
	for _, s := range e.rt.Status() {
		if s.Topic == "acme/pump" {
			st = &s
		}
	}
	if st == nil || st.Published != 2 || st.DPE != "System1:Hall2_P101." {
		t.Fatalf("status %+v", st)
	}
	if v := st.Value.(map[string]any); v["flow"] != 2.0 || v["set"] != 0.0 {
		t.Fatalf("status value %v", st.Value)
	}
	if got := e.rt.Topics(); len(got) != 1 || got[0] != "acme/pump" {
		t.Fatalf("topics %v", got)
	}

	// ..set accepts a partial object of selected elements.
	if h, err := e.rt.Write("acme/pump", []byte(`{"motor":{"speed":{"value":900}},"set":3}`)); !h || err != nil {
		t.Fatalf("write: %v %v", h, err)
	}
	m = payloadMap(t, e.cap.wait(t, "acme/pump", time.Second))
	if m["set"] != 3.0 || m["motor"].(map[string]any)["speed"].(map[string]any)["value"] != 900.0 {
		t.Fatalf("after write %v", m)
	}
	for _, bad := range []string{`{"pressure":1}`, `{"motor":{"torque":1}}`, `5`, `{}`} {
		if h, err := e.rt.Write("acme/pump", []byte(bad)); !h || err == nil {
			t.Fatalf("write %s accepted", bad)
		}
	}
}

func TestRuntimeObjectPattern(t *testing.T) {
	e := newRuntime(t)
	e.apply(t, settingsA("acme"), `[{"name":"pumps","overrides":{"encoding":"raw"},"binding":{"kind":"pattern","type":"PumpType","pattern":"Hall2_L2_P*","layout":"object",
		"elements":[{"path":"flow"},{"path":"pressure"}]}}]`)
	e.cap.drain()
	e.set(t, "System1:Hall2_L2_P2.flow", fl(3))
	for {
		p := e.cap.wait(t, "acme/pumps/Hall2_L2_P2", time.Second)
		if p.payload == `{"flow":3,"pressure":0}` {
			break
		}
	}
	// A datapoint created later gets its own object topic.
	if err := e.sim.CreateDP("System1", "Hall2_L2_P7", "PumpType"); err != nil {
		t.Fatal(err)
	}
	e.set(t, "System1:Hall2_L2_P7.pressure", fl(9))
	for {
		p := e.cap.wait(t, "acme/pumps/Hall2_L2_P7", time.Second)
		if p.payload == `{"flow":0,"pressure":9}` {
			break
		}
	}
}

func TestValidateObjectLayout(t *testing.T) {
	oa, _ := newSimOA(t)
	res := runValidate(t, oa, settingsA("acme"), docA(`[{"name":"pump","binding":{"kind":"datapoint","type":"PumpType","datapoint":"Hall2_P101","layout":"object",
		"elements":[{"path":"flow","topic":"f"},{"path":"flow"}]}}]`))
	want := map[string]string{
		"/tree/0/binding/elements/0/topic": "element topic is ignored",
		"/tree/0/binding/elements/1/path":  "selected twice",
	}
	for path, msg := range want {
		found := false
		for _, is := range res.Issues {
			if is.Path == path && strings.Contains(is.Message, msg) {
				found = true
			}
		}
		if !found {
			t.Errorf("no issue %s %q in %v", path, msg, res.Issues)
		}
	}
	res = runValidate(t, oa, settingsA("acme"), docA(`[{"name":"one","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow","layout":"object"}}]`))
	if len(res.Issues) != 1 || res.Issues[0].Path != "/tree/0/binding/layout" {
		t.Errorf("layout on an element binding: %v", res.Issues)
	}
	// An object topic does not conflict with itself.
	res = runValidate(t, oa, settingsA("acme"), docA(`[{"name":"pump","binding":{"kind":"datapoint","type":"PumpType","datapoint":"Hall2_P101","layout":"object"}}]`))
	if HasErrors(res.Issues) {
		t.Fatalf("issues %v", res.Issues)
	}
}
