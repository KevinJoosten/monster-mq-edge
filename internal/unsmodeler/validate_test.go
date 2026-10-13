package unsmodeler

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/oahost/simhost"
	"monstermq.io/edge/internal/winccoabrowse"
)

// newSimOA returns a browse service on a simulated host with the types and
// datapoints the validator tests use.
func newSimOA(t *testing.T) (*winccoabrowse.Service, *simhost.Host) {
	t.Helper()
	sim := simhost.New("System1", 256)
	client := oahost.NewClient(sim, oahost.Limits{MaxPending: 256, EventQueue: 4096, DefaultTimeout: 2 * time.Second})
	sim.Attach(client)
	t.Cleanup(func() { client.Close(); sim.Close() })
	f := uint32(oahost.KindFloat)
	sim.AddType(simhost.Type{Name: "PumpType", Elements: map[string]uint32{
		"":            simhost.ElemStruct,
		"flow":        f,
		"pressure":    f,
		"motor":       simhost.ElemStruct,
		"motor.speed": f,
		"state":       uint32(oahost.KindString),
		"label":       uint32(oahost.KindLangText),
		"history":     uint32(oahost.KindDyn),
		"ref":         oahost.ElemUnsupported,
		"set":         f,
		"a/b":         f,
	}, Order: []string{"flow", "pressure", "motor", "motor.speed", "state", "label", "history", "ref", "set", "a/b"}, OaTypes: map[string]uint32{"ref": 27}})
	sim.AddType(simhost.Type{Name: "Scalar", Elements: map[string]uint32{"": f}})
	sim.AddType(simhost.Type{Name: "MMQConfigs", Elements: map[string]uint32{"": simhost.ElemStruct, "config": uint32(oahost.KindString)}})
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, dp := range []string{"Hall2_P101", "Hall2_P102", "Hall2_L2_P1", "Hall2_L2_P2", "_Internal"} {
		must(sim.CreateDP("System1", dp, "PumpType"))
	}
	must(sim.CreateDP("System1", "Tag1", "Scalar"))
	must(sim.CreateDP("System1", "MMQConfigs_k1", "MMQConfigs"))
	sim.AddSystem("System2", true)
	must(sim.CreateDP("System2", "Hall2_L2_P9", "PumpType"))
	sim.AddSystem("System3", true)
	sim.AnnounceSystems()
	sim.SetSystemAvailable("System3", false)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if m, known := client.Systems(); known && len(m) == 2 && !m["System3"] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("systems snapshot not delivered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return winccoabrowse.New(client), sim
}

func settingsA(unsRoot string) *Settings {
	s := &Settings{Direction: DirectionWinccoaToUns, UnsRoot: unsRoot}
	s.applyDefaults()
	return s
}

func docA(tree string) []byte {
	return []byte(`{"schemaVersion":1,"direction":"winccoa_to_uns","tree":` + tree + `}`)
}

func runValidate(t *testing.T, oa OA, s *Settings, doc []byte) *Result {
	t.Helper()
	env := ValidationEnv{Settings: s, Roots: TopicRoots{Native: "winccoa"}}
	if oa != nil {
		env.OA = oa
	}
	res, err := Validate(context.Background(), doc, env)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// hasIssue reports an issue of severity at path whose message contains text.
func hasIssue(issues []Issue, sev, path, text string) bool {
	for _, is := range issues {
		if is.Severity == sev && is.Path == path && strings.Contains(is.Message, text) {
			return true
		}
	}
	return false
}

func topics(exp *Expansion) []string {
	var out []string
	for _, t := range exp.Targets {
		out = append(out, t.Topic+"="+t.DPE())
	}
	return out
}

func TestDocumentRoundTrip(t *testing.T) {
	for _, f := range testdataFiles(t, "valid") {
		raw := readTestdata(t, "valid", f[strings.LastIndex(f, "/")+1:])
		doc, err := ParseDocument(raw)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		enc, err := doc.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if issues := validate(t, enc); issues != nil {
			t.Fatalf("%s: re-encoded document invalid: %v\n%s", f, issues, enc)
		}
		var a, b any
		_ = json.Unmarshal(raw, &a)
		_ = json.Unmarshal(enc, &b)
		if !reflect.DeepEqual(stripEmpty(a), stripEmpty(b)) {
			t.Errorf("%s: round trip differs\n%s", f, enc)
		}
	}
}

// stripEmpty drops empty arrays, false booleans and empty strings that
// the typed model omits.
func stripEmpty(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			switch ev := e.(type) {
			case []any:
				if len(ev) == 0 && k != "tree" && k != "types" && k != "datapoints" {
					continue
				}
			case bool:
				if !ev && (k == "aiGenerated" || k == "reuse" || k == "excluded" || k == "allowWrite") {
					continue
				}
			case string:
				if ev == "" && (k == "description" || k == "system" || k == "level") {
					continue
				}
			case float64:
				out[k] = ev
				continue
			}
			out[k] = stripEmpty(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = stripEmpty(e)
		}
		return out
	}
	return v
}

func TestExpandBindings(t *testing.T) {
	oa, _ := newSimOA(t)
	doc := docA(`[{"id":"n1","name":"line-1","children":[
		{"id":"n2","name":"outlet","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow"}},
		{"id":"n3","name":"pump","binding":{"kind":"datapoint","type":"PumpType","datapoint":"Hall2_P101","elements":[
			{"path":"flow"},{"path":"motor.speed","topic":"speed"},{"path":"motor.speed","topic":"x/y"},{"path":"set"},{"path":"a/b"},{"path":"flow","topic":"extra"}]}},
		{"id":"n4","name":"pumps","binding":{"kind":"pattern","type":"PumpType","pattern":"Hall2_L2_P*","elements":[{"path":"flow"},{"path":"pressure","topic":""}]}},
		{"id":"n5","name":"remote","binding":{"kind":"pattern","type":"PumpType","system":"System2","pattern":"*","elements":[{"path":"flow"}]}},
		{"id":"n6","name":"tag","binding":{"kind":"datapoint","type":"Scalar","datapoint":"Tag1"}}
	]}]`)
	res := runValidate(t, oa, settingsA("acme/hall2"), doc)
	if HasErrors(res.Issues) {
		t.Fatalf("issues: %v", res.Issues)
	}
	want := []string{
		"acme/hall2/line-1/outlet=System1:Hall2_P101.flow",
		"acme/hall2/line-1/pump/flow=System1:Hall2_P101.flow",
		"acme/hall2/line-1/pump/speed=System1:Hall2_P101.motor.speed",
		"acme/hall2/line-1/pump/x/y=System1:Hall2_P101.motor.speed",
		"acme/hall2/line-1/pump/set=System1:Hall2_P101.set",
		"acme/hall2/line-1/pump/a%2Fb=System1:Hall2_P101.a/b",
		"acme/hall2/line-1/pump/extra=System1:Hall2_P101.flow",
		"acme/hall2/line-1/pumps/Hall2_L2_P1/flow=System1:Hall2_L2_P1.flow",
		"acme/hall2/line-1/pumps/Hall2_L2_P1=System1:Hall2_L2_P1.pressure",
		"acme/hall2/line-1/pumps/Hall2_L2_P2/flow=System1:Hall2_L2_P2.flow",
		"acme/hall2/line-1/pumps/Hall2_L2_P2=System1:Hall2_L2_P2.pressure",
		"acme/hall2/line-1/remote/Hall2_L2_P9/flow=System2:Hall2_L2_P9.flow",
		"acme/hall2/line-1/tag=System1:Tag1.",
	}
	if got := topics(res.Expansion); !reflect.DeepEqual(got, want) {
		t.Fatalf("topics\n got %v\nwant %v", got, want)
	}
	// All value elements: the unsupported dpid element is left out.
	doc = docA(`[{"name":"p","binding":{"kind":"datapoint","type":"PumpType","datapoint":"Hall2_P102"}}]`)
	res = runValidate(t, oa, settingsA("u"), doc)
	got := topics(res.Expansion)
	if len(got) != 8 || got[0] != "u/p/flow=System1:Hall2_P102.flow" || strings.Contains(fmt.Sprint(got), ".ref") {
		t.Fatalf("all elements: %v %v", got, res.Issues)
	}
}

func TestSemanticsInheritance(t *testing.T) {
	oa, _ := newSimOA(t)
	s := settingsA("u")
	s.TopicDefaults = json.RawMessage(`{"qos":1,"publishMode":"minInterval","intervalMs":500}`)
	s.PayloadDefaults = json.RawMessage(`["value","time","unit"]`)
	doc := docA(`[{"name":"a","overrides":{"retain":true,"publishMode":"deadband","deadband":0.5},"children":[
		{"name":"b","binding":{"kind":"datapoint","type":"PumpType","datapoint":"Hall2_P101","overrides":{"timestamp":"publish"},
		 "elements":[{"path":"flow"},{"path":"pressure","overrides":{"publishMode":"onChange","payload":["value"],"encoding":"raw"}}]}}]}]`)
	res := runValidate(t, oa, s, doc)
	if HasErrors(res.Issues) {
		t.Fatal(res.Issues)
	}
	flow, pressure := res.Expansion.Targets[0].Sem, res.Expansion.Targets[1].Sem
	if !flow.Retain || flow.Qos != 1 || flow.PublishMode != ModeDeadband || flow.Deadband != 0.5 || flow.IntervalMs != 0 || flow.Timestamp != TimestampPublish || flow.Encoding != EncodingJSON || len(flow.Payload) != 3 {
		t.Fatalf("flow %+v", flow)
	}
	if pressure.PublishMode != ModeOnChange || pressure.Deadband != 0 || pressure.Encoding != EncodingRaw || len(pressure.Payload) != 1 || !pressure.Retain {
		t.Fatalf("pressure %+v", pressure)
	}
	b := BuiltinSemantics()
	if b.Retain || b.Qos != 0 || b.PublishMode != ModeOnChange || b.Timestamp != TimestampSource || b.Encoding != EncodingJSON || !isDefaultPayload(b.Payload) {
		t.Fatalf("builtin %+v", b)
	}
}

// Every stage-2 check with a failing case and its severity (AC-08); the
// passing case of each is TestExpandBindings or the valid variant here.
func TestValidatorChecks(t *testing.T) {
	oa, _ := newSimOA(t)
	el := func(dp, e string) string {
		return fmt.Sprintf(`{"kind":"element","type":"PumpType","datapoint":%q,"element":%q}`, dp, e)
	}
	cases := []struct {
		name     string
		tree     string
		settings func(*Settings)
		roots    string
		sev      string
		path     string
		text     string
		online   bool
	}{
		{"duplicate sibling", `[{"name":"a"},{"name":"a"}]`, nil, "", SeverityError, "/tree/1/name", "same name", false},
		{"duplicate id", `[{"id":"x","name":"a"},{"id":"x","name":"b"}]`, nil, "", SeverityError, "/tree/1/id", "also used", false},
		{"duplicate topic", `[{"name":"a","binding":` + el("Hall2_P101", "flow") + `,"children":[]},{"name":"b","binding":{"kind":"datapoint","type":"PumpType","datapoint":"Hall2_P101","elements":[{"path":"flow","topic":""},{"path":"pressure","topic":""}]}}]`, nil, "", SeverityError, "/tree/1/binding/elements/1", "also published", false},
		{"duplicate topic child node", `[{"name":"a","binding":{"kind":"datapoint","type":"PumpType","datapoint":"Hall2_P101","elements":[{"path":"flow"}]},"children":[{"name":"flow","binding":` + el("Hall2_P102", "flow") + `}]}]`, nil, "", SeverityError, "/tree/0/children/0/binding", "also published", false},
		{"cyclic warning", `[{"name":"a","overrides":{"publishMode":"cyclic","intervalMs":100}}]`, nil, "", SeverityWarning, "/tree/0/overrides/intervalMs", "cyclic below", false},
		{"raw payload warning", `[{"name":"a","overrides":{"encoding":"raw","payload":["value","unit"]}}]`, nil, "", SeverityWarning, "/tree/0/overrides", "raw", false},
		{"attribute not allowed", `[{"name":"a","overrides":{"payload":["value",{"name":"addr","attribute":"_address.._reference"}]}}]`, nil, "", SeverityError, "/tree/0/overrides/payload/1/attribute", "cannot be published", false},
		{"alert state attribute", `[{"name":"a","overrides":{"payload":["value",{"name":"st","attribute":"_alert_hdl.._act_state"}]}}]`, nil, "", SeverityError, "/tree/0/overrides/payload/1/attribute", "alert state", false},
		{"duplicate payload key", `[{"name":"a","overrides":{"payload":["value",{"name":"inv","attribute":"_online.._invalid"},{"name":"inv","attribute":"_online.._bad"}]}}]`, nil, "", SeverityError, "/tree/0/overrides/payload/2/name", "also used", false},
		{"native root", `[{"name":"oa","binding":` + el("Hall2_P101", "flow") + `}]`, func(s *Settings) { s.UnsRoot = "plant" }, "plant/oa", SeverityError, "/tree/0/binding", "native", false},
		{"native root deeper", `[{"name":"oa","children":[{"name":"x","binding":` + el("Hall2_P101", "flow") + `}]}]`, func(s *Settings) { s.UnsRoot = "plant" }, "plant/oa", SeverityError, "/tree/0/children/0/binding", "native", false},
		{"unknown type", `[{"name":"a","binding":{"kind":"element","type":"Nope","datapoint":"Hall2_P101","element":"flow"}}]`, nil, "", SeverityError, "/tree/0/binding/type", "does not exist", true},
		{"missing dp", `[{"name":"a","binding":` + el("Missing", "flow") + `}]`, nil, "", SeverityError, "/tree/0/binding/datapoint", "does not exist", true},
		{"wrong dp type", `[{"name":"a","binding":{"kind":"element","type":"PumpType","datapoint":"Tag1","element":"flow"}}]`, nil, "", SeverityError, "/tree/0/binding/datapoint", "has type Scalar", true},
		{"missing element", `[{"name":"a","binding":` + el("Hall2_P101", "nope") + `}]`, nil, "", SeverityError, "/tree/0/binding/element", "does not exist", true},
		{"struct element", `[{"name":"a","binding":` + el("Hall2_P101", "motor") + `}]`, nil, "", SeverityError, "/tree/0/binding/element", "structure", true},
		{"root of struct type", `[{"name":"a","binding":` + el("Hall2_P101", "") + `}]`, nil, "", SeverityError, "/tree/0/binding/element", "scalar type", true},
		{"unsupported element", `[{"name":"a","binding":` + el("Hall2_P101", "ref") + `}]`, nil, "", SeverityError, "/tree/0/binding/element", "cannot be published", true},
		{"pattern no match", `[{"name":"a","binding":{"kind":"pattern","type":"PumpType","pattern":"Nothing*"}}]`, nil, "", SeverityWarning, "/tree/0/binding/pattern", "matches no datapoint", true},
		{"unknown system", `[{"name":"a","binding":{"kind":"pattern","type":"PumpType","system":"Nowhere","pattern":"*"}}]`, nil, "", SeverityError, "/tree/0/binding/system", "unknown", true},
		{"disconnected system", `[{"name":"a","binding":{"kind":"pattern","type":"PumpType","system":"System3","pattern":"*"}}]`, nil, "", SeverityWarning, "/tree/0/binding/system", "not connected", true},
		{"local system named", `[{"name":"a","binding":{"kind":"pattern","type":"PumpType","system":"System1","pattern":"*","elements":[{"path":"flow"}]}}]`, nil, "", SeverityWarning, "/tree/0/binding/system", "local system", true},
		{"protected type", `[{"name":"a","binding":{"kind":"datapoint","type":"MMQConfigs","datapoint":"MMQConfigs_k1"}}]`, nil, "", SeverityError, "/tree/0/binding/type", "MMQ store", true},
		{"deadband non numeric", `[{"name":"a","binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"state","overrides":{"publishMode":"deadband","deadband":1}}}]`, nil, "", SeverityError, "/tree/0/binding", "numeric", true},
		{"write langtext", `[{"name":"a","binding":` + el("Hall2_P101", "label") + `}]`, func(s *Settings) { s.AllowWrite = true }, "", SeverityWarning, "/tree/0/binding", "..set", true},
		{"pattern collision", `[{"name":"a","binding":{"kind":"pattern","type":"PumpType","pattern":"Hall2_L2_P*","elements":[{"path":"flow"}]},"children":[{"name":"Hall2_L2_P1","children":[{"name":"flow","binding":` + el("Hall2_P101", "flow") + `}]}]}]`, nil, "", SeverityError, "/tree/0/children/0/children/0/binding", "also published", true},
		{"all elements collision", `[{"name":"a","binding":{"kind":"datapoint","type":"PumpType","datapoint":"Hall2_P101"},"children":[{"name":"flow","binding":` + el("Hall2_P102", "flow") + `}]}]`, nil, "", SeverityError, "/tree/0/children/0/binding", "also published", true},
		{"bad unsRoot", `[]`, func(s *Settings) { s.UnsRoot = "a//b" }, "", SeverityError, "", "unsRoot", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := settingsA("acme")
			if c.settings != nil {
				c.settings(s)
			}
			roots := TopicRoots{Native: "winccoa"}
			if c.roots != "" {
				roots.Native = c.roots
			}
			env := ValidationEnv{Settings: s, Roots: roots}
			if c.online {
				env.OA = oa
			}
			res, err := Validate(context.Background(), docA(c.tree), env)
			if err != nil {
				t.Fatal(err)
			}
			if !hasIssue(res.Issues, c.sev, c.path, c.text) {
				t.Fatalf("want %s at %q containing %q, got %v", c.sev, c.path, c.text, res.Issues)
			}
		})
	}
}

func TestValidatorPassing(t *testing.T) {
	oa, _ := newSimOA(t)
	doc := docA(`[{"id":"a","name":"a","overrides":{"publishMode":"cyclic","intervalMs":1000,"payload":["value",{"name":"inv","attribute":"_online.._invalid"},{"name":"lo","attribute":"_alert_hdl.1._l_limit"}]},
		"binding":{"kind":"element","type":"PumpType","datapoint":"Hall2_P101","element":"flow","overrides":{"publishMode":"deadband","deadband":2}}},
		{"id":"b","name":"b","binding":{"kind":"element","type":"Scalar","datapoint":"Tag1","element":""}}]`)
	res := runValidate(t, oa, settingsA("acme"), doc)
	if len(res.Issues) != 0 {
		t.Fatalf("issues: %v", res.Issues)
	}
}

func TestValidatorOtherDevices(t *testing.T) {
	oa, _ := newSimOA(t)
	env := ValidationEnv{Settings: settingsA("acme"), Roots: TopicRoots{Native: "winccoa"}, OA: oa, OtherPublishers: func(topic string) []string {
		if topic == "acme/a" {
			return []string{"other"}
		}
		return nil
	}}
	res, _ := Validate(context.Background(), docA(`[{"name":"a","binding":{"kind":"element","type":"Scalar","datapoint":"Tag1","element":""}}]`), env)
	if !hasIssue(res.Issues, SeverityInfo, "/tree/0/binding", "device other") || HasErrors(res.Issues) {
		t.Fatalf("issues %v", res.Issues)
	}
}

func TestValidatorLimits(t *testing.T) {
	var b strings.Builder
	b.WriteString("[")
	for i := 0; i < MaxPatternBindings+1; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"name":"n%d","binding":{"kind":"pattern","type":"PumpType","pattern":"*"}}`, i)
	}
	b.WriteString("]")
	res := runValidate(t, nil, settingsA("acme"), docA(b.String()))
	if !hasIssue(res.Issues, SeverityError, "/tree", "pattern bindings") {
		t.Fatalf("issues %v", res.Issues)
	}
	// Too many issues are capped with a summary entry.
	b.Reset()
	b.WriteString("[")
	for i := 0; i < 60; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"name":"same"}`)
	}
	b.WriteString("]")
	res = runValidate(t, nil, settingsA("acme"), docA(b.String()))
	if len(res.Issues) != MaxSchemaIssues+1 || !strings.Contains(res.Issues[MaxSchemaIssues].Message, "more issues") {
		t.Fatalf("cap: %d %v", len(res.Issues), res.Issues[len(res.Issues)-1])
	}
}

func TestValidateSettings(t *testing.T) {
	roots := TopicRoots{Native: "plant/oa"}
	ok := settingsA("plant")
	ok.TopicDefaults = json.RawMessage(`{"qos":1}`)
	if issues := ValidateSettings(ok, roots); len(issues) != 0 {
		t.Fatalf("valid settings: %v", issues)
	}
	for _, c := range []struct {
		root, td, pd, want string
	}{
		{"", "", "", "required"},
		{"$SYS/x", "", "", "'$'"},
		{"plant/oa/x", "", "", "native"},
		{"a/+/b", "", "", "valid topic level"},
		{"a/..set", "", "", "valid topic level"},
		{"ok", `{"qos":3}`, "", "/topicDefaults/qos"},
		{"ok", "", `["time"]`, "/payloadDefaults"},
	} {
		s := settingsA(c.root)
		s.TopicDefaults = json.RawMessage(c.td)
		s.PayloadDefaults = json.RawMessage(c.pd)
		issues := ValidateSettings(s, roots)
		if !strings.Contains(fmt.Sprint(issues), c.want) || !HasErrors(issues) {
			t.Errorf("%+v: %v", c, issues)
		}
	}
	w := settingsA("ok")
	w.TopicDefaults = json.RawMessage(`{"publishMode":"cyclic","intervalMs":50,"encoding":"raw"}`)
	w.PayloadDefaults = json.RawMessage(`["value","unit"]`)
	issues := ValidateSettings(w, roots)
	if HasErrors(issues) || len(issues) != 2 {
		t.Fatalf("warnings: %v", issues)
	}
}
