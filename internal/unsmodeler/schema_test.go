package unsmodeler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
)

func testdataFiles(t *testing.T, kind string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("schema", "testdata", kind, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no %s testdata: %v", kind, err)
	}
	return files
}

func validate(t *testing.T, doc []byte) []SchemaIssue {
	t.Helper()
	issues, err := ValidateSchema(doc)
	if err != nil {
		t.Fatal(err)
	}
	return issues
}

func readTestdata(t *testing.T, kind, file string) []byte {
	t.Helper()
	doc, err := os.ReadFile(filepath.Join("schema", "testdata", kind, file))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func paths(issues []SchemaIssue) []string {
	var p []string
	for _, is := range issues {
		p = append(p, is.Path)
	}
	slices.Sort(p)
	return slices.Compact(p)
}

// Every file in schema/testdata/valid must pass, every file in
// schema/testdata/invalid must fail. With UNSMODELER_ISSUES_OUT set, the
// issues of all files are written there as JSON, so schema/ajvcheck.mjs
// can compare the paths.
func TestSchemaTestdata(t *testing.T) {
	all := map[string][]SchemaIssue{}
	for _, kind := range []string{"valid", "invalid"} {
		for _, f := range testdataFiles(t, kind) {
			name := kind + "/" + filepath.Base(f)
			t.Run(name, func(t *testing.T) {
				doc, err := os.ReadFile(f)
				if err != nil {
					t.Fatal(err)
				}
				issues := validate(t, doc)
				all[name] = issues
				switch {
				case kind == "valid" && issues != nil:
					t.Errorf("valid document rejected: %v", issues)
				case kind == "invalid" && len(issues) == 0:
					t.Error("invalid document accepted")
				default:
					for _, is := range issues {
						t.Logf("%s: %s", is.Path, is.Message)
					}
				}
			})
		}
	}
	if out := os.Getenv("UNSMODELER_ISSUES_OUT"); out != "" {
		b, _ := json.MarshalIndent(all, "", "  ")
		if err := os.WriteFile(out, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// The verdict of the all-errors evaluator must equal the verdict of
// jsonschema-go, an independent draft 2020-12 implementation.
func TestSchemaVerdictMatchesJSONSchemaGo(t *testing.T) {
	var s jsonschema.Schema
	if err := json.Unmarshal(mappingSchema, &s); err != nil {
		t.Fatal(err)
	}
	resolved, err := s.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"valid", "invalid"} {
		for _, f := range testdataFiles(t, kind) {
			doc, _ := os.ReadFile(f)
			// encoding/json rejects numbers beyond float64 (1e400), which
			// ajv sees as Infinity and rejects as "not a number".
			var inst any
			want := json.Unmarshal(doc, &inst) == nil && resolved.Validate(inst) == nil
			if got := validate(t, doc) == nil; got != want {
				t.Errorf("%s/%s: evaluator valid=%v, jsonschema-go valid=%v", kind, filepath.Base(f), got, want)
			}
		}
	}
}

func TestValidateSchemaPaths(t *testing.T) {
	cases := map[string][]string{
		"missing-schema-version.json":             {""},
		"a-with-b-types.json":                     {"/types"},
		"device-setting-unsroot-in-document.json": {"/unsRoot"},
		"segment-reserved-set.json":               {"/tree/0/children/0/name"},
		"element-binding-with-elements.json":      {"/tree/0/children/0/binding/elements"},
		"qos-3.json":                              {"/tree/0/children/1/binding/elements/0/overrides/qos"},
		"topic-override-reserved-set.json":        {"/tree/0/children/1/binding/elements/1/topic"},
		"deadband-missing-value.json":             {"/tree/0/overrides"},
		"b-archive-without-group.json":            {"/source"},
		"b-pvrange-on-bool.json":                  {"/types/0/elements/2/pvRange"},
		"b-alert-limit-without-class.json":        {"/types/0/elements/0/alert/limits/0"},
		"b-jsonpath-without-dollar.json":          {"/datapoints/0/bindings/0/jsonPath"},
		"payload-attribute-reserved-name.json":    {"/tree/0/overrides/payload/1/name"},
		"payload-item-number.json":                {"/tree/0/overrides/payload/1"},
		// Several rules broken at once: every one is reported.
		"multi-node-five-errors.json": {
			"/tree/0/children/0/binding/datapoint",
			"/tree/0/children/0/binding/overrides",
			"/tree/0/children/0/binding/overrides/qos",
			"/tree/0/children/0/level",
			"/tree/0/children/0/name",
		},
		"multi-a-with-types-and-bad-qos.json":          {"/tree/0/overrides/qos", "/types"},
		"multi-element-kind-elements-and-bad-qos.json": {"/tree/0/children/0/binding/elements", "/tree/0/children/0/binding/elements/0/overrides/qos"},
		"multi-top-level-device-settings.json":         {"/types", "/unsRoot"},
		"multi-b-type-and-datapoint.json":              {"/datapoints/0/bindings/0/jsonPath", "/types/0/elements/0/format", "/types/0/elements/2/unit"},
	}
	for file, want := range cases {
		issues := validate(t, readTestdata(t, "invalid", file))
		if got := paths(issues); !slices.Equal(got, want) {
			t.Errorf("%s: want issues at %q, got %v", file, want, issues)
		}
	}
}

// The same document always gives the same issues in the same order.
func TestValidateSchemaDeterministic(t *testing.T) {
	doc := readTestdata(t, "invalid", "multi-node-five-errors.json")
	first := validate(t, doc)
	for range 30 {
		if got := validate(t, doc); !slices.Equal(got, first) {
			t.Fatalf("issues changed between runs:\n%v\n%v", first, got)
		}
	}
}

func TestValidateSchemaMessages(t *testing.T) {
	cases := []struct{ file, path, want string }{
		{"datapoint-binding-with-element.json", "/tree/0/children/1/binding/element", `property "element" is not allowed (when kind is "datapoint")`},
		{"a-with-b-types.json", "/types", `property "types" is not allowed (when direction is "winccoa_to_uns")`},
		{"b-live-with-archive-group.json", "/source/archiveGroup", `property "archiveGroup" is not allowed (when mode is not "archive")`},
		{"segment-reserved-set.json", "/tree/0/children/0/name", `'..set' is reserved for write-through`},
		{"system-prefix-in-datapoint.json", "/tree/0/children/1/binding/datapoint", `is not a valid DpName: Datapoint name without system prefix`},
		{"qos-string.json", "/tree/0/children/1/binding/elements/0/overrides/qos", `must be one of 0, 1, 2, got string "1"`},
		{"qos-null.json", "/tree/0/children/1/binding/elements/0/overrides/qos", `got null`},
		{"device-setting-unsroot-in-document.json", "/unsRoot", `unsRoot is a device setting`},
		{"node-legacy-bind-key.json", "/tree/0/children/0/bind", `use binding`},
		{"deadband-without-publish-mode.json", "/tree/0/overrides", `property "deadband" requires "publishMode"`},
		{"deadband-missing-value.json", "/tree/0/overrides", `missing required property "deadband" (when publishMode is "deadband")`},
		{"b-format-s-on-float.json", "/types/0/elements/0/format", `is not a valid FloatFormat: printf-style format for float, e.g. %.1f, %8.3e or %g. (when elementType is "float")`},
		{"interval-below-minimum.json", "/tree/0/overrides/intervalMs", `must be at least 10, got 5`},
		{"payload-without-value.json", "/tree/0/overrides/payload", `must contain "value"`},
		{"b-element-type-long.json", "/types/0/elements/1/elementType", `must be one of "bool", "int", "uint", "float", "string", "time", "bit32", "blob", got string "long"`},
	}
	for _, c := range cases {
		issues := validate(t, readTestdata(t, "invalid", c.file))
		found := slices.ContainsFunc(issues, func(is SchemaIssue) bool {
			return is.Path == c.path && strings.Contains(is.Message, c.want)
		})
		if !found {
			t.Errorf("%s: want %q at %q, got %v", c.file, c.want, c.path, issues)
		}
	}
}

func TestValidateSchemaLongValueIsCut(t *testing.T) {
	long := strings.Repeat("x", 300)
	issues := validate(t, []byte(`{"schemaVersion":1,"direction":"winccoa_to_uns","tree":[{"name":"`+long+`/"}]}`))
	if len(issues) == 0 {
		t.Fatal("want issues")
	}
	for _, is := range issues {
		if strings.Contains(is.Message, long) || len(is.Message) > 600 {
			t.Errorf("message quotes the whole value: %d bytes", len(is.Message))
		}
	}
}

func TestValidateSchemaDuplicateKeys(t *testing.T) {
	doc := `{"schemaVersion":1,"direction":"winccoa_to_uns",
		"tree":[{"name":"a","binding":{"kind":"element","kind":"pattern","type":"T","pattern":"*"}}],
		"tree":[]}`
	issues := validate(t, []byte(doc))
	for _, want := range []string{"/tree/0/binding/kind", "/tree"} {
		if !slices.ContainsFunc(issues, func(is SchemaIssue) bool {
			return is.Path == want && strings.Contains(is.Message, "duplicate key")
		}) {
			t.Errorf("want duplicate key at %q, got %v", want, issues)
		}
	}
}

func TestValidateSchemaBadJSON(t *testing.T) {
	for _, doc := range []string{
		`{"schemaVersion": 1,`,
		`{"schemaVersion":1,"direction":"winccoa_to_uns","tree":[]} x`,
		"```json\n{\"schemaVersion\":1,\"direction\":\"winccoa_to_uns\",\"tree\":[]}\n```",
		``,
	} {
		issues := validate(t, []byte(doc))
		if len(issues) != 1 || issues[0].Path != "" || !strings.HasPrefix(issues[0].Message, "invalid JSON") {
			t.Errorf("%q: got %v", doc, issues)
		}
	}
}

// chain builds a tree of nested single-child nodes; the leaf has qos 9.
func chain(depth int) []byte {
	var b strings.Builder
	b.WriteString(`{"schemaVersion":1,"direction":"winccoa_to_uns","tree":[`)
	for i := 1; i < depth; i++ {
		fmt.Fprintf(&b, `{"name":"n%d","children":[`, i)
	}
	b.WriteString(`{"name":"leaf","overrides":{"qos":9}}`)
	b.WriteString(strings.Repeat("]}", depth-1))
	b.WriteString(`]}`)
	return []byte(b.String())
}

func TestValidateSchemaLimits(t *testing.T) {
	start := time.Now()
	issues := validate(t, chain(1000))
	if len(issues) != 1 || !strings.Contains(issues[0].Message, "JSON nesting deeper than 100 levels") {
		t.Errorf("depth 1000: got %v", issues)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("depth 1000 took %v", d)
	}

	issues = validate(t, chain(40))
	leaf := "/tree/0" + strings.Repeat("/children/0", 39) + "/overrides/qos"
	level33 := "/tree/0" + strings.Repeat("/children/0", 32)
	if got := paths(issues); !slices.Equal(got, []string{level33, leaf}) {
		t.Errorf("depth 40: got %v", issues)
	}

	var b strings.Builder
	b.WriteString(`{"schemaVersion":1,"direction":"winccoa_to_uns","tree":[`)
	for i := range 2000 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":"n%d","overrides":{"qos":9}}`, i)
	}
	b.WriteString(`]}`)
	start = time.Now()
	issues = validate(t, []byte(b.String()))
	if len(issues) != MaxSchemaIssues+1 || issues[MaxSchemaIssues].Message != "1950 more issues not shown" {
		t.Errorf("wide: got %d issues, last %v", len(issues), issues[len(issues)-1])
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("wide document took %v", d)
	}

	big := []byte(`{"schemaVersion":1,"direction":"winccoa_to_uns","tree":[],"x":"` + strings.Repeat("a", MaxDocumentBytes) + `"}`)
	if issues := validate(t, big); len(issues) != 1 || !strings.Contains(issues[0].Message, "more than the limit") {
		t.Errorf("size: got %v", issues)
	}
}

func TestMappingSchemaHeader(t *testing.T) {
	var s struct {
		Schema string `json:"$schema"`
		ID     string `json:"$id"`
	}
	if err := json.Unmarshal(MappingSchema(), &s); err != nil {
		t.Fatal(err)
	}
	if s.Schema != "https://json-schema.org/draft/2020-12/schema" || s.ID != "https://monstermq.io/schemas/uns-modeler/mapping.schema.json" {
		t.Errorf("unexpected header %+v", s)
	}
}

func TestCompileSchemaRejectsUnsupportedKeywords(t *testing.T) {
	for _, s := range []string{`{"oneOf":[true]}`, `{"properties":{"a":{"format":"date-time"}}}`, `{"additionalProperties":{"type":"string"}}`, `{"$ref":"#/$defs/Missing"}`} {
		if _, _, err := compileSchema([]byte(s)); err == nil {
			t.Errorf("%s: compiled without error", s)
		}
	}
}

// stripServerFields removes what the server or the user sets, which the
// prompt view leaves out.
func stripServerFields(v any) {
	switch x := v.(type) {
	case map[string]any:
		for _, k := range []string{"aiRun", "source", "aiGenerated", "excluded"} {
			delete(x, k)
		}
		for _, c := range x {
			stripServerFields(c)
		}
	case []any:
		for _, c := range x {
			stripServerFields(c)
		}
	}
}

func TestPromptSchema(t *testing.T) {
	for _, dir := range []string{DirectionWinccoaToUns, DirectionMqttToWinccoa} {
		view, err := PromptSchema(dir)
		if err != nil {
			t.Fatal(err)
		}
		text := string(view)
		for _, absent := range []string{`"aiRun"`, `"aiGenerated"`, `"excluded"`, `"AiRun"`, `"Source"`} {
			if strings.Contains(text, absent) {
				t.Errorf("%s view contains %s", dir, absent)
			}
		}
		var top struct {
			Properties map[string]any `json:"properties"`
		}
		if err := json.Unmarshal(view, &top); err != nil {
			t.Fatal(err)
		}
		if _, ok := top.Properties["source"]; ok {
			t.Errorf("%s view has the property source", dir)
		}
		otherDef := `"TypeProposal"`
		if dir == DirectionMqttToWinccoa {
			otherDef = `"Node"`
		}
		if strings.Contains(text, otherDef) {
			t.Errorf("%s view contains %s", dir, otherDef)
		}
		if _, _, err := compileSchema(view); err != nil {
			t.Fatalf("%s view: %v", dir, err)
		}
		var s jsonschema.Schema
		if err := json.Unmarshal(view, &s); err != nil {
			t.Fatal(err)
		}
		resolved, err := s.Resolve(nil)
		if err != nil {
			t.Fatalf("%s view: %v", dir, err)
		}
		for _, f := range testdataFiles(t, "valid") {
			doc, _ := os.ReadFile(f)
			var inst map[string]any
			if err := json.Unmarshal(doc, &inst); err != nil {
				t.Fatal(err)
			}
			if inst["direction"] != dir {
				continue
			}
			stripServerFields(inst)
			if err := resolved.Validate(inst); err != nil {
				t.Errorf("%s view rejects %s: %v", dir, filepath.Base(f), err)
			}
		}
	}
	if _, err := PromptSchema("both"); err == nil {
		t.Error("unknown direction accepted")
	}
}

func TestValidateDef(t *testing.T) {
	cases := []struct {
		def, value string
		paths      []string
	}{
		{"TopicSemantics", `{"retain":true,"qos":1,"publishMode":"deadband","deadband":0.5}`, nil},
		{"TopicSemantics", `{}`, nil},
		{"TopicSemantics", `{"qos":3}`, []string{"/qos"}},
		{"TopicSemantics", `{"payload":["value"]}`, []string{"/payload"}},
		{"TopicSemantics", `{"publishMode":"cyclic"}`, []string{""}},
		{"PayloadFields", `["value","time","pvRange"]`, nil},
		{"PayloadFields", `["time"]`, []string{""}},
		{"PayloadFields", `["value","ts"]`, []string{"/1"}},
	}
	for _, c := range cases {
		issues, err := ValidateDef(c.def, []byte(c.value))
		if err != nil {
			t.Fatalf("%s %s: %v", c.def, c.value, err)
		}
		if got := paths(issues); !slices.Equal(got, c.paths) {
			t.Errorf("%s %s: paths %q, want %q (%v)", c.def, c.value, got, c.paths, issues)
		}
	}
	if _, err := ValidateDef("Missing", []byte(`{}`)); err == nil {
		t.Error("unknown definition accepted")
	}
}
