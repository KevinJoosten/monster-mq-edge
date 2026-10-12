package unsmodeler

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// mappingSchema is the JSON Schema (draft 2020-12) of the mapping document
// for both directions. It checks structure only; existence, collisions and
// conflicts are left to the semantic validator.
//
//go:embed schema/mapping.schema.json
var mappingSchema []byte

// Mapping directions; a device has one, and its documents have the same.
const (
	DirectionWinccoaToUns  = "winccoa_to_uns"
	DirectionMqttToWinccoa = "mqtt_to_winccoa"
)

// Limits checked before the schema, because JSON Schema cannot express
// them. The dashboard does not evaluate the schema; it calls the server's
// validate operation.
const (
	MaxDocumentBytes = 192 << 10 // whole serialized document; leaves room in a 256 KiB store record
	MaxJSONDepth     = 100       // nesting of objects and arrays
	MaxTreeDepth     = 32        // node levels below unsRoot
	MaxSchemaIssues  = 50        // further issues are summarised in one entry
)

// MappingSchema returns the JSON Schema of the mapping document. It is the
// single source for the validator, the AI prompt view and the apply step.
func MappingSchema() []byte { return slices.Clone(mappingSchema) }

// SchemaIssue is one structural problem of a mapping document.
type SchemaIssue struct {
	// Path is a JSON Pointer (RFC 6901) to the offending value: the
	// property for a wrong or forbidden value, the object for a missing
	// property; "" is the document itself.
	Path    string `json:"path"`
	Message string `json:"message"`
}

type compiled struct {
	root *schemaNode
	defs map[string]*schemaNode
}

var compiledSchema = sync.OnceValues(func() (compiled, error) {
	root, defs, err := compileSchema(mappingSchema)
	if err != nil {
		return compiled{}, fmt.Errorf("mapping schema: %w", err)
	}
	return compiled{root: root, defs: defs}, nil
})

// ValidateSchema checks a mapping document against the JSON Schema and the
// limits above. It returns nil for a structurally valid document and
// otherwise every issue (at most MaxSchemaIssues plus a summary entry), in
// document order. The error is reserved for an unusable embedded schema.
func ValidateSchema(doc []byte) ([]SchemaIssue, error) {
	c, err := compiledSchema()
	if err != nil {
		return nil, err
	}
	return validateAgainst(c.root, doc, true), nil
}

// ValidateDef checks a value against one $defs entry of the mapping schema,
// e.g. a device's topicDefaults against "TopicSemantics" or its
// payloadDefaults against "PayloadFields". The result has the same form as
// ValidateSchema's; the error reports an unknown definition.
func ValidateDef(name string, value []byte) ([]SchemaIssue, error) {
	c, err := compiledSchema()
	if err != nil {
		return nil, err
	}
	def := c.defs[name]
	if def == nil {
		return nil, fmt.Errorf("unknown schema definition %q", name)
	}
	return validateAgainst(def, value, false), nil
}

// validateAgainst runs the limit pre-checks and evaluates doc against s;
// the tree depth only applies to whole documents.
func validateAgainst(s *schemaNode, doc []byte, document bool) []SchemaIssue {
	if len(doc) > MaxDocumentBytes {
		return []SchemaIssue{{Path: "", Message: fmt.Sprintf("document has %d bytes, more than the limit of %d", len(doc), MaxDocumentBytes)}}
	}
	inst, decodeIssues, ok := decodeDocument(doc)
	if !ok {
		return decodeIssues
	}
	e := &evaluator{}
	for _, is := range decodeIssues {
		e.add(is.Path, is.Message)
	}
	if document {
		if p, deep := treeTooDeep(inst); deep {
			e.add(p, fmt.Sprintf("tree has more than %d node levels", MaxTreeDepth))
		}
	}
	e.eval(s, inst, "", evalCtx{report: true})
	if e.total == 0 {
		return nil
	}
	if more := e.total - len(e.issues); more > 0 {
		e.issues = append(e.issues, SchemaIssue{Path: "", Message: fmt.Sprintf("%d more issues not shown", more)})
	}
	return e.issues
}

// promptOnly lists properties that the server or the user sets; they are
// left out of the schema shown to the LLM.
var promptOnly = map[string][]string{
	"Node":         {"aiGenerated"},
	"TypeProposal": {"excluded"},
	"Datapoint":    {"excluded"},
}

// PromptSchema returns the part of the mapping schema for one direction,
// for the system prompt of an AI run. It leaves out the other direction,
// the server-stamped aiRun and source, and the user's aiGenerated and
// excluded flags. Proposals are still validated with ValidateSchema after
// the server has stamped those fields.
func PromptSchema(direction string) ([]byte, error) {
	var root map[string]any
	if err := json.Unmarshal(mappingSchema, &root); err != nil {
		return nil, err
	}
	props, _ := root["properties"].(map[string]any)
	defs, _ := root["$defs"].(map[string]any)
	var keep []string
	desc := ""
	switch direction {
	case DirectionWinccoaToUns:
		keep = []string{"tree"}
		desc = "UNS Modeler mapping document, Direction A (WinCC OA into the UNS): a tree of UNS nodes below the device's unsRoot with WinCC OA bindings."
	case DirectionMqttToWinccoa:
		keep = []string{"types", "datapoints"}
		desc = "UNS Modeler mapping document, Direction B (MQTT into WinCC OA): datapoint types and datapoints fed from MQTT topics. The server adds source."
	default:
		return nil, fmt.Errorf("unknown direction %q", direction)
	}
	viewProps := map[string]any{
		"schemaVersion": props["schemaVersion"],
		"direction":     map[string]any{"const": direction},
	}
	for _, k := range keep {
		viewProps[k] = props[k]
	}
	for def, keys := range promptOnly {
		if d, ok := defs[def].(map[string]any); ok {
			if p, ok := d["properties"].(map[string]any); ok {
				for _, k := range keys {
					delete(p, k)
				}
			}
		}
	}
	view := map[string]any{
		"$schema":              root["$schema"],
		"$id":                  strings.Replace(fmt.Sprint(root["$id"]), "mapping.schema.json", "prompt-"+direction+".schema.json", 1),
		"title":                root["title"],
		"description":          desc + fmt.Sprintf(" Limits: at most %d KiB, tree at most %d node levels, no duplicate keys.", MaxDocumentBytes>>10, MaxTreeDepth),
		"type":                 "object",
		"required":             append([]string{"schemaVersion", "direction"}, keep...),
		"properties":           viewProps,
		"additionalProperties": false,
	}
	reachable := map[string]any{}
	queue := collectRefs(viewProps, nil)
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if _, done := reachable[name]; done {
			continue
		}
		reachable[name] = defs[name]
		queue = collectRefs(defs[name], queue)
	}
	view["$defs"] = reachable
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(view); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// collectRefs appends the $defs names referenced anywhere in v.
func collectRefs(v any, out []string) []string {
	switch x := v.(type) {
	case map[string]any:
		if r, ok := x["$ref"].(string); ok {
			if name, ok := strings.CutPrefix(r, "#/$defs/"); ok {
				out = append(out, name)
			}
		}
		for _, k := range sortedKeys(x) {
			out = collectRefs(x[k], out)
		}
	case []any:
		for _, it := range x {
			out = collectRefs(it, out)
		}
	}
	return out
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
