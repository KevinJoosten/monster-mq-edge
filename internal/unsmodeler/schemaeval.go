package unsmodeler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// This file evaluates the mapping schema with all-errors output. It
// supports exactly the draft 2020-12 keywords the schema uses; compiling a
// schema with any other keyword fails, so the schema cannot silently grow
// beyond what is checked here. The tests compare the verdict with
// jsonschema-go and ajv on all testdata.

// schemaNode is one compiled (sub)schema.
type schemaNode struct {
	always *bool // boolean schema true/false

	def  string // $defs name when this node is a $defs entry
	desc string

	typ                         string
	enum                        []any
	hasConst                    bool
	constVal                    any
	pattern                     *regexp.Regexp
	minLength, maxLength        int // -1: absent
	minimum, maximum, exclusMin *float64
	minItems                    int
	uniqueItems                 bool
	minProperties               int
	required                    []string
	props                       map[string]*schemaNode
	propNames                   []string // declared order
	closed                      bool     // additionalProperties: false
	items, contains, not        *schemaNode
	allOf                       []*schemaNode
	ifS, thenS, elseS           *schemaNode
	cond, condNot               string // for if: the condition and its negation in words
	ref                         string
	refNode                     *schemaNode
	depReq                      map[string][]string
	depReqKeys                  []string
}

var supportedKeywords = map[string]bool{
	"$schema": true, "$id": true, "$defs": true, "title": true, "description": true,
	"type": true, "enum": true, "const": true, "pattern": true, "minLength": true, "maxLength": true,
	"minimum": true, "maximum": true, "exclusiveMinimum": true, "minItems": true, "uniqueItems": true,
	"minProperties": true, "required": true, "properties": true, "additionalProperties": true,
	"items": true, "contains": true, "not": true, "allOf": true, "if": true, "then": true, "else": true,
	"$ref": true, "dependentRequired": true,
}

type schemaCompiler struct {
	defs map[string]*schemaNode
	refs []*schemaNode
}

// compileSchema compiles the root schema with its $defs.
func compileSchema(raw []byte) (*schemaNode, map[string]*schemaNode, error) {
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, nil, err
	}
	c := &schemaCompiler{defs: map[string]*schemaNode{}}
	defs, _ := root["$defs"].(map[string]any)
	for name, d := range defs {
		n, err := c.compile(d, "/$defs/"+name)
		if err != nil {
			return nil, nil, err
		}
		n.def = name
		c.defs[name] = n
	}
	top, err := c.compile(root, "")
	if err != nil {
		return nil, nil, err
	}
	for _, n := range c.refs {
		name, ok := strings.CutPrefix(n.ref, "#/$defs/")
		if n.refNode = c.defs[name]; !ok || n.refNode == nil {
			return nil, nil, fmt.Errorf("unresolved $ref %q", n.ref)
		}
	}
	return top, c.defs, nil
}

func (c *schemaCompiler) compile(v any, at string) (*schemaNode, error) {
	if b, ok := v.(bool); ok {
		return &schemaNode{always: &b}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: schema must be an object or boolean", at)
	}
	n := &schemaNode{minLength: -1, maxLength: -1, minItems: -1, minProperties: -1}
	for k := range m {
		if !supportedKeywords[k] {
			return nil, fmt.Errorf("%s: unsupported keyword %q", at, k)
		}
		if k == "$defs" && at != "" {
			return nil, fmt.Errorf("%s: $defs only at the root", at)
		}
	}
	var err error
	sub := func(key string) *schemaNode {
		if err != nil || m[key] == nil {
			return nil
		}
		var s *schemaNode
		s, err = c.compile(m[key], at+"/"+key)
		return s
	}
	intOf := func(key string) int {
		if f, ok := m[key].(float64); ok {
			return int(f)
		}
		return -1
	}
	floatOf := func(key string) *float64 {
		if f, ok := m[key].(float64); ok {
			return &f
		}
		return nil
	}
	n.desc, _ = m["description"].(string)
	if t, ok := m["type"]; ok {
		if n.typ, ok = t.(string); !ok {
			return nil, fmt.Errorf("%s: type must be a string", at)
		}
	}
	if e, ok := m["enum"].([]any); ok {
		n.enum = e
	}
	n.constVal, n.hasConst = m["const"]
	if p, ok := m["pattern"].(string); ok {
		if n.pattern, err = regexp.Compile(p); err != nil {
			return nil, fmt.Errorf("%s: %w", at, err)
		}
	}
	n.minLength, n.maxLength = intOf("minLength"), intOf("maxLength")
	n.minimum, n.maximum, n.exclusMin = floatOf("minimum"), floatOf("maximum"), floatOf("exclusiveMinimum")
	n.minItems, n.minProperties = intOf("minItems"), intOf("minProperties")
	n.uniqueItems, _ = m["uniqueItems"].(bool)
	for _, r := range asList(m["required"]) {
		n.required = append(n.required, r.(string))
	}
	if props, ok := m["properties"].(map[string]any); ok {
		n.props = map[string]*schemaNode{}
		for k, pv := range props {
			if n.props[k], err = c.compile(pv, at+"/properties/"+k); err != nil {
				return nil, err
			}
			n.propNames = append(n.propNames, k)
		}
		slices.Sort(n.propNames)
	}
	if ap, ok := m["additionalProperties"]; ok {
		if b, ok := ap.(bool); !ok || b {
			return nil, fmt.Errorf("%s: only additionalProperties false is supported", at)
		}
		n.closed = true
	}
	n.items, n.contains, n.not = sub("items"), sub("contains"), sub("not")
	n.ifS, n.thenS, n.elseS = sub("if"), sub("then"), sub("else")
	for i, a := range asList(m["allOf"]) {
		var s *schemaNode
		if s, err = c.compile(a, at+"/allOf/"+strconv.Itoa(i)); err != nil {
			return nil, err
		}
		n.allOf = append(n.allOf, s)
	}
	if err != nil {
		return nil, err
	}
	if n.ifS != nil {
		n.cond, n.condNot = conditionText(m["if"])
	}
	if r, ok := m["$ref"].(string); ok {
		n.ref = r
		c.refs = append(c.refs, n)
	}
	if dr, ok := m["dependentRequired"].(map[string]any); ok {
		n.depReq = map[string][]string{}
		for k, v := range dr {
			for _, r := range asList(v) {
				n.depReq[k] = append(n.depReq[k], r.(string))
			}
			n.depReqKeys = append(n.depReqKeys, k)
		}
		slices.Sort(n.depReqKeys)
	}
	return n, nil
}

func asList(v any) []any { l, _ := v.([]any); return l }

// conditionText describes an if schema of the form
// {"properties": {"k": {"const"|"enum": ...}}, "required": ["k"]}.
func conditionText(v any) (cond, condNot string) {
	m, _ := v.(map[string]any)
	props, _ := m["properties"].(map[string]any)
	if len(props) != 1 {
		return "", ""
	}
	for k, pv := range props {
		p, _ := pv.(map[string]any)
		if c, ok := p["const"]; ok {
			return k + " is " + quoteJSON(c), k + " is not " + quoteJSON(c)
		}
		if e, ok := p["enum"].([]any); ok {
			return k + " is one of " + joinJSON(e), k + " is none of " + joinJSON(e)
		}
	}
	return "", ""
}

// evaluator collects the issues of one document.
type evaluator struct {
	issues []SchemaIssue
	total  int
}

func (e *evaluator) add(ptr, msg string) {
	e.total++
	if len(e.issues) < MaxSchemaIssues {
		e.issues = append(e.issues, SchemaIssue{Path: ptr, Message: msg})
	}
}

// evalCtx carries what an issue message needs besides the failing node.
type evalCtx struct {
	report bool
	when   string      // condition of the enclosing then/else branch
	def    *schemaNode // nearest $defs entry, for unknown-property hints
}

func (c evalCtx) suffix() string {
	if c.when == "" {
		return ""
	}
	return " (when " + c.when + ")"
}

// eval reports whether v is valid against s. With ctx.report it records
// every failure; without, it stops at the first one.
func (e *evaluator) eval(s *schemaNode, v any, ptr string, ctx evalCtx) bool {
	if s.always != nil {
		if !*s.always && ctx.report {
			e.add(ptr, "not allowed here"+ctx.suffix())
		}
		return *s.always
	}
	ok := true
	fail := func(p, msg string) bool {
		ok = false
		if ctx.report {
			e.add(p, msg+ctx.suffix())
		}
		return ctx.report // continue only when collecting
	}
	if s.ref != "" {
		rctx := ctx
		rctx.def = s.refNode
		if !e.eval(s.refNode, v, ptr, rctx) {
			if ok = false; !ctx.report {
				return false
			}
		}
	}
	if s.typ != "" && !hasType(v, s.typ) {
		fail(ptr, fmt.Sprintf("must be %s, got %s", article(s.typ), describeValue(v)))
		return false // the remaining keywords are type-specific
	}
	if s.enum != nil && !slices.ContainsFunc(s.enum, func(x any) bool { return jsonEqual(v, x) }) {
		if !fail(ptr, fmt.Sprintf("must be one of %s, got %s", joinJSON(s.enum), describeValue(v))) {
			return false
		}
	}
	if s.hasConst && !jsonEqual(v, s.constVal) {
		if !fail(ptr, fmt.Sprintf("must be %s, got %s", quoteJSON(s.constVal), describeValue(v))) {
			return false
		}
	}
	switch x := v.(type) {
	case string:
		if !e.evalString(s, x, ptr, fail) {
			return false
		}
	case json.Number:
		f, _ := strconv.ParseFloat(string(x), 64)
		if s.minimum != nil && f < *s.minimum && !fail(ptr, fmt.Sprintf("must be at least %s, got %s", fmtNum(*s.minimum), x)) {
			return false
		}
		if s.maximum != nil && f > *s.maximum && !fail(ptr, fmt.Sprintf("must be at most %s, got %s", fmtNum(*s.maximum), x)) {
			return false
		}
		if s.exclusMin != nil && f <= *s.exclusMin && !fail(ptr, fmt.Sprintf("must be greater than %s, got %s", fmtNum(*s.exclusMin), x)) {
			return false
		}
	case []any:
		if !e.evalArray(s, x, ptr, fail, ctx) {
			if ok = false; !ctx.report {
				return false
			}
		}
	case *jsonObject:
		if !e.evalObject(s, x, ptr, fail, ctx) {
			if ok = false; !ctx.report {
				return false
			}
		}
	}
	if s.not != nil {
		if e.eval(s.not, v, ptr, evalCtx{def: ctx.def}) {
			msg := s.not.desc
			if msg == "" {
				msg = "value not allowed"
			}
			if !fail(ptr, describeValue(v)+": "+msg) {
				return false
			}
		}
	}
	for _, a := range s.allOf {
		if !e.eval(a, v, ptr, ctx) {
			if ok = false; !ctx.report {
				return false
			}
		}
	}
	if s.ifS != nil {
		branch, when := s.thenS, s.cond
		if !e.eval(s.ifS, v, ptr, evalCtx{def: ctx.def}) {
			branch, when = s.elseS, s.condNot
		}
		if branch != nil {
			bctx := ctx
			if when != "" {
				bctx.when = when
			}
			if !e.eval(branch, v, ptr, bctx) {
				ok = false
			}
		}
	}
	return ok
}

func (e *evaluator) evalString(s *schemaNode, x, ptr string, fail func(string, string) bool) bool {
	n := -1
	runes := func() int {
		if n < 0 {
			n = utf8.RuneCountInString(x)
		}
		return n
	}
	if s.minLength > 0 && runes() < s.minLength {
		msg := "must not be empty"
		if s.minLength > 1 {
			msg = fmt.Sprintf("must have at least %d characters", s.minLength)
		}
		if !fail(ptr, msg) {
			return false
		}
	}
	if s.maxLength >= 0 && runes() > s.maxLength {
		if !fail(ptr, fmt.Sprintf("must have at most %d characters, has %d", s.maxLength, runes())) {
			return false
		}
	}
	if s.pattern != nil && !s.pattern.MatchString(x) {
		msg := describeValue(x) + " does not match " + strconv.Quote(s.pattern.String())
		switch {
		case s.def != "":
			msg = describeValue(x) + " is not a valid " + s.def + ": " + s.desc
		case s.desc != "":
			msg = describeValue(x) + " is invalid: " + s.desc
		}
		if !fail(ptr, msg) {
			return false
		}
	}
	return true
}

func (e *evaluator) evalArray(s *schemaNode, x []any, ptr string, fail func(string, string) bool, ctx evalCtx) bool {
	if s.minItems >= 0 && len(x) < s.minItems {
		if !fail(ptr, fmt.Sprintf("must have at least %d item(s)", s.minItems)) {
			return false
		}
	}
	if s.uniqueItems {
	dup:
		for i := range x {
			for j := i + 1; j < len(x); j++ {
				if jsonEqual(x[i], x[j]) {
					if !fail(ptr, fmt.Sprintf("items %d and %d are equal; every item may appear only once", i, j)) {
						return false
					}
					break dup
				}
			}
		}
	}
	if s.contains != nil && !slices.ContainsFunc(x, func(it any) bool { return e.eval(s.contains, it, "", evalCtx{}) }) {
		msg := "must contain a matching item"
		if s.contains.hasConst {
			msg = "must contain " + quoteJSON(s.contains.constVal)
		}
		if !fail(ptr, msg) {
			return false
		}
	}
	if s.items != nil {
		ok := true
		for i, it := range x {
			if !e.eval(s.items, it, ptr+"/"+strconv.Itoa(i), ctx) {
				if ok = false; !ctx.report {
					return false
				}
			}
		}
		return ok
	}
	return true
}

func (e *evaluator) evalObject(s *schemaNode, x *jsonObject, ptr string, fail func(string, string) bool, ctx evalCtx) bool {
	ok := true
	for _, r := range s.required {
		if _, has := x.vals[r]; !has {
			ok = false
			if !fail(ptr, "missing required property "+quoteJSON(r)) {
				return false
			}
		}
	}
	for _, k := range s.depReqKeys {
		if _, has := x.vals[k]; !has {
			continue
		}
		for _, r := range s.depReq[k] {
			if _, has := x.vals[r]; !has {
				ok = false
				if !fail(ptr, fmt.Sprintf("property %s requires %s in the same object", quoteJSON(k), quoteJSON(r))) {
					return false
				}
			}
		}
	}
	if s.minProperties >= 0 && len(x.keys) < s.minProperties {
		ok = false
		if !fail(ptr, fmt.Sprintf("must have at least %d properties", s.minProperties)) {
			return false
		}
	}
	for _, k := range x.keys {
		p := ptr + "/" + pointerEscape(k)
		ps, declared := s.props[k]
		switch {
		case declared && ps.always != nil && !*ps.always:
			ok = false
			if !fail(p, "property "+quoteJSON(k)+" is not allowed") {
				return false
			}
		case declared:
			if !e.eval(ps, x.vals[k], p, ctx) {
				if ok = false; !ctx.report {
					return false
				}
			}
		case s.closed:
			ok = false
			msg := "unknown property " + quoteJSON(k)
			if h := unknownPropertyHint(ctx.def, k); h != "" {
				msg += "; " + h
			}
			if !fail(p, msg+"; allowed: "+strings.Join(s.propNames, ", ")) {
				return false
			}
		}
	}
	return ok
}

// unknownPropertyHint names the replacement for keys that LLMs and older
// sketches of the document tend to use.
func unknownPropertyHint(def *schemaNode, key string) string {
	name := ""
	if def != nil {
		name = def.def
	}
	switch key {
	case "unsRoot", "topicDefaults", "payloadDefaults", "oaPrefix", "allowWrite", "approvalMode", "aiProvider", "redundancy", "enabled":
		if name == "" {
			return key + " is a device setting, not part of the mapping document"
		}
	case "$schema":
		return "remove it; the document does not name its schema"
	case "retain", "qos", "publishMode", "deadband", "intervalMs", "timestamp", "encoding", "payload":
		if name == "Binding" || name == "ElementSelection" || name == "Node" {
			return "put topic semantics and payload into overrides"
		}
	case "bind":
		return "use binding"
	case "dpe":
		return "use datapoint and element (and system for a remote system)"
	case "exclude", "rename":
		return "leave the element out of elements to exclude it; use topic to rename it"
	case "format":
		if name == "Overrides" || name == "TopicSemantics" {
			return "use encoding (json or raw); format is a payload field"
		}
	case "field":
		return "use jsonPath"
	case "filter":
		return "use topicFilters (a list)"
	case "type":
		if name == "TypeElement" {
			return "use elementType"
		}
	case "above", "below":
		return `use "when": "above" or "below" together with "limit"`
	}
	return ""
}

func hasType(v any, typ string) bool {
	switch typ {
	case "object":
		_, ok := v.(*jsonObject)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "null":
		return v == nil
	case "number", "integer":
		n, ok := v.(json.Number)
		if !ok {
			return false
		}
		f, err := strconv.ParseFloat(string(n), 64)
		return err == nil && !math.IsInf(f, 0) && (typ == "number" || f == math.Trunc(f))
	}
	return false
}

// jsonEqual compares a decoded document value with another document value
// or a schema value (float64, map[string]any). Numbers compare by value.
func jsonEqual(a, b any) bool {
	num := func(v any) (float64, bool) {
		switch n := v.(type) {
		case json.Number:
			f, err := strconv.ParseFloat(string(n), 64)
			return f, err == nil
		case float64:
			return n, true
		}
		return 0, false
	}
	if fa, ok := num(a); ok {
		fb, ok := num(b)
		return ok && fa == fb
	}
	switch x := a.(type) {
	case string, bool, nil:
		return a == b
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !jsonEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	case *jsonObject:
		var yv map[string]any
		switch y := b.(type) {
		case *jsonObject:
			yv = y.vals
		case map[string]any:
			yv = y
		default:
			return false
		}
		if len(x.vals) != len(yv) {
			return false
		}
		for k, v := range x.vals {
			w, ok := yv[k]
			if !ok || !jsonEqual(v, w) {
				return false
			}
		}
		return true
	case map[string]any:
		return jsonEqual(&jsonObject{vals: x}, b)
	}
	return false
}

const maxQuotedRunes = 64

// describeValue renders a value for a message: strings and numbers as
// JSON (strings cut to maxQuotedRunes), containers by kind.
func describeValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case *jsonObject, map[string]any:
		return "an object"
	case []any:
		return "an array"
	case string:
		return "string " + quoteJSON(x)
	case json.Number:
		if _, err := strconv.ParseFloat(string(x), 64); err != nil {
			return "number " + string(x) + " (out of range)"
		}
		return "number " + string(x)
	case bool:
		return "boolean " + strconv.FormatBool(x)
	}
	return quoteJSON(v)
}

// quoteJSON renders a scalar as JSON without HTML escaping; long strings
// are cut.
func quoteJSON(v any) string {
	if s, ok := v.(string); ok && utf8.RuneCountInString(s) > maxQuotedRunes {
		v = string([]rune(s)[:maxQuotedRunes]) + "…"
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return fmt.Sprint(v)
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func joinJSON(l []any) string {
	parts := make([]string, len(l))
	for i, v := range l {
		parts[i] = quoteJSON(v)
	}
	return strings.Join(parts, ", ")
}

func fmtNum(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

func article(typ string) string {
	switch typ {
	case "object", "array", "integer":
		return "an " + typ
	}
	return "a " + typ
}
