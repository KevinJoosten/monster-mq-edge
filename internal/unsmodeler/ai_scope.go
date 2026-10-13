package unsmodeler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// SelectionItem is one entry of the user's WinCC OA browser selection, in
// the shape of the dashboard's drag items.
type SelectionItem struct {
	Kind      string `json:"kind"` // type, datapoint, element
	System    string `json:"system,omitempty"`
	Type      string `json:"type"`
	Datapoint string `json:"datapoint,omitempty"`
	Path      string `json:"path,omitempty"`
	Pattern   string `json:"pattern,omitempty"`
}

// Selection is the scope of a Direction A AI run.
type Selection struct {
	Items []SelectionItem `json:"items"`
	// AllowBeyond widens browsing beyond the selection; bindings stay
	// within it.
	AllowBeyond bool `json:"allowBeyond,omitempty"`
}

// scope answers selection membership questions with the local system
// name resolved.
type scope struct {
	sel   Selection
	local string
	res   map[string]*regexp.Regexp
}

func newScope(sel Selection, local string) *scope {
	return &scope{sel: sel, local: local, res: map[string]*regexp.Regexp{}}
}

func (s *scope) sys(system string) string {
	if system == "" {
		return s.local
	}
	return system
}

// matchPattern matches a datapoint name against a dpNames pattern:
// * any characters, ? one character, {a,b} alternatives.
func (s *scope) matchPattern(pattern, name string) bool {
	re, ok := s.res[pattern]
	if !ok {
		var b strings.Builder
		b.WriteString("^")
		in := false
		for _, c := range pattern {
			switch {
			case c == '*':
				b.WriteString(".*")
			case c == '?':
				b.WriteString(".")
			case c == '{':
				in = true
				b.WriteString("(?:")
			case c == '}' && in:
				in = false
				b.WriteString(")")
			case c == ',' && in:
				b.WriteString("|")
			default:
				b.WriteString(regexp.QuoteMeta(string(c)))
			}
		}
		b.WriteString("$")
		re, _ = regexp.Compile(b.String())
		s.res[pattern] = re
	}
	return re != nil && re.MatchString(name)
}

func (s *scope) inType(system, typ string) bool {
	for _, it := range s.sel.Items {
		if s.sys(it.System) == s.sys(system) && it.Type == typ {
			return true
		}
	}
	return false
}

// inDP reports a datapoint selected as a whole (datapoint item, or a type
// item whose pattern matches it).
func (s *scope) inDP(system, dp, typ string) bool {
	for _, it := range s.sel.Items {
		if s.sys(it.System) != s.sys(system) {
			continue
		}
		switch it.Kind {
		case "datapoint":
			if it.Datapoint == dp {
				return true
			}
		case "type":
			p := it.Pattern
			if p == "" {
				p = "*"
			}
			if (typ == "" || it.Type == typ) && s.matchPattern(p, dp) {
				return true
			}
		}
	}
	return false
}

func (s *scope) inDPE(system, dp, path, typ string) bool {
	if s.inDP(system, dp, typ) {
		return true
	}
	for _, it := range s.sel.Items {
		if it.Kind == "element" && s.sys(it.System) == s.sys(system) && it.Datapoint == dp && it.Path == path {
			return true
		}
	}
	return false
}

// touchesDP reports a datapoint of which at least one element is selected.
func (s *scope) touchesDP(system, dp, typ string) bool {
	if s.inDP(system, dp, typ) {
		return true
	}
	for _, it := range s.sel.Items {
		if it.Kind == "element" && s.sys(it.System) == s.sys(system) && it.Datapoint == dp {
			return true
		}
	}
	return false
}

// bindingIn reports whether a binding only uses data of the selection.
func (s *scope) bindingIn(b *Binding) bool {
	if b == nil {
		return true
	}
	switch b.Kind {
	case KindElement:
		el := ""
		if b.Element != nil {
			el = *b.Element
		}
		return s.inDPE(b.System, b.Datapoint, el, b.Type)
	case KindDatapoint:
		if s.inDP(b.System, b.Datapoint, b.Type) {
			return true
		}
		if b.Elements == nil {
			return false
		}
		for _, e := range b.Elements {
			if !s.inDPE(b.System, b.Datapoint, e.Path, b.Type) {
				return false
			}
		}
		return true
	case KindPattern:
		for _, it := range s.sel.Items {
			if it.Kind == "type" && s.sys(it.System) == s.sys(b.System) && it.Type == b.Type && (it.Pattern == "" || it.Pattern == "*" || it.Pattern == b.Pattern) {
				return true
			}
		}
	}
	return false
}

// stripAnswer removes code fences around a JSON answer.
func stripAnswer(text string) string {
	t := strings.TrimSpace(text)
	if strings.HasPrefix(t, "```") {
		if i := strings.IndexByte(t, '\n'); i >= 0 {
			t = t[i+1:]
		}
		t = strings.TrimSuffix(strings.TrimSpace(t), "```")
	}
	if i, j := strings.IndexByte(t, '{'), strings.LastIndexByte(t, '}'); i > 0 && j > i && !strings.HasPrefix(t, "{") {
		t = t[i : j+1]
	}
	return strings.TrimSpace(t)
}

// stripSchemaKey removes an echoed top-level $schema key.
func stripSchemaKey(raw []byte) []byte {
	v, _, ok := decodeDocument(raw)
	if !ok {
		return raw
	}
	root, isObj := v.(*jsonObject)
	if !isObj {
		return raw
	}
	if _, has := root.vals["$schema"]; !has {
		return raw
	}
	delete(root.vals, "$schema")
	keys := root.keys[:0]
	for _, k := range root.keys {
		if k != "$schema" {
			keys = append(keys, k)
		}
	}
	root.keys = keys
	var b bytes.Buffer
	encodeOrdered(&b, root)
	return b.Bytes()
}

type nodeRef struct {
	n      *Node
	parent string
	sig    string
}

func nodeSig(n *Node, parent string) string {
	b, _ := json.Marshal(struct {
		Name, Level, Description, Parent string
		Binding                          *Binding
		Overrides                        *Overrides
	}{n.Name, n.Level, n.Description, parent, n.Binding, n.Overrides})
	return string(b)
}

func indexNodes(doc *Document) map[string]nodeRef {
	out := map[string]nodeRef{}
	if doc == nil {
		return out
	}
	var walk func(list []*Node, parent string)
	walk = func(list []*Node, parent string) {
		for _, n := range list {
			if n.ID != "" {
				out[n.ID] = nodeRef{n: n, parent: parent, sig: nodeSig(n, parent)}
			}
			walk(n.Children, n.ID)
		}
	}
	walk(doc.Tree, "")
	return out
}

// PreparedAnswer is an AI document after the server preparation of §4.2.
type PreparedAnswer struct {
	Doc    []byte
	Issues []Issue
}

// prepareA runs the AI answer preparation of a Direction A document:
// strip fences and $schema, check against the draft and the selection,
// fix node ids, stamp aiGenerated and aiRun, and validate.
func (m *Manager) prepareA(ctx context.Context, device string, s *Settings, text string, draft *Document, sc *scope, run AiRun) PreparedAnswer {
	raw := stripSchemaKey([]byte(stripAnswer(text)))
	if len(raw) > MaxAIDocumentBytes {
		return PreparedAnswer{Issues: []Issue{{Severity: SeverityError, Path: "", Message: fmt.Sprintf("document has %d bytes, more than the AI run limit of %d", len(raw), MaxAIDocumentBytes)}}}
	}
	if issues, _ := ValidateSchema(raw); len(issues) > 0 {
		out := make([]Issue, len(issues))
		for i, is := range issues {
			out[i] = Issue{Severity: SeverityError, Path: is.Path, Message: is.Message}
		}
		return PreparedAnswer{Issues: out}
	}
	doc, err := ParseDocument(raw)
	if err != nil {
		return PreparedAnswer{Issues: []Issue{{Severity: SeverityError, Path: "", Message: err.Error()}}}
	}
	if doc.Direction != DirectionWinccoaToUns {
		return PreparedAnswer{Issues: []Issue{{Severity: SeverityError, Path: "/direction", Message: "the device direction is winccoa_to_uns"}}}
	}
	before := indexNodes(draft)
	var ctxIssues []Issue
	addCtx := func(path, format string, args ...any) {
		ctxIssues = append(ctxIssues, Issue{Severity: SeverityError, Path: path, Message: "AI run context: " + fmt.Sprintf(format, args...)})
	}
	// Node ids: keep draft ids, replace invented ones, assign missing ones.
	used := map[string]bool{}
	doc.Walk(func(_ string, _ []*Node, n *Node) {
		if n.ID != "" && (before[n.ID].n == nil || used[n.ID]) {
			n.ID = ""
		}
		if n.ID != "" {
			used[n.ID] = true
		}
	})
	doc.Walk(func(_ string, _ []*Node, n *Node) {
		if n.ID == "" {
			id := NewNodeID()
			for used[id] {
				id = NewNodeID()
			}
			used[id] = true
			n.ID = id
		}
	})
	after := indexNodes(doc)
	// Selection: no binding outside it unless unchanged from the draft, and
	// draft nodes bound outside it stay unchanged.
	doc.Walk(func(ptr string, anc []*Node, n *Node) {
		parent := ""
		if len(anc) > 0 {
			parent = anc[len(anc)-1].ID
		}
		old, existed := before[n.ID]
		changed := !existed || old.sig != nodeSig(n, parent)
		if existed && !changed {
			n.AiGenerated = old.n.AiGenerated
		} else {
			n.AiGenerated = true
		}
		if n.Binding != nil && !sc.bindingIn(n.Binding) {
			if !existed || !bindingEqual(old.n.Binding, n.Binding) {
				addCtx(ptr+"/binding", "binds data outside the selection (%s %s)", n.Binding.Kind, bindingName(n.Binding))
			}
		}
		if existed && old.n.Binding != nil && !sc.bindingIn(old.n.Binding) && changed {
			addCtx(ptr, "node %s is bound outside the selection and must stay unchanged", quoteJSON(n.Name))
		}
	})
	for id, old := range before {
		if _, kept := after[id]; !kept && old.n.Binding != nil && !sc.bindingIn(old.n.Binding) {
			addCtx("/tree", "node %s (id %s) is bound outside the selection and cannot be removed", quoteJSON(old.n.Name), id)
		}
	}
	run.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	doc.AiRun = &run
	enc, err := doc.Encode()
	if err != nil {
		return PreparedAnswer{Issues: []Issue{{Severity: SeverityError, Path: "", Message: err.Error()}}}
	}
	res, err := Validate(ctx, enc, m.Env(device, s))
	if err != nil {
		return PreparedAnswer{Doc: enc, Issues: append(ctxIssues, Issue{Severity: SeverityError, Message: err.Error()})}
	}
	return PreparedAnswer{Doc: enc, Issues: capIssues(append(ctxIssues, res.Issues...))}
}

func bindingEqual(a, b *Binding) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return bytes.Equal(ja, jb)
}

func bindingName(b *Binding) string {
	sys := ""
	if b.System != "" {
		sys = b.System + ":"
	}
	switch b.Kind {
	case KindElement:
		el := ""
		if b.Element != nil {
			el = *b.Element
		}
		return sys + b.Datapoint + "." + el
	case KindDatapoint:
		return sys + b.Datapoint
	}
	return sys + b.Type + " " + b.Pattern
}

// promptDraft returns the draft without the server-only fields for the
// system prompt (Direction A: aiGenerated, aiRun).
func promptDraft(doc *Document) []byte {
	if doc == nil {
		return nil
	}
	cp, _ := ParseDocument(mustEncode(doc))
	cp.AiRun = nil
	cp.Walk(func(_ string, _ []*Node, n *Node) { n.AiGenerated = false })
	return mustEncode(cp)
}

func mustEncode(d *Document) []byte {
	b, _ := d.Encode()
	return b
}
