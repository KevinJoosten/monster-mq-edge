package unsmodeler

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/winccoabrowse"
	"monstermq.io/edge/internal/winccoanative"
)

// OA is the WinCC OA access expansion and validation need. It is backed
// by the browse service (*winccoabrowse.Service).
type OA interface {
	LocalSystem(ctx context.Context) (string, error)
	SystemState(ctx context.Context, system string) (known, connected bool)
	TypeStructure(ctx context.Context, system, typeName string) (*winccoabrowse.TypeStructure, error)
	DatapointTypes(ctx context.Context, system string, dps []string) ([]string, error)
	MatchDatapoints(ctx context.Context, system, pattern, typeName string) ([]string, error)
}

// Limits of a Direction A document and device (§6.5).
const (
	MaxNodes           = 5000
	MaxBindings        = 2000
	MaxPatternBindings = 100
	MaxExpandedDPEs    = 10000
	MaxTopicBytes      = 65535
)

// Target is one published UNS topic bound to one DPE.
type Target struct {
	Topic string
	// Ptr points to the element selection or node that defines the topic.
	Ptr        string
	NodeID     string
	NodePtr    string
	BindingPtr string
	Kind       string // binding kind
	System     string
	DP         string
	Element    string
	ElemKind   uint32
	OaType     string
	Sem        Semantics
	// Pattern is set for the targets of a pattern binding.
	Pattern *PatternSpec
}

// DPE returns the DPE name, System:DP.element (the root as System:DP.).
func (t *Target) DPE() string { return t.System + ":" + t.DP + "." + t.Element }

// PatternSpec is a pattern binding: every datapoint of Type on System
// matching Pattern publishes Elements below NodeTopic/<dp>.
type PatternSpec struct {
	BindingPtr string
	NodeID     string
	NodePtr    string
	NodeTopic  string
	System     string
	Type       string
	Pattern    string
	Elements   []PatternElement
	// BindingSem are the semantics of the binding level (element overrides
	// not applied), used by the union of query fields.
	BindingSem Semantics
}

// PatternElement is one selected element of a pattern binding.
type PatternElement struct {
	Path     string
	Rel      *string // element topic override; nil: the encoded path
	Ptr      string
	Sem      Semantics
	ElemKind uint32
	OaType   string
}

// TopicFor returns the topic of element e of datapoint dp.
func (p *PatternSpec) TopicFor(dp string, e PatternElement) string {
	base := p.NodeTopic + "/" + winccoanative.EncodeName(dp, false)
	return joinRel(base, elementRel(e.Path, e.Rel))
}

// Expansion is the result of expanding a Direction A document.
type Expansion struct {
	Targets  []*Target
	Patterns []*PatternSpec
	Issues   []Issue
}

// ExpandOptions are the device settings expansion needs.
type ExpandOptions struct {
	UnsRoot         string
	TopicDefaults   *Overrides
	PayloadDefaults []PayloadField
	AllowWrite      bool
}

// elementRel is the topic part of an element: the override, or the path
// with each level percent-encoded and '.' as '/'.
func elementRel(path string, topic *string) string {
	if topic != nil {
		return *topic
	}
	if path == "" {
		return ""
	}
	levels := strings.Split(path, ".")
	for i, l := range levels {
		levels[i] = winccoanative.EncodeName(l, false)
	}
	return strings.Join(levels, "/")
}

func joinRel(base, rel string) string {
	if rel == "" {
		return base
	}
	return base + "/" + rel
}

type expander struct {
	ctx      context.Context
	oa       OA
	opts     ExpandOptions
	local    string
	out      *Expansion
	types    map[string]*winccoabrowse.TypeStructure
	typeErr  map[string]error
	dpTypes  map[string]string
	expanded int
}

func (x *expander) issue(sev, ptr, format string, args ...any) {
	x.out.Issues = append(x.out.Issues, Issue{Severity: sev, Path: ptr, Message: fmt.Sprintf(format, args...)})
}

// Expand resolves every binding of a Direction A document into targets. With
// oa nil only what the document states is expanded (element bindings and
// explicit element selections, kinds unknown) and no WinCC OA checks run.
func Expand(ctx context.Context, doc *Document, opts ExpandOptions, oa OA) *Expansion {
	x := &expander{ctx: ctx, oa: oa, opts: opts, out: &Expansion{}, types: map[string]*winccoabrowse.TypeStructure{}, typeErr: map[string]error{}, dpTypes: map[string]string{}}
	if oa != nil {
		if l, err := oa.LocalSystem(ctx); err == nil {
			x.local = l
		} else {
			x.issue(SeverityError, "", "WinCC OA not available: %v", err)
			x.oa = nil
		}
	}
	dev := DeviceSemantics(opts.TopicDefaults, opts.PayloadDefaults)
	var walk func(nodes []*Node, ptr, topic string, sem Semantics)
	walk = func(nodes []*Node, ptr, topic string, sem Semantics) {
		for i, n := range nodes {
			p := fmt.Sprintf("%s/%d", ptr, i)
			t := topic + "/" + n.Name
			s := sem.Apply(n.Overrides)
			if n.Binding != nil {
				x.binding(n, p, t, s)
			}
			walk(n.Children, p+"/children", t, s)
		}
	}
	walk(doc.Tree, "/tree", opts.UnsRoot, dev)
	if x.expanded > MaxExpandedDPEs {
		x.issue(SeverityError, "", "the mapping expands to %d DPEs, more than the limit of %d", x.expanded, MaxExpandedDPEs)
	}
	return x.out
}

func (x *expander) system(b *Binding) string {
	if b.System == "" {
		return x.local
	}
	return b.System
}

func (x *expander) typeStructure(system, typ string) (*winccoabrowse.TypeStructure, error) {
	key := system + ":" + typ
	if ts, ok := x.types[key]; ok {
		return ts, nil
	}
	if err, ok := x.typeErr[key]; ok {
		return nil, err
	}
	ts, err := x.oa.TypeStructure(x.ctx, system, typ)
	if err != nil {
		x.typeErr[key] = err
		return nil, err
	}
	x.types[key] = ts
	return ts, nil
}

func (x *expander) dpType(system, dp string) (string, error) {
	key := system + ":" + dp
	if t, ok := x.dpTypes[key]; ok {
		return t, nil
	}
	ts, err := x.oa.DatapointTypes(x.ctx, system, []string{dp})
	if err != nil {
		return "", err
	}
	x.dpTypes[key] = ts[0]
	return ts[0], nil
}

func findElement(ts *winccoabrowse.TypeStructure, path string) *winccoabrowse.TypeElement {
	for i := range ts.Elements {
		if ts.Elements[i].Path == path {
			return &ts.Elements[i]
		}
	}
	return nil
}

// checkElement verifies that path is a publishable value element of ts.
func (x *expander) checkElement(ts *winccoabrowse.TypeStructure, path, ptr string) (*winccoabrowse.TypeElement, bool) {
	e := findElement(ts, path)
	switch {
	case e == nil && path == "":
		x.issue(SeverityError, ptr, "the root of datapoint type %s is not a value element; \"\" only addresses the root of a scalar type", ts.Type)
	case e == nil:
		x.issue(SeverityError, ptr, "element %s does not exist in datapoint type %s", quoteJSON(path), ts.Type)
	case !e.ValueElement && path == "":
		x.issue(SeverityError, ptr, "the root of datapoint type %s is not a value element; \"\" only addresses the root of a scalar type", ts.Type)
	case !e.ValueElement:
		x.issue(SeverityError, ptr, "element %s of datapoint type %s is a structure, not a value element", quoteJSON(path), ts.Type)
	case e.Kind == oahost.ElemUnsupported:
		x.issue(SeverityError, ptr, "element %s has element type %s, which cannot be published", quoteJSON(path), e.ElementType)
	default:
		return e, true
	}
	return nil, false
}

// checkSemantics reports the per-element semantic errors and warnings.
func (x *expander) checkSemantics(sem Semantics, kind uint32, oaType, ptr string) {
	if sem.PublishMode == ModeDeadband && !numericKind(kind) {
		x.issue(SeverityError, ptr, "deadband needs a numeric element; element type is %s", oaType)
	}
	if x.opts.AllowWrite && (kind == uint32(oahost.KindLangText) || kind == uint32(oahost.KindDyn)) {
		x.issue(SeverityWarning, ptr, "element type %s cannot be written through ..set", oaType)
	}
}

func (x *expander) binding(n *Node, ptr, topic string, sem Semantics) {
	b := n.Binding
	bptr := ptr + "/binding"
	bsem := sem.Apply(b.Overrides)
	sys := x.system(b)
	online := x.oa != nil
	if online {
		if winccoanative.ProtectedType(b.Type) {
			x.issue(SeverityError, bptr+"/type", "datapoint type %s is internal or an MMQ store type and cannot be bound", b.Type)
			return
		}
		if b.System != "" {
			known, connected := x.oa.SystemState(x.ctx, b.System)
			switch {
			case !known:
				x.issue(SeverityError, bptr+"/system", "unknown WinCC OA system %s", b.System)
				return
			case !connected:
				x.issue(SeverityWarning, bptr+"/system", "WinCC OA system %s is not connected; its bindings connect when it comes back", b.System)
				online = false
			}
			if b.System == x.local {
				x.issue(SeverityWarning, bptr+"/system", "system %s is the local system; omit system for local datapoints", b.System)
			}
		}
	}
	var ts *winccoabrowse.TypeStructure
	if online {
		var err error
		ts, err = x.typeStructure(sys, b.Type)
		if err != nil {
			if errors.Is(err, winccoabrowse.ErrNotFound) {
				x.issue(SeverityError, bptr+"/type", "datapoint type %s does not exist on system %s", b.Type, sys)
			} else {
				x.issue(SeverityError, bptr+"/type", "datapoint type %s: %v", b.Type, err)
			}
			return
		}
	}
	add := func(t *Target) {
		x.expanded++
		x.out.Targets = append(x.out.Targets, t)
	}
	base := Target{NodeID: n.ID, NodePtr: ptr, BindingPtr: bptr, Kind: b.Kind, System: sys}
	checkDP := func() bool {
		if !online {
			return true
		}
		if winccoanative.Protected(b.Datapoint) {
			x.issue(SeverityError, bptr+"/datapoint", "datapoint %s is internal or an MMQ store datapoint and cannot be bound", b.Datapoint)
			return false
		}
		typ, err := x.dpType(sys, b.Datapoint)
		if err != nil {
			x.issue(SeverityError, bptr+"/datapoint", "datapoint %s: %v", b.Datapoint, err)
			return false
		}
		if typ == "" {
			x.issue(SeverityError, bptr+"/datapoint", "datapoint %s does not exist on system %s", b.Datapoint, sys)
			return false
		}
		if typ != b.Type {
			x.issue(SeverityError, bptr+"/datapoint", "datapoint %s has type %s, not %s", b.Datapoint, typ, b.Type)
			return false
		}
		return true
	}
	switch b.Kind {
	case KindElement:
		if !checkDP() {
			return
		}
		el := *b.Element
		t := base
		t.Topic, t.Ptr, t.DP, t.Element, t.Sem = topic, bptr, b.Datapoint, el, bsem
		if online {
			e, ok := x.checkElement(ts, el, bptr+"/element")
			if !ok {
				return
			}
			t.ElemKind, t.OaType = e.Kind, e.ElementType
			x.checkSemantics(bsem, e.Kind, e.ElementType, bptr)
		}
		add(&t)
	case KindDatapoint:
		if !checkDP() {
			return
		}
		for _, pe := range x.elements(b, bptr, bsem, ts, online) {
			t := base
			t.Topic, t.Ptr, t.DP, t.Element, t.Sem, t.ElemKind, t.OaType = joinRel(topic, elementRel(pe.Path, pe.Rel)), pe.Ptr, b.Datapoint, pe.Path, pe.Sem, pe.ElemKind, pe.OaType
			add(&t)
		}
	case KindPattern:
		spec := &PatternSpec{BindingPtr: bptr, NodeID: n.ID, NodePtr: ptr, NodeTopic: topic, System: sys, Type: b.Type, Pattern: b.Pattern, BindingSem: bsem}
		spec.Elements = x.elements(b, bptr, bsem, ts, online)
		x.out.Patterns = append(x.out.Patterns, spec)
		if !online {
			return
		}
		dps, err := x.oa.MatchDatapoints(x.ctx, sys, b.Pattern, b.Type)
		if err != nil {
			x.issue(SeverityError, bptr+"/pattern", "pattern %s: %v", b.Pattern, err)
			return
		}
		if len(dps) == 0 {
			x.issue(SeverityWarning, bptr+"/pattern", "pattern %s matches no datapoint of type %s now; datapoints created later are picked up", quoteJSON(b.Pattern), b.Type)
		}
		for _, dp := range dps {
			for _, pe := range spec.Elements {
				t := base
				t.Topic, t.Ptr, t.DP, t.Element, t.Sem, t.ElemKind, t.OaType, t.Pattern = spec.TopicFor(dp, pe), pe.Ptr, dp, pe.Path, pe.Sem, pe.ElemKind, pe.OaType, spec
				add(&t)
			}
		}
	}
}

// elements returns the selected elements of a datapoint or pattern
// binding, or all publishable value elements of the type.
func (x *expander) elements(b *Binding, bptr string, bsem Semantics, ts *winccoabrowse.TypeStructure, online bool) []PatternElement {
	var out []PatternElement
	if b.Elements == nil {
		if !online {
			return nil
		}
		for _, e := range ts.Elements {
			if !e.ValueElement || e.Kind == oahost.ElemUnsupported {
				continue
			}
			x.checkSemantics(bsem, e.Kind, e.ElementType, bptr)
			out = append(out, PatternElement{Path: e.Path, Ptr: bptr, Sem: bsem, ElemKind: e.Kind, OaType: e.ElementType})
		}
		if len(out) == 0 {
			x.issue(SeverityError, bptr, "datapoint type %s has no publishable value element", b.Type)
		}
		return out
	}
	for i, sel := range b.Elements {
		ptr := fmt.Sprintf("%s/elements/%d", bptr, i)
		sem := bsem.Apply(sel.Overrides)
		pe := PatternElement{Path: sel.Path, Rel: sel.Topic, Ptr: ptr, Sem: sem}
		if online {
			e, ok := x.checkElement(ts, sel.Path, ptr+"/path")
			if !ok {
				continue
			}
			pe.ElemKind, pe.OaType = e.Kind, e.ElementType
			x.checkSemantics(sem, e.Kind, e.ElementType, ptr)
		}
		out = append(out, pe)
	}
	return out
}
