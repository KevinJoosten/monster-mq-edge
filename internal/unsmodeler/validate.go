package unsmodeler

import (
	"context"
	"fmt"
	"strings"
)

// ValidationEnv is what the validator needs beyond the document.
type ValidationEnv struct {
	Settings *Settings
	Roots    TopicRoots
	// OA is nil when WinCC OA is not available; the WinCC OA checks are
	// then skipped.
	OA OA
	// OtherPublishers returns the other devices that publish a topic.
	OtherPublishers func(topic string) []string
	// DirectionAPublishes reports whether a Direction A device publishes a
	// topic (Direction B loop check); it returns the device name.
	DirectionAPublishes func(topic string) string
	// ArchiveGroupExists checks a Direction B source archive group.
	ArchiveGroupExists func(name string) bool
	// Sample returns a current payload of a source topic (Direction B
	// sample checks).
	Sample func(topic string) ([]byte, bool)
}

// Result is the outcome of validating one document.
type Result struct {
	Issues []Issue
	// Doc is the typed document when stage 1 passed.
	Doc *Document
	// Expansion is the Direction A expansion when stage 1 passed.
	Expansion *Expansion
}

// Validate checks a document in two stages (§6.5): stage 1 is the
// structure (ValidateSchema, every issue an error); stage 2 runs only on a
// document without stage-1 issues. The returned list holds the issues of
// one stage, at most MaxSchemaIssues plus a summary entry.
func Validate(ctx context.Context, raw []byte, env ValidationEnv) (*Result, error) {
	schemaIssues, err := ValidateSchema(raw)
	if err != nil {
		return nil, err
	}
	if len(schemaIssues) > 0 {
		out := make([]Issue, len(schemaIssues))
		for i, is := range schemaIssues {
			out[i] = Issue{Severity: SeverityError, Path: is.Path, Message: is.Message}
		}
		return &Result{Issues: out}, nil
	}
	doc, err := ParseDocument(raw)
	if err != nil {
		return &Result{Issues: []Issue{{Severity: SeverityError, Path: "", Message: err.Error()}}}, nil
	}
	res := &Result{Doc: doc}
	var issues []Issue
	if env.Settings != nil && doc.Direction != env.Settings.Direction {
		issues = append(issues, Issue{Severity: SeverityError, Path: "/direction", Message: fmt.Sprintf("document direction %s differs from the device direction %s", doc.Direction, env.Settings.Direction)})
		res.Issues = issues
		return res, nil
	}
	switch doc.Direction {
	case DirectionWinccoaToUns:
		var exp *Expansion
		issues, exp = validateA(ctx, doc, env)
		res.Expansion = exp
	case DirectionMqttToWinccoa:
		issues = validateB(ctx, doc, env)
	}
	res.Issues = capIssues(dedupIssues(issues))
	return res, nil
}

func dedupIssues(in []Issue) []Issue {
	seen := map[Issue]bool{}
	out := in[:0]
	for _, is := range in {
		if !seen[is] {
			seen[is] = true
			out = append(out, is)
		}
	}
	return out
}

func capIssues(in []Issue) []Issue {
	if len(in) <= MaxSchemaIssues {
		return in
	}
	out := append([]Issue(nil), in[:MaxSchemaIssues]...)
	sev := SeverityInfo
	for _, is := range in[MaxSchemaIssues:] {
		if is.Severity == SeverityError {
			sev = SeverityError
			break
		}
		if is.Severity == SeverityWarning {
			sev = SeverityWarning
		}
	}
	return append(out, Issue{Severity: sev, Path: "", Message: fmt.Sprintf("%d more issues not shown", len(in)-MaxSchemaIssues)})
}

// validateA runs the Direction A checks of stage 2.
func validateA(ctx context.Context, doc *Document, env ValidationEnv) ([]Issue, *Expansion) {
	var out []Issue
	add := func(sev, path, format string, args ...any) {
		out = append(out, Issue{Severity: sev, Path: path, Message: fmt.Sprintf(format, args...)})
	}
	s := env.Settings
	if s == nil {
		s = &Settings{Direction: DirectionWinccoaToUns}
		s.applyDefaults()
	}
	unsRoot := s.UnsRoot
	if unsRoot == "" {
		unsRoot = "uns"
	} else {
		for _, is := range ValidateUnsRoot(s.UnsRoot, env.Roots) {
			add(SeverityError, "", "device setting unsRoot %s", is.Message)
		}
	}
	td, pd := s.Defaults()
	devSem := DeviceSemantics(td, pd)

	// Document only: names, ids, counts, semantics warnings, payload fields.
	nodes, bindings, patterns := 0, 0, 0
	ids := map[string]string{}
	var walk func(list []*Node, ptr string, sem Semantics)
	walk = func(list []*Node, ptr string, sem Semantics) {
		names := map[string]int{}
		for i, n := range list {
			p := fmt.Sprintf("%s/%d", ptr, i)
			nodes++
			if j, dup := names[n.Name]; dup {
				add(SeverityError, p+"/name", "sibling node %d has the same name %s", j, quoteJSON(n.Name))
			} else {
				names[n.Name] = i
			}
			if n.ID != "" {
				if q, dup := ids[n.ID]; dup {
					add(SeverityError, p+"/id", "node id %s is also used at %s", quoteJSON(n.ID), q)
				} else {
					ids[n.ID] = p
				}
			}
			ns := sem.Apply(n.Overrides)
			checkOverrides(n.Overrides, ns, p+"/overrides", add)
			if b := n.Binding; b != nil {
				bindings++
				if b.Kind == KindPattern {
					patterns++
				}
				bs := ns.Apply(b.Overrides)
				checkOverrides(b.Overrides, bs, p+"/binding/overrides", add)
				for k, e := range b.Elements {
					checkOverrides(e.Overrides, bs.Apply(e.Overrides), fmt.Sprintf("%s/binding/elements/%d/overrides", p, k), add)
				}
			}
			walk(n.Children, p+"/children", ns)
		}
	}
	walk(doc.Tree, "/tree", devSem)
	if nodes > MaxNodes {
		add(SeverityError, "/tree", "the tree has %d nodes, more than the limit of %d", nodes, MaxNodes)
	}
	if bindings > MaxBindings {
		add(SeverityError, "/tree", "the tree has %d bindings, more than the limit of %d", bindings, MaxBindings)
	}
	if patterns > MaxPatternBindings {
		add(SeverityError, "/tree", "the tree has %d pattern bindings, more than the limit of %d", patterns, MaxPatternBindings)
	}

	// WinCC OA (and the document's own topics when OA is not available).
	exp := Expand(ctx, doc, ExpandOptions{UnsRoot: unsRoot, TopicDefaults: td, PayloadDefaults: pd, AllowWrite: s.AllowWrite}, env.OA)
	out = append(out, exp.Issues...)

	// Topics: unique within the device, not reserved, length, other devices.
	seen := map[string]*Target{}
	for _, t := range exp.Targets {
		if first, dup := seen[t.Topic]; dup {
			if first.Ptr != t.Ptr || first.DPE() != t.DPE() {
				add(SeverityError, t.Ptr, "topic %s is also published by %s", quoteJSON(t.Topic), first.Ptr)
			}
			continue
		}
		seen[t.Topic] = t
		if why := reservedTopic(t.Topic, env.Roots); why != "" {
			add(SeverityError, t.Ptr, "topic %s %s", quoteJSON(t.Topic), why)
		}
		if len(t.Topic) > MaxTopicBytes {
			add(SeverityError, t.Ptr, "topic is %d bytes, more than %d", len(t.Topic), MaxTopicBytes)
		}
		if env.OtherPublishers != nil {
			if others := env.OtherPublishers(t.Topic); len(others) > 0 {
				add(SeverityInfo, t.Ptr, "topic %s is also published by device %s", quoteJSON(t.Topic), strings.Join(others, ", "))
			}
		}
	}
	// Unbound nodes and the node levels of pattern bindings can lie in the
	// native branch too.
	doc.Walk(func(ptr string, anc []*Node, n *Node) {
		topic := unsRoot
		for _, a := range anc {
			topic += "/" + a.Name
		}
		topic += "/" + n.Name
		if why := reservedTopic(topic, env.Roots); why != "" && n.Binding != nil && n.Binding.Kind == KindPattern {
			add(SeverityError, ptr, "topic %s %s", quoteJSON(topic), why)
		}
	})
	return out, exp
}

// checkOverrides reports the semantic warnings of one override level and
// the payload field rules.
func checkOverrides(o *Overrides, eff Semantics, ptr string, add func(sev, path, format string, args ...any)) {
	if o == nil {
		return
	}
	if o.PublishMode != nil && *o.PublishMode == ModeCyclic && o.IntervalMs != nil && *o.IntervalMs < 1000 {
		add(SeverityWarning, ptr+"/intervalMs", "cyclic below 1000 ms publishes every DPE of the binding at that rate whether it changed or not")
	}
	if (o.Encoding != nil || o.Payload != nil) && eff.Encoding == EncodingRaw && !isDefaultPayload(eff.Payload) {
		add(SeverityWarning, ptr, "encoding raw publishes only the value; the payload fields are ignored")
	}
	names := map[string]int{}
	for i, f := range o.Payload {
		if f.Field != "" {
			continue
		}
		p := fmt.Sprintf("%s/payload/%d", ptr, i)
		if j, dup := names[f.Name]; dup {
			add(SeverityError, p+"/name", "payload key %s is also used by field %d", quoteJSON(f.Name), j)
		} else {
			names[f.Name] = i
		}
		if why := attributeAllowed(f.Attribute); why != "" {
			add(SeverityError, p+"/attribute", "%s", why)
		}
	}
}
