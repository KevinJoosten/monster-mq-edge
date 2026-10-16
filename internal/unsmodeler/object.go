package unsmodeler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/winccoanative"
)

// objNode is one level of an object payload: struct levels nest, the
// elements are the leaves.
type objNode struct {
	keys []string
	kids map[string]*objNode
	leaf json.RawMessage
	set  bool
}

func (n *objNode) child(k string) *objNode {
	if n.kids == nil {
		n.kids = map[string]*objNode{}
	}
	c := n.kids[k]
	if c == nil {
		c = &objNode{}
		n.kids[k] = c
		n.keys = append(n.keys, k)
	}
	return c
}

func (n *objNode) put(path string, leaf json.RawMessage) {
	cur := n
	if path != "" {
		for _, k := range strings.Split(path, ".") {
			cur = cur.child(k)
		}
	}
	cur.leaf, cur.set = leaf, true
}

func (n *objNode) write(b *bytes.Buffer) {
	if n.set && len(n.keys) == 0 {
		b.Write(n.leaf)
		return
	}
	b.WriteByte('{')
	for i, k := range n.keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := marshalNoEscape(k)
		b.Write(kb)
		b.WriteByte(':')
		n.kids[k].write(b)
	}
	b.WriteByte('}')
}

// ordered returns the elements of an object in selection order.
func (o *ostate) ordered() []*tstate {
	list := slices.Clone(o.members)
	slices.SortStableFunc(list, func(a, b *tstate) int { return a.ord - b.ord })
	return list
}

// buildObjectPayload renders an object topic: the element paths as nested
// JSON objects, each leaf the element's payload by its semantics (§4.4).
// Elements without a value (connect failed) are left out.
func buildObjectPayload(o *ostate, now time.Time) []byte {
	root := &objNode{}
	for _, m := range o.ordered() {
		if m.last == nil {
			continue
		}
		root.put(m.t.Element, buildPayload(m.t.Sem, m.last, m.meta, now))
	}
	var b bytes.Buffer
	root.write(&b)
	return b.Bytes()
}

// objectValue is the live monitor value of an object: the element values
// in the shape of the payload.
func objectValue(o *ostate) (any, bool) {
	root := &objNode{}
	has := false
	for _, m := range o.ordered() {
		if m.last == nil {
			continue
		}
		v, _ := marshalNoEscape(m.last.v.PlainJSON())
		root.put(m.t.Element, v)
		has = true
	}
	if !has {
		return nil, false
	}
	var b bytes.Buffer
	root.write(&b)
	var out any
	if err := json.Unmarshal(b.Bytes(), &out); err != nil {
		return nil, false
	}
	return out, true
}

// objectCommand converts a ..set on an object topic: a JSON object in the
// shape of the payload holding the elements to write, each leaf a bare
// value or {"value": ...}. Keys that are no selected element are rejected.
func objectCommand(members []*Target, payload []byte) ([]string, []oahost.Value, error) {
	byPath := map[string]*Target{}
	for _, t := range members {
		byPath[t.Element] = t
	}
	var names []string
	var vals []oahost.Value
	add := func(t *Target, raw json.RawMessage) error {
		cmd, err := winccoanative.ParseCommand(raw)
		if err != nil {
			return fmt.Errorf("element %s: %w", t.Element, err)
		}
		v, err := winccoanative.ConvertValue(cmd.Value, oahost.Kind(t.ElemKind))
		if err != nil {
			return fmt.Errorf("element %s: %w", t.Element, err)
		}
		names = append(names, t.DPE()+":_original.._value")
		vals = append(vals, v)
		return nil
	}
	if t := byPath[""]; t != nil {
		if err := add(t, payload); err != nil {
			return nil, nil, err
		}
		return names, vals, nil
	}
	hasPrefix := func(p string) bool {
		for path := range byPath {
			if strings.HasPrefix(path, p+".") {
				return true
			}
		}
		return false
	}
	var walk func(prefix string, raw json.RawMessage) error
	walk = func(prefix string, raw json.RawMessage) error {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return fmt.Errorf("%w: %s must be a JSON object of elements", winccoanative.ErrValue, nonEmpty(prefix, "the payload"))
		}
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			path := k
			if prefix != "" {
				path = prefix + "." + k
			}
			switch {
			case byPath[path] != nil:
				if err := add(byPath[path], obj[k]); err != nil {
					return err
				}
			case hasPrefix(path):
				if err := walk(path, obj[k]); err != nil {
					return err
				}
			default:
				return fmt.Errorf("%w: %s is not an element of this topic", winccoanative.ErrValue, quoteJSON(path))
			}
		}
		return nil
	}
	if err := walk("", payload); err != nil {
		return nil, nil, err
	}
	if len(names) == 0 {
		return nil, nil, fmt.Errorf("%w: no element to write", winccoanative.ErrValue)
	}
	return names, vals, nil
}

func nonEmpty(s, alt string) string {
	if s == "" {
		return alt
	}
	return s
}
