package simhost

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"monstermq.io/edge/internal/oahost"
)

// oaTypeOfKind is the raw WinCC OA element type reported for a kind when
// the type does not override it.
var oaTypeOfKind = map[uint32]uint32{
	ElemStruct:                  oahost.OaRecord,
	uint32(oahost.KindBool):     23,
	uint32(oahost.KindInt):      21,
	uint32(oahost.KindUint):     20,
	uint32(oahost.KindFloat):    22,
	uint32(oahost.KindString):   25,
	uint32(oahost.KindTime):     26,
	uint32(oahost.KindBytes):    46,
	uint32(oahost.KindDyn):      6,
	uint32(oahost.KindLangText): 42,
	uint32(oahost.KindBit32):    24,
	oahost.ElemUnsupported:      54,
}

func (t *Type) oaType(el string) uint32 {
	if v, ok := t.OaTypes[el]; ok {
		return v
	}
	return oaTypeOfKind[t.Elements[el]]
}

// orderedPaths returns the element paths of t depth-first: Order first,
// then the rest sorted; parents always before their children.
func (t *Type) orderedPaths() []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range t.Order {
		if _, ok := t.Elements[p]; ok && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	var rest []string
	for p := range t.Elements {
		if !seen[p] {
			rest = append(rest, p)
		}
	}
	sort.Strings(rest)
	out = append(out, rest...)
	// Stable depth-first: emit each node after its parent, keeping the
	// relative order of siblings.
	children := map[string][]string{}
	for _, p := range out {
		if p == "" {
			continue
		}
		parent := ""
		if i := strings.LastIndex(p, "."); i >= 0 {
			parent = p[:i]
		}
		children[parent] = append(children[parent], p)
	}
	var walk func(string, *[]string)
	walk = func(p string, acc *[]string) {
		*acc = append(*acc, p)
		for _, c := range children[p] {
			walk(c, acc)
		}
	}
	var res []string
	walk("", &res)
	return res
}

func (h *Host) systemUp(name string) (*system, error) {
	if name == "" {
		name = h.local
	}
	s := h.systems[name]
	if s == nil || !s.available {
		return nil, fmt.Errorf("%w: system %s", oahost.ErrUnavailable, name)
	}
	return s, nil
}

func (h *Host) typeNames(m oahost.Message, w *oahost.Writer) error {
	if _, err := h.systemUp(m.String(oahost.TagSysName)); err != nil {
		return err
	}
	pattern := m.String(oahost.TagName)
	if pattern == "" {
		pattern = "*"
	}
	var out []string
	for name := range h.types {
		if ok, _ := path.Match(pattern, name); ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	for _, n := range out {
		w.String(oahost.TagTypeName, n)
	}
	return nil
}

func (h *Host) typeStructure(m oahost.Message, w *oahost.Writer) error {
	if _, err := h.systemUp(m.String(oahost.TagSysName)); err != nil {
		return err
	}
	name := m.String(oahost.TagTypeName)
	t := h.types[name]
	if t == nil {
		return fmt.Errorf("%w: datapoint type not found: %s", oahost.ErrNotFound, name)
	}
	paths := t.orderedPaths()
	count := map[string]uint32{}
	for _, p := range paths {
		if p == "" {
			continue
		}
		parent := ""
		if i := strings.LastIndex(p, "."); i >= 0 {
			parent = p[:i]
		}
		count[parent]++
	}
	for _, p := range paths {
		var row oahost.Writer
		row.String(oahost.TagName, p)
		row.U32(oahost.TagElemType, t.Elements[p])
		row.U32(oahost.TagOaType, t.oaType(p))
		row.U32(oahost.TagCount, count[p])
		w.Raw(oahost.TagRow, row.Bytes())
	}
	return nil
}

func (h *Host) elementMeta(m oahost.Message, w *oahost.Writer) {
	for _, raw := range m.All(oahost.TagName) {
		var row oahost.Writer
		name := string(raw)
		if _, rest := splitSys(name, h.local); !strings.Contains(rest, ".") {
			name += "."
		}
		s, d, el, _, err := h.lookup(name)
		switch {
		case err != nil && (s == nil || errors.Is(err, oahost.ErrUnavailable)):
			row.Bool(oahost.TagExists, false)
			row.String(oahost.TagError, err.Error())
		case err != nil:
			row.Bool(oahost.TagExists, false)
		default:
			t := h.types[d.typ]
			row.Bool(oahost.TagExists, true)
			row.String(oahost.TagTypeName, d.typ)
			row.U32(oahost.TagElemType, t.Elements[el])
			row.U32(oahost.TagOaType, t.oaType(el))
			row.String(oahost.TagUnit, d.values[el+"#unit"].Str)
			row.String(oahost.TagFormat, d.values[el+"#format"].Str)
			row.String(oahost.TagComment, d.values[el+"#description"].Str)
		}
		w.Raw(oahost.TagRow, row.Bytes())
	}
}

func (h *Host) setMeta(m oahost.Message) error {
	_, d, el, _, err := h.lookup(m.String(oahost.TagName))
	if err != nil {
		return err
	}
	for tag, key := range map[byte]string{oahost.TagUnit: "#unit", oahost.TagFormat: "#format", oahost.TagComment: "#description"} {
		if b, ok := m.First(tag); ok {
			d.values[el+key] = oahost.Value{Kind: oahost.KindString, Str: string(b)}
			return nil
		}
	}
	return fmt.Errorf("%w: set meta needs a unit, format or description", oahost.ErrInvalid)
}

// SetMeta sets unit, format and description of a DPE (test setup).
func (h *Host) SetMeta(address, unit, format, description string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, d, el, _, err := h.lookup(address)
	if err != nil {
		return err
	}
	d.values[el+"#unit"] = oahost.Value{Kind: oahost.KindString, Str: unit}
	d.values[el+"#format"] = oahost.Value{Kind: oahost.KindString, Str: format}
	d.values[el+"#description"] = oahost.Value{Kind: oahost.KindString, Str: description}
	return nil
}

// Meta returns unit, format and description of a DPE.
func (h *Host) Meta(address string) (unit, format, description string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, d, el, _, err := h.lookup(address)
	if err != nil {
		return "", "", "", err
	}
	return d.values[el+"#unit"].Str, d.values[el+"#format"].Str, d.values[el+"#description"].Str, nil
}
