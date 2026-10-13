package oahost

import (
	"context"
	"fmt"
	"time"
)

// Browse and metadata operations (UNS Modeler, plan-uns-modeler-design.md
// §6.1 and §8). Hosts that do not know them answer "unknown operation".
const (
	OpTypeNames     uint32 = 13
	OpTypeStructure uint32 = 14
	OpElementMeta   uint32 = 15
	OpSetMeta       uint32 = 16
)

// Tags of the browse and metadata operations.
const (
	TagOaType  byte = 19 // u32 raw WinCC OA DpElementType
	TagUnit    byte = 20 // UTF-8 unit (active language on read)
	TagFormat  byte = 21 // UTF-8 format (active language on read)
	TagComment byte = 22 // UTF-8 description (active language on read)
)

// ElemStruct and ElemUnsupported are the element kinds reported for a
// structure node and for an element type the embedding cannot read or write.
const (
	ElemStruct      uint32 = 0
	ElemUnsupported uint32 = 255
)

// Raw WinCC OA element types (DpElementType) the browse API names.
const (
	OaRecord        uint32 = 1
	OaTypeReference uint32 = 41
)

// oaTypeNames are the WinCC OA names of DpElementType values, as shown in
// PARA and returned by dpElementType-style functions.
var oaTypeNames = map[uint32]string{
	1: "struct", 2: "array", 3: "dyn_char", 4: "dyn_uint", 5: "dyn_int", 6: "dyn_float",
	7: "dyn_bool", 8: "dyn_bit32", 9: "dyn_string", 10: "dyn_time", 11: "char_array",
	12: "uint_array", 13: "int_array", 14: "float_array", 15: "bool_array", 16: "bit32_array",
	17: "string_array", 18: "time_array", 19: "char", 20: "uint", 21: "int", 22: "float",
	23: "bool", 24: "bit32", 25: "string", 26: "time", 27: "dpid", 28: "novalue", 29: "dyn_dpid",
	30: "dyn_char_array", 31: "dyn_uint_array", 32: "dyn_int_array", 33: "dyn_float_array",
	34: "dyn_bool_array", 35: "dyn_bit32_array", 36: "dyn_string_array", 37: "dyn_time_array",
	38: "dyn_dpid_array", 39: "dpid_array", 40: "novalue_array", 41: "typeref", 42: "langString",
	43: "langString_array", 44: "dyn_langString", 45: "dyn_langString_array", 46: "blob",
	47: "blob_array", 48: "dyn_blob", 49: "dyn_blob_array", 50: "bit64", 51: "dyn_bit64",
	52: "bit64_array", 53: "dyn_bit64_array", 54: "long", 55: "dyn_long", 56: "long_array",
	57: "dyn_long_array", 58: "ulong", 59: "dyn_ulong", 60: "ulong_array", 61: "dyn_ulong_array",
}

// OaTypeName returns the WinCC OA name of a raw element type ("float",
// "struct", "dyn_string", ...), or "unknown(<n>)".
func OaTypeName(t uint32) string {
	if s, ok := oaTypeNames[t]; ok {
		return s
	}
	return fmt.Sprintf("unknown(%d)", t)
}

// TypeElement is one node of a datapoint type tree, in depth-first order.
type TypeElement struct {
	Path     string // dotted path below the type root; "" is the root
	Kind     uint32 // value kind (Kind as uint32), ElemStruct or ElemUnsupported
	OaType   uint32 // raw DpElementType
	RefType  string // referenced type of a typeref node
	Children int    // number of direct children
}

// ElementMeta is the answer of OpElementMeta for one DPE.
type ElementMeta struct {
	Name        string
	Exists      bool
	Error       string
	TypeName    string // datapoint type of the DP
	Kind        uint32
	OaType      uint32
	Unit        string
	Format      string
	Description string
}

// TypeNames lists datapoint type names matching a dpTypes-style pattern on
// system ("" is the local system), without system prefix. Internal types
// (starting with '_') are included; callers filter.
func (a API) TypeNames(ctx context.Context, system, pattern string, timeout time.Duration) ([]string, error) {
	var w Writer
	w.U32(TagOp, OpTypeNames)
	w.String(TagName, pattern)
	if system != "" {
		w.String(TagSysName, system)
	}
	start := time.Now()
	m, err := a.call(ctx, &w, timeout)
	names := m.All(TagTypeName)
	a.logCall("dpTypes", start, err, "system", system, "pattern", pattern, "found", len(names))
	if err != nil {
		return nil, err
	}
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = string(n)
	}
	return out, nil
}

// TypeStructure returns the element tree of a datapoint type in depth-first
// order (parents before children, children in type order). An unknown type
// is ErrNotFound; an unknown or disconnected system is ErrUnavailable.
func (a API) TypeStructure(ctx context.Context, system, typeName string, timeout time.Duration) ([]TypeElement, error) {
	var w Writer
	w.U32(TagOp, OpTypeStructure)
	w.String(TagTypeName, typeName)
	if system != "" {
		w.String(TagSysName, system)
	}
	start := time.Now()
	m, err := a.call(ctx, &w, timeout)
	rows := m.All(TagRow)
	a.logCall("dpTypeGet", start, err, "system", system, "type", typeName, "elements", len(rows))
	if err != nil {
		return nil, err
	}
	out := make([]TypeElement, 0, len(rows))
	for _, r := range rows {
		rm, err := ParseMessage(r)
		if err != nil {
			return nil, err
		}
		e := TypeElement{Path: rm.String(TagName), RefType: rm.String(TagTypeName)}
		e.Kind, _ = rm.U32(TagElemType)
		e.OaType, _ = rm.U32(TagOaType)
		if n, ok := rm.U32(TagCount); ok {
			e.Children = int(n)
		}
		out = append(out, e)
	}
	return out, nil
}

// ElementMeta reads element type, unit, format and description of DPEs
// (names with optional system prefix, without config). A missing DPE is
// reported per item, never as an error of the call.
func (a API) ElementMeta(ctx context.Context, names []string, timeout time.Duration) ([]ElementMeta, error) {
	if len(names) == 0 {
		return nil, nil
	}
	var w Writer
	w.U32(TagOp, OpElementMeta)
	for _, n := range names {
		w.String(TagName, n)
	}
	start := time.Now()
	m, err := a.call(ctx, &w, timeout)
	a.logCall("dpGetUnit/Format/Description", start, err, "count", len(names), "names", nameList(names))
	if err != nil {
		return nil, err
	}
	rows := m.All(TagRow)
	if len(rows) != len(names) {
		return nil, fmt.Errorf("%w: element meta returned %d rows for %d names", ErrOA, len(rows), len(names))
	}
	out := make([]ElementMeta, len(rows))
	for i, r := range rows {
		rm, err := ParseMessage(r)
		if err != nil {
			return nil, err
		}
		e := ElementMeta{
			Name:        names[i],
			Exists:      rm.Bool(TagExists),
			Error:       rm.String(TagError),
			TypeName:    rm.String(TagTypeName),
			Unit:        rm.String(TagUnit),
			Format:      rm.String(TagFormat),
			Description: rm.String(TagComment),
		}
		e.Kind, _ = rm.U32(TagElemType)
		e.OaType, _ = rm.U32(TagOaType)
		out[i] = e
	}
	return out, nil
}

// MetaField selects the element metadata written by SetElementMeta.
type MetaField byte

const (
	MetaUnit        = MetaField(TagUnit)
	MetaFormat      = MetaField(TagFormat)
	MetaDescription = MetaField(TagComment)
)

// SetElementMeta writes one metadata text of a DPE with the same text in
// all project languages (dpSetUnit, dpSetFormat, dpSetDescription) and waits
// for the answer.
func (a API) SetElementMeta(ctx context.Context, dpe string, field MetaField, text string, timeout time.Duration) error {
	var w Writer
	w.U32(TagOp, OpSetMeta)
	w.String(TagName, dpe)
	w.String(byte(field), text)
	start := time.Now()
	_, err := a.call(ctx, &w, timeout)
	a.logCall("dpSetMeta", start, err, "dpe", dpe, "field", field, "text", text)
	return err
}

// FlagAttrs (connect): connect each name together with the TagAttr
// attributes of its element (e.g. "_online.._status"); every event item
// then carries a TagRow with one value per attribute (null where the
// attribute does not exist) after its value and time.
const FlagAttrs uint32 = 1 << 7

// TagAttr is a repeated attribute name of a FlagAttrs connect.
const TagAttr byte = 23

// DpConnectAttrs connects a batch of DPEs like DpConnect and adds attrs
// (config attributes without DPE) to each registration.
func (a API) DpConnectAttrs(ctx context.Context, names []string, flags uint32, attrs []string, h EventHandler, timeout time.Duration) (uint64, error) {
	if len(names) == 0 {
		return 0, ErrInvalid
	}
	ref := a.C.NewRef(h)
	var w Writer
	w.U32(TagOp, OpDpConnect)
	w.U64(TagRef, ref)
	if len(attrs) > 0 {
		flags |= FlagAttrs
	}
	w.U32(TagFlags, flags)
	for _, n := range names {
		w.String(TagName, n)
	}
	for _, at := range attrs {
		w.String(TagAttr, at)
	}
	start := time.Now()
	_, err := a.call(ctx, &w, timeout)
	a.logCall("dpConnect", start, err, "ref", ref, "count", len(names), "names", nameList(names), "attrs", nameList(attrs), "answer", flags&FlagAnswer != 0)
	if err != nil {
		a.C.DropRef(ref)
		return 0, err
	}
	return ref, nil
}

// HotlinkAttrs returns the attribute values of every item of a FlagAttrs
// DP_CONNECT event, parallel to HotlinkItems; nil where the host sent none.
func HotlinkAttrs(m Message) ([][]Value, error) {
	var out [][]Value
	for _, f := range m.Fields {
		switch f.Tag {
		case TagValue:
			out = append(out, nil)
		case TagRow:
			if len(out) == 0 {
				continue
			}
			fields, err := Parse(f.Data)
			if err != nil {
				return nil, err
			}
			var vals []Value
			for _, cf := range fields {
				if cf.Tag != TagValue {
					continue
				}
				v, err := DecodeValue(cf.Data)
				if err != nil {
					return nil, err
				}
				vals = append(vals, v)
			}
			out[len(out)-1] = vals
		}
	}
	return out, nil
}
