package unsmodeler

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"monstermq.io/edge/internal/oahost"
)

// sample is one value of a DPE with its source time and the dynamic
// attributes connected with it.
type sample struct {
	v     oahost.Value
	stime time.Time
	attrs map[string]oahost.Value
}

func (s *sample) same(o *sample) bool {
	if s == nil || o == nil {
		return false
	}
	if !s.stime.Equal(o.stime) || !valueEqual(s.v, o.v) {
		return false
	}
	if len(s.attrs) != len(o.attrs) {
		return false
	}
	for k, v := range s.attrs {
		if !valueEqual(v, o.attrs[k]) {
			return false
		}
	}
	return true
}

func valueEqual(a, b oahost.Value) bool {
	ja, _ := json.Marshal(a.PlainJSON())
	jb, _ := json.Marshal(b.PlainJSON())
	return a.Kind == b.Kind && bytes.Equal(ja, jb)
}

// numeric returns the value as float64 for the deadband check.
func numeric(v oahost.Value) (float64, bool) {
	switch v.Kind {
	case oahost.KindInt:
		return float64(v.Int), true
	case oahost.KindUint, oahost.KindBit32:
		return float64(v.Uint), true
	case oahost.KindFloat:
		return v.Float, !math.IsNaN(v.Float)
	}
	return 0, false
}

// staticMeta is the metadata of a DPE read on apply, start and reconnect.
type staticMeta struct {
	unit, description, format string
	pvRange                   *PvRange
	attrs                     map[string]any // static config attributes, PlainJSON form
}

// buildPayload renders the payload of one publication (§4.4).
func buildPayload(sem Semantics, s *sample, meta *staticMeta, now time.Time) []byte {
	if sem.Encoding == EncodingRaw {
		b, _ := marshalNoEscape(s.v.PlainJSON())
		return b
	}
	var b bytes.Buffer
	b.WriteByte('{')
	first := true
	put := func(key string, v any) {
		if !first {
			b.WriteByte(',')
		}
		first = false
		k, _ := marshalNoEscape(key)
		b.Write(k)
		b.WriteByte(':')
		vb, err := marshalNoEscape(v)
		if err != nil {
			vb = []byte("null")
		}
		b.Write(vb)
	}
	if meta == nil {
		meta = &staticMeta{}
	}
	for _, f := range sem.Payload {
		switch f.Field {
		case FieldValue:
			put(FieldValue, s.v.PlainJSON())
		case FieldTime:
			ts := now
			if sem.Timestamp == TimestampSource && !s.stime.IsZero() {
				ts = s.stime
			}
			put(FieldTime, ts.UTC().Format("2006-01-02T15:04:05.000Z"))
		case FieldUnit:
			put(FieldUnit, meta.unit)
		case FieldDescription:
			put(FieldDescription, meta.description)
		case FieldFormat:
			put(FieldFormat, meta.format)
		case FieldPvRange:
			if meta.pvRange != nil {
				put(FieldPvRange, map[string]float64{"min": meta.pvRange.Min, "max": meta.pvRange.Max})
			}
		case FieldStatus:
			if st, ok := s.attrs[attrStatus]; ok && st.Kind != oahost.KindNull {
				put(FieldStatus, st.Uint)
			}
		case "":
			if AttributeDynamic(f.Attribute) {
				if v, ok := s.attrs[f.Attribute]; ok && v.Kind != oahost.KindNull {
					put(f.Name, v.PlainJSON())
				}
			} else if v, ok := meta.attrs[f.Attribute]; ok {
				put(f.Name, v)
			}
		}
	}
	b.WriteByte('}')
	return b.Bytes()
}

const (
	attrStatus = "_online.._status"
	attrSTime  = "_online.._stime"
	attrValue  = "_online.._value"
)

// dynamicAttrs returns the attributes connected together with the value
// for the payload fields (status and dynamic config attributes), sorted.
func dynamicAttrs(payloads ...[]PayloadField) []string {
	set := map[string]bool{}
	for _, p := range payloads {
		for _, f := range p {
			switch {
			case f.Field == FieldStatus:
				set[attrStatus] = true
			case f.Field == "" && AttributeDynamic(f.Attribute):
				set[f.Attribute] = true
			}
		}
	}
	return sortedSet(set)
}

func sortedSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// needsStatic reports whether a payload needs static metadata.
func needsStatic(p []PayloadField) (meta bool, pv bool, attrs []string) {
	for _, f := range p {
		switch f.Field {
		case FieldUnit, FieldDescription, FieldFormat:
			meta = true
		case FieldPvRange:
			pv = true
		case "":
			if !AttributeDynamic(f.Attribute) {
				attrs = append(attrs, f.Attribute)
			}
		}
	}
	return
}

// readStatic reads the static metadata of DPEs: unit, description and
// format for every DPE in metaDPEs, _pv_range for pvDPEs and the static
// attributes per DPE. A config that is not configured is left out.
func readStatic(ctx context.Context, api oahost.API, metaDPEs, pvDPEs []string, attrs map[string][]string, timeout time.Duration) map[string]*staticMeta {
	out := map[string]*staticMeta{}
	get := func(dpe string) *staticMeta {
		m := out[dpe]
		if m == nil {
			m = &staticMeta{attrs: map[string]any{}}
			out[dpe] = m
		}
		return m
	}
	for start := 0; start < len(metaDPEs); start += 200 {
		end := min(start+200, len(metaDPEs))
		metas, err := api.ElementMeta(ctx, metaDPEs[start:end], timeout)
		if err != nil {
			continue
		}
		for i, m := range metas {
			sm := get(metaDPEs[start+i])
			sm.unit, sm.format, sm.description = m.Unit, m.Format, m.Description
		}
	}
	if len(pvDPEs) > 0 {
		names := make([]string, len(pvDPEs))
		for i, d := range pvDPEs {
			names[i] = d + ":_pv_range.._type"
		}
		types := getEach(ctx, api, names, timeout)
		var withRange []string
		for i, v := range types {
			if v != nil && (v.Uint != 0 || v.Int != 0) {
				withRange = append(withRange, pvDPEs[i])
			}
		}
		var mm []string
		for _, d := range withRange {
			mm = append(mm, d+":_pv_range.._min", d+":_pv_range.._max")
		}
		vals := getEach(ctx, api, mm, timeout)
		for i, d := range withRange {
			lo, hi := vals[2*i], vals[2*i+1]
			if lo == nil || hi == nil {
				continue
			}
			l, ok1 := numeric(*lo)
			h, ok2 := numeric(*hi)
			if ok1 && ok2 {
				get(d).pvRange = &PvRange{Min: l, Max: h}
			}
		}
	}
	var names []string
	var owners []string
	var keys []string
	for dpe, as := range attrs {
		for _, a := range as {
			names = append(names, dpe+":"+a)
			owners = append(owners, dpe)
			keys = append(keys, a)
		}
	}
	vals := getEach(ctx, api, names, timeout)
	for i, v := range vals {
		if v != nil && v.Kind != oahost.KindNull {
			get(owners[i]).attrs[keys[i]] = v.PlainJSON()
		}
	}
	return out
}

// getEach reads names in batches; a batch that fails (one attribute not
// configured fails the whole dpGet) is read name by name. Missing values
// are nil.
func getEach(ctx context.Context, api oahost.API, names []string, timeout time.Duration) []*oahost.Value {
	out := make([]*oahost.Value, len(names))
	var read func(lo, hi int)
	read = func(lo, hi int) {
		if lo >= hi {
			return
		}
		vals, err := api.DpGet(ctx, names[lo:hi], timeout)
		if err == nil {
			for i := range vals {
				v := vals[i]
				out[lo+i] = &v
			}
			return
		}
		if hi-lo == 1 || ctx.Err() != nil {
			return
		}
		mid := (lo + hi) / 2
		read(lo, mid)
		read(mid, hi)
	}
	for start := 0; start < len(names); start += 200 {
		read(start, min(start+200, len(names)))
	}
	return out
}

// splitRowName splits a query row name "Sys:DP.el.el" (or "Sys:DP.").
func splitRowName(name string) (sys, dp, el string, ok bool) {
	sys, rest, found := strings.Cut(name, ":")
	if !found {
		return "", "", "", false
	}
	dp, el, _ = strings.Cut(rest, ".")
	if i := strings.Index(el, ":"); i >= 0 {
		el = el[:i]
	}
	return sys, dp, el, dp != ""
}

func itoa(i int64) string { return strconv.FormatInt(i, 10) }
