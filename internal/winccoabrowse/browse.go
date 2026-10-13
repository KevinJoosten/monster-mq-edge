// Package winccoabrowse is the WinCC OA browse service of the embedded
// broker: systems, datapoint types and their structure, datapoints,
// elements, element metadata and current values (plan-uns-modeler-design.md
// §6.1). The GraphQL Query.winccoa resolvers and the UNS Modeler LLM tools
// both call it, so the user and the AI see the same data.
package winccoabrowse

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/winccoanative"
)

// Limits of one browse call.
const (
	DefaultLimit       = 500
	MaxLimit           = 1000
	MaxElementInfoDPEs = 200
	// metaBatch bounds the names of one element metadata request.
	metaBatch = 200
)

var (
	// ErrUnavailable is returned for an unknown or disconnected system; a
	// browse call never falls back to the local system.
	ErrUnavailable = errors.New("system not available")
	ErrNotFound    = errors.New("not found")
	ErrInvalid     = errors.New("invalid argument")
)

// System is a WinCC OA system known to the broker.
type System struct {
	Name      string
	Local     bool
	Connected bool
}

// TypeElement is one node of a datapoint type, in depth-first order.
type TypeElement struct {
	Path string // dotted path below the root; "" is the root
	Name string // last path level; the type name for the root
	// Parent is the path of the parent node; "" for children of the root
	// and for the root itself (Depth 0).
	Parent        string
	Depth         int
	ElementType   string // WinCC OA element type, e.g. float, struct, dyn_string
	Kind          uint32 // value kind, oahost.ElemStruct or oahost.ElemUnsupported
	ValueElement  bool
	TypeReference string
	Children      int
}

// TypeStructure is the element tree of one datapoint type.
type TypeStructure struct {
	System   string
	Type     string
	Elements []TypeElement
}

// Datapoint is one datapoint of a datapoints page.
type Datapoint struct {
	Name   string `json:"name"` // without system prefix
	System string `json:"system"`
	Type   string `json:"type"`
}

// DatapointPage is one page of a datapoint listing, sorted by name.
type DatapointPage struct {
	Items  []Datapoint `json:"items"`
	Total  int         `json:"total"`
	Offset int         `json:"offset"`
	Limit  int         `json:"limit"`
}

// Element is one element of a datapoint, in depth-first order.
type Element struct {
	DPE string // System:DP.path (the root as System:DP.)
	TypeElement
}

// ElementInfo describes one DPE with its metadata and current value.
type ElementInfo struct {
	DPE          string
	Exists       bool
	Error        string
	Type         string // datapoint type
	ElementType  string
	Kind         uint32
	ValueElement bool
	Unit         string
	Description  string
	Format       string
	Value        any // PlainJSON form; nil for structure nodes and on error
	HasValue     bool
	STime        time.Time
	Status       uint64
}

// Service browses WinCC OA through the embedding host.
type Service struct {
	api     oahost.API
	timeout time.Duration

	mu    sync.Mutex
	local string
}

// New returns a browse service on the embedding host client.
func New(c *oahost.Client) *Service {
	return &Service{api: oahost.API{C: c}, timeout: 10 * time.Second}
}

// API returns the host API the service uses.
func (s *Service) API() oahost.API { return s.api }

// LocalSystem returns the name of the local system.
func (s *Service) LocalSystem(ctx context.Context) (string, error) {
	s.mu.Lock()
	local := s.local
	s.mu.Unlock()
	if local != "" {
		return local, nil
	}
	info, err := s.api.SysInfo(ctx)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.local = info.LocalSystem
	s.mu.Unlock()
	return info.LocalSystem, nil
}

// Systems lists the local system and the remote systems reported by the
// host, local first, then by name.
func (s *Service) Systems(ctx context.Context) ([]System, error) {
	local, err := s.LocalSystem(ctx)
	if err != nil {
		return nil, err
	}
	out := []System{{Name: local, Local: true, Connected: true}}
	remote, _ := s.api.C.Systems()
	names := make([]string, 0, len(remote))
	for n := range remote {
		if n != local {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		out = append(out, System{Name: n, Connected: remote[n]})
	}
	return out, nil
}

// System resolves a system argument: "" and the local name are the local
// system; a remote system must be known and connected.
func (s *Service) System(ctx context.Context, system string) (name string, local bool, err error) {
	l, err := s.LocalSystem(ctx)
	if err != nil {
		return "", false, err
	}
	system = strings.TrimSuffix(system, ":")
	if system == "" || system == l {
		return l, true, nil
	}
	remote, _ := s.api.C.Systems()
	up, known := remote[system]
	if !known {
		return "", false, fmt.Errorf("%w: unknown system %s", ErrUnavailable, system)
	}
	if !up {
		return "", false, fmt.Errorf("%w: system %s is not connected", ErrUnavailable, system)
	}
	return system, false, nil
}

// hostSystem is the system argument passed to the host ("" for local).
func hostSystem(name string, local bool) string {
	if local {
		return ""
	}
	return name
}

// Types lists datapoint type names matching pattern (dpTypes syntax),
// sorted, without internal and MMQ store types.
func (s *Service) Types(ctx context.Context, system, pattern string) ([]string, error) {
	name, local, err := s.System(ctx, system)
	if err != nil {
		return nil, err
	}
	if pattern == "" {
		pattern = "*"
	}
	all, err := s.api.TypeNames(ctx, hostSystem(name, local), pattern, s.timeout)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]string, 0, len(all))
	for _, t := range all {
		if !winccoanative.ProtectedType(t) {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out, nil
}

// TypeStructure returns the element tree of a datapoint type.
func (s *Service) TypeStructure(ctx context.Context, system, typeName string) (*TypeStructure, error) {
	name, local, err := s.System(ctx, system)
	if err != nil {
		return nil, err
	}
	if typeName == "" {
		return nil, fmt.Errorf("%w: type is required", ErrInvalid)
	}
	if winccoanative.ProtectedType(typeName) {
		return nil, fmt.Errorf("%w: datapoint type %s", ErrNotFound, typeName)
	}
	raw, err := s.api.TypeStructure(ctx, hostSystem(name, local), typeName, s.timeout)
	if err != nil {
		return nil, mapErr(err)
	}
	ts := &TypeStructure{System: name, Type: typeName, Elements: make([]TypeElement, 0, len(raw))}
	for _, e := range raw {
		ts.Elements = append(ts.Elements, typeElement(typeName, e))
	}
	return ts, nil
}

func typeElement(typeName string, e oahost.TypeElement) TypeElement {
	te := TypeElement{
		Path:          e.Path,
		ElementType:   oahost.OaTypeName(e.OaType),
		Kind:          e.Kind,
		ValueElement:  e.Kind != oahost.ElemStruct,
		TypeReference: e.RefType,
		Children:      e.Children,
	}
	if e.Path == "" {
		te.Name = typeName
		return te
	}
	te.Depth = strings.Count(e.Path, ".") + 1
	if i := strings.LastIndex(e.Path, "."); i >= 0 {
		te.Parent, te.Name = e.Path[:i], e.Path[i+1:]
	} else {
		te.Name = e.Path
	}
	return te
}

// Datapoints lists datapoints matching pattern (dpNames syntax, without
// system and element part), optionally of one type, sorted by name. The
// limit is capped at MaxLimit. Internal and MMQ store datapoints are left
// out.
func (s *Service) Datapoints(ctx context.Context, system, pattern, typeName string, limit, offset int) (*DatapointPage, error) {
	name, _, err := s.System(ctx, system)
	if err != nil {
		return nil, err
	}
	if pattern == "" {
		pattern = "*"
	}
	if strings.ContainsAny(pattern, ".:") {
		return nil, fmt.Errorf("%w: pattern must not contain '.' or ':'", ErrInvalid)
	}
	if limit <= 0 {
		limit = DefaultLimit
	}
	if limit > MaxLimit {
		limit = MaxLimit
	}
	if offset < 0 {
		offset = 0
	}
	if typeName != "" && winccoanative.ProtectedType(typeName) {
		return &DatapointPage{Items: []Datapoint{}, Offset: offset, Limit: limit}, nil
	}
	all, err := s.api.DpNames(ctx, name+":"+pattern, typeName, s.timeout)
	if err != nil {
		return nil, mapErr(err)
	}
	dps := make([]string, 0, len(all))
	for _, full := range all {
		_, dp := splitSystem(full)
		if !winccoanative.Protected(dp) {
			dps = append(dps, dp)
		}
	}
	sort.Strings(dps)
	dps = dedupSorted(dps)
	page := &DatapointPage{Items: []Datapoint{}, Total: len(dps), Offset: offset, Limit: limit}
	if offset >= len(dps) {
		return page, nil
	}
	end := min(offset+limit, len(dps))
	sel := dps[offset:end]
	types := make([]string, len(sel))
	if typeName != "" {
		for i := range types {
			types[i] = typeName
		}
	} else {
		names := make([]string, len(sel))
		for i, dp := range sel {
			names[i] = name + ":" + dp + "."
		}
		metas, err := s.meta(ctx, names)
		if err != nil {
			return nil, err
		}
		for i, m := range metas {
			types[i] = m.TypeName
		}
	}
	for i, dp := range sel {
		page.Items = append(page.Items, Datapoint{Name: dp, System: name, Type: types[i]})
	}
	return page, nil
}

// Elements returns the element tree of a datapoint (name with optional
// system prefix, without element part).
func (s *Service) Elements(ctx context.Context, dp string) ([]Element, error) {
	sys, name := splitSystem(strings.TrimSuffix(dp, "."))
	if name == "" || strings.Contains(name, ".") {
		return nil, fmt.Errorf("%w: datapoint name without element part expected", ErrInvalid)
	}
	sysName, local, err := s.System(ctx, sys)
	if err != nil {
		return nil, err
	}
	if winccoanative.Protected(name) {
		return nil, fmt.Errorf("%w: datapoint %s", ErrNotFound, name)
	}
	metas, err := s.meta(ctx, []string{sysName + ":" + name + "."})
	if err != nil {
		return nil, err
	}
	if !metas[0].Exists {
		return nil, fmt.Errorf("%w: datapoint %s:%s", ErrNotFound, sysName, name)
	}
	raw, err := s.api.TypeStructure(ctx, hostSystem(sysName, local), metas[0].TypeName, s.timeout)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]Element, 0, len(raw))
	for _, e := range raw {
		te := typeElement(metas[0].TypeName, e)
		if e.Path == "" {
			te.Name = name
		}
		out = append(out, Element{DPE: sysName + ":" + name + "." + e.Path, TypeElement: te})
	}
	return out, nil
}

// ElementInfo returns metadata and current value of at most
// MaxElementInfoDPEs DPEs. A missing DPE is reported in its item.
func (s *Service) ElementInfo(ctx context.Context, dpes []string) ([]ElementInfo, error) {
	if len(dpes) > MaxElementInfoDPEs {
		return nil, fmt.Errorf("%w: at most %d DPEs per call, got %d", ErrInvalid, MaxElementInfoDPEs, len(dpes))
	}
	local, err := s.LocalSystem(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ElementInfo, len(dpes))
	var query []string
	var qidx []int
	for i, d := range dpes {
		out[i].DPE = d
		sys, rest := splitSystem(d)
		if sys == "" {
			sys = local
		}
		dpName, _, _ := strings.Cut(rest, ".")
		if strings.Contains(rest, ":") || dpName == "" {
			out[i].Error = "invalid DPE name"
			continue
		}
		if winccoanative.Protected(dpName) {
			out[i].Error = "datapoint not found"
			continue
		}
		if _, _, err := s.System(ctx, sys); err != nil {
			out[i].Error = err.Error()
			continue
		}
		full := sys + ":" + rest
		if !strings.Contains(rest, ".") {
			full += "."
		}
		out[i].DPE = full
		query = append(query, full)
		qidx = append(qidx, i)
	}
	metas, err := s.meta(ctx, query)
	if err != nil {
		return nil, err
	}
	var reads []string
	var ridx []int
	for j, m := range metas {
		i := qidx[j]
		if !m.Exists {
			out[i].Error = m.Error
			if out[i].Error == "" {
				out[i].Error = "datapoint element not found"
			}
			continue
		}
		out[i].Exists = true
		out[i].Type = m.TypeName
		out[i].ElementType = oahost.OaTypeName(m.OaType)
		out[i].Kind = m.Kind
		out[i].ValueElement = m.Kind != oahost.ElemStruct
		out[i].Unit, out[i].Format, out[i].Description = m.Unit, m.Format, m.Description
		if m.Kind != oahost.ElemStruct && m.Kind != oahost.ElemUnsupported {
			d := out[i].DPE
			reads = append(reads, d+":_online.._value", d+":_online.._stime", d+":_online.._status")
			ridx = append(ridx, i)
		}
	}
	if len(reads) > 0 {
		vals, err := s.api.DpGet(ctx, reads, s.timeout)
		if err != nil {
			// One failing element fails the whole dpGet; read one by one.
			for k, i := range ridx {
				v, err := s.api.DpGet(ctx, reads[3*k:3*k+3], s.timeout)
				if err != nil {
					out[i].Error = mapErr(err).Error()
					continue
				}
				setValue(&out[i], v)
			}
		} else {
			for k, i := range ridx {
				setValue(&out[i], vals[3*k:3*k+3])
			}
		}
	}
	return out, nil
}

func setValue(e *ElementInfo, v []oahost.Value) {
	e.Value = v[0].PlainJSON()
	e.HasValue = true
	e.STime = v[1].Time
	e.Status = v[2].Uint
}

// meta reads element metadata in batches.
func (s *Service) meta(ctx context.Context, names []string) ([]oahost.ElementMeta, error) {
	out := make([]oahost.ElementMeta, 0, len(names))
	for start := 0; start < len(names); start += metaBatch {
		end := min(start+metaBatch, len(names))
		m, err := s.api.ElementMeta(ctx, names[start:end], s.timeout)
		if err != nil {
			return nil, mapErr(err)
		}
		out = append(out, m...)
	}
	return out, nil
}

// splitSystem splits "Sys:rest" when the prefix is a system name (no dot).
func splitSystem(name string) (string, string) {
	if i := strings.Index(name, ":"); i >= 0 && !strings.Contains(name[:i], ".") {
		return name[:i], name[i+1:]
	}
	return "", name
}

func dedupSorted(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, oahost.ErrUnavailable):
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	case errors.Is(err, oahost.ErrNotFound):
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case errors.Is(err, oahost.ErrInvalid):
		if strings.Contains(err.Error(), "unknown operation") {
			return fmt.Errorf("the WinCC OA manager does not support browsing; update WCCOAmmq: %w", err)
		}
	}
	return err
}

// SystemState reports whether a system is known and connected; the local
// system ("" or its name) always is.
func (s *Service) SystemState(ctx context.Context, system string) (known, connected bool) {
	local, err := s.LocalSystem(ctx)
	if err != nil {
		return false, false
	}
	if system == "" || system == local {
		return true, true
	}
	remote, _ := s.api.C.Systems()
	up, ok := remote[system]
	return ok, ok && up
}

// DatapointTypes returns the datapoint type of each datapoint (names
// without system and element part), "" for a missing one.
func (s *Service) DatapointTypes(ctx context.Context, system string, dps []string) ([]string, error) {
	name, _, err := s.System(ctx, system)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(dps))
	for i, dp := range dps {
		names[i] = name + ":" + dp + "."
	}
	metas, err := s.meta(ctx, names)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(dps))
	for i, m := range metas {
		if m.Exists && !winccoanative.Protected(dps[i]) {
			out[i] = m.TypeName
		}
	}
	return out, nil
}

// MatchDatapoints returns every datapoint name (without system) matching a
// dpNames pattern, optionally of one type, sorted, without internal and
// MMQ store datapoints. Unlike Datapoints it is not paged.
func (s *Service) MatchDatapoints(ctx context.Context, system, pattern, typeName string) ([]string, error) {
	name, _, err := s.System(ctx, system)
	if err != nil {
		return nil, err
	}
	all, err := s.api.DpNames(ctx, name+":"+pattern, typeName, s.timeout)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]string, 0, len(all))
	for _, full := range all {
		_, dp := splitSystem(full)
		if !winccoanative.Protected(dp) {
			out = append(out, dp)
		}
	}
	sort.Strings(out)
	return dedupSorted(out), nil
}
