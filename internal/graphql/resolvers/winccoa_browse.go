package resolvers

import (
	"context"
	"encoding/json"
	"strings"

	"monstermq.io/edge/internal/graphql/generated"
	"monstermq.io/edge/internal/winccoabrowse"
)

type winCCOaQueryResolver struct{ *Resolver }

func (r *Resolver) WinCCOaQuery() generated.WinCCOaQueryResolver { return &winCCOaQueryResolver{r} }

// Winccoa is null unless the broker runs embedded in WinCC OA.
func (r *queryResolver) Winccoa(ctx context.Context) (*generated.WinCCOaQuery, error) {
	if r.Browse == nil {
		return nil, nil
	}
	return &generated.WinCCOaQuery{}, nil
}

// requireBrowse checks read permission on the native branch of a system.
func (r *Resolver) requireBrowse(ctx context.Context, system string) error {
	root := "winccoa"
	systems := "systems"
	if r.NativeNames != nil {
		n := r.NativeNames()
		root, systems = n.Root, n.Systems
	}
	if system == "" {
		local, err := r.Browse.LocalSystem(ctx)
		if err != nil {
			return err
		}
		system = local
	}
	return r.requireTopic(ctx, root+"/"+systems+"/"+system, false)
}

func (r *winCCOaQueryResolver) Systems(ctx context.Context, _ *generated.WinCCOaQuery) ([]*generated.WinCCOaSystem, error) {
	if err := r.requireBrowse(ctx, ""); err != nil {
		return nil, err
	}
	sys, err := r.Browse.Systems(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*generated.WinCCOaSystem, len(sys))
	for i, s := range sys {
		out[i] = &generated.WinCCOaSystem{Name: s.Name, Local: s.Local, Connected: s.Connected}
	}
	return out, nil
}

func (r *winCCOaQueryResolver) Types(ctx context.Context, _ *generated.WinCCOaQuery, system *string, pattern *string) ([]string, error) {
	if err := r.requireBrowse(ctx, derefStr(system)); err != nil {
		return nil, err
	}
	return r.Browse.Types(ctx, derefStr(system), derefStr(pattern))
}

func (r *winCCOaQueryResolver) TypeStructure(ctx context.Context, _ *generated.WinCCOaQuery, system *string, typeArg string) (*generated.WinCCOaTypeStructure, error) {
	if err := r.requireBrowse(ctx, derefStr(system)); err != nil {
		return nil, err
	}
	ts, err := r.Browse.TypeStructure(ctx, derefStr(system), typeArg)
	if err != nil {
		return nil, err
	}
	out := &generated.WinCCOaTypeStructure{System: ts.System, Type: ts.Type, Elements: make([]*generated.WinCCOaTypeElement, len(ts.Elements))}
	for i, e := range ts.Elements {
		out.Elements[i] = &generated.WinCCOaTypeElement{
			Path: e.Path, Name: e.Name, Parent: e.Parent, Depth: e.Depth, ElementType: e.ElementType,
			ValueElement: e.ValueElement, TypeReference: optString(e.TypeReference), ChildCount: e.Children,
		}
	}
	return out, nil
}

func (r *winCCOaQueryResolver) Datapoints(ctx context.Context, _ *generated.WinCCOaQuery, system *string, pattern *string, typeArg *string, limit *int, offset *int) (*generated.WinCCOaDatapointPage, error) {
	if err := r.requireBrowse(ctx, derefStr(system)); err != nil {
		return nil, err
	}
	l, o := winccoabrowse.DefaultLimit, 0
	if limit != nil {
		l = *limit
	}
	if offset != nil {
		o = *offset
	}
	p, err := r.Browse.Datapoints(ctx, derefStr(system), derefStr(pattern), derefStr(typeArg), l, o)
	if err != nil {
		return nil, err
	}
	out := &generated.WinCCOaDatapointPage{Items: make([]*generated.WinCCOaDatapoint, len(p.Items)), Total: p.Total, Offset: p.Offset, Limit: p.Limit}
	for i, d := range p.Items {
		out.Items[i] = &generated.WinCCOaDatapoint{Name: d.Name, System: d.System, Type: d.Type}
	}
	return out, nil
}

func (r *winCCOaQueryResolver) Elements(ctx context.Context, _ *generated.WinCCOaQuery, dp string) ([]*generated.WinCCOaElement, error) {
	sys := ""
	if i := strings.Index(dp, ":"); i >= 0 {
		sys = dp[:i]
	}
	if err := r.requireBrowse(ctx, sys); err != nil {
		return nil, err
	}
	els, err := r.Browse.Elements(ctx, dp)
	if err != nil {
		return nil, err
	}
	out := make([]*generated.WinCCOaElement, len(els))
	for i, e := range els {
		out[i] = &generated.WinCCOaElement{
			Dpe: e.DPE, Path: e.Path, Name: e.Name, Parent: e.Parent, Depth: e.Depth, ElementType: e.ElementType,
			ValueElement: e.ValueElement, TypeReference: optString(e.TypeReference), ChildCount: e.Children,
		}
	}
	return out, nil
}

func (r *winCCOaQueryResolver) ElementInfo(ctx context.Context, _ *generated.WinCCOaQuery, dpes []string) ([]*generated.WinCCOaElementInfo, error) {
	checked := map[string]bool{}
	for _, d := range dpes {
		sys := ""
		if i := strings.Index(d, ":"); i >= 0 && !strings.Contains(d[:i], ".") {
			sys = d[:i]
		}
		if checked[sys] {
			continue
		}
		checked[sys] = true
		if err := r.requireBrowse(ctx, sys); err != nil {
			return nil, err
		}
	}
	infos, err := r.Browse.ElementInfo(ctx, dpes)
	if err != nil {
		return nil, err
	}
	out := make([]*generated.WinCCOaElementInfo, len(infos))
	for i, e := range infos {
		g := &generated.WinCCOaElementInfo{Dpe: e.DPE, Exists: e.Exists, Error: optString(e.Error), ValueElement: e.ValueElement}
		if e.Exists {
			g.Type, g.ElementType = optString(e.Type), optString(e.ElementType)
			g.Unit, g.Description, g.Format = &e.Unit, &e.Description, &e.Format
		}
		if e.HasValue {
			b, _ := json.Marshal(e.Value)
			v := string(b)
			g.Value = &v
			st := e.STime.UTC().Format("2006-01-02T15:04:05.000Z")
			g.Stime = &st
			status := int64(e.Status)
			g.Status = &status
		}
		out[i] = g
	}
	return out, nil
}

func optString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
