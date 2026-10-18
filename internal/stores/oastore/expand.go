package oastore

import (
	"context"
	"errors"
	"sort"
	"strings"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/topic"
	"monstermq.io/edge/internal/winccoanative"
)

var errStopIteration = errors.New("stop iteration")

type expandedTarget struct {
	exact  string
	wild   winccoanative.WildTarget
	isWild bool
}

func (r *reader) availableSystems(local string) []string {
	out := []string{local}
	if r.opts.API.C == nil {
		return out
	}
	remotes, _ := r.opts.API.C.Systems()
	var names []string
	for sys, up := range remotes {
		if up && sys != local {
			names = append(names, sys)
		}
	}
	sort.Strings(names)
	return append(out, names...)
}

// expandFilter turns an MQTT topic filter into a list of exact or wildcard
// native read targets per plan §2.3.
func (r *reader) expandFilter(ctx context.Context, filter string) (string, []expandedTarget, error) {
	f, _ := winccoanative.SplitShared(filter)
	if f == "" {
		return "", nil, nil
	}
	n := r.names()

	// 1. No wildcard.
	if !winccoanative.HasWildcard(f) {
		if n.Classify(f) == winccoanative.KindNative {
			return f, []expandedTarget{{exact: f}}, nil
		}
		return f, nil, nil
	}

	local, err := r.localSystem(ctx)
	if err != nil {
		return f, nil, err
	}

	// 2. Scoped native filter.
	if n.Classify(f) == winccoanative.KindNative {
		w, err := n.ParseWildcard(f)
		if err != nil {
			return f, nil, nil
		}
		if w.TypeName != "" && winccoanative.ProtectedType(w.TypeName) {
			return f, nil, nil
		}
		w.Remote = w.System != "" && w.System != local
		return f, []expandedTarget{{wild: w, isWild: true}}, nil
	}

	// 3. Broad filter.
	L := strings.Split(f, "/")
	R := strings.Split(n.Root, "/")
	firstWild := -1
	for idx, seg := range L {
		if seg == "+" || seg == "#" {
			firstWild = idx
			break
		}
	}
	if firstWild < 0 {
		return f, nil, nil
	}

	var prefixes [][]string
	if firstWild <= len(R) {
		p := append([]string(nil), R...)
		if n.NoShortcut {
			p = append(p, n.Systems, local, n.Tags)
		} else {
			p = append(p, n.Tags)
		}
		prefixes = append(prefixes, p)
	} else if firstWild == len(R)+1 && len(L) > len(R) && slicesEqual(L[:len(R)], R) && L[len(R)] == n.Systems {
		for _, sys := range r.availableSystems(local) {
			p := append([]string(nil), R...)
			p = append(p, n.Systems, sys, n.Tags)
			prefixes = append(prefixes, p)
		}
	}

	var out []expandedTarget
	for _, p := range prefixes {
		rewritten, ok := matchBroadPrefix(L, p)
		if !ok {
			continue
		}
		if !winccoanative.HasWildcard(rewritten) {
			if n.Classify(rewritten) == winccoanative.KindNative {
				out = append(out, expandedTarget{exact: rewritten})
			}
			continue
		}
		if n.Classify(rewritten) != winccoanative.KindNative {
			continue
		}
		w, err := n.ParseWildcard(rewritten)
		if err != nil {
			continue
		}
		if w.TypeName != "" && winccoanative.ProtectedType(w.TypeName) {
			continue
		}
		w.Remote = w.System != "" && w.System != local
		out = append(out, expandedTarget{wild: w, isWild: true})
	}
	return f, out, nil
}

func matchBroadPrefix(L, P []string) (string, bool) {
	for k := 0; k < len(P); k++ {
		if k >= len(L) {
			return "", false
		}
		if L[k] == "#" {
			if k != len(L)-1 {
				return "", false
			}
			rewritten := append(append([]string(nil), P...), "#")
			return strings.Join(rewritten, "/"), true
		}
		if L[k] != "+" && L[k] != P[k] {
			return "", false
		}
	}
	if len(L) <= len(P) {
		return "", false
	}
	rewritten := append(append([]string(nil), P...), L[len(P):]...)
	return strings.Join(rewritten, "/"), true
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// runWildQuery executes a DpQuery for w with attrs, resolves dynamic DP types
// for types/+ or types/# in batches of 200, and invokes yield for each matching row.
func (r *reader) runWildQuery(
	ctx context.Context,
	origFilter string,
	w winccoanative.WildTarget,
	attrs []string,
	maxRows int,
	dpTypeCache map[string]string,
	yield func(target winccoanative.Target, sys string, topicName string, row []oahost.Value) bool,
) (int, bool, error) {
	if maxRows <= 0 {
		return 0, false, nil
	}
	local, err := r.localSystem(ctx)
	if err != nil {
		return 0, false, err
	}
	sys := w.System
	if sys == "" {
		sys = local
	}
	q := w.QueryAttrs(attrs...)
	needCols := 1 + len(attrs)
	delivered := 0

	truncated, err := r.opts.API.DpQuery(ctx, q, maxRows, queryTimeout, func(page oahost.QueryPage) error {
		rows := page.Rows
		if w.Types && w.TypeName == "" {
			if err := r.populateDPTypes(ctx, rows, dpTypeCache); err != nil {
				return err
			}
		}
		for _, row := range rows {
			if len(row) < needCols {
				continue
			}
			dpeFull := row[0].Str
			typeName := w.TypeName
			if w.Types && typeName == "" {
				dpRoot := dpRootOf(dpeFull)
				if dpRoot == "" {
					continue
				}
				typeName = dpTypeCache[dpRoot]
				if typeName == "" || winccoanative.ProtectedType(typeName) {
					continue
				}
			}
			t, ok := w.RowTarget(dpeFull, typeName)
			if !ok {
				continue
			}
			top := t.Topic()
			if !topic.MatchFilter(origFilter, top) {
				continue
			}
			delivered++
			if !yield(t, sys, top, row) {
				return errStopIteration
			}
		}
		return nil
	})
	if errors.Is(err, errStopIteration) {
		return delivered, false, nil
	}
	if err != nil {
		return delivered, false, err
	}
	if truncated {
		r.warnTruncated(delivered, q)
	}
	return delivered, truncated, nil
}

// runArchiveWildQuery executes w.QueryArchived() to find DPEs that have an
// active value-archive configuration, filters out alert-only archive configs
// (_NGA_G_ALERT) and duplicate rows, resolves dynamic DP types when w.Types is
// set, and invokes yield for each matching DPE.
func (r *reader) runArchiveWildQuery(
	ctx context.Context,
	origFilter string,
	w winccoanative.WildTarget,
	maxRows int,
	dpTypeCache map[string]string,
	yield func(target winccoanative.Target, sys string, topicName string) bool,
) (int, bool, error) {
	if maxRows <= 0 {
		return 0, false, nil
	}
	local, err := r.localSystem(ctx)
	if err != nil {
		return 0, false, err
	}
	sys := w.System
	if sys == "" {
		sys = local
	}
	q := w.QueryArchived()
	delivered := 0
	seen := map[string]bool{}

	truncated, err := r.opts.API.DpQuery(ctx, q, maxRows, queryTimeout, func(page oahost.QueryPage) error {
		rows := page.Rows
		if w.Types {
			if err := r.populateDPTypes(ctx, rows, dpTypeCache); err != nil {
				return err
			}
		}
		for _, row := range rows {
			if len(row) < 2 {
				continue
			}
			dpeFull := row[0].Str
			archClass := row[1].Str
			if archClass == "" || archClass == "_NGA_G_ALERT" || strings.HasSuffix(archClass, ":_NGA_G_ALERT") {
				continue
			}
			if seen[dpeFull] {
				continue
			}
			typeName := w.TypeName
			if w.Types {
				dpRoot := dpRootOf(dpeFull)
				if dpRoot == "" {
					continue
				}
				actualType := dpTypeCache[dpRoot]
				if actualType == "" || winccoanative.ProtectedType(actualType) {
					continue
				}
				if w.TypeName != "" && actualType != w.TypeName {
					continue
				}
				typeName = actualType
			}
			t, ok := w.RowTarget(dpeFull, typeName)
			if !ok {
				continue
			}
			top := t.Topic()
			if !topic.MatchFilter(origFilter, top) {
				continue
			}
			seen[dpeFull] = true
			delivered++
			if !yield(t, sys, top) {
				return errStopIteration
			}
		}
		return nil
	})
	if errors.Is(err, errStopIteration) {
		return delivered, false, nil
	}
	if err != nil {
		return delivered, false, err
	}
	if truncated {
		r.warnTruncated(delivered, q)
	}
	return delivered, truncated, nil
}

func dpRootOf(full string) string {
	sys, rest, ok := strings.Cut(full, ":")
	if !ok {
		return ""
	}
	dp, _, _ := strings.Cut(rest, ".")
	if dp == "" || winccoanative.Protected(dp) {
		return ""
	}
	return sys + ":" + dp + "."
}

func (r *reader) populateDPTypes(ctx context.Context, rows [][]oahost.Value, cache map[string]string) error {
	var missing []string
	seen := map[string]bool{}
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		root := dpRootOf(row[0].Str)
		if root == "" {
			continue
		}
		if _, ok := cache[root]; ok || seen[root] {
			continue
		}
		seen[root] = true
		missing = append(missing, root)
	}
	for start := 0; start < len(missing); start += typeLookupBatch {
		end := min(start+typeLookupBatch, len(missing))
		batch := missing[start:end]
		metas, err := r.opts.API.ElementMeta(ctx, batch, queryTimeout)
		if err != nil {
			return err
		}
		for i, m := range metas {
			if m.Exists {
				cache[batch[i]] = m.TypeName
			} else {
				cache[batch[i]] = ""
			}
		}
	}
	return nil
}
