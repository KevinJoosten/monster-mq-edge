package oastore

import (
	"context"
	"errors"
	"strings"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/winccoabrowse"
	"monstermq.io/edge/internal/winccoanative"
)

// BrowseLevel implements stores.LevelBrowser on LastValStore.
func (s *LastValStore) BrowseLevel(ctx context.Context, pattern string, yield func(name string, leaf bool) bool) (bool, error) {
	segs := strings.Split(pattern, "/")
	if len(segs) == 0 || segs[len(segs)-1] != "+" {
		return false, nil
	}
	prefix := segs[:len(segs)-1]
	for _, p := range prefix {
		if p == "" || strings.ContainsAny(p, "+#") {
			return false, nil
		}
	}

	n := s.r.names()
	rootSegs := strings.Split(n.Root, "/")
	if len(prefix) < len(rootSegs) {
		if !slicesEqual(prefix, rootSegs[:len(prefix)]) {
			return false, nil
		}
		yield(rootSegs[len(prefix)], false)
		return true, nil
	}
	if !slicesEqual(prefix[:len(rootSegs)], rootSegs) {
		return false, nil
	}

	rest := prefix[len(rootSegs):]
	if len(rest) == 0 {
		if !n.NoShortcut {
			if !yield(n.Tags, false) {
				return true, nil
			}
			if !yield(n.Types, false) {
				return true, nil
			}
		}
		yield(n.Systems, false)
		return true, nil
	}

	if rest[0] == n.Systems {
		if len(rest) == 1 {
			systems, err := s.browse.Systems(ctx)
			if err != nil {
				return true, err
			}
			maxRows := s.r.maxRows()
			for i, sys := range systems {
				if i >= maxRows {
					break
				}
				if !yield(winccoanative.EncodeName(sys.Name, false), false) {
					break
				}
			}
			return true, nil
		}
		sys, err := winccoanative.DecodeName(rest[1], false)
		if err != nil || sys == "" {
			return false, nil
		}
		if len(rest) == 2 {
			if !yield(n.Tags, false) {
				return true, nil
			}
			yield(n.Types, false)
			return true, nil
		}
		return s.browseBranch(ctx, sys, rest[2:], yield)
	}

	if n.NoShortcut {
		return false, nil
	}
	return s.browseBranch(ctx, "", rest, yield)
}

func (s *LastValStore) browseBranch(ctx context.Context, sys string, branch []string, yield func(name string, leaf bool) bool) (bool, error) {
	n := s.r.names()
	maxRows := s.r.maxRows()

	switch branch[0] {
	case n.Tags:
		if len(branch) == 1 {
			return true, s.browseDatapoints(ctx, sys, "", maxRows, yield)
		}
		dp, err := winccoanative.DecodeName(branch[1], false)
		if err != nil || dp == "" {
			return false, nil
		}
		elSegs := make([]string, 0, len(branch)-2)
		for _, seg := range branch[2:] {
			el, err := winccoanative.DecodeName(seg, false)
			if err != nil || el == "" {
				return false, nil
			}
			elSegs = append(elSegs, el)
		}
		return true, s.browseDPElements(ctx, sys, dp, "", elSegs, maxRows, yield)

	case n.Types:
		if len(branch) == 1 {
			types, err := s.browse.Types(ctx, sys, "*")
			if err != nil {
				return true, err
			}
			for i, t := range types {
				if i >= maxRows {
					break
				}
				if !yield(winccoanative.EncodeName(t, false), false) {
					break
				}
			}
			return true, nil
		}
		dpt, err := winccoanative.DecodeName(branch[1], false)
		if err != nil || dpt == "" {
			return false, nil
		}
		if len(branch) == 2 {
			return true, s.browseDatapoints(ctx, sys, dpt, maxRows, yield)
		}
		dp, err := winccoanative.DecodeName(branch[2], false)
		if err != nil || dp == "" {
			return false, nil
		}
		elSegs := make([]string, 0, len(branch)-3)
		for _, seg := range branch[3:] {
			el, err := winccoanative.DecodeName(seg, false)
			if err != nil || el == "" {
				return false, nil
			}
			elSegs = append(elSegs, el)
		}
		return true, s.browseDPElements(ctx, sys, dp, dpt, elSegs, maxRows, yield)

	default:
		return false, nil
	}
}

func (s *LastValStore) browseDatapoints(ctx context.Context, sys, dpt string, maxRows int, yield func(name string, leaf bool) bool) error {
	if dpt != "" && winccoanative.ProtectedType(dpt) {
		return nil
	}
	typeLeaf := map[string]bool{}
	count := 0
	for offset := 0; count < maxRows; offset += winccoabrowse.MaxLimit {
		page, err := s.browse.Datapoints(ctx, sys, "*", dpt, winccoabrowse.MaxLimit, offset)
		if err != nil {
			return err
		}
		if len(page.Items) == 0 {
			break
		}
		for _, item := range page.Items {
			if count >= maxRows {
				break
			}
			if item.Type == "" || winccoanative.ProtectedType(item.Type) {
				continue
			}
			leaf, known := typeLeaf[item.Type]
			if !known {
				ts, err := s.browse.TypeStructure(ctx, sys, item.Type)
				if err != nil {
					return err
				}
				for _, e := range ts.Elements {
					if e.Path == "" {
						leaf = e.ValueElement && e.Children == 0
						break
					}
				}
				typeLeaf[item.Type] = leaf
			}
			count++
			if !yield(winccoanative.EncodeName(item.Name, leaf), leaf) {
				return nil
			}
		}
		if offset+len(page.Items) >= page.Total {
			break
		}
	}
	return nil
}

func (s *LastValStore) browseDPElements(ctx context.Context, sys, dp, wantType string, elSegs []string, maxRows int, yield func(name string, leaf bool) bool) error {
	if winccoanative.Protected(dp) || (wantType != "" && winccoanative.ProtectedType(wantType)) {
		return nil
	}
	if wantType != "" {
		local, err := s.r.localSystem(ctx)
		if err != nil {
			return err
		}
		targetSys := sys
		if targetSys == "" {
			targetSys = local
		}
		rctx, cancel := context.WithTimeout(ctx, getTimeout)
		res, err := s.r.opts.API.Resolve(rctx, targetSys+":"+dp+".")
		cancel()
		if err != nil {
			if errors.Is(err, oahost.ErrNotFound) {
				return nil
			}
			return err
		}
		if !res.Exists || res.TypeName != wantType {
			return nil
		}
	}

	dpArg := dp
	if sys != "" {
		dpArg = sys + ":" + dp
	}
	els, err := s.browse.Elements(ctx, dpArg)
	if err != nil {
		if errors.Is(err, winccoabrowse.ErrNotFound) {
			return nil
		}
		return err
	}

	parentPath := strings.Join(elSegs, ".")
	count := 0
	for _, e := range els {
		if e.Path == "" || e.Parent != parentPath {
			continue
		}
		if count >= maxRows {
			break
		}
		leaf := e.ValueElement && e.Children == 0
		count++
		if !yield(winccoanative.EncodeName(e.Name, leaf), leaf) {
			return nil
		}
	}
	return nil
}
