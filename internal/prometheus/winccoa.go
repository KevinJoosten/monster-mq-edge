package prometheus

import (
	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/winccoanative"
)

func isWinCCOAGroup(g *archive.Group) bool {
	if g == nil {
		return false
	}
	return g.Config().LastValType == stores.MessageStoreWinCCOA || g.Config().ArchiveType == stores.ArchiveWinCCOA
}

func (s *Server) isScopedFilter(filter string) bool {
	if s.deps.NativeNames == nil {
		return false
	}
	f, _ := winccoanative.SplitShared(filter)
	return s.deps.NativeNames().Classify(f) == winccoanative.KindNative
}

func (s *Server) isBroadFilter(filter string) bool {
	return !s.isScopedFilter(filter)
}

func (s *Server) scopedFilters(g *archive.Group) []string {
	if g == nil {
		return nil
	}
	var out []string
	for _, f := range g.Config().TopicFilters {
		if s.isScopedFilter(f) {
			out = append(out, f)
		}
	}
	return out
}

func (s *Server) shouldSkipWinCCOAEnumeration(g *archive.Group, groupFilter, topicFilter string) bool {
	if !isWinCCOAGroup(g) {
		return false
	}
	if groupFilter != "" && groupFilter == g.Name() {
		return false
	}
	if topicFilter != "" && s.isScopedFilter(topicFilter) {
		return false
	}
	return true
}

type groupForTopicResolver interface {
	ResolveGroupForTopic(requested, topic string, needArchive bool) *archive.Group
}

func (s *Server) resolveGroup(requested, topic string, needArchive bool) (*archive.Group, string) {
	if s.deps.Archives == nil {
		if requested == "" {
			return nil, "Default"
		}
		return nil, requested
	}
	if r, ok := any(s.deps.Archives).(groupForTopicResolver); ok {
		g := r.ResolveGroupForTopic(requested, topic, needArchive)
		if g != nil {
			return g, g.Name()
		}
		if requested == "" {
			return nil, "Default"
		}
		return nil, requested
	}
	name := requested
	if name == "" {
		name = "Default"
	}
	return s.deps.Archives.Get(name), name
}
