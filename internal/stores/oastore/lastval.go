package oastore

import (
	"context"
	"errors"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/topic"
	"monstermq.io/edge/internal/winccoabrowse"
	"monstermq.io/edge/internal/winccoanative"
)

// LastValStore implements stores.MessageStore and stores.LevelBrowser over
// WinCC OA online values. Writes and deletes are no-ops.
type LastValStore struct {
	name   string
	r      *reader
	browse *winccoabrowse.Service
}

var (
	_ stores.MessageStore = (*LastValStore)(nil)
	_ stores.LevelBrowser = (*LastValStore)(nil)
)

func (s *LastValStore) Name() string                          { return s.name }
func (s *LastValStore) Type() stores.MessageStoreType         { return stores.MessageStoreWinCCOA }
func (s *LastValStore) AddAll(context.Context, []stores.BrokerMessage) error { return nil }
func (s *LastValStore) DelAll(context.Context, []string) error               { return nil }
func (s *LastValStore) PurgeOlderThan(context.Context, time.Time) (stores.PurgeResult, error) {
	return stores.PurgeResult{}, nil
}
func (s *LastValStore) PurgeExpired(context.Context) (stores.PurgeResult, error) {
	return stores.PurgeResult{}, nil
}
func (s *LastValStore) EnsureTable(ctx context.Context) error { return s.r.probe(ctx) }
func (s *LastValStore) Close() error                          { return nil }

func (s *LastValStore) Get(ctx context.Context, topicName string) (*stores.BrokerMessage, error) {
	rt, err := s.r.resolveExact(ctx, topicName, false)
	if err != nil || rt == nil {
		return nil, err
	}
	addrs := []string{
		rt.target.ReadAddress(rt.system),
		rt.target.DPE(rt.system) + ":_online.._stime",
	}
	vals, err := s.r.opts.API.DpGet(ctx, addrs, getTimeout)
	if err != nil {
		if errors.Is(err, oahost.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if len(vals) < 2 {
		return nil, oahost.ErrMalformed
	}
	ts := vals[1].Time
	return &stores.BrokerMessage{
		TopicName: rt.topic,
		Payload:   winccoanative.ValuePayload(vals[0], ts),
		QoS:       1,
		Time:      ts,
	}, nil
}

func (s *LastValStore) FindMatchingMessages(ctx context.Context, pattern string, yield func(stores.BrokerMessage) bool) error {
	f, _ := winccoanative.SplitShared(pattern)
	if !winccoanative.HasWildcard(f) {
		msg, err := s.Get(ctx, f)
		if err != nil || msg == nil {
			return err
		}
		yield(*msg)
		return nil
	}
	origFilter, targets, err := s.r.expandFilter(ctx, pattern)
	if err != nil || len(targets) == 0 {
		return err
	}
	remaining := s.r.maxRows()
	dpTypeCache := map[string]string{}
	for _, et := range targets {
		if remaining <= 0 {
			break
		}
		if !et.isWild {
			msg, err := s.Get(ctx, et.exact)
			if err != nil {
				return err
			}
			if msg != nil && topic.MatchFilter(origFilter, msg.TopicName) {
				remaining--
				if !yield(*msg) {
					return nil
				}
			}
			continue
		}
		stopped := false
		used, _, err := s.r.runWildQuery(
			ctx,
			origFilter,
			et.wild,
			[]string{"_online.._value", "_online.._stime"},
			remaining,
			dpTypeCache,
			func(_ winccoanative.Target, _ string, top string, row []oahost.Value) bool {
				ts := row[2].Time
				msg := stores.BrokerMessage{
					TopicName: top,
					Payload:   winccoanative.ValuePayload(row[1], ts),
					QoS:       1,
					Time:      ts,
				}
				if !yield(msg) {
					stopped = true
					return false
				}
				return true
			},
		)
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
		remaining -= used
	}
	return nil
}

func (s *LastValStore) FindMatchingTopics(ctx context.Context, pattern string, yield func(string) bool) error {
	f, _ := winccoanative.SplitShared(pattern)
	if !winccoanative.HasWildcard(f) {
		rt, err := s.r.resolveExact(ctx, f, false)
		if err != nil || rt == nil {
			return err
		}
		yield(rt.topic)
		return nil
	}
	origFilter, targets, err := s.r.expandFilter(ctx, pattern)
	if err != nil || len(targets) == 0 {
		return err
	}
	remaining := s.r.maxRows()
	dpTypeCache := map[string]string{}
	for _, et := range targets {
		if remaining <= 0 {
			break
		}
		if !et.isWild {
			rt, err := s.r.resolveExact(ctx, et.exact, false)
			if err != nil {
				return err
			}
			if rt != nil && topic.MatchFilter(origFilter, rt.topic) {
				remaining--
				if !yield(rt.topic) {
					return nil
				}
			}
			continue
		}
		stopped := false
		used, _, err := s.r.runWildQuery(
			ctx,
			origFilter,
			et.wild,
			[]string{"_online.._stime"},
			remaining,
			dpTypeCache,
			func(_ winccoanative.Target, _ string, top string, _ []oahost.Value) bool {
				if !yield(top) {
					stopped = true
					return false
				}
				return true
			},
		)
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
		remaining -= used
	}
	return nil
}
