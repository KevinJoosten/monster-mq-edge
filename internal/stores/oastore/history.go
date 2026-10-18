package oastore

import (
	"context"
	"sort"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/topic"
	"monstermq.io/edge/internal/winccoanative"
)

// MessageArchive implements stores.MessageArchive over WinCC OA historical
// archives (dpGetPeriod). Writes and purges are no-ops.
type MessageArchive struct {
	name string
	r    *reader
}

var _ stores.MessageArchive = (*MessageArchive)(nil)

func (a *MessageArchive) Name() string                                      { return a.name }
func (a *MessageArchive) Type() stores.MessageArchiveType                   { return stores.ArchiveWinCCOA }
func (a *MessageArchive) AddHistory(context.Context, []stores.BrokerMessage) error { return nil }
func (a *MessageArchive) GetArchiveStats(context.Context, *time.Time, *time.Time) (*time.Time, []stores.DailyCount, error) {
	return nil, []stores.DailyCount{}, nil
}
func (a *MessageArchive) PurgeOlderThan(context.Context, time.Time) (stores.PurgeResult, error) {
	return stores.PurgeResult{}, nil
}
func (a *MessageArchive) EnsureTable(ctx context.Context) error { return a.r.probe(ctx) }
func (a *MessageArchive) Close() error                          { return nil }

type histTarget struct {
	sys   string
	dpe   string
	topic string
}

func (a *MessageArchive) GetHistory(ctx context.Context, topicFilter string, from, to *time.Time, limit int) ([]stores.ArchivedMessage, error) {
	end := time.Now().UTC()
	if to != nil {
		end = to.UTC()
	}
	start := end.Add(-defaultHistoryWindow)
	if from != nil {
		start = from.UTC()
	}
	if start.After(end) {
		return []stores.ArchivedMessage{}, nil
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > maxHistoryLimit {
		limit = maxHistoryLimit
	}

	dpes, err := a.resolveHistoryTargets(ctx, topicFilter)
	if err != nil {
		return nil, err
	}
	if len(dpes) == 0 {
		return []stores.ArchivedMessage{}, nil
	}

	var sysOrder []string
	bySys := map[string][]histTarget{}
	for _, ht := range dpes {
		if _, ok := bySys[ht.sys]; !ok {
			sysOrder = append(sysOrder, ht.sys)
		}
		bySys[ht.sys] = append(bySys[ht.sys], ht)
	}

	all := make([]stores.ArchivedMessage, 0, limit)
	for _, sys := range sysOrder {
		sysTargets := bySys[sys]
		for i := 0; i < len(sysTargets); i += oahost.MaxPeriodNames {
			batch := sysTargets[i:min(i+oahost.MaxPeriodNames, len(sysTargets))]
			names := make([]string, len(batch))
			for j, ht := range batch {
				names[j] = ht.dpe
			}
			res, err := a.r.opts.API.DpGetPeriod(ctx, oahost.PeriodRequest{
				Names:  names,
				Start:  start,
				End:    end,
				Max:    limit,
				Newest: true,
			}, queryTimeout, func(samples []oahost.PeriodSample) error {
				for _, s := range samples {
					if s.Index < 0 || s.Index >= len(batch) {
						continue
					}
					all = append(all, stores.ArchivedMessage{
						Topic:     batch[s.Index].topic,
						Timestamp: s.Time,
						Payload:   winccoanative.ValuePayload(s.Value, s.Time),
						QoS:       1,
					})
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			for idx, reason := range res.Failed {
				if idx >= 0 && idx < len(batch) {
					a.r.opts.Logger.Debug("winccoa history DPE skipped", "dpe", batch[idx].dpe, "err", reason)
				}
			}
		}
	}

	sort.Slice(all, func(i, j int) bool {
		if !all[i].Timestamp.Equal(all[j].Timestamp) {
			return all[i].Timestamp.After(all[j].Timestamp)
		}
		return all[i].Topic < all[j].Topic
	})
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func (a *MessageArchive) resolveHistoryTargets(ctx context.Context, topicFilter string) ([]histTarget, error) {
	f, _ := winccoanative.SplitShared(topicFilter)
	if !winccoanative.HasWildcard(f) {
		rt, err := a.r.resolveExact(ctx, f, true)
		if err != nil || rt == nil {
			return nil, err
		}
		return []histTarget{{
			sys:   rt.system,
			dpe:   rt.target.DPE(rt.system) + ":_offline.._value",
			topic: rt.topic,
		}}, nil
	}

	origFilter, targets, err := a.r.expandFilter(ctx, topicFilter)
	if err != nil || len(targets) == 0 {
		return nil, err
	}

	remaining := a.r.maxRows()
	dpTypeCache := map[string]string{}
	var out []histTarget
	for _, et := range targets {
		if remaining <= 0 {
			break
		}
		if !et.isWild {
			rt, err := a.r.resolveExact(ctx, et.exact, true)
			if err != nil {
				return nil, err
			}
			if rt != nil && topic.MatchFilter(origFilter, rt.topic) {
				remaining--
				out = append(out, histTarget{
					sys:   rt.system,
					dpe:   rt.target.DPE(rt.system) + ":_offline.._value",
					topic: rt.topic,
				})
			}
			continue
		}
		used, _, err := a.r.runArchiveWildQuery(
			ctx,
			origFilter,
			et.wild,
			remaining,
			dpTypeCache,
			func(t winccoanative.Target, sys string, top string) bool {
				out = append(out, histTarget{
					sys:   sys,
					dpe:   t.DPE(sys) + ":_offline.._value",
					topic: top,
				})
				return true
			},
		)
		if err != nil {
			return nil, err
		}
		remaining -= used
	}
	return out, nil
}
