package broker

import (
	"math"
	"strconv"
	"time"

	"monstermq.io/edge/internal/metrics"
	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/winccoanative"
)

func formatRate(v float64) string {
	return strconv.FormatFloat(math.Round(v*100)/100, 'f', -1, 64)
}

// brokerRateSysTopics returns the $SYS/broker/load/messages/{in,out} per-second
// rate topics from the metrics collector.
func brokerRateSysTopics(collector *metrics.Collector) func() map[string]string {
	return func() map[string]string {
		snap := collector.Latest()
		return map[string]string{
			"broker/load/messages/in":  formatRate(snap.MessagesIn),
			"broker/load/messages/out": formatRate(snap.MessagesOut),
		}
	}
}

// winccoaSysTopics returns the $SYS/winccoa/... counters of the embedding
// host: host (Go side of the C ABI), native (namespace service, nil when
// the namespace is off) and manager (pushed by the host via mmq_stats).
func winccoaSysTopics(oa *oahost.Client, native *winccoanative.Service) func() map[string]string {
	u := func(v uint64) string { return strconv.FormatUint(v, 10) }
	i := func(v int) string { return strconv.Itoa(v) }
	var (
		prevTime          time.Time
		prevIn, prevOut   uint64
		rateIn, rateOut   float64
	)
	return func() map[string]string {
		now := time.Now()
		h := oa.Stats()
		if !prevTime.IsZero() {
			if dt := now.Sub(prevTime).Seconds(); dt > 0 {
				rateIn = float64(h.ValuesIn-prevIn) / dt
				rateOut = float64(h.ValuesOut-prevOut) / dt
			}
		}
		prevTime = now
		prevIn = h.ValuesIn
		prevOut = h.ValuesOut

		t := map[string]string{
			"winccoa/host/submitted":        u(h.Submitted),
			"winccoa/host/completed":        u(h.Completed),
			"winccoa/host/timedOut":         u(h.TimedOut),
			"winccoa/host/overloaded":       u(h.Overloaded),
			"winccoa/host/late":             u(h.LateCompletions),
			"winccoa/host/eventsDelivered":  u(h.EventsDelivered),
			"winccoa/host/eventsDropped":    u(h.EventsDropped),
			"winccoa/host/eventsUnrouted":   u(h.EventsUnrouted),
			"winccoa/host/pendingHighWater": u(h.PendingHighWater),
			"winccoa/host/pending":          i(oa.Pending()),
			"winccoa/host/refs":             i(oa.Refs()),
			"winccoa/host/valuesIn":         u(h.ValuesIn),
			"winccoa/host/valuesOut":        u(h.ValuesOut),
			"winccoa/rate/valuesIn":         formatRate(rateIn),
			"winccoa/rate/valuesOut":        formatRate(rateOut),
		}
		if native != nil {
			n := native.Stats()
			t["winccoa/native/interests"] = i(n.Interests)
			t["winccoa/native/dpes"] = i(n.DPEs)
			t["winccoa/native/batches"] = i(n.Batches)
			t["winccoa/native/connects"] = u(n.Connects)
			t["winccoa/native/disconnects"] = u(n.Disconnects)
			t["winccoa/native/connectErrors"] = u(n.ConnectErrors)
			t["winccoa/native/published"] = u(n.Published)
			t["winccoa/native/commands"] = u(n.Commands)
			t["winccoa/native/commandErrors"] = u(n.CommandErrors)
			t["winccoa/native/duplicates"] = u(n.Duplicates)
			t["winccoa/native/wildQueries"] = i(n.WildQueries)
			t["winccoa/native/wildSubs"] = i(n.WildSubs)
			t["winccoa/native/topicDps"] = i(n.TopicDPs)
			t["winccoa/native/topicSubs"] = i(n.TopicSubs)
			t["winccoa/native/topicWilds"] = i(n.TopicWilds)
			t["winccoa/native/topicDirs"] = i(n.TopicDirs)
		}
		if m := oa.HostStats(); m != nil {
			for k, v := range m.Values {
				t["winccoa/manager/"+k] = u(v)
			}
			t["winccoa/manager/updated"] = strconv.FormatInt(m.Updated.UnixMilli(), 10)
		}
		return t
	}
}
