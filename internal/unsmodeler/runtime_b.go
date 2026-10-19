package unsmodeler

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/winccoabrowse"
)

// flowTarget is one topic binding of the Direction B flow.
type flowTarget struct {
	dpe        string
	kind       oahost.Kind
	jsonPath   string
	timePath   string // empty: receive time
	timeFormat string
}

// RuntimeB feeds MQTT topics into WinCC OA datapoint elements (§5): it
// subscribes to the binding topics, converts each payload and writes it
// with DpSetTimed when the binding has a timePath, else with DpSet (the
// source time is then the time WinCC OA receives the write).
type RuntimeB struct {
	name   string
	api    oahost.API
	gate   RoleGate
	log    *slog.Logger
	cnt    *Counters
	cancel func()

	mu       sync.Mutex
	targets  map[string][]flowTarget
	errLog   map[string]time.Time
	lastErr  map[string]string
	errAt    map[string]time.Time
	received map[string]int64
	last     map[string]flowLast // topic + "\x00" + dpe
	stopped  bool
}

// flowLast is the last value written to one DPE of a topic.
type flowLast struct {
	v     oahost.Value
	at    time.Time
	stime time.Time // source time from the payload; zero: receive time
}

// flowWrite is one dpSet (zero stime) or dpSetTimed of a message.
type flowWrite struct {
	stime time.Time
	names []string
	dpes  []string
	vals  []oahost.Value
}

// flowTargets resolves the topic bindings of an applied Direction B
// document. Datapoints that do not exist are skipped and reported.
func flowTargets(ctx context.Context, b *winccoabrowse.Service, doc *Document) (map[string][]flowTarget, []string) {
	local, err := b.LocalSystem(ctx)
	if err != nil {
		return nil, []string{err.Error()}
	}
	types := map[string]*TypeProposal{}
	for _, t := range doc.Types {
		types[t.Name] = t
	}
	real := map[string]map[string]oahost.Kind{}
	var names []string
	for _, d := range doc.Datapoints {
		names = append(names, d.Name)
	}
	exists, _ := b.DatapointTypes(ctx, "", names)
	out := map[string][]flowTarget{}
	var problems []string
	for i, d := range doc.Datapoints {
		t := types[d.Type]
		if d.Excluded || t == nil || t.Excluded {
			continue
		}
		if i >= len(exists) || exists[i] != d.Type {
			problems = append(problems, "datapoint "+d.Name+" does not exist with type "+d.Type)
			continue
		}
		kinds, ok := real[d.Type]
		if !ok {
			kinds = map[string]oahost.Kind{}
			if ts, err := b.TypeStructure(ctx, "", d.Type); err == nil {
				for _, e := range ts.Elements {
					if e.ValueElement && oaToDirectionB(e.ElementType) != "" {
						kinds[e.Path] = elementKind(oaToDirectionB(e.ElementType))
					}
				}
			}
			real[d.Type] = kinds
		}
		for _, bnd := range d.Bindings {
			k, ok := kinds[bnd.Element]
			if !ok {
				problems = append(problems, "element "+d.Name+"."+bnd.Element+" cannot be written")
				continue
			}
			dpe := local + ":" + d.Name + "."
			if bnd.Element != "" {
				dpe += bnd.Element
			}
			out[bnd.Topic] = append(out[bnd.Topic], flowTarget{dpe: dpe, kind: k, jsonPath: bnd.JSONPath, timePath: bnd.TimePath, timeFormat: bnd.TimeFormat})
		}
	}
	return out, problems
}

// NewRuntimeB starts the flow for targets.
func NewRuntimeB(name string, api oahost.API, sub Subscriber, gate RoleGate, cnt *Counters, log *slog.Logger, targets map[string][]flowTarget) (*RuntimeB, error) {
	if gate == nil {
		gate = AlwaysActive{}
	}
	r := &RuntimeB{name: name, api: api, gate: gate, log: log, cnt: cnt, targets: targets,
		errLog: map[string]time.Time{}, lastErr: map[string]string{}, errAt: map[string]time.Time{}, received: map[string]int64{}, last: map[string]flowLast{}}
	cnt.DPEs.Store(int64(countTargets(targets)))
	if sub == nil {
		return r, errors.New("no in-process subscriber")
	}
	if len(targets) == 0 {
		return r, nil
	}
	topics := make([]string, 0, len(targets))
	for t := range targets {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	cancel, err := sub.Subscribe(topics, r.onMessage)
	if err != nil {
		return r, err
	}
	r.cancel = cancel
	return r, nil
}

func countTargets(t map[string][]flowTarget) int {
	n := 0
	for _, l := range t {
		n += len(l)
	}
	return n
}

func (r *RuntimeB) onMessage(topic string, payload []byte, _ time.Time) {
	r.mu.Lock()
	targets := r.targets[topic]
	stopped := r.stopped
	r.received[topic]++
	r.mu.Unlock()
	if stopped || len(targets) == 0 || !r.gate.Active() {
		return
	}
	// One write per source time: targets without timePath, or whose time
	// cannot be read, go with the receive time (plain dpSet).
	now := time.Now()
	var writes []*flowWrite
	group := func(stime time.Time) *flowWrite {
		for _, w := range writes {
			if w.stime.Equal(stime) {
				return w
			}
		}
		w := &flowWrite{stime: stime}
		writes = append(writes, w)
		return w
	}
	for _, t := range targets {
		v, err := convertPayload(payload, t.jsonPath, t.kind)
		if err != nil {
			r.fail(topic, t.dpe, err)
			continue
		}
		var stime time.Time
		if t.timePath != "" {
			st, err := sourceTime(payload, t.timePath, t.timeFormat)
			if err == nil {
				err = checkSourceTime(st, now)
			}
			if err != nil {
				r.timeProblem(topic, t.dpe, err)
			} else {
				stime = st
			}
		}
		w := group(stime)
		w.names = append(w.names, t.dpe+":_original.._value")
		w.dpes = append(w.dpes, t.dpe)
		w.vals = append(w.vals, v)
	}
	for _, w := range writes {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var err error
		if w.stime.IsZero() {
			err = r.api.DpSet(ctx, w.names, w.vals, 10*time.Second)
		} else {
			err = r.api.DpSetTimed(ctx, w.names, w.vals, w.stime, 10*time.Second)
		}
		cancel()
		if err != nil {
			r.fail(topic, w.names[0], err)
			continue
		}
		r.cnt.Writes.Add(int64(len(w.names)))
		at := time.Now()
		r.mu.Lock()
		for i, dpe := range w.dpes {
			r.last[topic+"\x00"+dpe] = flowLast{v: w.vals[i], at: at, stime: w.stime}
		}
		r.mu.Unlock()
	}
}

// timeProblem records a source time that cannot be used; the value is
// still written, with the receive time. Logged once per topic and minute.
func (r *RuntimeB) timeProblem(topic, dpe string, err error) {
	r.mu.Lock()
	last := r.errLog[topic]
	r.lastErr[topic] = "receive time used: " + err.Error()
	r.errAt[topic] = time.Now()
	logIt := time.Since(last) > time.Minute
	if logIt {
		r.errLog[topic] = time.Now()
	}
	r.mu.Unlock()
	if logIt {
		r.log.Warn("uns flow source time not usable, receive time used", "device", r.name, "topic", topic, "dpe", dpe, "err", err)
	}
}

// fail counts a payload that could not be written; logging is limited to
// one line per topic and minute.
func (r *RuntimeB) fail(topic, dpe string, err error) {
	r.cnt.Errors.Add(1)
	r.mu.Lock()
	last := r.errLog[topic]
	r.lastErr[topic] = err.Error()
	r.errAt[topic] = time.Now()
	logIt := time.Since(last) > time.Minute
	if logIt {
		r.errLog[topic] = time.Now()
	}
	r.mu.Unlock()
	if logIt {
		r.log.Warn("uns flow payload not written", "device", r.name, "topic", topic, "dpe", dpe, "err", err)
	}
}

// Stop ends the subscription.
func (r *RuntimeB) Stop() {
	r.mu.Lock()
	r.stopped = true
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.cnt.DPEs.Store(0)
}

// Seed feeds the current cached payload of each target topic that has not yet
// received a live message so the flow and Live Monitor populate immediately.
func (r *RuntimeB) Seed(ctx context.Context, src TopicSource) {
	if src == nil {
		return
	}
	r.mu.Lock()
	topics := make([]string, 0, len(r.targets))
	for t := range r.targets {
		if r.received[t] == 0 {
			topics = append(topics, t)
		}
	}
	r.mu.Unlock()
	sort.Strings(topics)
	for _, t := range topics {
		r.mu.Lock()
		seen := r.received[t] > 0 || r.stopped
		r.mu.Unlock()
		if seen {
			continue
		}
		if payload, ok := src.Last(ctx, t); ok && len(payload) > 0 {
			r.onMessage(t, payload, time.Now())
		}
	}
}

// Status returns the flow state per source topic.
func (r *RuntimeB) Status() []TopicStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []TopicStatus
	for topic, ts := range r.targets {
		for _, t := range ts {
			st := TopicStatus{Topic: topic, DPE: t.dpe, Published: r.received[topic], LastError: r.lastErr[topic], LastErrorAt: r.errAt[topic]}
			if l, ok := r.last[topic+"\x00"+t.dpe]; ok {
				st.Value, st.HasValue, st.LastPublish, st.SourceTime = l.v.PlainJSON(), true, l.at, l.stime
			}
			out = append(out, st)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic+out[i].DPE < out[j].Topic+out[j].DPE })
	return out
}
