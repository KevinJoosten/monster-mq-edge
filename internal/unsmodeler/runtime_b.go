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
	dpe      string
	kind     oahost.Kind
	jsonPath string
}

// RuntimeB feeds MQTT topics into WinCC OA datapoint elements (§5): it
// subscribes to the binding topics, converts each payload and writes it
// with DpSet. The source time is the time WinCC OA receives the write.
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
	received map[string]int64
	stopped  bool
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
			out[bnd.Topic] = append(out[bnd.Topic], flowTarget{dpe: dpe, kind: k, jsonPath: bnd.JSONPath})
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
		errLog: map[string]time.Time{}, lastErr: map[string]string{}, received: map[string]int64{}}
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
	var names []string
	var vals []oahost.Value
	for _, t := range targets {
		v, err := convertPayload(payload, t.jsonPath, t.kind)
		if err != nil {
			r.fail(topic, t.dpe, err)
			continue
		}
		names = append(names, t.dpe+":_original.._value")
		vals = append(vals, v)
	}
	if len(names) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.api.DpSet(ctx, names, vals, 10*time.Second); err != nil {
		r.fail(topic, names[0], err)
		return
	}
	r.cnt.Writes.Add(int64(len(names)))
}

// fail counts a payload that could not be written; logging is limited to
// one line per topic and minute.
func (r *RuntimeB) fail(topic, dpe string, err error) {
	r.cnt.Errors.Add(1)
	r.mu.Lock()
	last := r.errLog[topic]
	r.lastErr[topic] = err.Error()
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

// Status returns the flow state per source topic.
func (r *RuntimeB) Status() []TopicStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []TopicStatus
	for topic, ts := range r.targets {
		for _, t := range ts {
			out = append(out, TopicStatus{Topic: topic, DPE: t.dpe, Published: r.received[topic], LastError: r.lastErr[topic]})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic+out[i].DPE < out[j].Topic+out[j].DPE })
	return out
}
