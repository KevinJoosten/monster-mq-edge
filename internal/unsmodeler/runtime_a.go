package unsmodeler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/winccoanative"
)

// Publisher publishes into the local broker.
type Publisher func(topic string, payload []byte, retain bool, qos byte) error

// Counters are the per-device counters ($SYS/unsmodeler/<device>/...).
type Counters struct {
	DPEs      atomic.Int64
	Published atomic.Int64
	Writes    atomic.Int64
	Errors    atomic.Int64
}

// connectBatch is the number of DPEs of one DP_CONNECT request.
const connectBatch = 100

// ErrWriteDisabled rejects a ..set on a device without allowWrite.
var ErrWriteDisabled = errors.New("write-through is disabled for this device")

// ErrStandby rejects writes, applies and AI runs on a STANDBY broker.
var ErrStandby = errors.New("this broker is STANDBY")

// tstate is the runtime state of one published topic.
type tstate struct {
	t         *Target
	meta      *staticMeta
	needsMeta bool
	last      *sample
	lastPub   *sample
	lastPubAt time.Time
	pending   *time.Timer
	published int64
	lastErr   string
	lastErrAt time.Time
	// dynamic marks a target a pattern binding added at runtime.
	dynamic bool
}

type batch struct {
	ref    uint64
	system string
	dpes   []string
	attrs  string
}

type patternRun struct {
	spec    *PatternSpec
	key     string
	query   string
	ref     uint64
	pending bool // registration lost (system down), retried on return
}

// TopicStatus is the live state of one topic (A3 live monitor).
type TopicStatus struct {
	Topic       string
	DPE         string
	Value       any
	HasValue    bool
	Status      *uint64
	Published   int64
	LastPublish time.Time
	LastError   string
	LastErrorAt time.Time
}

// RuntimeA runs the live data flow of an applied Direction A mapping:
// DpConnect for element and datapoint bindings, one dpQueryConnectSingle
// per pattern binding, publishing by the topic semantics, and the optional
// <topic>/..set write-through.
type RuntimeA struct {
	name    string
	api     oahost.API
	pub     Publisher
	gate    RoleGate
	log     *slog.Logger
	timeout time.Duration
	cnt     *Counters

	applyMu sync.Mutex

	mu         sync.Mutex
	allowWrite bool
	targets    map[string]*tstate
	byDPE      map[string][]*tstate
	batches    map[uint64]*batch
	dpeRef     map[string]uint64
	wantAttrs  map[string]string
	lostDPEs   map[string]bool // DPEs of a disconnected system, reconnected on return
	patterns   map[string]*patternRun
	collided   map[string]bool
	cyclic     map[int64]chan struct{}
	stopped    bool
	unwatch    []func()
}

// NewRuntimeA creates the runtime of a device; Apply starts it.
func NewRuntimeA(name string, api oahost.API, pub Publisher, gate RoleGate, cnt *Counters, log *slog.Logger) *RuntimeA {
	if gate == nil {
		gate = AlwaysActive{}
	}
	if cnt == nil {
		cnt = &Counters{}
	}
	r := &RuntimeA{
		name: name, api: api, pub: pub, gate: gate, log: log, timeout: 10 * time.Second, cnt: cnt,
		targets: map[string]*tstate{}, byDPE: map[string][]*tstate{}, batches: map[uint64]*batch{},
		dpeRef: map[string]uint64{}, wantAttrs: map[string]string{}, lostDPEs: map[string]bool{}, patterns: map[string]*patternRun{},
		collided: map[string]bool{}, cyclic: map[int64]chan struct{}{},
	}
	r.unwatch = append(r.unwatch, gate.Watch(r.onRole))
	if api.C != nil {
		r.unwatch = append(r.unwatch, api.C.WatchSystems(r.onSystem))
	}
	return r
}

type pubItem struct {
	ts      *tstate
	topic   string
	payload []byte
	retain  bool
	qos     byte
}

func (r *RuntimeA) send(items []pubItem) {
	for _, it := range items {
		if err := r.pub(it.topic, it.payload, it.retain, it.qos); err != nil {
			r.cnt.Errors.Add(1)
			if it.ts != nil {
				r.mu.Lock()
				it.ts.lastErr, it.ts.lastErrAt = err.Error(), time.Now()
				r.mu.Unlock()
			}
			r.log.Warn("uns publish failed", "device", r.name, "topic", it.topic, "err", err)
		}
	}
}

// publishLocked renders and records one publication of ts.
func (r *RuntimeA) publishLocked(ts *tstate, s *sample, now time.Time) pubItem {
	ts.lastPub = s
	ts.lastPubAt = now
	ts.published++
	r.cnt.Published.Add(1)
	return pubItem{ts: ts, topic: ts.t.Topic, payload: buildPayload(ts.t.Sem, s, ts.meta, now), retain: ts.t.Sem.Retain, qos: ts.t.Sem.Qos}
}

// offerLocked applies the publish mode to a new sample.
func (r *RuntimeA) offerLocked(ts *tstate, s *sample, now time.Time, out []pubItem) []pubItem {
	ts.last = s
	if r.stopped || !r.gate.Active() || ts.needsMeta {
		return out
	}
	sem := ts.t.Sem
	switch sem.PublishMode {
	case ModeCyclic:
		return out
	case ModeDeadband:
		if ts.lastPub != nil {
			a, ok1 := numeric(s.v)
			b, ok2 := numeric(ts.lastPub.v)
			if ok1 && ok2 && abs(a-b) < sem.Deadband {
				return out
			}
			if (!ok1 || !ok2) && s.same(ts.lastPub) {
				return out
			}
		}
	case ModeMinInterval:
		if s.same(ts.lastPub) {
			return out
		}
		iv := time.Duration(sem.IntervalMs) * time.Millisecond
		if wait := ts.lastPubAt.Add(iv).Sub(now); ts.lastPub != nil && wait > 0 {
			if ts.pending == nil {
				ts.pending = time.AfterFunc(wait, func() { r.flushPending(ts) })
			}
			return out
		}
	default:
		if s.same(ts.lastPub) {
			return out
		}
	}
	return append(out, r.publishLocked(ts, s, now))
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// flushPending publishes the latest value of a minInterval topic at the end
// of its interval.
func (r *RuntimeA) flushPending(ts *tstate) {
	r.mu.Lock()
	ts.pending = nil
	var out []pubItem
	if !r.stopped && r.gate.Active() && r.targets[ts.t.Topic] == ts && ts.last != nil && !ts.last.same(ts.lastPub) {
		out = append(out, r.publishLocked(ts, ts.last, time.Now()))
	}
	r.mu.Unlock()
	r.send(out)
}

// Apply connects an expansion. A re-apply connects only added DPEs and
// pattern queries and disconnects removed ones; unchanged topics keep
// publishing. Retained messages of removed topics are cleared.
func (r *RuntimeA) Apply(ctx context.Context, exp *Expansion, allowWrite bool) error {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	now := time.Now()

	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return errors.New("runtime stopped")
	}
	r.allowWrite = allowWrite
	old := r.targets
	next := map[string]*tstate{}
	byDPE := map[string][]*tstate{}
	want := map[string]string{} // DPE -> attrs key
	wantAttrList := map[string][]string{}
	var fresh []*tstate
	for _, t := range exp.Targets {
		if _, dup := next[t.Topic]; dup {
			continue
		}
		ts := &tstate{t: t}
		if o := old[t.Topic]; o != nil && o.t.DPE() == t.DPE() {
			ts.last, ts.lastPub, ts.lastPubAt, ts.published, ts.lastErr, ts.lastErrAt = o.last, o.lastPub, o.lastPubAt, o.published, o.lastErr, o.lastErrAt
			if o.t.Sem.Equal(t.Sem) {
				ts.meta = o.meta
			}
		}
		if ts.meta == nil {
			fresh = append(fresh, ts)
		}
		next[t.Topic] = ts
		if t.Pattern == nil {
			byDPE[t.DPE()] = append(byDPE[t.DPE()], ts)
		}
	}
	for dpe, list := range byDPE {
		var payloads [][]PayloadField
		for _, ts := range list {
			payloads = append(payloads, ts.t.Sem.Payload)
		}
		attrs := dynamicAttrs(payloads...)
		want[dpe] = strings.Join(attrs, ",")
		wantAttrList[dpe] = attrs
	}
	// Removed topics: stop timers, clear retained messages.
	var clear []pubItem
	for topic, o := range old {
		if o.pending != nil {
			o.pending.Stop()
		}
		if n, keep := next[topic]; keep && n.t.DPE() == o.t.DPE() {
			continue
		}
		if _, keep := next[topic]; !keep && o.t.Sem.Retain && o.lastPub != nil {
			clear = append(clear, pubItem{topic: topic, payload: []byte{}, retain: true, qos: o.t.Sem.Qos})
		}
	}
	// Batches whose members all stay with the same attributes are kept.
	var drop []uint64
	keepDPE := map[string]bool{}
	for ref, b := range r.batches {
		ok := true
		for _, d := range b.dpes {
			if a, w := want[d]; !w || a != b.attrs {
				ok = false
				break
			}
		}
		if ok {
			for _, d := range b.dpes {
				keepDPE[d] = true
			}
			continue
		}
		drop = append(drop, ref)
		for _, d := range b.dpes {
			delete(r.dpeRef, d)
		}
		delete(r.batches, ref)
	}
	var connect []string
	for dpe := range want {
		if !keepDPE[dpe] {
			connect = append(connect, dpe)
		}
	}
	slices.Sort(connect)
	for d := range r.lostDPEs {
		if _, w := want[d]; !w {
			delete(r.lostDPEs, d)
		}
	}
	// Pattern queries.
	nextPat := map[string]*patternRun{}
	var newPat []*patternRun
	var dropPat []uint64
	for _, spec := range exp.Patterns {
		q := patternQuery(spec)
		key := q
		if p, ok := r.patterns[key]; ok {
			p.spec = spec
			nextPat[key] = p
			continue
		}
		if _, dup := nextPat[key]; dup {
			// Two bindings with the same query share one registration.
			key = fmt.Sprintf("%s|%s", key, spec.BindingPtr)
		}
		p := &patternRun{spec: spec, key: key, query: q}
		nextPat[key] = p
		newPat = append(newPat, p)
	}
	for key, p := range r.patterns {
		if _, keep := nextPat[key]; !keep && p.ref != 0 {
			dropPat = append(dropPat, p.ref)
		}
	}
	r.targets, r.byDPE, r.wantAttrs, r.patterns = next, byDPE, want, nextPat
	r.collided = map[string]bool{}
	r.cnt.DPEs.Store(int64(len(next)))
	r.rebuildCyclicLocked()
	r.mu.Unlock()

	r.send(clear)
	for _, ref := range drop {
		_ = r.api.DpDisconnect(ctx, ref)
	}
	for _, ref := range dropPat {
		_ = r.api.QueryDisconnect(ctx, ref)
	}
	r.readMeta(ctx, fresh)
	r.connectDPEs(ctx, connect, wantAttrList)
	for _, p := range newPat {
		r.registerPattern(ctx, p)
	}
	// Topics whose static metadata changed republish their last value.
	r.mu.Lock()
	var out []pubItem
	for _, ts := range fresh {
		if ts.last != nil && ts.lastPub != nil && r.gate.Active() && ts.t.Sem.PublishMode != ModeCyclic && r.targets[ts.t.Topic] == ts {
			out = append(out, r.publishLocked(ts, ts.last, now))
		}
	}
	r.mu.Unlock()
	r.send(out)
	r.log.Info("uns mapping applied", "logger", "UnsModeler", "device", r.name, "topics", len(next), "connected", len(connect), "disconnected batches", len(drop), "patterns", len(nextPat))
	return nil
}

// readMeta reads the static metadata of targets whose payload needs it.
func (r *RuntimeA) readMeta(ctx context.Context, list []*tstate) {
	metaSet, pvSet := map[string]bool{}, map[string]bool{}
	attrs := map[string][]string{}
	for _, ts := range list {
		m, pv, as := needsStatic(ts.t.Sem.Payload)
		d := ts.t.DPE()
		if m {
			metaSet[d] = true
		}
		if pv {
			pvSet[d] = true
		}
		for _, a := range as {
			if !slices.Contains(attrs[d], a) {
				attrs[d] = append(attrs[d], a)
			}
		}
	}
	got := map[string]*staticMeta{}
	if len(metaSet)+len(pvSet)+len(attrs) > 0 {
		got = readStatic(ctx, r.api, sortedSet(metaSet), sortedSet(pvSet), attrs, r.timeout)
	}
	r.mu.Lock()
	for _, ts := range list {
		if m := got[ts.t.DPE()]; m != nil {
			ts.meta = m
		} else {
			ts.meta = &staticMeta{}
		}
	}
	r.mu.Unlock()
}

// RefreshMeta re-reads the static metadata of every topic (device start,
// WinCC OA reconnect).
func (r *RuntimeA) RefreshMeta(ctx context.Context) {
	r.mu.Lock()
	list := make([]*tstate, 0, len(r.targets))
	for _, ts := range r.targets {
		list = append(list, ts)
	}
	r.mu.Unlock()
	r.readMeta(ctx, list)
}

func connectName(dpe string) string { return dpe + ":" + attrValue }

// connectDPEs connects DPEs in batches per system and attribute set. A batch
// that fails (a DPE no longer exists) is connected DPE by DPE; missing ones
// are skipped and logged until the next apply.
func (r *RuntimeA) connectDPEs(ctx context.Context, dpes []string, attrs map[string][]string) {
	groups := map[string][]string{}
	var order []string
	for _, d := range dpes {
		sys, _, _ := strings.Cut(d, ":")
		k := sys + "|" + strings.Join(attrs[d], ",")
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], d)
	}
	for _, k := range order {
		list := groups[k]
		sys, _, _ := strings.Cut(k, "|")
		at := attrs[list[0]]
		for start := 0; start < len(list); start += connectBatch {
			chunk := list[start:min(start+connectBatch, len(list))]
			if err := r.connectChunk(ctx, sys, chunk, at); err != nil {
				if errors.Is(err, oahost.ErrUnavailable) {
					r.markLost(chunk)
					continue
				}
				for _, d := range chunk {
					if err := r.connectChunk(ctx, sys, []string{d}, at); err != nil {
						if errors.Is(err, oahost.ErrUnavailable) {
							r.markLost([]string{d})
							continue
						}
						r.cnt.Errors.Add(1)
						r.setErr(d, err)
						r.log.Warn("uns binding skipped", "logger", "UnsModeler", "device", r.name, "dpe", d, "err", err)
					}
				}
			}
		}
	}
}

func (r *RuntimeA) markLost(dpes []string) {
	r.mu.Lock()
	for _, d := range dpes {
		r.lostDPEs[d] = true
	}
	r.mu.Unlock()
}

func (r *RuntimeA) setErr(dpe string, err error) {
	r.mu.Lock()
	for _, ts := range r.byDPE[dpe] {
		ts.lastErr, ts.lastErrAt = err.Error(), time.Now()
	}
	r.mu.Unlock()
}

func (r *RuntimeA) connectChunk(ctx context.Context, sys string, dpes []string, attrs []string) error {
	names := make([]string, len(dpes))
	for i, d := range dpes {
		names[i] = connectName(d)
	}
	var ref uint64
	var mu sync.Mutex
	mu.Lock()
	h := func(m oahost.Message) {
		mu.Lock()
		cur := ref
		mu.Unlock()
		r.onConnect(cur, m)
	}
	got, err := r.api.DpConnectAttrs(ctx, names, oahost.FlagAnswer|oahost.FlagSourceTime, attrs, h, r.timeout)
	if err != nil {
		mu.Unlock()
		return err
	}
	ref = got
	mu.Unlock()
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		_ = r.api.DpDisconnect(ctx, got)
		return nil
	}
	r.batches[got] = &batch{ref: got, system: sys, dpes: dpes, attrs: strings.Join(attrs, ",")}
	for _, d := range dpes {
		r.dpeRef[d] = got
		delete(r.lostDPEs, d)
	}
	r.mu.Unlock()
	return nil
}

// onConnect handles the answer and hotlinks of a connect batch.
func (r *RuntimeA) onConnect(ref uint64, m oahost.Message) {
	names, values, err := oahost.HotlinkItems(m)
	if err != nil {
		return
	}
	times := oahost.HotlinkTimes(m)
	attrVals, _ := oahost.HotlinkAttrs(m)
	now := time.Now()
	r.mu.Lock()
	b := r.batches[ref]
	var out []pubItem
	for i, n := range names {
		dpe := strings.TrimSuffix(n, ":"+attrValue)
		s := &sample{v: values[i], attrs: map[string]oahost.Value{}}
		if i < len(times) {
			s.stime = times[i]
		}
		if b != nil && i < len(attrVals) && attrVals[i] != nil {
			for j, a := range strings.Split(b.attrs, ",") {
				if j < len(attrVals[i]) && a != "" {
					s.attrs[a] = attrVals[i][j]
				}
			}
		}
		for _, ts := range r.byDPE[dpe] {
			out = r.offerLocked(ts, s, now, out)
		}
	}
	r.mu.Unlock()
	r.send(out)
}

// patternQuery builds the dpQueryConnectSingle of a pattern binding (§4.3).
func patternQuery(spec *PatternSpec) string {
	var payloads [][]PayloadField
	paths := map[string]bool{}
	for _, e := range spec.Elements {
		payloads = append(payloads, e.Sem.Payload)
		paths[e.Path] = true
	}
	cols := []string{"'" + attrValue + "'", "'" + attrSTime + "'"}
	for _, a := range dynamicAttrs(payloads...) {
		cols = append(cols, "'"+a+"'")
	}
	var elems []string
	switch {
	case len(paths) == 0:
		elems = []string{"**"}
	case len(paths) == 1 && paths[""]:
		elems = []string{""}
	default:
		for p := range paths {
			if p != "" {
				elems = append(elems, p)
			}
		}
		slices.Sort(elems)
	}
	var alts []string
	for _, dp := range expandBraces(spec.Pattern) {
		for _, e := range elems {
			alts = append(alts, dp+"."+e)
		}
	}
	from := alts[0]
	if len(alts) > 1 {
		from = "{" + strings.Join(alts, ",") + "}"
	}
	q := "SELECT " + strings.Join(cols, ", ") + " FROM '" + from + "'"
	if spec.System != "" {
		q += " REMOTE '" + spec.System + "'"
	}
	return q + ` WHERE _DPT = "` + spec.Type + `"`
}

// expandBraces expands the one level of {a,b} alternatives a dpNames
// pattern may have, so the query FROM list never nests braces.
func expandBraces(p string) []string {
	i := strings.IndexByte(p, '{')
	j := strings.IndexByte(p, '}')
	if i < 0 || j < i {
		return []string{p}
	}
	var out []string
	for _, alt := range strings.Split(p[i+1:j], ",") {
		for _, rest := range expandBraces(p[j+1:]) {
			out = append(out, p[:i]+alt+rest)
		}
	}
	return out
}

func (r *RuntimeA) registerPattern(ctx context.Context, p *patternRun) {
	var ref uint64
	var mu sync.Mutex
	mu.Lock()
	h := func(m oahost.Message) {
		mu.Lock()
		cur := ref
		mu.Unlock()
		r.onRows(p, cur, m)
	}
	got, err := r.api.QueryConnect(ctx, p.query, true, h, r.timeout)
	if err != nil {
		mu.Unlock()
		r.mu.Lock()
		p.pending = true
		r.mu.Unlock()
		if !errors.Is(err, oahost.ErrUnavailable) {
			r.cnt.Errors.Add(1)
			r.log.Warn("uns pattern binding not connected", "logger", "UnsModeler", "device", r.name, "query", p.query, "err", err)
		}
		return
	}
	ref = got
	mu.Unlock()
	r.mu.Lock()
	if r.stopped || r.patterns[p.key] != p {
		r.mu.Unlock()
		_ = r.api.QueryDisconnect(ctx, got)
		return
	}
	p.ref, p.pending = got, false
	r.mu.Unlock()
}

// onRows publishes the rows of a pattern query. Datapoints that match
// later get targets at runtime; a topic another binding already publishes
// keeps its owner (logged once, counted as error).
func (r *RuntimeA) onRows(p *patternRun, ref uint64, m oahost.Message) {
	rows, err := oahost.QueryRows(m)
	if err != nil || len(rows) < 1 {
		return
	}
	cols := map[string]int{}
	for i, h := range rows[0] {
		if h.Kind == oahost.KindString {
			cols[strings.TrimPrefix(h.Str, ":")] = i
		}
	}
	vi, ok := cols[attrValue]
	if !ok {
		vi = 1
	}
	si, hasST := cols[attrSTime]
	now := time.Now()
	var out []pubItem
	var needMeta []*tstate
	r.mu.Lock()
	if r.stopped || r.patterns[p.key] != p {
		r.mu.Unlock()
		return
	}
	spec := p.spec
	for _, row := range rows[1:] {
		if len(row) <= vi || row[0].Kind != oahost.KindString {
			continue
		}
		sys, dp, el, ok := splitRowName(row[0].Str)
		if !ok || winccoanative.Protected(dp) {
			continue
		}
		s := &sample{v: row[vi], attrs: map[string]oahost.Value{}}
		if hasST && si < len(row) && row[si].Kind == oahost.KindTime {
			s.stime = row[si].Time
		}
		for a, i := range cols {
			if a != attrValue && a != attrSTime && a != "" && i < len(row) {
				s.attrs[a] = row[i]
			}
		}
		for i := range spec.Elements {
			pe := spec.Elements[i]
			if pe.Path != el {
				continue
			}
			topic := spec.TopicFor(dp, pe)
			ts := r.targets[topic]
			if ts == nil {
				if int(r.cnt.DPEs.Load()) >= MaxExpandedDPEs {
					if !r.collided[topic] {
						r.collided[topic] = true
						r.cnt.Errors.Add(1)
						r.log.Warn("uns topic not published: expanded DPE limit reached", "logger", "UnsModeler", "device", r.name, "topic", topic, "limit", MaxExpandedDPEs)
					}
					continue
				}
				t := &Target{Topic: topic, Ptr: pe.Ptr, NodeID: spec.NodeID, NodePtr: spec.NodePtr, BindingPtr: spec.BindingPtr, Kind: KindPattern,
					System: sys, DP: dp, Element: el, ElemKind: pe.ElemKind, OaType: pe.OaType, Sem: pe.Sem, Pattern: spec}
				ts = &tstate{t: t, dynamic: true}
				if m, pv, as := needsStatic(t.Sem.Payload); m || pv || len(as) > 0 {
					ts.needsMeta = true
					needMeta = append(needMeta, ts)
				} else {
					ts.meta = &staticMeta{}
				}
				r.targets[topic] = ts
				r.cnt.DPEs.Add(1)
			} else if ts.t.Pattern != spec || ts.t.DP != dp || ts.t.Element != el {
				if !r.collided[topic] {
					r.collided[topic] = true
					r.cnt.Errors.Add(1)
					r.log.Warn("uns topic collision: datapoint not published", "logger", "UnsModeler", "device", r.name, "topic", topic, "dpe", sys+":"+dp+"."+el, "owner", ts.t.DPE())
				}
				continue
			}
			out = r.offerLocked(ts, s, now, out)
		}
	}
	r.mu.Unlock()
	r.send(out)
	if len(needMeta) > 0 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
			defer cancel()
			r.readMeta(ctx, needMeta)
			now := time.Now()
			r.mu.Lock()
			var out []pubItem
			for _, ts := range needMeta {
				ts.needsMeta = false
				if ts.last != nil && r.targets[ts.t.Topic] == ts {
					s := ts.last
					out = r.offerLocked(ts, s, now, out)
				}
			}
			r.mu.Unlock()
			r.send(out)
		}()
	}
}

// rebuildCyclicLocked runs one ticker per distinct cyclic interval.
func (r *RuntimeA) rebuildCyclicLocked() {
	want := map[int64]bool{}
	for _, ts := range r.targets {
		if ts.t.Sem.PublishMode == ModeCyclic && ts.t.Sem.IntervalMs > 0 {
			want[ts.t.Sem.IntervalMs] = true
		}
	}
	for iv, stop := range r.cyclic {
		if !want[iv] {
			close(stop)
			delete(r.cyclic, iv)
		}
	}
	for iv := range want {
		if _, ok := r.cyclic[iv]; ok {
			continue
		}
		stop := make(chan struct{})
		r.cyclic[iv] = stop
		go r.cyclicLoop(iv, stop)
	}
}

func (r *RuntimeA) cyclicLoop(iv int64, stop chan struct{}) {
	t := time.NewTicker(time.Duration(iv) * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			r.mu.Lock()
			var out []pubItem
			if !r.stopped && r.gate.Active() {
				for _, ts := range r.targets {
					if ts.t.Sem.PublishMode == ModeCyclic && ts.t.Sem.IntervalMs == iv && ts.last != nil && !ts.needsMeta {
						out = append(out, r.publishLocked(ts, ts.last, now))
					}
				}
			}
			r.mu.Unlock()
			r.send(out)
		}
	}
}

// onRole publishes what changed while the broker was STANDBY.
func (r *RuntimeA) onRole(active bool) {
	if !active {
		return
	}
	now := time.Now()
	r.mu.Lock()
	var out []pubItem
	for _, ts := range r.targets {
		if ts.last != nil && !ts.last.same(ts.lastPub) && ts.t.Sem.PublishMode != ModeCyclic && !ts.needsMeta {
			out = append(out, r.publishLocked(ts, ts.last, now))
		}
	}
	r.mu.Unlock()
	r.send(out)
}

// onSystem drops the registrations of a lost remote system and registers
// them again when it comes back.
func (r *RuntimeA) onSystem(system string, available bool) {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	if !available {
		for ref, b := range r.batches {
			if b.system == system {
				for _, d := range b.dpes {
					delete(r.dpeRef, d)
					r.lostDPEs[d] = true
				}
				delete(r.batches, ref)
				r.api.C.DropRef(ref)
			}
		}
		for _, p := range r.patterns {
			if p.spec.System == system && p.ref != 0 {
				r.api.C.DropRef(p.ref)
				p.ref, p.pending = 0, true
			}
		}
		r.mu.Unlock()
		return
	}
	var dpes []string
	attrs := map[string][]string{}
	for d := range r.lostDPEs {
		if sys, _, _ := strings.Cut(d, ":"); sys == system {
			dpes = append(dpes, d)
			if a := r.wantAttrs[d]; a != "" {
				attrs[d] = strings.Split(a, ",")
			}
		}
	}
	var pats []*patternRun
	for _, p := range r.patterns {
		if p.pending && (p.spec.System == system || (p.spec.System == "" && system == "")) {
			pats = append(pats, p)
		}
	}
	r.mu.Unlock()
	slices.Sort(dpes)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		r.connectDPEs(ctx, dpes, attrs)
		for _, p := range pats {
			r.registerPattern(ctx, p)
		}
	}()
}

// Stop releases every registration and stops publishing.
func (r *RuntimeA) Stop() {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	refs := make([]uint64, 0, len(r.batches))
	for ref := range r.batches {
		refs = append(refs, ref)
	}
	var qrefs []uint64
	for _, p := range r.patterns {
		if p.ref != 0 {
			qrefs = append(qrefs, p.ref)
		}
	}
	for _, ts := range r.targets {
		if ts.pending != nil {
			ts.pending.Stop()
		}
	}
	for iv, stop := range r.cyclic {
		close(stop)
		delete(r.cyclic, iv)
	}
	unwatch := r.unwatch
	r.batches, r.dpeRef, r.patterns = map[uint64]*batch{}, map[string]uint64{}, map[string]*patternRun{}
	r.cnt.DPEs.Store(0)
	r.mu.Unlock()
	for _, u := range unwatch {
		u()
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	for _, ref := range refs {
		_ = r.api.DpDisconnect(ctx, ref)
	}
	for _, ref := range qrefs {
		_ = r.api.QueryDisconnect(ctx, ref)
	}
}

// Target returns the target of a topic, or nil.
func (r *RuntimeA) Target(topic string) *Target {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ts := r.targets[topic]; ts != nil {
		return ts.t
	}
	return nil
}

// Topics returns every published topic of the runtime.
func (r *RuntimeA) Topics() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.targets))
	for t := range r.targets {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// Status returns the live state of every topic, sorted by topic.
func (r *RuntimeA) Status() []TopicStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]TopicStatus, 0, len(r.targets))
	for topic, ts := range r.targets {
		st := TopicStatus{Topic: topic, DPE: ts.t.DPE(), Published: ts.published, LastPublish: ts.lastPubAt, LastError: ts.lastErr, LastErrorAt: ts.lastErrAt}
		if ts.last != nil {
			st.Value, st.HasValue = ts.last.v.PlainJSON(), true
			if v, ok := ts.last.attrs[attrStatus]; ok && v.Kind != oahost.KindNull {
				u := v.Uint
				st.Status = &u
			}
		}
		out = append(out, st)
	}
	slices.SortFunc(out, func(a, b TopicStatus) int { return strings.Compare(a.Topic, b.Topic) })
	return out
}

// Write handles a publish on <topic>/..set: it writes the command value to
// :_original.._value of the mapped DPE with the native typed-write rules.
// handled is false when the topic is not one of this runtime's topics.
func (r *RuntimeA) Write(topic string, payload []byte) (handled bool, err error) {
	r.mu.Lock()
	ts := r.targets[topic]
	allow := r.allowWrite
	stopped := r.stopped
	r.mu.Unlock()
	if ts == nil || stopped {
		return false, nil
	}
	if !allow {
		return true, ErrWriteDisabled
	}
	if !r.gate.Active() {
		r.log.Info("uns ..set rejected on STANDBY", "logger", "UnsModeler", "device", r.name, "topic", topic)
		return true, ErrStandby
	}
	cmd, err := winccoanative.ParseCommand(payload)
	if err != nil {
		return true, err
	}
	v, err := winccoanative.ConvertValue(cmd.Value, oahost.Kind(ts.t.ElemKind))
	if err != nil {
		return true, err
	}
	dpe := ts.t.DPE()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		defer cancel()
		if err := r.api.DpSet(ctx, []string{dpe + ":_original.._value"}, []oahost.Value{v}, r.timeout); err != nil {
			r.cnt.Errors.Add(1)
			r.setErr(dpe, err)
			r.log.Warn("uns ..set write failed", "logger", "UnsModeler", "device", r.name, "topic", topic, "dpe", dpe, "err", err)
			return
		}
		r.cnt.Writes.Add(1)
	}()
	return true, nil
}
