package oastore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/winccoabrowse"
	"monstermq.io/edge/internal/winccoanative"
)

const (
	defaultQueryMaxRows    = 10000
	maxWildcardHistoryDPEs = 1000
	maxHistoryLimit        = 100000
	aggPageSamples         = 50000
	maxAggregationCells    = 1000000
	defaultHistoryWindow   = 24 * time.Hour

	getTimeout   = 5 * time.Second
	queryTimeout = 10 * time.Second
	aggTimeout   = 30 * time.Second

	typeLookupBatch = 200
)

var aggPageSamplesOverride atomic.Int32

// SetAggPageSamplesForTest overrides the per-call DpGetPeriod sample limit used
// by GetAggregatedHistory and returns a function that restores the previous value.
func SetAggPageSamplesForTest(n int) func() {
	prev := aggPageSamplesOverride.Swap(int32(n))
	return func() { aggPageSamplesOverride.Store(prev) }
}

// ReadOptions configures a WinCC OA last-value store or message archive.
type ReadOptions struct {
	API     oahost.API
	Names   func() winccoanative.Names // late-bound; Local may be "" before the native service started
	MaxRows int                        // WinCCOaNative.QueryMaxRows
	Logger  *slog.Logger
}

// NewLastValStore constructs a read-only WinCC OA online last-value store.
func NewLastValStore(name string, o ReadOptions) *LastValStore {
	r := newReader(o)
	return &LastValStore{
		name:   name,
		r:      r,
		browse: winccoabrowse.New(o.API.C),
	}
}

// NewMessageArchive constructs a read-only WinCC OA historical archive store.
func NewMessageArchive(name string, o ReadOptions) *MessageArchive {
	return &MessageArchive{
		name: name,
		r:    newReader(o),
	}
}

type reader struct {
	opts ReadOptions

	mu       sync.Mutex
	local    string
	lastWarn time.Time
}

func newReader(o ReadOptions) *reader {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &reader{opts: o}
}

func (r *reader) names() winccoanative.Names {
	if r.opts.Names != nil {
		return r.opts.Names().WithDefaults()
	}
	return winccoanative.DefaultNames
}

func (r *reader) maxRows() int {
	if r.opts.MaxRows > 0 {
		return r.opts.MaxRows
	}
	return defaultQueryMaxRows
}

func (r *reader) aggPageSamples() int {
	if n := aggPageSamplesOverride.Load(); n > 0 {
		return int(n)
	}
	return aggPageSamples
}

func (r *reader) localSystem(ctx context.Context) (string, error) {
	if n := r.names(); n.Local != "" {
		return n.Local, nil
	}
	r.mu.Lock()
	local := r.local
	r.mu.Unlock()
	if local != "" {
		return local, nil
	}
	cctx, cancel := context.WithTimeout(ctx, getTimeout)
	defer cancel()
	info, err := r.opts.API.SysInfo(cctx)
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	r.local = info.LocalSystem
	r.mu.Unlock()
	return info.LocalSystem, nil
}

func (r *reader) probe(ctx context.Context) error {
	err := r.opts.API.CursorClose(ctx, 0, getTimeout)
	if err != nil {
		if strings.Contains(err.Error(), "unknown operation") {
			return fmt.Errorf("WCCOAmmq does not support archive reads (unknown operation 20); update the manager: %w", err)
		}
		return err
	}
	return nil
}

func (r *reader) warnTruncated(n int, query string) {
	r.mu.Lock()
	if time.Since(r.lastWarn) < time.Minute {
		r.mu.Unlock()
		return
	}
	r.lastWarn = time.Now()
	r.mu.Unlock()
	r.opts.Logger.Warn(fmt.Sprintf("WinCC OA query truncated at %d rows", n), "rows", n, "query", query)
}

type resolvedTarget struct {
	topic  string
	system string
	target winccoanative.Target
}

// resolveExact parses and resolves an exact native topic for online reading.
// If the topic is not a readable WinCC OA tag, it returns (nil, nil).
func (r *reader) resolveExact(ctx context.Context, topic string, historyOnly bool) (*resolvedTarget, error) {
	n := r.names()
	if n.Classify(topic) != winccoanative.KindNative || winccoanative.HasWildcard(topic) {
		return nil, nil
	}
	t, err := n.Parse(topic)
	if err != nil || t.Command || winccoanative.Protected(t.DP) {
		return nil, nil
	}
	if historyOnly && t.Explicit && t.Attr != winccoanative.DefaultAttr {
		return nil, nil
	}
	local, err := r.localSystem(ctx)
	if err != nil {
		return nil, err
	}
	sys := t.System
	if sys == "" {
		sys = local
	}
	rctx, cancel := context.WithTimeout(ctx, getTimeout)
	defer cancel()
	addr := t.ReadAddress(sys)
	if historyOnly {
		addr = t.DPE(sys)
	}
	res, err := r.opts.API.Resolve(rctx, addr)
	if err != nil {
		if errors.Is(err, oahost.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if !res.Exists || winccoanative.ProtectedType(res.TypeName) || res.ElemType == oahost.ElemStruct || res.ElemType == oahost.ElemUnsupported {
		return nil, nil
	}
	if res.System != "" && res.System != sys {
		return nil, nil
	}
	if t.TypeName != "" && t.TypeName != res.TypeName {
		return nil, nil
	}
	return &resolvedTarget{
		topic:  t.Topic(),
		system: sys,
		target: t,
	}, nil
}
