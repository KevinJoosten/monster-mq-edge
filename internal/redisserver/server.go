package redisserver

import (
	"fmt"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type Server struct {
	deps      Deps
	listener  net.Listener
	conns     map[*Conn]struct{}
	connsMu   sync.Mutex
	connSeq   atomic.Int64
	stripes   [256]sync.Mutex
	closed    atomic.Bool
	closeOnce sync.Once
}

func New(deps Deps) *Server {
	return &Server{
		deps:  deps,
		conns: make(map[*Conn]struct{}),
	}
}

func (s *Server) Start() error {
	// Startup check: duplicate RedisDbNumber mappings
	if s.deps.Archives != nil {
		groups := s.deps.Archives.Snapshot()
		dbToGroups := make(map[int][]string)
		for _, g := range groups {
			eff := g.Config().EffectiveRedisDbNumber()
			if eff != nil {
				dbToGroups[*eff] = append(dbToGroups[*eff], g.Name())
			}
		}

		var dups []int
		for db, names := range dbToGroups {
			if len(names) > 1 {
				dups = append(dups, db)
			}
		}
		if len(dups) > 0 {
			sort.Ints(dups)
			var parts []string
			for _, db := range dups {
				names := dbToGroups[db]
				sort.Strings(names)
				parts = append(parts, fmt.Sprintf("%d=[%s]", db, strings.Join(names, ", ")))
			}
			return fmt.Errorf("Duplicate RedisDbNumber mapping: {%s}", strings.Join(parts, ", "))
		}
	}

	addr := net.JoinHostPort(s.deps.Cfg.RedisServer.EffectiveHost(), fmt.Sprintf("%d", s.deps.Cfg.RedisServer.EffectivePort()))
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = l
	s.deps.Logger.Info("Redis protocol server listening", "host", s.deps.Cfg.RedisServer.EffectiveHost(), "port", s.deps.Cfg.RedisServer.EffectivePort())

	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			s.deps.Logger.Warn("redis server accept error", "err", err)
			continue
		}

		id := s.connSeq.Add(1)
		c := newConn(s, id, conn)

		s.connsMu.Lock()
		if s.closed.Load() {
			s.connsMu.Unlock()
			conn.Close()
			return
		}
		s.conns[c] = struct{}{}
		s.connsMu.Unlock()

		go c.serve()
	}
}

func (s *Server) removeConn(c *Conn) {
	s.connsMu.Lock()
	delete(s.conns, c)
	s.connsMu.Unlock()
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		if s.listener != nil {
			s.listener.Close()
		}
		s.connsMu.Lock()
		conns := make([]*Conn, 0, len(s.conns))
		for c := range s.conns {
			conns = append(conns, c)
		}
		s.conns = make(map[*Conn]struct{})
		s.connsMu.Unlock()

		for _, c := range conns {
			c.Close()
		}
	})
	return nil
}

// stripeKey returns fnv32a(group + "\x00" + topic) % 256.
func stripeIndex(group, topic string) uint32 {
	var h uint32 = 2166136261
	s := group + "\x00" + topic
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h % 256
}

// lockStripes locks the distinct stripes for group and topics in ascending order (F6).
// It returns an unlock function.
func (s *Server) lockStripes(group string, topics []string) func() {
	if len(topics) == 0 {
		return func() {}
	}
	seen := make(map[uint32]struct{})
	for _, t := range topics {
		seen[stripeIndex(group, t)] = struct{}{}
	}
	stripes := make([]uint32, 0, len(seen))
	for idx := range seen {
		stripes = append(stripes, idx)
	}
	slices.Sort(stripes)

	for _, idx := range stripes {
		s.stripes[idx].Lock()
	}

	return func() {
		for i := len(stripes) - 1; i >= 0; i-- {
			s.stripes[stripes[i]].Unlock()
		}
	}
}
