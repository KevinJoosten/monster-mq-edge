package nats

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/auth"
	"monstermq.io/edge/internal/pubsub"
	"monstermq.io/edge/internal/stores"
)

// SessionRegistry tracks active protocol sessions for dashboard disconnect (S5).
type SessionRegistry interface {
	Register(clientID string, closeFn func())
	Unregister(clientID string)
}

// ClientCounter counts inbound and outbound messages per client (S5).
type ClientCounter interface {
	IncClientIn(clientID string)
	IncClientOut(clientID string)
	ForgetClient(clientID string)
}

// Deps contains the dependencies required to run the NATS server.
type Deps struct {
	Port                  int
	NodeID                string
	UserManagementEnabled bool
	AuthCache             *auth.Cache
	Bus                   *pubsub.Bus
	Publish               func(clientID string, msg stores.BrokerMessage) error
	Release               func(clientID string)
	NativeValueWrite      func(topic string) (string, bool)
	Sessions              stores.SessionStore
	ProtoSessions         SessionRegistry
	ArchiveManager        *archive.Manager
	Counter               ClientCounter
	Logger                *slog.Logger
}

// Server is the plain-TCP NATS protocol server.
type Server struct {
	deps     Deps
	listener net.Listener
	mu       sync.Mutex
	conns    map[*Conn]struct{}
	closed   bool
	wg       sync.WaitGroup
}

// New creates and binds the NATS server on the configured port.
// A bind failure returns an error immediately to abort startup (NS:36-39).
// It also cleans up leftover nats- session records for this node (E9).
func New(deps Deps) (*Server, error) {
	addr := fmt.Sprintf(":%d", deps.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		if deps.Logger != nil {
			deps.Logger.Error("Error starting NATS Server", "err", err, "port", deps.Port)
		}
		return nil, err
	}

	if deps.Logger != nil {
		deps.Logger.Info("NATS Server is listening", "port", ln.Addr().(*net.TCPAddr).Port)
	}

	// E9 [edge-only]: delete leftover nats- session records for this node.
	if deps.Sessions != nil {
		var toDelete []string
		_ = deps.Sessions.IterateSessions(context.Background(), func(info stores.SessionInfo) bool {
			if strings.HasPrefix(info.ClientID, "nats-") && (deps.NodeID == "" || info.NodeID == deps.NodeID) {
				toDelete = append(toDelete, info.ClientID)
			}
			return true
		})
		for _, id := range toDelete {
			_ = deps.Sessions.DelClient(context.Background(), id)
		}
	}

	return &Server{
		deps:     deps,
		listener: ln,
		conns:    make(map[*Conn]struct{}),
	}, nil
}

// Port returns the actual listening port.
func (s *Server) Port() int {
	if s.listener == nil {
		return 0
	}
	return s.listener.Addr().(*net.TCPAddr).Port
}

// Serve runs the accept loop until the server is closed.
func (s *Server) Serve() {
	for {
		netConn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return
			}
			if s.deps.Logger != nil {
				s.deps.Logger.Warn("NATS accept error", "err", err)
			}
			continue
		}

		if tcpConn, ok := netConn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
		}

		conn := newConn(s, netConn)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = netConn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()

		go func() {
			defer func() {
				s.removeConn(conn)
				s.wg.Done()
			}()
			conn.serve()
		}()
	}
}

func (s *Server) removeConn(c *Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// Close closes the listener and terminates all active connections.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}
	for conn := range s.conns {
		conn.close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	return err
}
