package broker

import "sync"

// ProtocolSessions tracks active non-MQTT protocol sessions (NATS, Redis) so
// the dashboard removeSessions mutation can close their connections.
type ProtocolSessions struct {
	mu    sync.Mutex
	close map[string]func()
}

func NewProtocolSessions() *ProtocolSessions {
	return &ProtocolSessions{close: map[string]func(){}}
}

func (p *ProtocolSessions) Register(clientID string, closeFn func()) {
	if p == nil || clientID == "" || closeFn == nil {
		return
	}
	p.mu.Lock()
	p.close[clientID] = closeFn
	p.mu.Unlock()
}

func (p *ProtocolSessions) Unregister(clientID string) {
	if p == nil || clientID == "" {
		return
	}
	p.mu.Lock()
	delete(p.close, clientID)
	p.mu.Unlock()
}

func (p *ProtocolSessions) Close(clientID string) bool {
	if p == nil || clientID == "" {
		return false
	}
	p.mu.Lock()
	fn, ok := p.close[clientID]
	delete(p.close, clientID)
	p.mu.Unlock()
	if ok && fn != nil {
		fn()
		return true
	}
	return false
}
