package unsmodeler

import "sync"

// RoleGate tells the device whether this broker is ACTIVE (§3.4). Every
// publish, WinCC OA write, ..set, apply and AI run checks it; a role change
// suppresses or resumes them without re-apply. Until the PeerLink role
// manager exists, the only production implementation is AlwaysActive.
type RoleGate interface {
	Active() bool
	// Watch calls fn on every role change; the returned func stops it.
	Watch(fn func(active bool)) func()
}

// AlwaysActive is the dummy gate: always ACTIVE.
type AlwaysActive struct{}

func (AlwaysActive) Active() bool                   { return true }
func (AlwaysActive) Watch(func(active bool)) func() { return func() {} }

// SwitchGate is a gate whose role is set by hand (tests).
type SwitchGate struct {
	mu       sync.Mutex
	standby  bool
	watchers map[int]func(bool)
	next     int
}

func (g *SwitchGate) Active() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.standby
}

// Set switches the role and notifies the watchers.
func (g *SwitchGate) Set(active bool) {
	g.mu.Lock()
	g.standby = !active
	fns := make([]func(bool), 0, len(g.watchers))
	for _, fn := range g.watchers {
		fns = append(fns, fn)
	}
	g.mu.Unlock()
	for _, fn := range fns {
		fn(active)
	}
}

func (g *SwitchGate) Watch(fn func(active bool)) func() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.watchers == nil {
		g.watchers = map[int]func(bool){}
	}
	id := g.next
	g.next++
	g.watchers[id] = fn
	return func() {
		g.mu.Lock()
		delete(g.watchers, id)
		g.mu.Unlock()
	}
}
