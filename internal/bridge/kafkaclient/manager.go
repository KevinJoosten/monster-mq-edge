package kafkaclient

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"monstermq.io/edge/internal/stores"
)

// Manager loads KAFKA-Client device configs from the DeviceConfigStore and
// runs one Connector per enabled device on this node.
type Manager struct {
	store     stores.DeviceConfigStore
	publisher LocalPublisher
	subBus    LocalSubscriber
	logger    *slog.Logger
	nodeID    string

	// srvCtx is the long-lived server context saved by Start; Reload receives
	// a short-lived GraphQL request context that must not own the loops.
	srvCtx context.Context

	mu         sync.Mutex
	connectors map[string]*Connector
	lastConfig map[string]string // device name -> namespace + raw JSON last deployed

	incIn  func(int)
	incOut func(int)
}

func NewManager(store stores.DeviceConfigStore, publisher LocalPublisher, subBus LocalSubscriber, nodeID string, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		store: store, publisher: publisher, subBus: subBus, logger: logger, nodeID: nodeID,
		srvCtx:     context.Background(),
		connectors: map[string]*Connector{},
		lastConfig: map[string]string{},
	}
}

func (m *Manager) SetCounters(incIn, incOut func(int)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incIn = incIn
	m.incOut = incOut
	for _, c := range m.connectors {
		m.attachCounters(c)
	}
}

func (m *Manager) attachCounters(c *Connector) {
	if m.incIn != nil {
		c.IncIn = func() { m.incIn(1) }
	}
	if m.incOut != nil {
		c.IncOut = func() { m.incOut(1) }
	}
}

func (m *Manager) StartMetrics(ctx context.Context, store stores.MetricsStore, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				for _, c := range m.Snapshot() {
					snap := c.SampleMetrics(now, interval)
					if store == nil {
						continue
					}
					payload, _ := json.Marshal(snap)
					if err := store.StoreMetrics(ctx, stores.MetricKafkaClient, now.UTC(), c.Name(), string(payload)); err != nil {
						m.logger.Warn("kafka client metrics persist failed", "name", c.Name(), "err", err)
					}
				}
			}
		}
	}()
}

func (m *Manager) Start(ctx context.Context) error {
	m.srvCtx = ctx
	return m.Reload(ctx)
}

func (m *Manager) Stop() {
	m.mu.Lock()
	current := m.connectors
	m.connectors = map[string]*Connector{}
	m.lastConfig = map[string]string{}
	m.mu.Unlock()
	for _, c := range current {
		c.Stop()
	}
}

type wantedDevice struct {
	namespace string
	cfg       Config
	key       string
}

// Reload reconciles the running connectors with the stored device configs:
// starts new ones, restarts changed ones and stops removed or disabled ones.
// Call it after every mutation that touches a Kafka client.
func (m *Manager) Reload(ctx context.Context) error {
	devices, err := m.store.GetEnabledByNode(ctx, m.nodeID)
	if err != nil {
		return err
	}
	wanted := map[string]wantedDevice{}
	for _, d := range devices {
		if d.Type != DeviceTypeKafkaClient {
			continue
		}
		cfg, err := ParseConfig(d.Config)
		if err != nil {
			m.logger.Warn("kafka client config parse failed", "name", d.Name, "err", err)
			continue
		}
		wanted[d.Name] = wantedDevice{namespace: d.Namespace, cfg: cfg, key: d.Namespace + "\x00" + d.Config}
	}

	m.mu.Lock()
	var stale []*Connector
	for name, c := range m.connectors {
		if w, keep := wanted[name]; !keep || w.key != m.lastConfig[name] {
			stale = append(stale, c)
			delete(m.connectors, name)
			delete(m.lastConfig, name)
		}
	}
	m.mu.Unlock()
	for _, c := range stale {
		c.Stop()
	}

	for name, w := range wanted {
		m.mu.Lock()
		_, running := m.connectors[name]
		m.mu.Unlock()
		if running {
			continue
		}
		c := NewConnector(name, w.namespace, w.cfg, m.publisher, m.subBus, m.logger)
		m.mu.Lock()
		m.attachCounters(c)
		m.mu.Unlock()
		if err := c.Start(m.srvCtx); err != nil {
			m.logger.Warn("kafka client start failed", "name", name, "err", err)
			continue
		}
		m.mu.Lock()
		m.connectors[name] = c
		m.lastConfig[name] = w.key
		m.mu.Unlock()
	}
	return nil
}

func (m *Manager) Active() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.connectors))
	for name := range m.connectors {
		out = append(out, name)
	}
	return out
}

func (m *Manager) Snapshot() []*Connector {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Connector, 0, len(m.connectors))
	for _, c := range m.connectors {
		out = append(out, c)
	}
	return out
}

func (m *Manager) Get(name string) *Connector {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connectors[name]
}
