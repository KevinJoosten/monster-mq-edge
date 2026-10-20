package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
	storecratedb "monstermq.io/edge/internal/stores/cratedb"
	storememory "monstermq.io/edge/internal/stores/memory"
	storemongo "monstermq.io/edge/internal/stores/mongodb"
	"monstermq.io/edge/internal/stores/oastore"
	storepg "monstermq.io/edge/internal/stores/postgres"
	storequestdb "monstermq.io/edge/internal/stores/questdb"
	storesqlite "monstermq.io/edge/internal/stores/sqlite"
	"monstermq.io/edge/internal/winccoanative"
)

const (
	// WinCCOAGroupName is the auto-provisioned WinCC OA archive group name.
	WinCCOAGroupName = "WinCCOA"
	// WinCCOAUnavailableMessage is the deploy/validation error when WINCCOA stores are used outside WCCOAmmq.
	WinCCOAUnavailableMessage = "WINCCOA stores need MonsterMQ Edge embedded in the WinCC OA manager (WCCOAmmq) with WinCCOaNative.Namespace enabled"
)

// WinCCOAGroupConfig returns the auto-provisioned WinCCOA archive group configuration.
func WinCCOAGroupConfig(root string) stores.ArchiveGroupConfig {
	if root == "" {
		root = winccoanative.DefaultNames.Root
	}
	return stores.ArchiveGroupConfig{
		Name:            WinCCOAGroupName,
		Enabled:         true,
		TopicFilters:    []string{root + "/#"},
		LastValType:     stores.MessageStoreWinCCOA,
		ArchiveType:     stores.ArchiveWinCCOA,
		PayloadFormat:   stores.PayloadDefault,
		LastValReadOnly: true,
		ArchiveReadOnly: true,
	}
}

// EffectiveConfig forces the read-only flag on any WINCCOA store side.
func EffectiveConfig(c stores.ArchiveGroupConfig) stores.ArchiveGroupConfig {
	if c.LastValType == stores.MessageStoreWinCCOA {
		c.LastValReadOnly = true
	}
	if c.ArchiveType == stores.ArchiveWinCCOA {
		c.ArchiveReadOnly = true
	}
	return c
}

// Manager owns the active set of archive groups and dispatches every published
// message to all matching groups. It supports SQLite, PostgreSQL and MongoDB
// per-group last-value and archive stores.
type Manager struct {
	cfg      *config.Config
	storage  *stores.Storage
	logger   *slog.Logger
	sqliteDB *storesqlite.DB
	pgDB     *storepg.DB
	mongoDB  *storemongo.DB

	mu           sync.RWMutex
	oa           *oastore.ReadOptions
	groups       map[string]*Group
	groupsSnap   atomic.Pointer[[]*Group]
	groupCount   atomic.Int32      // mirrors len(groups) for the lock-free HasGroups probe
	deployError  map[string]string // last deploy error per group name (for the dashboard)
	onChange     func()            // PeerLink interest routing: the group filters may have changed
	loggedDbDups sync.Map          // db int -> struct{} to log duplicate warnings once
}

// SetWinCCOA wires the embedded WinCC OA store reader before Load is called.
func (m *Manager) SetWinCCOA(o oastore.ReadOptions) {
	m.mu.Lock()
	m.oa = &o
	m.mu.Unlock()
}

// WinCCOAEnabled reports whether the embedded WinCC OA store reader is wired.
func (m *Manager) WinCCOAEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.oa != nil
}

// HasGroups reports whether any archive group is running. Lock-free so the
// broker publish hot path can skip message construction when idle.
func (m *Manager) HasGroups() bool {
	return m.groupCount.Load() > 0
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
				for _, g := range m.Snapshot() {
					snap := g.SampleMetrics(now, interval)
					if store == nil {
						continue
					}
					payload, _ := json.Marshal(snap)
					if err := store.StoreMetrics(ctx, stores.MetricArchive, now.UTC(), g.Name(), string(payload)); err != nil {
						m.logger.Warn("archive metrics persist failed", "group", g.Name(), "err", err)
					}
				}
			}
		}
	}()
}

func NewManager(cfg *config.Config, storage *stores.Storage,
	sqliteDB *storesqlite.DB, pgDB *storepg.DB, mongoDB *storemongo.DB,
	logger *slog.Logger) *Manager {
	return &Manager{
		cfg: cfg, storage: storage, logger: logger,
		sqliteDB: sqliteDB, pgDB: pgDB, mongoDB: mongoDB,
		groups:      map[string]*Group{},
		deployError: map[string]string{},
	}
}

// DeployError returns the last error encountered while trying to start the
// named group, or empty string if the group is healthy.
func (m *Manager) DeployError(name string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.deployError[name]
}

// Load creates archive groups from the persistent ArchiveConfigStore. If no
// "Default" group exists yet, one is provisioned with in-memory last-value
// storage and no history archive.
func (m *Manager) Load(ctx context.Context) error {
	configs, err := m.storage.ArchiveConfig.GetAll(ctx)
	if err != nil {
		return err
	}
	hasDefault := false
	for _, c := range configs {
		if c.Name == "Default" {
			hasDefault = true
			break
		}
	}
	if !hasDefault {
		def := stores.ArchiveGroupConfig{
			Name:          "Default",
			Enabled:       true,
			TopicFilters:  []string{"#"},
			LastValType:   stores.MessageStoreMemory,
			ArchiveType:   stores.ArchiveNone,
			PayloadFormat: stores.PayloadDefault,
		}
		if err := m.storage.ArchiveConfig.Save(ctx, def); err != nil {
			return err
		}
		configs = append(configs, def)
	}
	m.mu.RLock()
	oa := m.oa
	m.mu.RUnlock()
	if oa != nil {
		hasWinCCOA := false
		for _, c := range configs {
			if c.Name == WinCCOAGroupName {
				hasWinCCOA = true
				break
			}
		}
		if !hasWinCCOA {
			root := winccoanative.DefaultNames.Root
			if oa.Names != nil {
				if n := oa.Names().WithDefaults(); n.Root != "" {
					root = n.Root
				}
			}
			oaCfg := WinCCOAGroupConfig(root)
			if err := m.storage.ArchiveConfig.Save(ctx, oaCfg); err != nil {
				return err
			}
			configs = append(configs, oaCfg)
		}
	}
	for _, c := range configs {
		if err := m.startGroup(ctx, c); err != nil {
			m.recordDeployError(c.Name, err)
			m.logger.Error("archive group start failed", "name", c.Name, "err", err)
		}
	}
	m.notifyChange()
	return nil
}

// SetOnChange installs f, called after Load and Reload changed the running groups.
func (m *Manager) SetOnChange(f func()) {
	m.mu.Lock()
	m.onChange = f
	m.mu.Unlock()
}

func (m *Manager) notifyChange() {
	m.mu.RLock()
	f := m.onChange
	m.mu.RUnlock()
	if f != nil {
		f()
	}
}

// InterestFilters returns the topic filters of the running groups that archive live messages.
// Retained-only groups are left out: retained records reach every peer anyway.
func (m *Manager) InterestFilters() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for _, g := range m.groups {
		c := g.Config()
		if c.RetainedOnly {
			continue
		}
		if c.LastValReadOnly && c.ArchiveReadOnly {
			continue
		}
		out = append(out, c.TopicFilters...)
	}
	return out
}

func (m *Manager) recordDeployError(name string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		delete(m.deployError, name)
		return
	}
	m.deployError[name] = err.Error()
}

func (m *Manager) startGroup(ctx context.Context, c stores.ArchiveGroupConfig) error {
	c = EffectiveConfig(c)
	if !c.Enabled {
		return nil
	}
	handles, err := m.openGroupDatabaseHandles(ctx, c)
	if err != nil {
		return err
	}
	closeOnErr := true
	defer func() {
		if closeOnErr {
			handles.close(m.logger, c.Name)
		}
	}()
	lastVal, err := m.buildLastValStore(ctx, c, handles)
	if err != nil {
		return err
	}
	arch, err := m.buildArchiveStore(ctx, c, &handles)
	if err != nil {
		return err
	}
	g := NewGroup(c, lastVal, arch, m.logger, handles.closeFunc(m.logger, c.Name))
	g.Start()
	m.mu.Lock()
	m.groups[c.Name] = g
	m.refreshGroupsSnapLocked()
	delete(m.deployError, c.Name)
	m.mu.Unlock()
	closeOnErr = false

	// Populate in-memory last-value store with existing retained messages from storage.
	if c.LastValType == stores.MessageStoreMemory && lastVal != nil && m.storage.Retained != nil {
		var batch []stores.BrokerMessage
		_ = m.storage.Retained.FindMatchingMessages(ctx, "#", func(msg stores.BrokerMessage) bool {
			if g.Matches(msg.TopicName, msg.IsRetain) {
				batch = append(batch, msg)
			}
			return true
		})
		if len(batch) > 0 {
			if err := lastVal.AddAll(ctx, batch); err != nil {
				m.logger.Warn("failed to populate retained messages to memory store", "group", c.Name, "err", err)
			} else {
				m.logger.Info("populated retained messages to memory store", "group", c.Name, "count", len(batch))
			}
		}
	}

	m.logger.Info("archive group started",
		"name", c.Name, "filters", c.TopicFilters,
		"lastValType", c.LastValType, "archiveType", c.ArchiveType)
	return nil
}

// Per-group store and archive names match the Kotlin/JVM broker:
//   last-value store →  "<groupName>Lastval"   (SQL table: <groupname>lastval)
//   message archive  →  "<groupName>Archive"   (SQL table: <groupname>archive)
// so the same physical database can be opened by either implementation.

// LastValName / ArchiveName are exported so tests and the dashboard can derive
// the same names production uses. Both are deterministic from the group name.
func LastValName(group string) string { return group + "Lastval" }
func ArchiveName(group string) string { return group + "Archive" }

// validGroupNamePattern restricts group names to characters that are safe to
// embed in a SQL identifier. Group names flow into CREATE TABLE statements via
// fmt.Sprintf, so anything outside this set could be SQL injection.
var validGroupNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,62}$`)

func ValidateGroupName(name string) error {
	if !validGroupNamePattern.MatchString(name) {
		return fmt.Errorf("invalid group name %q: must match [A-Za-z][A-Za-z0-9_]{0,62}", name)
	}
	return nil
}

type groupDatabaseHandles struct {
	sqliteDB  *storesqlite.DB
	pgDB      *storepg.DB
	questdbDB *storequestdb.DB
	cratedbDB *storecratedb.DB
	mongoDB   *storemongo.DB
	owned     []func() error
}

func (h groupDatabaseHandles) close(logger *slog.Logger, group string) {
	for _, closeFn := range h.owned {
		if closeFn == nil {
			continue
		}
		if err := closeFn(); err != nil {
			logger.Warn("archive group database close failed", "group", group, "err", err)
		}
	}
}

func (h groupDatabaseHandles) closeFunc(logger *slog.Logger, group string) func() error {
	if len(h.owned) == 0 {
		return nil
	}
	return func() error {
		h.close(logger, group)
		return nil
	}
}

func (m *Manager) openGroupDatabaseHandles(ctx context.Context, c stores.ArchiveGroupConfig) (groupDatabaseHandles, error) {
	handles := groupDatabaseHandles{sqliteDB: m.sqliteDB, pgDB: m.pgDB, mongoDB: m.mongoDB}
	selectedName := strings.TrimSpace(c.DatabaseConnectionName)
	required := RequiredDatabaseConnectionTypes(c.LastValType, c.ArchiveType)
	if selectedName == "" {
		return handles, nil
	}
	if len(required) == 0 {
		return handles, fmt.Errorf("group %s: databaseConnectionName can only be used with SQLite, Postgres, CrateDB, QuestDB, or MongoDB stores", c.Name)
	}
	if len(required) > 1 {
		if IsDefaultDatabaseConnectionName(selectedName) {
			return handles, nil
		}
		return handles, fmt.Errorf("group %s: cannot use one named database connection for mixed database stores", c.Name)
	}
	if IsDefaultDatabaseConnectionName(selectedName) {
		switch required[0] {
		case stores.DatabaseConnectionSQLite:
			if m.sqliteDB == nil {
				return handles, fmt.Errorf("group %s: default SQLite database connection is not configured", c.Name)
			}
		case stores.DatabaseConnectionPostgres:
			if m.pgDB == nil {
				return handles, fmt.Errorf("group %s: default Postgres database connection is not configured", c.Name)
			}
		case stores.DatabaseConnectionQuestDB:
			if m.cfg.QuestDB.URL == "" {
				return handles, fmt.Errorf("group %s: default QuestDB database connection is not configured", c.Name)
			}
			db, err := storequestdb.Open(ctx, m.cfg.QuestDB.URL, m.cfg.QuestDB.User, m.cfg.QuestDB.Pass)
			if err != nil {
				return handles, fmt.Errorf("group %s: open default QuestDB connection: %w", c.Name, err)
			}
			handles.questdbDB = db
			handles.owned = append(handles.owned, db.Close)
		case stores.DatabaseConnectionCrateDB:
			if m.cfg.CrateDB.URL == "" {
				return handles, fmt.Errorf("group %s: default CrateDB database connection is not configured", c.Name)
			}
			db, err := storecratedb.Open(ctx, m.cfg.CrateDB.URL, m.cfg.CrateDB.User, m.cfg.CrateDB.Pass)
			if err != nil {
				return handles, fmt.Errorf("group %s: open default CrateDB connection: %w", c.Name, err)
			}
			handles.cratedbDB = db
			handles.owned = append(handles.owned, db.Close)
		case stores.DatabaseConnectionMongoDB:
			if m.mongoDB == nil {
				return handles, fmt.Errorf("group %s: default MongoDB database connection is not configured", c.Name)
			}
		}
		return handles, nil
	}
	conn, err := m.storage.ArchiveConfig.GetDatabaseConnection(ctx, selectedName)
	if err != nil {
		return handles, err
	}
	if conn == nil {
		return handles, fmt.Errorf("group %s: database connection %q not found", c.Name, selectedName)
	}
	if conn.Type != required[0] {
		return handles, fmt.Errorf("group %s: selected %s connection %q but requires %s", c.Name, conn.Type, selectedName, required[0])
	}
	switch conn.Type {
	case stores.DatabaseConnectionSQLite:
		db, err := storesqlite.Open(conn.URL)
		if err != nil {
			return handles, err
		}
		handles.sqliteDB = db
		handles.owned = append(handles.owned, db.Close)
	case stores.DatabaseConnectionPostgres:
		db, err := storepg.Open(ctx, postgresDSN(conn.URL, conn.Username, conn.Password))
		if err != nil {
			return handles, err
		}
		handles.pgDB = db
		handles.owned = append(handles.owned, db.Close)
	case stores.DatabaseConnectionQuestDB:
		db, err := storequestdb.Open(ctx, conn.URL, conn.Username, conn.Password)
		if err != nil {
			return handles, err
		}
		handles.questdbDB = db
		handles.owned = append(handles.owned, db.Close)
	case stores.DatabaseConnectionCrateDB:
		db, err := storecratedb.Open(ctx, conn.URL, conn.Username, conn.Password)
		if err != nil {
			return handles, err
		}
		handles.cratedbDB = db
		handles.owned = append(handles.owned, db.Close)
	case stores.DatabaseConnectionMongoDB:
		dbName := conn.Database
		if dbName == "" {
			dbName = "monstermq"
		}
		db, err := storemongo.Open(ctx, MongoURLWithCredentials(conn.URL, conn.Username, conn.Password), dbName)
		if err != nil {
			return handles, err
		}
		handles.mongoDB = db
		handles.owned = append(handles.owned, db.Close)
	default:
		return handles, fmt.Errorf("group %s: unsupported database connection type %q", c.Name, conn.Type)
	}
	return handles, nil
}

func postgresDSN(raw, username, password string) string {
	raw = strings.TrimPrefix(raw, "jdbc:")
	if username == "" && password == "" {
		return raw
	}
	u, err := url.Parse(raw)
	if err == nil && u.Scheme != "" {
		if password != "" {
			u.User = url.UserPassword(username, password)
		} else {
			u.User = url.User(username)
		}
		return u.String()
	}
	sep := "?"
	if strings.Contains(raw, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%suser=%s&password=%s", raw, sep, url.QueryEscape(username), url.QueryEscape(password))
}

func (m *Manager) buildLastValStore(ctx context.Context, c stores.ArchiveGroupConfig, handles groupDatabaseHandles) (stores.MessageStore, error) {
	name := LastValName(c.Name)
	switch c.LastValType {
	case stores.MessageStoreMemory:
		return storememory.NewMessageStore(name), nil
	case stores.MessageStoreSQLite:
		if handles.sqliteDB == nil {
			return nil, fmt.Errorf("group %s: lastValType=SQLITE but no SQLite DB configured", c.Name)
		}
		s := storesqlite.NewMessageStore(name, handles.sqliteDB)
		if err := s.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on sqlite: %w", name, err)
		}
		return s, nil
	case stores.MessageStorePostgres:
		if handles.pgDB == nil {
			return nil, fmt.Errorf("group %s: lastValType=POSTGRES but no Postgres DB configured", c.Name)
		}
		s := storepg.NewMessageStore(name, handles.pgDB)
		if err := s.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on postgres: %w", name, err)
		}
		return s, nil
	case stores.MessageStoreMongoDB:
		if handles.mongoDB == nil {
			return nil, fmt.Errorf("group %s: lastValType=MONGODB but no MongoDB configured", c.Name)
		}
		s := storemongo.NewMessageStore(name, handles.mongoDB)
		if err := s.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on mongo: %w", name, err)
		}
		return s, nil
	case stores.MessageStoreWinCCOA:
		m.mu.RLock()
		oa := m.oa
		m.mu.RUnlock()
		if oa == nil {
			return nil, fmt.Errorf("group %s: %s", c.Name, WinCCOAUnavailableMessage)
		}
		s := oastore.NewLastValStore(name, *oa)
		if err := s.EnsureTable(ctx); err != nil {
			return nil, err
		}
		return s, nil
	case stores.MessageStoreNone, "":
		return nil, nil
	}
	return nil, fmt.Errorf("group %s: unsupported lastValType %q", c.Name, c.LastValType)
}

// buildArchiveStore takes handles by pointer: a default connection it opens
// itself is added to handles.owned and closed with the group.
func (m *Manager) buildArchiveStore(ctx context.Context, c stores.ArchiveGroupConfig, handles *groupDatabaseHandles) (stores.MessageArchive, error) {
	name := ArchiveName(c.Name)
	switch c.ArchiveType {
	case stores.ArchiveSQLite:
		if handles.sqliteDB == nil {
			return nil, fmt.Errorf("group %s: archiveType=SQLITE but no SQLite DB configured", c.Name)
		}
		a := storesqlite.NewMessageArchive(name, handles.sqliteDB, c.PayloadFormat)
		if err := a.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on sqlite: %w", name, err)
		}
		return a, nil
	case stores.ArchivePostgres:
		if handles.pgDB == nil {
			return nil, fmt.Errorf("group %s: archiveType=POSTGRES but no Postgres DB configured", c.Name)
		}
		a := storepg.NewMessageArchive(name, handles.pgDB, c.PayloadFormat)
		if err := a.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on postgres: %w", name, err)
		}
		return a, nil
	case stores.ArchiveQuestDB:
		if handles.questdbDB == nil {
			if m.cfg.QuestDB.URL != "" {
				db, err := storequestdb.Open(ctx, m.cfg.QuestDB.URL, m.cfg.QuestDB.User, m.cfg.QuestDB.Pass)
				if err != nil {
					return nil, fmt.Errorf("group %s: open default QuestDB: %w", c.Name, err)
				}
				handles.questdbDB = db
				handles.owned = append(handles.owned, db.Close)
			} else {
				return nil, fmt.Errorf("group %s: archiveType=QUESTDB but no QuestDB connection configured", c.Name)
			}
		}
		a := storequestdb.NewMessageArchive(name, handles.questdbDB, c.PayloadFormat)
		if err := a.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on questdb: %w", name, err)
		}
		return a, nil
	case stores.ArchiveCrateDB:
		if handles.cratedbDB == nil {
			if m.cfg.CrateDB.URL == "" {
				return nil, fmt.Errorf("group %s: archiveType=CRATEDB but no CrateDB connection configured", c.Name)
			}
			db, err := storecratedb.Open(ctx, m.cfg.CrateDB.URL, m.cfg.CrateDB.User, m.cfg.CrateDB.Pass)
			if err != nil {
				return nil, fmt.Errorf("group %s: open default CrateDB: %w", c.Name, err)
			}
			handles.cratedbDB = db
			handles.owned = append(handles.owned, db.Close)
		}
		a := storecratedb.NewMessageArchive(name, handles.cratedbDB, c.PayloadFormat)
		if err := a.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on cratedb: %w", name, err)
		}
		return a, nil
	case stores.ArchiveMongoDB:
		if handles.mongoDB == nil {
			return nil, fmt.Errorf("group %s: archiveType=MONGODB but no MongoDB configured", c.Name)
		}
		a := storemongo.NewMessageArchive(name, handles.mongoDB, c.PayloadFormat)
		if err := a.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on mongo: %w", name, err)
		}
		return a, nil
	case stores.ArchiveWinCCOA:
		m.mu.RLock()
		oa := m.oa
		m.mu.RUnlock()
		if oa == nil {
			return nil, fmt.Errorf("group %s: %s", c.Name, WinCCOAUnavailableMessage)
		}
		a := oastore.NewMessageArchive(name, *oa)
		if err := a.EnsureTable(ctx); err != nil {
			return nil, err
		}
		return a, nil
	case stores.ArchiveNone, "":
		return nil, nil
	}
	return nil, fmt.Errorf("group %s: unsupported archiveType %q", c.Name, c.ArchiveType)
}

// Reload re-reads the archive config table and reconciles the running groups
// with the persisted state: starts new ones, restarts changed ones, stops
// removed ones. Call this after any GraphQL mutation that touched an archive
// group config.
func (m *Manager) Reload(ctx context.Context) error {
	configs, err := m.storage.ArchiveConfig.GetAll(ctx)
	if err != nil {
		return err
	}
	wanted := map[string]stores.ArchiveGroupConfig{}
	for _, c := range configs {
		if c.Enabled {
			wanted[c.Name] = EffectiveConfig(c)
		}
	}

	m.mu.Lock()
	current := m.groups
	m.mu.Unlock()

	// Stop groups that are gone or now disabled or whose config changed.
	for name, g := range current {
		w, keep := wanted[name]
		if !keep || !configsEqual(g.Config(), w) {
			g.Stop()
			m.mu.Lock()
			delete(m.groups, name)
			m.refreshGroupsSnapLocked()
			m.mu.Unlock()
		}
	}
	// Start groups that should be running but aren't.
	for name, c := range wanted {
		m.mu.RLock()
		_, running := m.groups[name]
		m.mu.RUnlock()
		if running {
			continue
		}
		if err := m.startGroup(ctx, c); err != nil {
			m.recordDeployError(name, err)
			m.logger.Error("archive group reload failed", "name", name, "err", err)
		}
	}
	m.notifyChange()
	return nil
}

func configsEqual(a, b stores.ArchiveGroupConfig) bool {
	if a.Name != b.Name || a.Enabled != b.Enabled || a.RetainedOnly != b.RetainedOnly {
		return false
	}
	if a.LastValType != b.LastValType || a.ArchiveType != b.ArchiveType {
		return false
	}
	if a.LastValReadOnly != b.LastValReadOnly || a.ArchiveReadOnly != b.ArchiveReadOnly {
		return false
	}
	if a.DatabaseConnectionName != b.DatabaseConnectionName {
		return false
	}
	if a.PayloadFormat != b.PayloadFormat {
		return false
	}
	effA := a.EffectiveRedisDbNumber()
	effB := b.EffectiveRedisDbNumber()
	if (effA == nil) != (effB == nil) {
		return false
	}
	if effA != nil && effB != nil && *effA != *effB {
		return false
	}
	if a.LastValRetention != b.LastValRetention || a.ArchiveRetention != b.ArchiveRetention {
		return false
	}
	if len(a.TopicFilters) != len(b.TopicFilters) {
		return false
	}
	for i := range a.TopicFilters {
		if a.TopicFilters[i] != b.TopicFilters[i] {
			return false
		}
	}
	return true
}

func (m *Manager) refreshGroupsSnapLocked() {
	snap := make([]*Group, 0, len(m.groups))
	for _, g := range m.groups {
		snap = append(snap, g)
	}
	m.groupsSnap.Store(&snap)
	m.groupCount.Store(int32(len(snap)))
}

// Dispatch routes msg to every matching group.
func (m *Manager) Dispatch(msg stores.BrokerMessage) {
	snap := m.groupsSnap.Load()
	if snap == nil {
		return
	}
	for _, g := range *snap {
		if g.Matches(msg.TopicName, msg.IsRetain) {
			g.Submit(msg)
		}
	}
}

func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.groups {
		g.Stop()
	}
	m.groups = map[string]*Group{}
	m.refreshGroupsSnapLocked()
}

// Snapshot returns a stable view of the current groups for resolvers.
func (m *Manager) Snapshot() []*Group {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Group, 0, len(m.groups))
	for _, g := range m.groups {
		out = append(out, g)
	}
	return out
}

func (m *Manager) Get(name string) *Group {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.groups[name]
}

// ByRedisDB returns the running group whose effective RedisDbNumber is db, or nil.
// If duplicates appear after start, the lowest group name wins and a warning is logged once per DB.
func (m *Manager) ByRedisDB(db int) *Group {
	snap := m.groupsSnap.Load()
	if snap == nil {
		return nil
	}
	var best *Group
	dupCount := 0
	for _, g := range *snap {
		eff := g.Config().EffectiveRedisDbNumber()
		if eff != nil && *eff == db {
			if best == nil || g.Name() < best.Name() {
				best = g
			}
			dupCount++
		}
	}
	if dupCount > 1 {
		if _, loaded := m.loggedDbDups.LoadOrStore(db, struct{}{}); !loaded {
			m.logger.Warn("duplicate RedisDbNumber mapping after start", "db", db)
		}
	}
	return best
}

// ResolveGroupForTopic returns the running group that serves a read of topic
// (an exact topic or a filter), or nil. requested is the caller's group name;
// "" and "Default" both mean "not chosen". needArchive selects history reads.
func (m *Manager) ResolveGroupForTopic(requested, topic string, needArchive bool) *Group {
	if requested != "" && requested != "Default" {
		return m.Get(requested) // explicit choice, no fallback
	}
	def := m.Get("Default")
	m.mu.RLock()
	oa := m.oa
	m.mu.RUnlock()
	if oa == nil {
		return def
	}
	t, _ := winccoanative.SplitShared(topic)
	names := winccoanative.DefaultNames
	if oa.Names != nil {
		names = oa.Names().WithDefaults()
	}
	if names.Classify(t) != winccoanative.KindNative {
		return def // #, <root>/#, topics branch, status, non-native
	}
	w := m.winccoaGroup(needArchive) // "WinCCOA" if its relevant type is WINCCOA, else the first such group by name
	switch {
	case w == nil:
		return def
	case def == nil:
		return w
	case isWinCCOA(def, needArchive): // Default itself uses WINCCOA for this read
		return def
	case !needArchive:
		return w
	case def.Archive() != nil && def.MatchesFilter(t): // S4; filter levels are compared literally
		return def
	}
	return w
}

func isWinCCOA(g *Group, needArchive bool) bool {
	if g == nil {
		return false
	}
	if needArchive {
		return g.Config().ArchiveType == stores.ArchiveWinCCOA
	}
	return g.Config().LastValType == stores.MessageStoreWinCCOA
}

func (m *Manager) winccoaGroup(needArchive bool) *Group {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if w, ok := m.groups[WinCCOAGroupName]; ok && isWinCCOA(w, needArchive) {
		return w
	}
	var best *Group
	for _, g := range m.groups {
		if isWinCCOA(g, needArchive) {
			if best == nil || g.Name() < best.Name() {
				best = g
			}
		}
	}
	return best
}
