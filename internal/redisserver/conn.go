package redisserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"monstermq.io/edge/internal/stores"
)

type subKey struct {
	isPattern bool
	name      string
}

type pubSubDelivery struct {
	holder    *subscriptionHolder
	isPattern bool
	pattern   string
	msg       stores.BrokerMessage
}

type Conn struct {
	server            *Server
	id                int64
	clientID          string
	publisherID       string
	rwc               net.Conn
	reader            *bufio.Reader
	writer            *bufio.Writer
	writeMu           sync.Mutex
	authenticated     bool
	username          string
	clientName        string
	selectedDb        int
	sessionRegistered bool
	subs              map[subKey]*subscriptionHolder
	subsMu            sync.Mutex
	subCh             chan pubSubDelivery
	subWriterOnce     sync.Once
	subStopCh         chan struct{}
	subOverloaded     atomic.Bool
	subDropped        atomic.Int64
	lastWriteDeadline int64
	subSeq            atomic.Int64
	closed            atomic.Bool
	closeOnce         sync.Once
}

type subscriptionHolder struct {
	id     uint64
	active atomic.Bool
	cancel func()
}

func newConn(server *Server, id int64, rwc net.Conn) *Conn {
	c := &Conn{
		server:      server,
		id:          id,
		clientID:    fmt.Sprintf("redis-%d", id),
		publisherID: fmt.Sprintf("redis-server-%d", id),
		rwc:         rwc,
		reader:      bufio.NewReaderSize(rwc, 65536),
		writer:      bufio.NewWriterSize(rwc, 65536),
		selectedDb:  server.deps.Cfg.RedisServer.DefaultDb,
		subs:        make(map[subKey]*subscriptionHolder),
		subStopCh:   make(chan struct{}),
	}

	requireAuth := server.deps.Cfg.RedisServer.EffectiveRequireAuth()
	userMgmtOn := server.deps.Cfg.UserManagement.Enabled
	c.authenticated = !requireAuth || !userMgmtOn

	return c
}

func (c *Conn) serve() {
	defer c.Close()

	if c.authenticated {
		c.registerSession()
	}

	maxMsgSize := c.server.deps.Cfg.MaxMessageSize

	for {
		if c.closed.Load() {
			return
		}

		args, err := readCommand(c.reader, maxMsgSize)
		if err != nil {
			var pe *protoError
			if errors.As(err, &pe) {
				c.writeMu.Lock()
				_ = c.writeError(pe.Error())
				_ = c.flush()
				c.writeMu.Unlock()
			}
			return
		}

		if err := c.dispatch(args); err != nil {
			return
		}

		c.writeMu.Lock()
		if c.reader.Buffered() == 0 || c.writer.Buffered() >= 65536 {
			if err := c.flush(); err != nil {
				c.writeMu.Unlock()
				return
			}
		}
		c.writeMu.Unlock()
	}
}

func (c *Conn) dispatch(args [][]byte) error {
	if len(args) == 0 {
		return c.writeError("ERR empty command")
	}

	cmd := strings.ToUpper(string(args[0]))

	allowedBeforeAuth := map[string]bool{
		"AUTH":   true,
		"HELLO":  true,
		"PING":   true,
		"QUIT":   true,
		"CLIENT": true,
	}

	if !c.authenticated && !allowedBeforeAuth[cmd] {
		return c.writeError("NOAUTH Authentication required.")
	}

	switch cmd {
	case "AUTH":
		return c.handleAuth(args)
	case "HELLO":
		return c.handleHello(args)
	case "PING":
		return c.handlePing(args)
	case "ECHO":
		return c.handleEcho(args)
	case "QUIT":
		return c.handleQuit(args)
	case "SELECT":
		return c.handleSelect(args)
	case "CLIENT":
		return c.handleClient(args)
	case "COMMAND":
		return c.handleCommand(args)
	case "INFO":
		return c.handleInfo(args)
	case "GET":
		return c.handleGet(args)
	case "GETRANGE", "SUBSTR":
		return c.handleGetRange(args)
	case "MGET":
		return c.handleMget(args)
	case "SET":
		return c.handleSet(args)
	case "MSET":
		return c.handleMset(args)
	case "HSET":
		return c.handleHset(args)
	case "HGET":
		return c.handleHget(args)
	case "HMGET":
		return c.handleHmget(args)
	case "HGETALL":
		return c.handleHgetall(args)
	case "HDEL":
		return c.handleHdel(args)
	case "HEXISTS":
		return c.handleHexists(args)
	case "HLEN":
		return c.handleHlen(args)
	case "HKEYS":
		return c.handleHkeys(args)
	case "HVALS":
		return c.handleHvals(args)
	case "HSCAN":
		return c.handleHscan(args)
	case "SADD":
		return c.handleSadd(args)
	case "SREM":
		return c.handleSrem(args)
	case "SMEMBERS":
		return c.handleSmembers(args)
	case "SISMEMBER":
		return c.handleSismember(args)
	case "SCARD":
		return c.handleScard(args)
	case "LPUSH":
		return c.handleListPush(args, true)
	case "RPUSH":
		return c.handleListPush(args, false)
	case "LPOP":
		return c.handleListPop(args, true)
	case "RPOP":
		return c.handleListPop(args, false)
	case "LRANGE":
		return c.handleLrange(args)
	case "LLEN":
		return c.handleLlen(args)
	case "LINDEX":
		return c.handleLindex(args)
	case "ZADD":
		return c.handleZadd(args)
	case "ZRANGE":
		return c.handleZrange(args)
	case "ZREM":
		return c.handleZrem(args)
	case "ZSCORE":
		return c.handleZscore(args)
	case "ZCARD":
		return c.handleZcard(args)
	case "JSON.SET":
		return c.handleJsonSet(args)
	case "JSON.GET":
		return c.handleJsonGet(args)
	case "JSON.DEL":
		return c.handleJsonDel(args)
	case "JSON.TYPE":
		return c.handleJsonType(args)
	case "DEL":
		return c.handleDel(args)
	case "EXISTS":
		return c.handleExists(args)
	case "TTL", "PTTL":
		return c.handleTtl(args)
	case "SCAN":
		return c.handleScan(args)
	case "KEYS":
		return c.handleKeys(args)
	case "PUBLISH":
		return c.handlePublish(args)
	case "SUBSCRIBE":
		return c.handleSubscribe(args, false)
	case "PSUBSCRIBE":
		return c.handleSubscribe(args, true)
	case "UNSUBSCRIBE":
		return c.handleUnsubscribe(args, false)
	case "PUNSUBSCRIBE":
		return c.handleUnsubscribe(args, true)
	case "TYPE":
		return c.handleType(args)
	default:
		return c.writeError(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}

func (c *Conn) registerSession() {
	if c.sessionRegistered {
		return
	}
	c.sessionRegistered = true

	info := map[string]any{
		"RemoteAddress":   c.rwc.RemoteAddr().String(),
		"LocalAddress":    c.rwc.LocalAddr().String(),
		"ProtocolVersion": "REDIS",
		"SSL":             false,
		"SelectedDb":      c.selectedDb,
	}
	if c.username != "" {
		info["User"] = c.username
	}
	if c.clientName != "" {
		info["ClientName"] = c.clientName
	}
	rawInfo, _ := json.Marshal(info)

	if c.server.deps.Sessions != nil {
		client := stores.SessionInfo{
			ClientID:      c.clientID,
			NodeID:        c.server.deps.Cfg.NodeID,
			CleanSession:  true,
			Connected:     true,
			UpdateTime:    time.Now(),
			ClientAddress: c.rwc.RemoteAddr().String(),
			Information:   string(rawInfo),
		}
		_ = c.server.deps.Sessions.SetClient(context.Background(), client)
		_ = c.server.deps.Sessions.SetConnected(context.Background(), c.clientID, true)
	}

	if c.server.deps.Registry != nil {
		c.server.deps.Registry.Register(c.clientID, c.Close)
	}
}

func (c *Conn) updateSessionInfo() {
	if !c.sessionRegistered {
		return
	}
	info := map[string]any{
		"RemoteAddress":   c.rwc.RemoteAddr().String(),
		"LocalAddress":    c.rwc.LocalAddr().String(),
		"ProtocolVersion": "REDIS",
		"SSL":             false,
		"SelectedDb":      c.selectedDb,
	}
	if c.username != "" {
		info["User"] = c.username
	}
	if c.clientName != "" {
		info["ClientName"] = c.clientName
	}
	rawInfo, _ := json.Marshal(info)

	if c.server.deps.Sessions != nil {
		client := stores.SessionInfo{
			ClientID:      c.clientID,
			NodeID:        c.server.deps.Cfg.NodeID,
			CleanSession:  true,
			Connected:     true,
			UpdateTime:    time.Now(),
			ClientAddress: c.rwc.RemoteAddr().String(),
			Information:   string(rawInfo),
		}
		_ = c.server.deps.Sessions.SetClient(context.Background(), client)
	}
}

func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.subStopCh)

		c.subsMu.Lock()
		for _, holder := range c.subs {
			holder.cancel()
		}
		c.subs = make(map[subKey]*subscriptionHolder)
		c.subsMu.Unlock()

		if c.sessionRegistered {
			if c.server.deps.Sessions != nil {
				_ = c.server.deps.Sessions.DelClient(context.Background(), c.clientID)
			}
		}

		if c.server.deps.Registry != nil {
			c.server.deps.Registry.Unregister(c.clientID)
		}

		if c.server.deps.Release != nil {
			c.server.deps.Release(c.publisherID)
		}

		_ = c.rwc.Close()
		c.server.removeConn(c)
	})
}

func (c *Conn) flush() error {
	now := time.Now().UnixNano()
	if now-c.lastWriteDeadline >= int64(time.Second) {
		c.lastWriteDeadline = now
		_ = c.rwc.SetWriteDeadline(time.Unix(0, now+int64(10*time.Second)))
	}
	return c.writer.Flush()
}

func (c *Conn) flushIfNeeded() {
	if c.writer.Buffered() >= 65536 {
		_ = c.flush()
	}
}

func (c *Conn) writeSimple(s string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return respSimple(s).writeResp(c.writer)
}

func (c *Conn) writeError(s string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return respErr(s).writeResp(c.writer)
}

func (c *Conn) writeInteger(n int64) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return respInteger(n).writeResp(c.writer)
}

func (c *Conn) writeBulk(b []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return respBulk(b).writeResp(c.writer)
}

func (c *Conn) writeBulkStr(s string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return respBulk([]byte(s)).writeResp(c.writer)
}

func (c *Conn) writeNullBulk() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return respBulk(nil).writeResp(c.writer)
}

func (c *Conn) writeArray(items []respValue) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return respArray(items).writeResp(c.writer)
}

func (c *Conn) canRead(topic string) bool {
	if c.server.deps.Auth == nil || !c.server.deps.Cfg.UserManagement.Enabled {
		return true
	}
	user := c.username
	if user == "" {
		user = "Anonymous"
	}
	if !c.server.deps.Auth.AllowClient(user, c.publisherID, topic, false) {
		return false
	}
	if c.server.deps.NativeNames != nil {
		names := c.server.deps.NativeNames()
		if canonical, ok := names.CanonicalOf(topic); ok && canonical != topic {
			return c.server.deps.Auth.AllowClient(user, c.publisherID, canonical, false)
		}
	}
	return true
}

func (c *Conn) canWrite(topic string) bool {
	if c.server.deps.Auth == nil || !c.server.deps.Cfg.UserManagement.Enabled {
		return true
	}
	user := c.username
	if user == "" {
		user = "Anonymous"
	}
	if !c.server.deps.Auth.AllowClient(user, c.publisherID, topic, true) {
		return false
	}
	if c.server.deps.NativeNames != nil {
		names := c.server.deps.NativeNames()
		if canonical, ok := names.CanonicalOf(topic); ok && canonical != topic {
			return c.server.deps.Auth.AllowClient(user, c.publisherID, canonical, true)
		}
	}
	return true
}
