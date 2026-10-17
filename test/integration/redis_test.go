package integration

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
)

func getFreeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startBrokerWithRedis(t *testing.T, cfgFns ...func(*config.Config)) (*broker.Server, *config.Config, int) {
	t.Helper()
	mqttPort := getFreeTCPPort(t)
	gqlPort := getFreeTCPPort(t)
	redisPort := getFreeTCPPort(t)

	cfg := config.Default()
	cfg.NodeID = fmt.Sprintf("redis-node-%d", redisPort)
	cfg.TCP.Enabled = true
	cfg.TCP.Port = mqttPort
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = true
	cfg.GraphQL.Port = gqlPort
	cfg.GraphQL.TLSPort = gqlPort + 1000
	cfg.Features.RedisServer = true
	cfg.RedisServer.Enabled = true
	cfg.RedisServer.Host = "127.0.0.1"
	cfg.RedisServer.Port = redisPort
	noAuth := false
	cfg.RedisServer.RequireAuth = &noAuth
	cfg.SQLite.Path = filepath.Join(t.TempDir(), "redis.db")

	for _, fn := range cfgFns {
		fn(cfg)
	}

	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
	if err != nil {
		t.Fatalf("broker init: %v", err)
	}
	go func() { _ = srv.Serve() }()

	addr := fmt.Sprintf("127.0.0.1:%d", redisPort)
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			lastErr = nil
			break
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("redis server did not start on %s: %v", addr, lastErr)
	}

	t.Cleanup(func() {
		_ = srv.Close()
	})

	return srv, cfg, redisPort
}

func TestRedisStartup(t *testing.T) {
	t.Run("Duplicate RedisDbNumber fails Serve", func(t *testing.T) {
		cfg := config.Default()
		cfg.Features.RedisServer = true
		cfg.RedisServer.Enabled = true
		cfg.RedisServer.Port = getFreeTCPPort(t)
		cfg.TCP.Port = getFreeTCPPort(t)
		cfg.GraphQL.Port = getFreeTCPPort(t)
		cfg.SQLite.Path = filepath.Join(t.TempDir(), "dup.db")

		srv, err := broker.New(cfg, slog.New(slog.DiscardHandler), nil)
		if err != nil {
			t.Fatalf("broker init: %v", err)
		}
		defer srv.Close()

		ctx := context.Background()
		zero := 0
		grp2 := stores.ArchiveGroupConfig{
			Name:          "Group2",
			Enabled:       true,
			TopicFilters:  []string{"grp2/#"},
			LastValType:   stores.MessageStoreMemory,
			RedisDbNumber: &zero,
		}
		if err := srv.Storage().ArchiveConfig.Save(ctx, grp2); err != nil {
			t.Fatalf("save group2: %v", err)
		}
		if err := srv.Archives().Load(ctx); err != nil {
			t.Fatalf("archives load: %v", err)
		}

		err = srv.Serve()
		if err == nil || !strings.Contains(err.Error(), "Duplicate RedisDbNumber mapping") {
			t.Fatalf("expected Duplicate RedisDbNumber mapping error, got %v", err)
		}
	})
}

func TestRedisAuthAndHello(t *testing.T) {
	srv, _, port := startBrokerWithRedis(t, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = false
		reqAuth := true
		c.RedisServer.RequireAuth = &reqAuth
	})
	_ = srv

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// GET before auth -> NOAUTH
	fmt.Fprintf(conn, "*2\r\n$3\r\nGET\r\n$1\r\na\r\n")
	line, _ := reader.ReadString('\n')
	if !strings.HasPrefix(line, "-NOAUTH") {
		t.Fatalf("expected -NOAUTH, got %q", line)
	}

	// PING before auth -> PONG
	fmt.Fprintf(conn, "*1\r\n$4\r\nPING\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, "+PONG") {
		t.Fatalf("expected +PONG, got %q", line)
	}

	// CLIENT ID before auth -> integer
	fmt.Fprintf(conn, "*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, ":") {
		t.Fatalf("expected :<id>, got %q", line)
	}

	// ECHO before auth -> NOAUTH
	fmt.Fprintf(conn, "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, "-NOAUTH") {
		t.Fatalf("expected -NOAUTH for ECHO, got %q", line)
	}

	// HELLO 3 -> NOPROTO
	fmt.Fprintf(conn, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, "-NOPROTO") {
		t.Fatalf("expected -NOPROTO, got %q", line)
	}

	// AUTH with wrong password -> WRONGPASS
	fmt.Fprintf(conn, "*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, "-WRONGPASS") {
		t.Fatalf("expected -WRONGPASS, got %q", line)
	}

	// AUTH with Admin password via fallback (default admin user has password "Admin")
	fmt.Fprintf(conn, "*2\r\n$4\r\nAUTH\r\n$5\r\nAdmin\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, "+OK") {
		t.Fatalf("expected +OK for Admin fallback auth, got %q", line)
	}

	// After auth, GET works (nil bulk for missing key)
	fmt.Fprintf(conn, "*2\r\n$3\r\nGET\r\n$1\r\na\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, "$-1") {
		t.Fatalf("expected $-1, got %q", line)
	}
}

func TestRedisStringsAndKV(t *testing.T) {
	_, _, port := startBrokerWithRedis(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", port),
	})
	defer rdb.Close()
	ctx := context.Background()

	// SET and GET
	err := rdb.Set(ctx, "sensor/temp", "23.5", 0).Err()
	if err != nil {
		t.Fatalf("SET: %v", err)
	}
	val, err := rdb.Get(ctx, "sensor/temp").Result()
	if err != nil || val != "23.5" {
		t.Fatalf("GET: val=%q, err=%v", val, err)
	}

	// GETRANGE
	part, err := rdb.GetRange(ctx, "sensor/temp", 0, 1).Result()
	if err != nil || part != "23" {
		t.Fatalf("GETRANGE: part=%q, err=%v", part, err)
	}
	partNeg, err := rdb.GetRange(ctx, "sensor/temp", -2, -1).Result()
	if err != nil || partNeg != ".5" {
		t.Fatalf("GETRANGE negative: part=%q, err=%v", partNeg, err)
	}

	// MGET
	mvals, err := rdb.MGet(ctx, "sensor/temp", "sensor/missing").Result()
	if err != nil || len(mvals) != 2 || mvals[0] != "23.5" || mvals[1] != nil {
		t.Fatalf("MGET: %v, err=%v", mvals, err)
	}

	// EXISTS
	count, err := rdb.Exists(ctx, "sensor/temp", "sensor/missing").Result()
	if err != nil || count != 1 {
		t.Fatalf("EXISTS: count=%d, err=%v", count, err)
	}

	// TTL
	ttl, err := rdb.TTL(ctx, "sensor/temp").Result()
	if err != nil || ttl != -1 {
		t.Fatalf("TTL existing: %v, err=%v", ttl, err)
	}
	ttlMiss, err := rdb.TTL(ctx, "sensor/missing").Result()
	if err != nil || ttlMiss != -2 {
		t.Fatalf("TTL missing: %v, err=%v", ttlMiss, err)
	}

	// TYPE
	typ, err := rdb.Type(ctx, "sensor/temp").Result()
	if err != nil || typ != "string" {
		t.Fatalf("TYPE string: %q, err=%v", typ, err)
	}
	typNone, err := rdb.Type(ctx, "sensor/missing").Result()
	if err != nil || typNone != "none" {
		t.Fatalf("TYPE none: %q, err=%v", typNone, err)
	}

	// DEL
	delCount, err := rdb.Del(ctx, "sensor/temp", "sensor/missing").Result()
	if err != nil || delCount != 1 {
		t.Fatalf("DEL: count=%d, err=%v", delCount, err)
	}
	// Verify deleted
	valAfter, err := rdb.Get(ctx, "sensor/temp").Result()
	if err != redis.Nil {
		t.Fatalf("GET after DEL expected Nil, got val=%q, err=%v", valAfter, err)
	}

	// Wildcards rejected in SET
	err = rdb.Set(ctx, "sensor/+/temp", "1", 0).Err()
	if err == nil || !strings.Contains(err.Error(), "ERR topic must not contain wildcard characters") {
		t.Fatalf("expected wildcard error in SET, got %v", err)
	}
}

func TestRedisReadOnlyGroup(t *testing.T) {
	srv, _, port := startBrokerWithRedis(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", port),
	})
	defer rdb.Close()
	ctx := context.Background()

	// Provision a read-only group with RedisDbNumber 2
	two := 2
	roGrp := stores.ArchiveGroupConfig{
		Name:            "ReadOnlyGroup",
		Enabled:         true,
		TopicFilters:    []string{"readonly/#"},
		LastValType:     stores.MessageStoreMemory,
		LastValReadOnly: true,
		RedisDbNumber:   &two,
	}
	if err := srv.Storage().ArchiveConfig.Save(ctx, roGrp); err != nil {
		t.Fatalf("save roGrp: %v", err)
	}
	if err := srv.Archives().Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}

	// SELECT 2
	if err := rdb.Do(ctx, "SELECT", 2).Err(); err != nil {
		t.Fatalf("SELECT 2: %v", err)
	}

	// SET should fail with read-only error (F7)
	err := rdb.Set(ctx, "readonly/k", "v", 0).Err()
	if err == nil || !strings.Contains(err.Error(), "ERR last value store of archive group [ReadOnlyGroup] is read-only") {
		t.Fatalf("expected read-only error for SET, got: %v", err)
	}

	// DEL should fail with read-only error (F7)
	err = rdb.Del(ctx, "readonly/k").Err()
	if err == nil || !strings.Contains(err.Error(), "ERR last value store of archive group [ReadOnlyGroup] is read-only") {
		t.Fatalf("expected read-only error for DEL, got: %v", err)
	}

	// PUBLISH should succeed (F7 read-only does not apply to PUBLISH)
	err = rdb.Publish(ctx, "readonly/k", "v").Err()
	if err != nil {
		t.Fatalf("PUBLISH on read-only group should succeed: %v", err)
	}
}

func TestRedisTypedValues(t *testing.T) {
	_, _, port := startBrokerWithRedis(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", port),
	})
	defer rdb.Close()
	ctx := context.Background()

	// 1. Hash commands
	hAdded, err := rdb.HSet(ctx, "myhash", "f1", "v1", "f2", "v2").Result()
	if err != nil || hAdded != 2 {
		t.Fatalf("HSET: added=%d, err=%v", hAdded, err)
	}
	hval, err := rdb.HGet(ctx, "myhash", "f1").Result()
	if err != nil || hval != "v1" {
		t.Fatalf("HGET: %q, err=%v", hval, err)
	}
	hmvals, err := rdb.HMGet(ctx, "myhash", "f1", "f2", "f3").Result()
	if err != nil || len(hmvals) != 3 || hmvals[0] != "v1" || hmvals[1] != "v2" || hmvals[2] != nil {
		t.Fatalf("HMGET: %v, err=%v", hmvals, err)
	}
	hall, err := rdb.HGetAll(ctx, "myhash").Result()
	if err != nil || hall["f1"] != "v1" || hall["f2"] != "v2" {
		t.Fatalf("HGetAll: %v, err=%v", hall, err)
	}
	hlen, err := rdb.HLen(ctx, "myhash").Result()
	if err != nil || hlen != 2 {
		t.Fatalf("HLen: %d, err=%v", hlen, err)
	}
	hexists, err := rdb.HExists(ctx, "myhash", "f1").Result()
	if err != nil || !hexists {
		t.Fatalf("HExists: %v, err=%v", hexists, err)
	}
	hkeys, err := rdb.HKeys(ctx, "myhash").Result()
	if err != nil || len(hkeys) != 2 || hkeys[0] != "f1" || hkeys[1] != "f2" {
		t.Fatalf("HKeys: %v, err=%v", hkeys, err)
	}
	hvals, err := rdb.HVals(ctx, "myhash").Result()
	if err != nil || len(hvals) != 2 {
		t.Fatalf("HVals: %v, err=%v", hvals, err)
	}
	// HSCAN with MATCH
	hscanRes, cursor, err := rdb.HScan(ctx, "myhash", 0, "f1", 0).Result()
	if err != nil || cursor != 0 || len(hscanRes) != 2 || hscanRes[0] != "f1" || hscanRes[1] != "v1" {
		t.Fatalf("HScan: res=%v, cursor=%d, err=%v", hscanRes, cursor, err)
	}
	hdel, err := rdb.HDel(ctx, "myhash", "f1").Result()
	if err != nil || hdel != 1 {
		t.Fatalf("HDel: %d, err=%v", hdel, err)
	}

	// 2. Set commands
	sadd, err := rdb.SAdd(ctx, "myset", "m1", "m2", "m3").Result()
	if err != nil || sadd != 3 {
		t.Fatalf("SAdd: %d, err=%v", sadd, err)
	}
	smembers, err := rdb.SMembers(ctx, "myset").Result()
	if err != nil || len(smembers) != 3 {
		t.Fatalf("SMembers: %v, err=%v", smembers, err)
	}
	sismember, err := rdb.SIsMember(ctx, "myset", "m2").Result()
	if err != nil || !sismember {
		t.Fatalf("SIsMember: %v, err=%v", sismember, err)
	}
	scard, err := rdb.SCard(ctx, "myset").Result()
	if err != nil || scard != 3 {
		t.Fatalf("SCard: %d, err=%v", scard, err)
	}
	srem, err := rdb.SRem(ctx, "myset", "m2").Result()
	if err != nil || srem != 1 {
		t.Fatalf("SRem: %d, err=%v", srem, err)
	}

	// 3. List commands: LPUSH k a b gives [b, a]
	lpush, err := rdb.LPush(ctx, "mylist", "a", "b").Result()
	if err != nil || lpush != 2 {
		t.Fatalf("LPush: %d, err=%v", lpush, err)
	}
	lrange, err := rdb.LRange(ctx, "mylist", 0, -1).Result()
	if err != nil || len(lrange) != 2 || lrange[0] != "b" || lrange[1] != "a" {
		t.Fatalf("LRange: %v, err=%v", lrange, err)
	}
	llen, err := rdb.LLen(ctx, "mylist").Result()
	if err != nil || llen != 2 {
		t.Fatalf("LLen: %d, err=%v", llen, err)
	}
	lindex, err := rdb.LIndex(ctx, "mylist", 1).Result()
	if err != nil || lindex != "a" {
		t.Fatalf("LIndex: %q, err=%v", lindex, err)
	}
	lpop, err := rdb.LPop(ctx, "mylist").Result()
	if err != nil || lpop != "b" {
		t.Fatalf("LPop: %q, err=%v", lpop, err)
	}

	// 4. Sorted Set (ZSET) commands
	zadd, err := rdb.ZAdd(ctx, "myzset",
		redis.Z{Score: 1.5, Member: "m1"},
		redis.Z{Score: 1.0, Member: "m2"},
	).Result()
	if err != nil || zadd != 2 {
		t.Fatalf("ZAdd: %d, err=%v", zadd, err)
	}
	zrange, err := rdb.ZRangeWithScores(ctx, "myzset", 0, -1).Result()
	if err != nil || len(zrange) != 2 {
		t.Fatalf("ZRangeWithScores: %v, err=%v", zrange, err)
	}
	if zrange[0].Member != "m2" || zrange[0].Score != 1.0 || zrange[1].Member != "m1" || zrange[1].Score != 1.5 {
		t.Fatalf("ZRange ordering or score mismatch: %v", zrange)
	}
	zscore, err := rdb.ZScore(ctx, "myzset", "m2").Result()
	if err != nil || zscore != 1.0 {
		t.Fatalf("ZScore: %v, err=%v", zscore, err)
	}
	zcard, err := rdb.ZCard(ctx, "myzset").Result()
	if err != nil || zcard != 2 {
		t.Fatalf("ZCard: %d, err=%v", zcard, err)
	}
	zrem, err := rdb.ZRem(ctx, "myzset", "m1").Result()
	if err != nil || zrem != 1 {
		t.Fatalf("ZRem: %d, err=%v", zrem, err)
	}

	// 5. JSON commands via Do
	jsetRes, err := rdb.Do(ctx, "JSON.SET", "myjson", "$", `{"num":42,"str":"hello"}`).Result()
	if err != nil || jsetRes != "OK" {
		t.Fatalf("JSON.SET: res=%v, err=%v", jsetRes, err)
	}
	jgetRes, err := rdb.Do(ctx, "JSON.GET", "myjson", "$").Result()
	if err != nil {
		t.Fatalf("JSON.GET: %v", err)
	}
	if !strings.Contains(fmt.Sprintf("%s", jgetRes), "42") {
		t.Fatalf("JSON.GET mismatch: %v", jgetRes)
	}
	jtypeRes, err := rdb.Do(ctx, "JSON.TYPE", "myjson", "$").Result()
	if err != nil || jtypeRes != "object" {
		t.Fatalf("JSON.TYPE: %v, err=%v", jtypeRes, err)
	}
	jdelRes, err := rdb.Do(ctx, "JSON.DEL", "myjson", "$").Result()
	if err != nil || jdelRes != int64(1) {
		t.Fatalf("JSON.DEL: %v, err=%v", jdelRes, err)
	}

	// 6. WRONGTYPE test: HGET on a string key
	_ = rdb.Set(ctx, "stringkey", "simple", 0).Err()
	err = rdb.HGet(ctx, "stringkey", "f").Err()
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("expected WRONGTYPE, got: %v", err)
	}
}

func TestRedisPipeliningAndRace(t *testing.T) {
	_, _, port := startBrokerWithRedis(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", port),
	})
	defer rdb.Close()
	ctx := context.Background()

	// 1000 pipelined commands
	pipe := rdb.Pipeline()
	var cmds []*redis.StatusCmd
	for i := 0; i < 1000; i++ {
		cmd := pipe.Set(ctx, fmt.Sprintf("pipe/key%d", i), fmt.Sprintf("val%d", i), 0)
		cmds = append(cmds, cmd)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("pipeline exec: %v", err)
	}
	for i, cmd := range cmds {
		if cmd.Val() != "OK" {
			t.Fatalf("cmd %d failed: %v", i, cmd.Err())
		}
	}

	// Concurrent HSET on single key under race (F6)
	var wg sync.WaitGroup
	for c := 0; c < 20; c++ {
		wg.Add(1)
		go func(connIdx int) {
			defer wg.Done()
			client := redis.NewClient(&redis.Options{
				Addr: fmt.Sprintf("127.0.0.1:%d", port),
			})
			defer client.Close()
			for i := 0; i < 50; i++ {
				field := fmt.Sprintf("field-%d-%d", connIdx, i)
				_ = client.HSet(ctx, "concurrent-hash", field, "val").Err()
			}
		}(c)
	}
	wg.Wait()

	hlen, err := rdb.HLen(ctx, "concurrent-hash").Result()
	if err != nil || hlen != 1000 {
		t.Fatalf("HLEN concurrent: expected 1000, got %d (err: %v)", hlen, err)
	}
}

func TestRedisPatternsAndScan(t *testing.T) {
	_, _, port := startBrokerWithRedis(t, func(c *config.Config) {
		limit := 10
		c.RedisServer.KeysLimit = &limit
	})
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", port),
	})
	defer rdb.Close()
	ctx := context.Background()

	// Populate keys
	_ = rdb.Set(ctx, "a", "1", 0).Err()
	_ = rdb.Set(ctx, "a/b1", "1", 0).Err()
	_ = rdb.Set(ctx, "a/b2", "1", 0).Err()
	_ = rdb.Set(ctx, "a/b1/c", "1", 0).Err()
	_ = rdb.Set(ctx, "sensor/temp/1", "1", 0).Err()
	_ = rdb.Set(ctx, "sensor/press/1", "1", 0).Err()

	// KEYS a/* should not include "a" (F1)
	keys, err := rdb.Keys(ctx, "a/*").Result()
	if err != nil {
		t.Fatalf("KEYS a/*: %v", err)
	}
	for _, k := range keys {
		if k == "a" {
			t.Fatalf("KEYS a/* included 'a': %v", keys)
		}
	}

	// SCAN cursor 0 with COUNT (F2: hint, cursor returns 0)
	scanKeys, cursor, err := rdb.Scan(ctx, 0, "sensor*", 5).Result()
	if err != nil || cursor != 0 {
		t.Fatalf("SCAN sensor*: keys=%v, cursor=%d, err=%v", scanKeys, cursor, err)
	}
	if len(scanKeys) != 2 {
		t.Fatalf("expected 2 sensor keys, got: %v", scanKeys)
	}
}

func TestRedisPubSub(t *testing.T) {
	_, _, port := startBrokerWithRedis(t)
	rdbSub := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", port),
	})
	defer rdbSub.Close()

	rdbPub := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", port),
	})
	defer rdbPub.Close()

	ctx := context.Background()

	// SUBSCRIBE
	pubsub := rdbSub.Subscribe(ctx, "chat/room1")
	defer pubsub.Close()

	// Wait for subscription confirmation
	_, err := pubsub.Receive(ctx)
	if err != nil {
		t.Fatalf("receive subscribe conf: %v", err)
	}

	// PUBLISH
	count, err := rdbPub.Publish(ctx, "chat/room1", "hello world").Result()
	if err != nil || count != 0 {
		t.Fatalf("PUBLISH: count=%d, err=%v", count, err)
	}

	// Receive message
	msgCh := pubsub.Channel()
	select {
	case msg := <-msgCh:
		if msg.Payload != "hello world" || msg.Channel != "chat/room1" {
			t.Fatalf("received message mismatch: %v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pubsub message")
	}

	// PSUBSCRIBE
	pPubsub := rdbSub.PSubscribe(ctx, "sensor/*")
	defer pPubsub.Close()
	_, err = pPubsub.Receive(ctx)
	if err != nil {
		t.Fatalf("receive psubscribe conf: %v", err)
	}

	_ = rdbPub.Publish(ctx, "sensor/temp", "42").Err()
	pMsgCh := pPubsub.Channel()
	select {
	case msg := <-pMsgCh:
		if msg.Payload != "42" || msg.Channel != "sensor/temp" || msg.Pattern != "sensor/*" {
			t.Fatalf("pmessage mismatch: %v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pmessage")
	}
}

func TestRedisSessionAndSelect(t *testing.T) {
	srv, _, port := startBrokerWithRedis(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Query CLIENT ID
	fmt.Fprintf(conn, "*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
	line, _ := reader.ReadString('\n')
	var id int64
	fmt.Sscanf(line, ":%d", &id)
	clientID := fmt.Sprintf("redis-%d", id)

	ctx := context.Background()
	time.Sleep(50 * time.Millisecond)

	sess, err := srv.Storage().Sessions.GetSession(ctx, clientID)
	if err != nil || sess == nil {
		t.Fatalf("GetSession %s: %v", clientID, err)
	}
	if !strings.Contains(sess.Information, `"ProtocolVersion":"REDIS"`) {
		t.Fatalf("session information missing REDIS: %s", sess.Information)
	}

	// CLIENT SETNAME my-client
	fmt.Fprintf(conn, "*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$9\r\nmy-client\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, "+OK") {
		t.Fatalf("CLIENT SETNAME: %q", line)
	}

	// Verify session info updated with ClientName
	sess, _ = srv.Storage().Sessions.GetSession(ctx, clientID)
	if !strings.Contains(sess.Information, `"ClientName":"my-client"`) {
		t.Fatalf("session information missing ClientName: %s", sess.Information)
	}

	// SELECT 0 (Default group)
	fmt.Fprintf(conn, "*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, "+OK") {
		t.Fatalf("SELECT 0: %q", line)
	}

	// SELECT 99 (unmapped DB)
	fmt.Fprintf(conn, "*2\r\n$6\r\nSELECT\r\n$2\r\n99\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, "-ERR DB index is not mapped to an archive group") {
		t.Fatalf("expected unmapped error for SELECT 99, got: %q", line)
	}

	// SELECT abc (invalid DB index)
	fmt.Fprintf(conn, "*2\r\n$6\r\nSELECT\r\n$3\r\nabc\r\n")
	line, _ = reader.ReadString('\n')
	if !strings.HasPrefix(line, "-ERR invalid DB index") {
		t.Fatalf("expected invalid DB index error, got: %q", line)
	}

	// ProtocolSessions.Close closes TCP connection
	if protoSessions := srv.ProtocolSessions(); protoSessions != nil {
		protoSessions.Close(clientID)
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, err := reader.ReadByte()
		if err == nil {
			t.Fatal("expected connection to be closed by ProtocolSessions.Close")
		}
	}
}

func TestRedisPubSubUnsubscribe(t *testing.T) {
	_, _, port := startBrokerWithRedis(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Subscribe to ch1 and ch2
	fmt.Fprintf(conn, "*3\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n$3\r\nch2\r\n")
	// Read ch1 conf
	line, _ := reader.ReadString('\n') // *3
	line, _ = reader.ReadString('\n')  // $9
	line, _ = reader.ReadString('\n')  // subscribe
	line, _ = reader.ReadString('\n')  // $3
	line, _ = reader.ReadString('\n')  // ch1
	line, _ = reader.ReadString('\n')  // :1
	if !strings.HasPrefix(line, ":1") {
		t.Fatalf("expected :1, got %q", line)
	}
	// Read ch2 conf
	line, _ = reader.ReadString('\n') // *3
	line, _ = reader.ReadString('\n') // $9
	line, _ = reader.ReadString('\n') // subscribe
	line, _ = reader.ReadString('\n') // $3
	line, _ = reader.ReadString('\n') // ch2
	line, _ = reader.ReadString('\n') // :2
	if !strings.HasPrefix(line, ":2") {
		t.Fatalf("expected :2, got %q", line)
	}

	// Duplicate subscribe to ch1 (F4: count unchanged)
	fmt.Fprintf(conn, "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n")
	line, _ = reader.ReadString('\n') // *3
	line, _ = reader.ReadString('\n') // $9
	line, _ = reader.ReadString('\n') // subscribe
	line, _ = reader.ReadString('\n') // $3
	line, _ = reader.ReadString('\n') // ch1
	line, _ = reader.ReadString('\n') // :2 (unchanged!)
	if !strings.HasPrefix(line, ":2") {
		t.Fatalf("expected unchanged count :2 on duplicate subscribe, got %q", line)
	}

	// UNSUBSCRIBE ch1 (F3: honors argument and reports remaining total :1)
	fmt.Fprintf(conn, "*2\r\n$11\r\nUNSUBSCRIBE\r\n$3\r\nch1\r\n")
	line, _ = reader.ReadString('\n') // *3
	line, _ = reader.ReadString('\n') // $11
	line, _ = reader.ReadString('\n') // unsubscribe
	line, _ = reader.ReadString('\n') // $3
	line, _ = reader.ReadString('\n') // ch1
	line, _ = reader.ReadString('\n') // :1
	if !strings.HasPrefix(line, ":1") {
		t.Fatalf("expected :1 remaining count, got %q", line)
	}

	// UNSUBSCRIBE with no args (removes remaining ch2, returns :0)
	fmt.Fprintf(conn, "*1\r\n$11\r\nUNSUBSCRIBE\r\n")
	line, _ = reader.ReadString('\n') // *3
	line, _ = reader.ReadString('\n') // $11
	line, _ = reader.ReadString('\n') // unsubscribe
	line, _ = reader.ReadString('\n') // $3
	line, _ = reader.ReadString('\n') // ch2
	line, _ = reader.ReadString('\n') // :0
	if !strings.HasPrefix(line, ":0") {
		t.Fatalf("expected :0 remaining count, got %q", line)
	}
}

func TestRedisWinCCOA(t *testing.T) {
	sim, client := newSim(0)
	t.Cleanup(sim.Close)

	mqttPort := getFreeTCPPort(t)
	redisPort := getFreeTCPPort(t)

	env := startNative(t, mqttPort, filepath.Join(t.TempDir(), "oa_redis.db"), sim, client, func(c *config.Config) {
		c.Features.RedisServer = true
		c.RedisServer.Enabled = true
		c.RedisServer.Host = "127.0.0.1"
		c.RedisServer.Port = redisPort
		noAuth := false
		c.RedisServer.RequireAuth = &noAuth
	}, broker.Options{})
	t.Cleanup(func() { env.srv.Close() })

	addr := fmt.Sprintf("127.0.0.1:%d", redisPort)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	defer rdb.Close()
	ctx := context.Background()

	// E4: typed writes on native topic rejected: HSET
	err := rdb.HSet(ctx, "winccoa/tags/Pump1/speed", "f", "v").Err()
	if err == nil || !strings.Contains(err.Error(), "ERR 'hset' is not supported on WinCC OA topics, use SET or MSET") {
		t.Fatalf("expected E4 rejection for HSET on WinCC OA topic, got: %v", err)
	}

	// E4: DEL on native topic rejected
	err = rdb.Del(ctx, "winccoa/tags/Pump1/speed").Err()
	if err == nil || !strings.Contains(err.Error(), "ERR 'del' is not supported on WinCC OA topics, use SET or MSET") {
		t.Fatalf("expected E4 rejection for DEL on WinCC OA topic, got: %v", err)
	}

	// E3: SET on writable native value topic takes S2 path
	err = rdb.Set(ctx, "winccoa/tags/Pump1/speed", "1500", 0).Err()
	if err != nil {
		t.Fatalf("SET on WinCC OA tag topic: %v", err)
	}
}
