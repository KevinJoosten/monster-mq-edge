# Plan: shared building blocks for the NATS, Redis and Prometheus servers and the WinCC OA archive

**Status:** approved (2026-10-17). The owner signed off on the decisions below.
**Issue:** [vogler75/monster-mq-edge#34](https://github.com/vogler75/monster-mq-edge/issues/34).
**Scope:** the decisions D1-D6 and the edge building blocks S1-S7 that the four plans share. Each plan
refers here instead of repeating them:

- [plan-winccoa-archive.md](plan-winccoa-archive.md)
- [plan-nats-server.md](plan-nats-server.md)
- [plan-redis-server.md](plan-redis-server.md)
- [plan-prometheus-server.md](plan-prometheus-server.md)

## Owner decisions (2026-10-17)

| # | Decision | Effect |
|---|---|---|
| D1 | **Sign-off: the NATS server and the Redis server may be added to edge.** | This overrides the "off-limits" note in `AGENTS.md:21-28`. The NATS and Redis plans each update AGENTS.md (subsystem list, repository layout). Only the *servers* are approved. The NATS and Redis *client bridges* stay off-limits. |
| D2 | **GraphQL: `WINCCOA` is added to `MessageStoreType` and `MessageArchiveType` in both repos, in lockstep.** | Main gets the SDL values and the Kotlin enum values (vogler75/monster-mq#205). Main loads such a group without crashing and reports a clear deploy error, because only edge embedded in `WCCOAmmq` can run it. Details: archive plan. |
| D3 | **Bugs found in main are fixed in edge and in main, not copied.** | The main-side fixes are GitHub issues in vogler75/monster-mq: monster-mq#202 (NATS), monster-mq#203 (Redis), monster-mq#204 (Prometheus), monster-mq#206 (PeerLink interest) and monster-mq#207 (SQL aggregation). Each plan's "Main-side tasks" section links to them. Edge implements the corrected behaviour and the main change lands in the same release, so both brokers answer the same again. |
| D4 | **NATS KV put and Redis `SET`/`MSET` may write WinCC OA tag values** [edge-only]. | They go through the native value write rule (S2), which is a deliberate edge-only extension. Plain NATS `PUB` and Redis `PUBLISH` keep MQTT semantics: they publish the topic as given. |
| D5 | **Prometheus reads check topic ACLs, in both repos** [fix: both]. | Non-admin users only see topics they may read, in scrape, label values, series and queries. Broker metrics are not topic data and stay unchecked. Details: Prometheus plan. |
| D6 | **Redis `SET`/`MSET` on a read-only last-value store are rejected, in both repos** [fix: both]. | They fail with main's NATS KV text (`last value store of archive group [<name>] is read-only`). `PUBLISH` still publishes. Details: Redis plan F7. |

## Conventions for the four plans

- **Deviations are explicit.** Each one is marked inline and listed again in one table per plan:
  - **[edge-only]**: behaviour main does not have.
  - **[fix: both]**: a main bug that both repos fix (D3).
  - **[fix: edge]**: an edge bug whose fix brings edge back in line with main.

  Anything unmarked is a 1:1 port of main and cites the Kotlin file and line.
- **Config checklist** (AGENTS.md:143-145). Every new key goes into all four of:
  - `internal/config/config.go` (struct, defaults, validation)
  - `yaml-json-schema.json`. The root object and `Features` have `additionalProperties:false`, so
    `test/integration/config_schema_test.go` fails without the schema entry.
  - `config.yaml.example`
  - `scripts/deb/config.yaml`, only if the key matters for the default deployment
- **Tests live in `test/integration/`** and drive the broker through its real listeners
  (AGENTS.md:146-148). WinCC OA cases use the existing simhost harness, the way
  `winccoa_native_*_test.go` does. Client libraries (`nats.go`, `go-redis`) are test-only imports.
- **Docs.** Add a README feature line plus a `doc/<feature>.md` page that mirrors main's page where one
  exists.

## Shared building blocks

These land with the first plan that needs them; they are listed with their first user. Names are
binding, so all plans use the same ones.

### S1. Protocol publish path (`internal/broker/protocol_publish.go`), first used by NATS

Today's `publishFn` (`internal/broker/server.go:562-575`) is
`func(topic string, payload []byte, retain bool, qos byte) error` and ends in `server.Publish`. It loses
four things a protocol session needs:

- the client ID (`nats-<uuid>`, `redis-server-<n>`)
- the response topic and correlation data (NATS reply-to)
- user properties (NATS headers)
- the message time (NATS KV revision)

New:

```go
// ProtocolPublish publishes msg on behalf of a protocol session. The caller
// has already checked the ACL (inline clients bypass it) and sets msg.Time.
type ProtocolPublish func(clientID string, msg stores.BrokerMessage) error
```

It tries these in order, and the first that handles the message wins:

1. `unsHook.InlinePublish(msg.TopicName, msg.Payload)`. UNS Modeler `.../set` topics; unchanged.
2. `nativeHook.InlinePublishMessage(clientID, msg)`. New. It works like `InlinePublish`
   (`hook_winccoa.go:129`), but it carries `clientID`, retain, QoS, `ResponseTopic` and
   `CorrelationData` into the packet. That way:
   - native command replies (`hook_winccoa.go:157`) reach a `nats request` caller;
   - the `NoSource` echo policy sees the real publisher.
3. `InlinePublisher.PublishMessage(clientID, msg)` (`inline_publisher.go:44`). It keeps the time, user
   properties and response topic, so storage hooks, archives and bus subscribers see the session as the
   publisher.

The function returns the hook's error unchanged (a `packets.Code` with `Reason`), so each server can map
it to its own error reply.

`InlinePublisher` gains `Release(clientID string)`. Servers call it when a connection closes, because
today its `clients` map (`inline_publisher.go:56-66`) keeps one engine client per ID forever.

### S2. Native value write rule (`internal/broker/protocol_publish.go`), first used by NATS KV [edge-only, D4]

```go
// NativeValueWrite maps a write to a WinCC OA tag value topic onto its command
// topic. ok is false when topic is not a writable native value topic.
func NativeValueWrite(n winccoanative.Names, topic string) (setTopic string, ok bool)
```

`ok` is true only when all of these hold:

- the native namespace is on;
- `n.Classify(topic) == KindNative`;
- `n.Parse(topic)` gives a target that is not a command, not `Protected`, and has no explicit
  `_online.._<attr>` attribute.

The result is `topic + "/" + winccoanative.SegSet`.

Callers: NATS KV put and Redis `SET`/`MSET`. The rule applies whatever bucket or DB is selected, because
a plain publish to a native value topic is always denied (`winccoanative/service.go:1075-1077`).

For a rewritten write:

- **ACL.** The ACL is checked on `setTopic`, the topic that is actually published, as for an MQTT client.
- **Group checks are skipped.** The read-only flags and the topic-filter check of the target group are
  not applied. The write goes to WinCC OA, not into a MonsterMQ store.
- **Retain is forced to false.** Native commands reject retain (`service.go:1078-1079`).
- **Publish and errors.** The write is published through S1. The command verdict (unknown DPE, type
  mismatch, read-only element) comes back as the error.

### S3. Bus subscriptions for protocol sessions, first used by NATS

NATS `SUB`, NATS KV watch consumers and Redis `SUBSCRIBE`/`PSUBSCRIBE` use `pubsub.Bus`. Unlike inline
MQTT subscriptions, the bus registers WinCC OA native interest (`native_bus.go`). Rules:

1. **Validate first.** Call `bus.Validate(filters)` before subscribing (`bus.go:98`), as GraphQL
   (`resolver.go:1596`) and SSE (`sse.go:31`) do. Otherwise an invalid native filter subscribes and
   silently receives nothing (`native_bus.go:56`). Each plan defines the protocol error to return.
2. **Detect overflow.** Use `bus.SubscribeWithOverflow(filters, 1024)`, not `Subscribe`. The bus drops
   messages for a full subscriber (`bus.go:117-121`, `178-183`). On overflow the server logs, sends its
   slow-consumer error and closes the connection, so loss is never silent. Main never drops (its
   listeners are unbounded); closing is the bounded-memory equivalent and is listed as [edge-only] in
   each plan.
3. **The initial burst counts.** The interest hook runs synchronously inside `subscribe` and can
   `Deliver` the current values of a whole DP tree before the caller reads the channel
   (`bus.go:79-86`, `native_bus.go:73-85`). Start the reader goroutine before anything else blocks, and
   size the buffer for it (1024).
4. **Clean up.** Call `bus.Unsubscribe(id)` on UNSUB/UNSUBSCRIBE and on connection close. It removes the
   WinCC OA `dpConnect`/`dpQueryConnectSingle` interest when the last subscriber of a filter leaves.

### S4. Topic-filter check (`internal/archive/group.go`), first used by NATS KV

```go
// MatchesFilter reports whether topic matches the group's topic filters.
// Unlike Matches it ignores the read-only and retained-only flags.
func (g *Group) MatchesFilter(topic string) bool
```

Main checks only the filter tree (`group.filterTree.isTopicNameMatching`). `Matches` (`group.go:132-150`)
also returns false for retained-only groups and for groups with both stores read-only, so it must not
be used as a filter check. NATS KV and Redis use `MatchesFilter` and check the read-only flags themselves,
with main's error replies.

### S5. Protocol session registry and dashboard removal, first used by NATS

Protocol sessions are stored with `SessionStore.SetClient` / `DelClient`, with the `Information` JSON
that main writes (`RemoteAddress`, `LocalAddress`, `ProtocolVersion` = `"NATS"` / `"REDIS"`, `SSL`, plus
protocol extras).

- **Disconnect from the dashboard.** Today `Resolver.RemoveSessions` (`resolver.go:2484-2501`) only
  disconnects MQTT engine clients. New `internal/broker/protosessions.go`:

  ```go
  type ProtocolSessions struct{ /* clientID -> close func */ }
  func (p *ProtocolSessions) Register(clientID string, closeFn func())
  func (p *ProtocolSessions) Unregister(clientID string)
  func (p *ProtocolSessions) Close(clientID string) bool
  ```

  `RemoveSessions` calls `Close` first. The server's close path deletes the session record and releases
  the S1 inline client.
- **Protocol badge.** The dashboard picks the badge from the client ID prefix (`nats-`, `redis-`;
  `dashboard/src/js/sessions.js:631-634`), not from `protocolVersion`. Main returns null
  `protocolVersion`, `receiveMaximum`, `maximumPacketSize` and `topicAliasMaximum` for these sessions
  (`MetricsResolver.kt:141`, `:268-270`). Edge's `sessionToGraphQL` (`resolver.go:838-865`) does the same for
  these prefixes when `ProtocolVersion` is 0. This is a resolver change only; GraphQL fields do not change.
  The NATS plan implements it.
- **Message counters [fix: both].** Inline publishes count toward `messages_in` but not per client
  (`hook_storage.go:343-347`). Protocol servers call `collector.IncClientIn(clientID)` per accepted publish
  and `IncClientOut(clientID)` per delivered message (the collector may be nil). Main's NATS and Redis
  sessions count nothing. That is a main bug, so main gets the same fix.

### S7. Delivery-time read ACL for protocol subscriptions [fix: both], first used by NATS

Main's ACL admits a wildcard subscription that only partly overlaps a deny rule. It relies on a re-check of
the concrete topic at delivery (`AclCache.kt:194-195`), which MQTT does (`MqttClient.kt:1328-1337`). NATS
and Redis never re-check, so main leaks denied topics to their wildcard subscribers. Edge must not copy that
leak. Edge's `AclCheckOnSubscription: false` admits wildcard filters without any rule check
(`auth.go:231-235`, `:311-316`), so the leak would be even wider there.

Rule for both repos:

- **When it applies.** User management is on and the session user is not an admin.
- **Which messages.** Every message delivered to a protocol subscription:
  - NATS `SUB`
  - NATS KV consumer snapshot and live entries
  - Redis `SUBSCRIBE`/`PSUBSCRIBE`
- **The check.** The read ACL on the message's concrete topic, with the same function and client ID the
  server uses for its subscribe check:
  - NATS: `Allow(user, topic, false)`. Main passes no client ID (`NatsClient.kt:336-338`).
  - Redis: `AllowClient(user, "redis-server-<n>", topic, false)` (`RedisServer.kt:1327`).

  Both add the canonical alias rule, as `hook_auth.go:184-192` does. A denied message is dropped silently and
  is not counted as out (S5).

### S6. Bounded WinCC OA reads (archive plan)

Broad filters (`#`, `<root>/#`) on a `WINCCOA` last-value store become a full-project `dpQuery`. Many
callers send them:

- GraphQL `searchTopics` and `browseTopics`
- NATS KV `STREAM.INFO`/`LIST` and `keys`
- Redis `KEYS`/`SCAN`
- Prometheus `/series`

The archive plan therefore puts a row limit into the host operation itself (`DP_QUERY` with `TagCount`),
not only into the Go yield loop. That limit is **"the S6 row limit"**: config key `WinCCOaNative.QueryMaxRows`,
default 10000. The archive plan's "Contract for other plans" defines:

- the bound of each store call (C2);
- what broad filters return (C3). A filter is **scoped** when `Names.Classify(filter) == KindNative`; every
  other filter is **broad**.

Protocol plans rely on these limits and must not add their own full-project scans. Each plan says which
store call it makes for a broad filter.

## Order of work

```text
shared S1, S3-S5, S7 ──┬── NATS core (phase 1) ── NATS KV (phase 2, adds S2) ──┐
                       └── Redis server ─────────────────────────────────────────┤
WinCC OA archive M1-M3 (S6) ────────────────────────────────────────────────────┴── WinCC OA cases of NATS KV, Redis, Prometheus
Prometheus (non-OA part) runs on its own; its WinCC OA queries wait for archive M2/M3.
```

- S1, S3, S4, S5 and S7 land with NATS phase 1. S2 lands with NATS phase 2, where KV put first needs it.
  Redis reuses all of them. If Redis is built first, it lands them exactly as specified here, with no
  Redis-specific variants.
- The WinCC OA archive does not depend on any server. Each server's WinCC OA test cases wait for archive
  M2 (stores) and M3 (group provisioning and fallback).
