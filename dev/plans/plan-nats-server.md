# Plan: NATS protocol server and JetStream KV facade for the edge broker

**Status:** approved 2026-10-17, owner decisions in
[plan-protocol-servers-shared.md](plan-protocol-servers-shared.md); revised after checking against main and edge.
**Issue:** [vogler75/monster-mq-edge#21](https://github.com/vogler75/monster-mq-edge/issues/21).
Deviations from main are marked **[edge-only]** or **[fix: both]** inline and listed again under
"Edge-only extensions and fixes". Anything unmarked is a 1:1 port and cites the Kotlin file and line.
**Scope:** port main's native NATS server (`NATS: <port>`), including its JetStream Key-Value subset, to
`internal/nats/`. The NATS *client bridge* (`Features.Nats` in main) is not part of this plan and stays
off-limits (D1).

Kotlin references below: `NC` = `NatsClient.kt`, `KV` = `NatsJetStreamKv.kt`, `NS` = `NatsServer.kt`, all
under `main/broker/src/main/kotlin/`.

## Sign-off points

1. **New subsystem (D1).** The owner signed off on the NATS server for edge. This overrides the
   "off-limits" note in `AGENTS.md:21-28`; phase 1 updates AGENTS.md.
2. **No GraphQL change.** `brokerConfig.natsPort` already exists (`schema.graphqls:84`) and is hard-wired
   to 0 (`resolver.go:761`). `enabledFeatures` does not change: main gates only the client bridge with
   `Features.Nats` (`Features.kt:8`, used in `NatsClientConfig*.kt`), and starts the server from the `NATS`
   port alone (`Monster.kt:1034`, `1511`). Resolver *behaviour* changes in two places (session
   `protocolVersion`, session removal), both to match main.
3. **Main bugs are fixed in both repos (D3).** See "Main-side tasks" (issue monster-mq#202). Two fixes were found during this
   revision and are added under D3: delivery-time ACL (F6) and bounded input (F7).
4. **KV put may write WinCC OA tag values (D4)** through S2. Plain `PUB` keeps MQTT semantics.
5. **Test-only dependency.** `github.com/nats-io/nats.go` is imported only by `test/integration`. The broker
   binary keeps a hand-rolled codec, like main (zero CGO, no protocol library).

## Context

**Main (source of truth)**

| File | Lines | Content |
|---|---|---|
| `NatsServer.kt` | 41 | Vert.x TCP server, `TCP_NODELAY` (NS:18-19), one verticle per socket (NS:27-29); a listen failure fails the deploy (NS:36-39) |
| `NatsClient.kt` | 536 | Parser, INFO, CONNECT/auth, SUB/UNSUB/PUB/HPUB, MSG/HMSG, headers, subject conversion, session |
| `NatsJetStreamKv.kt` | 564 | JetStream API subset and `$KV.*` put/delete on archive-group last-value stores |

- Config: top-level `NATS: <port>`, 0 = off (`Monster.kt:1034`). GraphQL `natsPort` reads the same key
  (`MetricsResolver.kt:1608`).
- Docs: `main/doc/nats.md:10-118` (server part). Design history: `main/dev/done/plan-nats-kv.md`.
- Tests: `main/tests/pytest_tests/nats/test_nats_kv.py` (KV only). Main has no tests for the core protocol, so
  edge's tests become the reference for both.

**Edge today**

- No NATS code; `internal/nats/` does not exist. No `NATS` key in `internal/config/config.go` or
  `yaml-json-schema.json`.
- Building blocks this plan uses (defined in the shared plan):
  - S1 protocol publish
  - S2 native value write
  - S3 bus subscription rules
  - S4 `Group.MatchesFilter`
  - S5 session registry, dashboard removal and counters
  - S6 bounded WinCC OA reads
  - S7 delivery-time read ACL (F6)
- `pubsub.Bus` is fed from `StorageHook.OnPublished` -> `dispatch` (`hook_storage.go:338-377`, `432`), with
  response topic and user properties (`hook_storage.go:416-432`).
- Dashboard: the NATS badge comes from the `nats-` client ID prefix (`dashboard/src/js/sessions.js:631-634`),
  not from `protocolVersion`.

## Parity rules

1. **Behaviour comes from main's code, not the upstream NATS spec.** Where main differs from nats-server
   (queue groups ignored, `a.>` matches `a`, no no-responders), edge differs the same way.
2. **Same config key and schema.** Top-level `NATS` as a plain integer, schema entry copied verbatim from
   `main/broker/yaml-json-schema.json:39-46` (title "NATS Port", 0-65535, default 0). It is not an edge
   `Listener` object: no `Address`, no TLS, no `Enabled`. No Features flag.
3. **Same wire bytes.** INFO, `-ERR` texts, JetStream JSON keys, error codes and descriptions match main
   byte for byte, except the marked deviations.
4. **Same session identity.** Client IDs `nats-<uuid>` (NC:24), `Information` JSON as main (NC:143-154).
5. **Same subject mapping.** Plain character replacement (NC:492-504); no escaping and no special cases.
6. **Bucket = archive group.** KV buckets are the deployed archive groups (KV:108-111). The bucket names the
   group, so NATS KV does not call `archive.Manager.ResolveGroupForTopic`.

## Behaviour to port

### Listener and config

| Item | Behaviour |
|---|---|
| Port | `NATS: <port>`, 0 = disabled (default). Binds all interfaces, like Vert.x `listen(port)` (NS:31). A bind failure aborts broker startup (NS:36-39). |
| Socket | `TCP_NODELAY` (NS:19; Go's default for TCP). Plain TCP only, no TLS. |
| GraphQL | `brokerConfig.natsPort` = `cfg.NATS` (`MetricsResolver.kt:1608`). |

### Framing and commands (`NatsClient.kt`)

| Area | Main behaviour |
|---|---|
| INFO | Sent on accept, before anything is read (NC:60-70). Exact bytes, keys in this order: `INFO {"server_id":"monstermq","server_name":"MonsterMQ","version":"2.10.0","proto":1,"headers":true,"max_payload":1048576,"jetstream":true,"auth_required":false}` + CRLF. `auth_required` is `UserManagement.Enabled`. Version `2.10.0` because clients gate KV on it (NC:48-49). |
| Lines | Delimited by exactly CRLF (NC:73-75); a bare LF is part of the line. Empty lines are ignored (NC:125). The verb is everything before the first space, case-insensitive (NC:127-129). |
| Unknown verb | `-ERR 'Unknown Protocol Operation'`; the connection stays open (NC:139). All errors are `-ERR '<text>'` (NC:465-467). |
| PING / PONG | `PING` -> `PONG`, also before CONNECT; `PONG` is ignored (NC:137-138). The server never pings and has no idle or auth timeout. |
| CONNECT | Invalid JSON -> `-ERR 'Invalid CONNECT JSON'` (NC:157-162). Reads only `verbose` (default false), `headers` (default **false**), `user`, `pass` (NC:164-175). `no_responders`, `echo`, `pedantic`, `name` and the rest are ignored. `+OK` is sent only after a successful CONNECT with `verbose` (NC:169, 183, 196). Other commands never send `+OK`. |
| Auth off | Every command is allowed, also before CONNECT; no ACL checks (NC:167-172, 432). |
| Auth on | Empty `user` is anonymous if allowed (NC:177-190), else bcrypt check (NC:192-202). Failure: `-ERR 'Authorization Violation'`, then close. `SUB`/`UNSUB`/`PUB`/`HPUB` before successful auth: `-ERR 'Authorization Violation'`, connection stays open (NC:431-438). Edge auth: see F3 and E7. |
| Session | After CONNECT succeeds: session `nats-<uuid>`, clean, connected. Information is `{"RemoteAddress":"<host:port>","LocalAddress":"<host:port>","ProtocolVersion":"NATS","SSL":false}` in this key order (NC:143-154). Client address = remote address (`SessionHandler.kt:1059`). Deleted, not marked offline, on close, socket error or dashboard disconnect (NC:79-86, 96-105, 440-459). |
| SUB | `SUB <subject> [queue] <sid>`. Tokens are split on single spaces and the SID is the last token (NC:209-215). Fewer than 2 tokens -> `-ERR 'Invalid SUB'` (NC:210-212). **The queue group is ignored:** every member gets every message (NC:214-215). Subscribe ACL on the converted filter -> `-ERR 'Permissions Violation for Subscription to "<subject>"'` (NC:220-224). No retained delivery. `AllowRootWildcardSubscription` is not applied. |
| UNSUB | `UNSUB <sid> [max_msgs]`. An unknown SID is silently ignored (NC:250-251). The underlying subscription goes when the last SID goes (NC:255-260). `max_msgs`: see F4. |
| PUB / HPUB | `PUB <subject> [reply] <#bytes>` / `HPUB <subject> [reply] <#hdr> <#total>`. Tokens are split on spaces, empty tokens dropped (NC:270). A wrong token count gives `-ERR 'Invalid PUB'` / `-ERR 'Invalid HPUB'` (NC:271-275). A non-numeric count, a negative count or `hdr > total` gives `-ERR 'Invalid PUB byte count'` / `-ERR 'Invalid HPUB byte count'` (NC:278-283; the negative-count check is already fixed in main). The publish ACL is checked except for `$JS.`/`$KV.` subjects, which the KV layer checks (NC:288-293): `-ERR 'Permissions Violation for Publish to "<subject>"'`. Then exactly `#total + 2` bytes are read, and the trailing 2 bytes are not checked (NC:296-297, 303). Payload skip on rejection: see F1. |
| Publish | `$JS.`/`$KV.` subjects go to the KV layer (NC:305-308). Otherwise main publishes QoS 0, retain false, publisher `nats-<uuid>`, response topic = converted reply subject, user properties = parsed headers or none (NC:310-322). No `$SYS` block and no wildcard rejection. |
| Headers in | The `NATS/1.0[ status]` line is dropped. Then `Key: Value` lines, both sides trimmed; the first value of a key wins (NC:472-482). |
| MSG / HMSG | Subject = converted concrete topic, reply = converted response topic (NC:415-416). Headers come from non-empty user properties (NC:417). `HMSG <subject> <sid> [reply] <#hdr> <#total>` is sent only if the client sent `headers:true`; otherwise `MSG <subject> <sid> [reply] <#bytes>` with the headers dropped (NC:385-404). There is one message per matching SID (NC:419-426), and a client receives its own publishes. |
| Headers out | `NATS/1.0[ <status>]` CRLF, then `Key: Value` CRLF per entry, then a final CRLF. CR/LF become spaces and `:` in keys becomes `_` (NC:527-536). Edge collapses duplicate user-property keys to the last value, as main's map does. |
| Subject -> topic | `.` -> `/`, `*` -> `+`, `>` -> `#`. Character-wise and global (NC:492-494). |
| Topic -> subject | `/` -> `.`, `+` -> `*`, `#` -> `>`, space -> `_` (NC:502-504). |
| SID matching | Main's matcher (NC:509-523): `a/#` matches `a`. |

**Consequences of the plain mapping** (documented in `doc/nats.md`, same as main):

- An MQTT topic level that contains `.` or a space does not round-trip. `a/1.5` arrives as `a.1.5` and
  publishes back to `a/1/5`; `room 1/t` arrives as `room_1.t`.
- **WinCC OA explicit attributes are not addressable from NATS.** The MQTT topic
  `winccoa/tags/Pump1/speed/_online.._stime` arrives as subject `winccoa.tags.Pump1.speed._online.._stime`
  on a wildcard subscription such as `winccoa.tags.Pump1.>`. The way back does not work:
  - The subject has an empty token. nats.go `Subscribe` rejects it with `ErrBadSubject`, and so does the `nats` CLI.
  - A raw client's `SUB` maps it to `.../_online//_stime`. `Names.Parse` rejects that because `_stime` is not
    an allowed attribute (`namespace.go:268-270`), so `bus.Validate` answers `-ERR 'Invalid Subject'` (E2).
  - KV keys containing `..` are rejected (KV:117).
  - Use the plain tag topic instead. Its payload carries value and time (`_online.._value`, `_online.._stime`).

### JetStream API (`NatsJetStreamKv.kt`)

Shared rules:

- **Requests and replies.** `$JS.API.*` requests without a reply subject are ignored (KV:77). Other `$JS.*`
  subjects (`$JS.ACK...`, flow control) are ignored (KV:80). Replies are written only to this connection's
  SIDs that match the reply subject; they are not published to the broker (KV:127-129, NC:352-356).
- **Error JSON.** `{"type":<t>?,"error":{"code":C,"err_code":E,"description":"..."}}` (KV:131-135).
- **Buckets.** Buckets are deployed groups whose name matches `^[a-zA-Z0-9_-]+$`, that have a last-value
  store, and whose `LastValType != NONE` (KV:31, 108-111). Stream `KV_<B>` maps to bucket `<B>` (KV:113-114).
- **Keys.** A key is rejected if it is empty, starts or ends with `.`, contains `..`, `*` or `>`. Otherwise it
  is converted like a subject (KV:116-120).
- **Revision.** The message time in epoch milliseconds (KV:36). There is no real stream sequence and the
  history is 1.

| Request | Reply |
|---|---|
| `INFO` | `account_info_response`: memory/storage 0, `streams` = bucket count, `consumers` = this connection's consumers, all limits -1, `api` total/errors 0 (KV:166-177) |
| `STREAM.NAMES` | `stream_names_response`, sorted `KV_<B>`, `total`, `offset` 0, `limit` 1024. A `subject` filter of `""`, `>`, `$KV.*.>` or `$KV.>` selects all; `$KV.<B>...` selects one bucket; any other filter selects none (KV:246-261) |
| `STREAM.LIST` | `stream_list_response`, info objects sorted by name, `limit` 256, `missing` [] (KV:263-282) |
| `STREAM.INFO.KV_<B>` | `stream_info_response` with `total`/`offset`/`limit` 0 (KV:232-243). Unknown bucket: 404/10059 `stream not found` (KV:137). |
| Stream config | `name`, `description` "MonsterMQ archive group <B>", `subjects` [`$KV.<B>.>`], `retention` limits, `max_consumers`/`max_msgs`/`max_bytes` -1, `max_age` 0, `max_msgs_per_subject` 1, `max_msg_size` -1, `discard` new, `storage` memory for a MEMORY store else file, `num_replicas` 1, `duplicate_window` 120000000000, `compression` none, **`allow_direct` false**, `mirror_direct` false, `sealed` = `LastValReadOnly`, `deny_delete` true, `deny_purge` false, `allow_rollup_hdrs` true (KV:179-197) |
| Stream state | One `FindMatchingMessages("#")` pass that counts only non-empty payloads and stops at 1,000,000 (KV:30, 200-211). `first_seq` is 1 if there are messages, else 0. `last_seq` = newest revision. `num_subjects` = messages, `consumer_count` = this connection's consumers on the bucket, timestamps ISO (KV:213-229). A scan error is logged and reported as zero state (KV:214-215). For a WINCCOA store the pass is bounded by the S6 row limit (see WinCC OA bucket). |
| `STREAM.MSG.GET.KV_<B>` | Only `last_by_subj` = `$KV.<B>.<key>`. Anything else gives 404/10037 `no message found` (KV:292-298). Bad key: 400/10052 `invalid key` (KV:299-303). Subscribe ACL on the topic: 403/10101 `Permissions Violation for Subscription to "<subj>"` (KV:304-307). Store error: 500/10010 `last value store error: <msg>`. Missing or empty payload: 404/10037. Success: `{"type":..., "message":{"subject","seq","data"(base64),"time"}}` (KV:308-322) |
| `DIRECT.GET.*` | Not implemented. It falls into the unsupported branch below; with `allow_direct:false` clients use `STREAM.MSG.GET` |
| `CONSUMER.CREATE.*` / `CONSUMER.DURABLE.CREATE.*` | See consumers below |
| `CONSUMER.DELETE.KV_<B>.<name>` | `{"type":"...consumer_delete_response","success":true}`, or 404/10014 `consumer not found` (KV:508-517) |
| `CONSUMER.INFO.KV_<B>.<name>` | `consumer_info_response`. While the snapshot is loading, the reply waits until it is loaded. Unknown consumer: 404/10014 (KV:519-531) |
| Anything else (`STREAM.CREATE/UPDATE/DELETE/PURGE`, `DIRECT.GET`, ...) | 400/10025, no `type`: `JetStream API [<api>] is not supported by MonsterMQ; only Key-Value buckets backed by archive groups are available` (KV:162) |

### KV put, delete and purge (`$KV.<B>.<key>`)

The checks run in main's order (KV:327-389). An ack is sent only if the publish has a reply subject. All
error replies have no `type`, except 10059.

| Step | Behaviour |
|---|---|
| 1. Bucket | Unknown bucket: 404/10059 `stream not found`, type `pub_ack_response` (KV:330-335) |
| 2. Key | Invalid key: 400/10052 `invalid key` (KV:336-340) |
| 2a. Native write [edge-only, D4] | Applies to a put (no `KV-Operation`) when S2 `NativeValueWrite(topic)` is ok. The topic becomes `<topic>/set` and steps 3-4 are skipped (S2). The ACL is checked on the `/set` topic (step 5). Step 7 then publishes with retain false. |
| 3. Filter | `!group.MatchesFilter(topic)` (S4, replaces main's filter tree): 400/10052 `key [<topic>] does not match the topic filter of archive group [<B>]` (KV:341-343) |
| 4. Read-only | `LastValReadOnly`: 400/10039 `last value store of archive group [<B>] is read-only` (KV:345-348) |
| 5. ACL | Publish ACL on the topic: 403/10101 `Permissions Violation for Publish to "<subject>"` (KV:349-352) |
| 6. Optimistic concurrency | With header `Nats-Expected-Last-Subject-Sequence`, the current revision is read from the bucket store (0 if missing or empty). Store error: 500/10010. Mismatch: 400/10071 `wrong last sequence: <current>` (KV:373-388). Used by `kv create` / `kv update`. |
| 7. Publish | `KV-Operation: DEL` or `PURGE` -> empty payload (KV:354-356). One message through S1: QoS 0, retain true, client `nats-<uuid>`, `Time` set once (KV:358-369). The ack is `{"stream":"KV_<B>","seq":<revision of that message>}` (KV:370). |

- **Edge never calls `AddAll`/`DelAll`.** The publish reaches every matching archive group through
  `Group.Submit`, which already writes the last-value store, and deletes on an empty payload
  (`group.go:153-166`). S1 keeps `msg.Time` (`InlinePublisher.PublishMessage`, `inline_publisher.go:44`), so
  the stored revision equals the ack `seq`.
- The retained store is updated or cleared like any retained publish.
- **PURGE is treated as DEL**, as in main.

### Consumers (watch, keys)

| Item | Behaviour |
|---|---|
| Path | `<stream>[.<name>[.<filter>]]`; `stream_name` in the body wins (KV:395-397). Unknown bucket: 404/10059 (KV:398-403). |
| Pull | No `deliver_subject`: 400/10025 `pull consumers are not supported by MonsterMQ, use a push consumer` (KV:405-409) |
| Filters | `filter_subject` plus `filter_subjects`, default `$KV.<B>.>`. A filter outside the bucket: 400/10093 `filter subject [<f>] is not part of bucket [<B>]`. A filter that fails the subscribe ACL: 403/10101 `Permissions Violation for Subscription to "<f>"` (KV:411-428) |
| Name | `name`, then `durable_name`, then the path token, then 8 random hex characters. An existing consumer with the same name is replaced (KV:430-432) |
| Options | `deliver_policy` (default `all`), `opt_start_seq` (default 0), `idle_heartbeat` in ns -> ms, `headers_only` (KV:434-439) |
| Order | Live subscription first, then the snapshot from the store on a worker; live messages are buffered until the snapshot is out (KV:442-456) |
| Snapshot | Skipped for `deliver_policy:"new"`. Otherwise every filter is scanned and entries are deduplicated by topic, keeping those with a non-empty payload and revision >= `opt_start_seq` (KV:445-456). Every other policy (`last_per_subject`, `by_start_sequence`, `by_start_time`, ...) gets the snapshot and **keeps the live subscription**. |
| Create reply | `consumer_create_response` with `num_pending` = snapshot size, `push_bound` true, `config` echoed with `name` (KV:467, 493-506). Store error: 500/10010 and the consumer is removed (KV:459-463) |
| Interest | Delivery starts only once a SID of this connection matches `deliver_subject`; every SUB re-checks (KV:93-95, 479-481). When the last such SID is unsubscribed, the consumer is removed (KV:98-100) |
| Delivery | Snapshot first with `pending` counting down to 0, then the buffered live messages, then live (KV:482-487). Ack subject `$JS.ACK.KV_<B>.<name>.1.<rev>.<deliveredSeq>.<nanos>.<pending>`, delivered on subject `$KV.<B>.<natsSubject(topic)>` (KV:541-546). Empty payload -> header `KV-Operation: DEL`. `headers_only` -> header `Nats-Msg-Size: <n>` with an empty body. Otherwise a plain body (KV:547-554) |
| Live match | Consumer filter and group filter (`MatchesFilter`, KV:64-66) |
| Heartbeat | If `idle_heartbeat > 0`: **periodic**, whether idle or not (KV:488-490). `NATS/1.0 100 Idle Heartbeat` with `Nats-Last-Consumer: <deliveredSeq>`, `Nats-Last-Stream: <lastRev>`, on subject `deliver_subject`, no reply, empty body (KV:557-563) |
| Cleanup | On delete, replace, unsubscribe or close: stop the timer and drop the subscription (KV:533-538, 102-104) |

### WinCC OA bucket (`WinCCOA`, archive plan)

- **Reads** use the group's `oastore.LastValStore` like any other store: `get` -> `Get`, snapshots and stream
  state -> `FindMatchingMessages`. With broad filters (`#` for `nats kv ls`/`info`/`keys`), the call is bounded
  by **the S6 row limit** in the host operation, as defined in the archive plan under "Contract for other
  plans". The NATS server adds no scan of its own, and its 1,000,000 count limit does not replace S6. A
  truncated scan reports fewer keys and smaller state; the store logs the truncation.
- **No double delivery on watch.** A consumer's bus subscription on a native filter triggers native interest,
  which delivers current values with `bus.Deliver` (`native_bus.go:73-85`), on top of the store snapshot.
  Fix, deterministic: `nativeBroker.PublishCurrentValue` sets a new non-persisted flag
  `stores.BrokerMessage.Initial = true`, and KV consumers drop `Initial` messages
  [edge-only, E6]. The snapshot is always the store's view, as in main; later hotlink changes arrive as
  normal publishes (`Initial` false). A change between subscribing and the snapshot read can be delivered
  twice with the same value, as in main (KV:442-456). Core `SUB` keeps the `Initial` values (E1).
- **Writes.** The group is `LastValReadOnly:true`, so step 4 rejects puts on non-native keys (10039), and
  DEL/PURGE on any key. A put on a native value key goes through step 2a (S2, D4) whatever the bucket. A
  DEL on a native value topic of a writable bucket reaches S1, and the native hook denies it (E4).
- **Live updates on broad filters.** `#` and `winccoa/#` are not native filters (`Classify` -> `KindOther`), so
  a whole-bucket watch gets live changes only for tags that other subscribers keep connected. A scoped watch
  (`winccoa.tags.Pump1.>`) registers its own `dpQueryConnectSingle`.

## Edge-only extensions and fixes

| # | Item | Main behaviour | Reason |
|---|---|---|---|
| F1 [fix: both] | A rejected `PUB`/`HPUB` (auth or ACL) still consumes its payload. Byte counts are parsed first, then auth, then the ACL check. | Returns before the fixed-size read (NC:265, 290-293), so the payload is parsed as a command. | Stream corruption, command injection through the payload. |
| F2 [fix: both] | `SUB` with a SID already in use replaces that SID's subscription; the old filter is released first. | The SID stays in the old filter's set (NC:227-230), and keeps receiving and leaking. | Leak. |
| F3 [fix: both] | Commands after CONNECT are processed only after auth completes. Edge authenticates synchronously with `auth.Cache.Validate` (`auth.go:179`). | Async auth (NC:192-202); a SUB sent right after CONNECT is refused (NC:433-435). | Lost subscriptions. |
| F4 [fix: both] | `UNSUB <sid> <max>` honours `max` with nats-server semantics: if the SID has already delivered `max` messages it is removed now; otherwise it is removed after its `max`-th message. `max <= 0` or no `max` removes at once. | `max` is ignored and the SID is removed at once (NC:248-260). | `AutoUnsubscribe` / old-style requests break. |
| F5 [fix: both] | Per-session counters (S5). In: each PUB/HPUB published through S1, each KV put/del. Out: each MSG/HMSG for a core subscription or a KV entry. Not counted: API requests and replies, heartbeats. | Nothing is counted (only `MqttClient.kt:1086, 1341, 1387, 1424`). | Dashboard session rates. |
| F6 [fix: both] | Delivery-time ACL (shared S7): with user management on and a non-admin user, each delivered message (core SUB, KV live and snapshot entries) is checked with subscribe ACL on its concrete topic and dropped if denied. | NATS checks only at subscribe time. The ACL admits wildcards that partly overlap a deny rule because delivery re-checks (`AclCache.kt:194-195`, `MqttClient.kt:1328-1337`), but NATS never re-checks (NC:414-429, KV:84-90). | Denied topics leak to NATS wildcard subscribers. |
| F7 [fix: both] | Bounded input. A payload above `max_payload` (1048576) gets `-ERR 'Maximum Payload Violation'` and the connection closes. A control line above 64 KiB gets `-ERR 'Maximum Control Line Exceeded'` and the connection closes (nats-server texts). | `max_payload` is advertised, not enforced (NC:278-297); the line length is unbounded (NC:73). | Memory exhaustion on small devices. |
| E1 [edge-only] | `SUB` on a WinCC OA native filter registers native interest and first receives the current value (S3, `native_bus.go:53-85`). | No native namespace. | Same as an MQTT SUBSCRIBE in edge. |
| E2 [edge-only] | Filters rejected by `bus.Validate` (S3 rule 1). `SUB` gets `-ERR 'Invalid Subject'`, and the reason is logged. CONSUMER.CREATE gets 400/10093 `filter subject [<f>] is not valid: <reason>`. | n/a | Invalid native filters would otherwise subscribe silently and receive nothing. |
| E3 [edge-only] | Slow consumer: a bus overflow (S3 rule 2), a full KV live buffer (1024 entries while the snapshot loads) or a socket write blocked for 10 s gets `-ERR 'Slow Consumer'` and the connection closes. | Unbounded buffers. | Bounded memory. |
| E4 [edge-only] | S1 can reject a publish (native hook). Core PUB/HPUB: `packets.ErrNotAuthorized` gets `-ERR 'Permissions Violation for Publish to "<subject>"'` and other errors are only logged. The native rejection reply still goes to the reply subject (`hook_winccoa.go:191-208`). KV put/del: 403/10101 with main's text, or 400/10052 `<reason>`. | `publishMessage` cannot fail. | Native value topics are broker-owned (`service.go:1075-1077`). |
| E5 [edge-only, D4] | KV put on a native value topic -> `/set` command (step 2a, S2). | Retained publish to the topic. | Write WinCC OA tags with `nats kv put`. |
| E6 [edge-only] | KV consumers drop `Initial` messages. | n/a | No double delivery on WinCC OA watches. |
| E7 [edge-only] | Anonymous login follows `auth.Cache.Validate("", "")`: `AnonymousEnabled`, and an `Anonymous` user record decides when it exists (`auth.go:180-188`). `AllowAnonymousLocalhost` is not applied. | Needs an enabled `Anonymous` user (NC:179-180). | Edge-wide convention, same as MQTT. |
| E8 [edge-only] | ACL through `auth.Cache.Allow(user, topic, write)` (`auth.go:237`). It applies the WinCC OA alias rule (`auth.go:241-250`) and skips rules for wildcard subscribe checks when `AclCheckOnSubscription` is false (`auth.go:314`); F6 still filters delivery. Anonymous sessions use username `""`. | `canSubscribe`/`canPublish` with rules always. | Same ACL as MQTT clients in edge. |
| E9 [edge-only] | At start, the NATS server deletes this node's leftover `nats-` session records. | n/a | Records survive a crash. |

## Main-side tasks

The main side is tracked in [vogler75/monster-mq#202](https://github.com/vogler75/monster-mq/issues/202). That issue holds the fix plan for F1-F7, with the `NatsClient.kt` /
`NatsJetStreamKv.kt` lines and the matching tests, plus the `doc/nats.md` update. It ships in the same release as
this plan (D3).

## Package layout

`internal/nats/` (imports `auth`, `archive`, `pubsub`, `stores`, `winccoanative`; never `internal/broker`):

- `server.go`
  - `Deps` holds: port, node ID, `UserManagement.Enabled`, `*auth.Cache`, `*pubsub.Bus`, `Publish` (S1
    `func(clientID string, msg stores.BrokerMessage) error`), `Release` (S1), `NativeValueWrite`
    (S2 `func(topic string) (string, bool)`, nil when the namespace is off), `stores.SessionStore`, the
    session registry (an interface with `Register`/`Unregister`, S5), `*archive.Manager`, a counter
    (an interface with `IncClientIn`/`IncClientOut`/`ForgetClient`, may be nil), and the logger.
  - `New(deps)` binds the port. `Serve()` runs the accept loop. `Close()` closes the listener and every
    connection. E9 cleanup runs in `New`.
- `conn.go`
  - Reader: one goroutine reads CRLF lines, then fixed-size payloads, with the F7 limits.
  - Handlers: CONNECT, PING/PONG, SUB/UNSUB/PUB/HPUB, auth, session registration, cleanup.
  - State: per-connection, under one mutex.
  - Writes: through a mutex-guarded `bufio.Writer` with a 10 s write deadline (E3).
- `subject.go`: `SubjectToTopic`, `TopicToSubject`, `topicMatchesFilter` (NC:492-523).
- `headers.go`: `parseHeaders`, `formatHeaders` (NC:472-482, 527-536).
- `jetstream.go`: API routing, stream info/names/list, msg get, put/del (KV:73-389).
- `kv_consumer.go`: consumers, snapshot, interest wait, delivery, heartbeat (KV:43-67, 393-563).

Design notes:

- **One bus subscription per SID** (`SubscribeWithOverflow([]string{filter}, 1024)`, S3). Each subscription
  has a reader goroutine that writes MSG/HMSG for its SID only.
  - This gives main's observable result, one message per matching SID (NC:419-426), without
    deduplication.
  - Each SID also gets its own native current value (E1).
- **One bus subscription per KV consumer** with all of its filters. The bus delivers each message once per
  subscription (`bus.go:177`), which replaces main's shared `kvTopicRefs` (NC:365-383).
- **Leading-wildcard rule for `$` topics.** The bus matcher does not apply the MQTT `$` rule
  (`bus.go:197-215`). The reader therefore drops `$`-topics for filters that start with `+` or `#`, as main's
  topic tree does (the same rule as `kafkaclient/connector.go:378-389`).
- **Store calls run off the connection mutex.** `Get` and `FindMatchingMessages` run in a goroutine, like
  `executeBlocking` (KV:139-143), and their results are applied under the mutex. This keeps main's state
  machine: `loaded`, `waitingInfoReplies`, `pendingLive` (KV:53-62).

## Wiring

| Edge file | Change |
|---|---|
| `internal/config/config.go` | `NATS int \`yaml:"NATS"\`` in `Config` (:416); 0 in `Default()` (:499). `Validate()` (:633) rejects values outside 0-65535. |
| `yaml-json-schema.json` | Root `properties`, after `WSS` (:16): the `NATS` entry copied from `main/broker/yaml-json-schema.json:39-46`. |
| `config.yaml.example` | After `WSS` (:6): `NATS: 0  # NATS protocol port, e.g. 4222 (0 = disabled)`. `scripts/deb/config.yaml` is unchanged (off by default). |
| `internal/broker/protocol_publish.go` | S1 `ProtocolPublish`, S2 `NativeValueWrite` (shared plan). |
| `internal/broker/hook_winccoa.go` | S1 `InlinePublishMessage` next to `InlinePublish` (:129-140). |
| `internal/broker/inline_publisher.go` | S1 `Release(clientID)` (`clients` map at :56-66). |
| `internal/broker/protosessions.go` | S5 `ProtocolSessions`. |
| `internal/archive/group.go` | S4 `MatchesFilter` next to `Matches` (:132-150). |
| `internal/broker/server.go` | New `nats *nats.Server` and `protoSessions *ProtocolSessions` fields (struct at :55). When `cfg.NATS > 0`, build the server after `unsHook` (:627) and before the GraphQL block (:697). Deps: S1 over `unsHook`/`nativeHook`/`NewInlinePublisher(server)`; S2 bound to `native.Names` when `native != nil`; the counter when `collector != nil` (nil-interface pattern at :398-401). A bind error goes through the existing undo path. `Serve()`: start `s.nats.Serve()` in a goroutine before `s.mqtt.Serve()` (:1059). `stopPublishers()` (:1125): `s.nats.Close()` first, so the PeerLink shutdown path (:1111) closes it too. `resolver.ProtocolSessions = protoSessions` next to `resolver.PeerLinkMgr` (:701). |
| `internal/broker/native_bus.go` | `PublishCurrentValue` (:78-84) sets `Initial: true` (E6). |
| `internal/stores/types.go` | `Initial bool \`json:"-" bson:"-"\`` next to `Inline` (:28-30): a current value delivered at subscribe time, not persisted. |
| `internal/graphql/resolvers/resolver.go` | `BrokerConfig` (:761): `NatsPort: c.NATS`. `sessionToGraphQL` (:838-865): for client IDs with prefix `nats-`, `redis-` or `mcp-` and `ProtocolVersion == 0`, return null `protocolVersion`, `receiveMaximum`, `maximumPacketSize` and `topicAliasMaximum`, as main does (`MetricsResolver.kt:141, 268-270`; `SessionHandler.kt:934-961` yields null for `"NATS"`). `RemoveSessions` (:2484-2501): call `ProtocolSessions.Close(id)` first (S5). |
| `go.mod` | `github.com/nats-io/nats.go`, imported only by `test/integration`. |

Connection lifecycle in edge:

- **On CONNECT success:**
  - `SessionStore.SetClient` with `ClientID`, `NodeID`, `CleanSession` true, `Connected` true, `UpdateTime`,
    `ClientAddress` (remote) and `Information`.
  - `ProtocolSessions.Register(id, close)`.
- **On close:**
  - Unsubscribe every bus subscription and stop the consumers.
  - `ProtocolSessions.Unregister`, then `SessionStore.DelClient`.
  - `Release(id)` (S1) and `counter.ForgetClient(id)`.

## Phases

1. **Core + S1, S3, S4, S5, S7.**
   - S1, S3, S4, S5 and S7 in the files listed under Wiring. S2 (`NativeValueWrite`) lands in phase 2 with
     its first user, KV put.
   - Config in the three files.
   - `internal/nats/{server,conn,subject,headers}.go`.
   - `server.go` and `resolver.go` wiring.
   - `test/integration/nats_test.go`.
   - Docs:
     - `doc/nats.md`: the server part, mirroring `main/doc/nats.md:10-118`, with an "Edge-only extensions
       and fixes" section.
     - README: a feature line under "Feature flags" (`README.md:141`) noting that NATS is a port, not a flag.
     - `AGENTS.md`: list the NATS server as signed off 2026-10-17 at :21-28 (the client bridge stays
       off-limits), and add `nats/  # NATS protocol server and JetStream KV facade` to the layout at :88-128.
   - Main: the core-protocol parts of monster-mq#202.
2. **KV.**
   - S2 `NativeValueWrite` in `internal/broker/protocol_publish.go`.
   - `internal/nats/{jetstream,kv_consumer}.go`.
   - `Initial` flag (`stores/types.go`, `native_bus.go`).
   - `test/integration/nats_kv_test.go`.
   - KV section in `doc/nats.md`.
   - Main: the KV parts of monster-mq#202 (counters and delivery-time ACL).
3. **WinCC OA cases** (`test/integration/winccoa_nats_test.go`).
   - W1-W5 need only simhost and may land with phase 1.
   - W6-W10 wait for archive M2 (stores) and M3 (group provisioning).
   - Add a WinCC OA section to `doc/nats.md`.

## Tests

All tests are in `test/integration/` and use real listeners. Raw cases use a TCP socket; client cases use
`nats.go`. Each test picks free ports, like the existing tests.

**`nats_test.go` (phase 1)**

1. INFO bytes:
   - With user management off, the first line equals
     `INFO {"server_id":"monstermq","server_name":"MonsterMQ","version":"2.10.0","proto":1,"headers":true,"max_payload":1048576,"jetstream":true,"auth_required":false}\r\n`.
   - With user management on, it ends in `"auth_required":true}\r\n`.
2. Raw protocol, one socket:
   - `PING` before CONNECT -> `PONG`.
   - `FOO` -> `-ERR 'Unknown Protocol Operation'`, and the next `PING` -> `PONG`.
   - `CONNECT {x` -> `-ERR 'Invalid CONNECT JSON'`.
   - `CONNECT {"verbose":true}` -> `+OK`.
   - `sub a.b 1` (lowercase), then `PUB a.b 5` + `hello` -> `MSG a.b 1 5\r\nhello\r\n`.
   - `SUB x` -> `-ERR 'Invalid SUB'`.
   - `PUB a -1` -> `-ERR 'Invalid PUB byte count'`.
   - `HPUB a 10 5` -> `-ERR 'Invalid HPUB byte count'`.
   - A PUB line and its payload in one TCP write.
3. MQTT interop:
   - MQTT publish `sensors/temp/room1` reaches nats.go `sensors.>` and `sensors.*.room1` as subject
     `sensors.temp.room1`.
   - NATS publish `test.hello` reaches MQTT `test/#`.
   - MQTT `a` reaches NATS `a.>`.
   - MQTT `room 1/t` arrives as `room_1.t`.
   - A NATS client receives its own publish.
   - Two connections in queue group `q` both receive every message.
4. Headers and reply:
   - A raw client without `headers` gets `MSG` for an MQTT v5 publish with user properties.
   - With `headers:true` it gets `HMSG` with `NATS/1.0\r\nk: v\r\n\r\n`.
   - HPUB `k: v` reaches an MQTT v5 subscriber as a user property.
   - `nc.Request` is answered by an MQTT v5 client publishing to the response topic `_INBOX/...`.
   - NATS to NATS request/reply works.
5. Auth:
   - A wrong password -> `-ERR 'Authorization Violation'`, then EOF.
   - Empty user with `AnonymousEnabled` -> works; with it false -> error and close.
   - With user management on, `PUB a 5\r\nhello\r\nPING\r\n` before CONNECT -> exactly one `-ERR 'Authorization Violation'`, then `PONG` (F1).
6. ACL (user allowed `allowed/#`, deny rule `allowed/secret/#`):
   - `SUB denied.x` -> `-ERR 'Permissions Violation for Subscription to "denied.x"'`.
   - `PUB denied.x 5` + payload -> `-ERR 'Permissions Violation for Publish to "denied.x"'`, then `PING` -> `PONG` (F1).
   - `SUB allowed.>` receives `allowed/a` but not `allowed/secret/b` (F6).
7. Fixes:
   - F2: `SUB a 1`, `SUB b 1`; publishing `a` and `b` delivers only `b`; after `UNSUB 1` nothing arrives.
   - F4: `UNSUB 1 2` delivers exactly 2 messages; `UNSUB 1 1` after 3 delivered removes the SID at once.
   - F3: `CONNECT {"user":..,"pass":..}\r\nSUB a 1\r\n` in one write receives a later publish.
   - F7: `PUB a 2000000` -> `-ERR 'Maximum Payload Violation'`, then EOF; a 70 KiB line -> `-ERR 'Maximum Control Line Exceeded'`, then EOF.
   - F5: with metrics at a 1 s interval, `session(clientId){ metrics { messagesIn messagesOut } }` is non-zero after traffic.
8. Session (S5):
   - While connected, `sessions` lists `nats-<uuid>` with `connected` true, null `protocolVersion`, and
     `information` with `"ProtocolVersion":"NATS"`.
   - On disconnect it is gone.
   - `removeSessions([id])` closes the NATS socket (EOF) and removes the record.
9. Slow consumer (E3): a raw subscriber that never reads, sent 64 KiB messages until the server closes it;
   its session disappears and the broker keeps serving.
10. Config: with `NATS: 0` the port refuses connections; `brokerConfig.natsPort` reports the configured port.
    `config_schema_test.go` passes with `NATS` in `config.yaml.example`.

**`nats_kv_test.go` (phase 2)** (MEMORY groups: `Plant` with `plant/#`, read-only `RO`, retained-only `Ret`; nats.go `jetstream` package)

1. Buckets and info:
   - `KeyValueStoreNames` -> `Default`, `Plant`, `RO`, `Ret`.
   - Raw `STREAM.INFO.KV_Plant` has `allow_direct:false`, `max_msgs_per_subject:1`, `deny_delete:true`, `sealed:false`.
   - `KV_RO` has `sealed:true`.
   - `kv.Status().Values()` counts non-empty keys.
2. Put and get:
   - `kv.Put("plant.line1.temp","21.5")` returns revision `r`.
   - An MQTT subscriber on `plant/#` gets it as retained.
   - `kv.Get` returns `21.5` with revision `r`.
   - An MQTT publish to `plant/line1/hum` is visible to `kv.Get`.
3. Delete: `kv.Delete` clears the retained message, and `kv.Get` returns `ErrKeyNotFound`.
4. Create and update:
   - `kv.Create` on an existing key fails (`wrong last sequence: <r>`, 10071).
   - `kv.Update(key, v, r)` succeeds; a stale revision fails.
5. Keys and watch:
   - `kv.Keys` lists every key.
   - `kv.Watch("plant.line1.>")` delivers the snapshot, then the nil marker, then a live MQTT update.
   - A raw consumer with `idle_heartbeat` 100 ms receives `HMSG` heartbeats `NATS/1.0 100 Idle Heartbeat`.
6. Consumer rules (raw):
   - `deliver_policy:"new"` sends no snapshot.
   - `opt_start_seq` = `r+1` skips older entries.
   - `by_start_sequence` still delivers live updates.
   - A consumer created before the SUB on `deliver_subject` delivers nothing until the SUB.
   - UNSUB of `deliver_subject` removes the consumer: `CONSUMER.INFO` then gives 404/10014.
7. Exact errors (raw JSON):
   - `STREAM.INFO.KV_Missing` -> 404/10059 `stream not found`.
   - `STREAM.CREATE.X` and `DIRECT.GET.KV_Plant` -> 400/10025 with main's text.
   - A put to `other.x` in `Plant` -> 400/10052 `key [other/x] does not match the topic filter of archive group [Plant]`.
   - A put in `RO` -> 400/10039.
   - A put on a denied topic -> 403/10101.
   - Key `a..b` -> 400/10052 `invalid key`.
   - A pull consumer -> 400/10025 `pull consumers are not supported by MonsterMQ, use a push consumer`.
8. Retained-only group (S4): a put in `Ret` is accepted and stored.
9. Delivery-time ACL (F6): a watch on `allowed.>` does not show `allowed/secret/...` entries.

**`winccoa_nats_test.go` (phase 3, simhost through `startNative`, `winccoa_native_test.go:83`)**

1. W1: SUB `winccoa.tags.Pump101.speed` gets the current value, then a live value after `sim.Set`. `sim.Connections()` is 1 while subscribed and 0 after UNSUB.
2. W2: SUB `winccoa.tags.Missing.x` -> `-ERR 'Invalid Subject'` (E2). SUB `winccoa.tags.Pump101.speed._online.._stime` (raw) -> `-ERR 'Invalid Subject'`, the documented consequence.
3. W3: PUB `winccoa.tags.Pump101.speed.set` with `{"value":99.5}` updates the DPE.
4. W4: `nc.Request` on the `.set` subject receives the native command reply (S1 carries the response topic).
5. W5: PUB on the value topic `winccoa.tags.Pump101.speed` -> `-ERR 'Permissions Violation for Publish to "winccoa.tags.Pump101.speed"'` (E4).
6. W6: `kv.Get` in `WinCCOA` returns `{"time":...,"value":...}`.
7. W7: `kv.Put` in `WinCCOA` and in `Default` on a native key writes the DPE (S2); `kv.Delete` in `WinCCOA` -> 400/10039.
8. W8: `kv.Watch("winccoa.tags.Pump101.>")` delivers each key exactly once before the nil marker (E6), then a live change.
9. W9: with more DPEs than the S6 row limit, `kv.Keys` and `STREAM.INFO.KV_WinCCOA` return at most that many keys, and the host query carries the limit.
10. W10: a `Default` bucket watch on a native filter does not see `Initial` duplicates either.

## Acceptance criteria

- For the same inputs, edge and main (after monster-mq#202) send the same INFO line, `-ERR` texts, MSG/HMSG frames,
  JetStream JSON and error codes. The only differences are the E items.
- `nats pub/sub/request/reply` and `nats kv ls/info/get/put/del/create/update/keys/watch` work against edge
  with bucket = archive group, and interoperate with MQTT clients both ways.
- `NATS: 0` binds nothing. `brokerConfig.natsPort` reports the port. `enabledFeatures` is unchanged.
- NATS sessions appear as `nats-<uuid>` with the dashboard's NATS badge, null `protocolVersion` and
  per-session rates, and the dashboard can disconnect them.
- No GraphQL SDL change. The standalone binary stays CGO-free, and
  `go version -m bin/monstermq-edge` lists no `nats-io` module.
- With WinCC OA: SUB/PUB/KV work on tag topics as in W1-W10, and broad KV reads stay within the S6 row limit.
- AGENTS.md, README, `doc/nats.md` and main's `doc/nats.md` are updated.

## Risks

- **Lossy subject mapping** (dots and spaces in topic levels, explicit attributes): this is main's design.
  It is documented, not fixed.
- **Revision = epoch ms.** Two writes to one key in the same millisecond share a revision, so `kv update` can
  pass where it should fail (as in main). For S2 native writes the ack `seq` is the publish time, and the
  value read back later carries `_online.._stime`, so `kv update` with the ack revision may fail.
- **WinCC OA scans.** `nats kv ls` builds info for every bucket, so each call scans the `WinCCOA` store up to
  the S6 row limit. Results above the limit are silently partial for the client.
- **Slow consumers are disconnected** (E3) where main buffers. Clients that pause reading for more than 10 s
  under load reconnect.
- **One bus subscription per SID.** Many SIDs on wide native filters each hold a native interest. This is
  the same cost as one MQTT subscription each.
- **Main changes (monster-mq#202) must ship in the same release**, or edge and main answer differently (D3).
- **nats.go behaviour** (ordered-consumer resets, header use) depends on its version. Pin it in `go.mod`.
