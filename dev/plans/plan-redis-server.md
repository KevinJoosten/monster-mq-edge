# Plan: Redis protocol server for the edge broker

**Status:** approved 2026-10-17, owner decisions in
[plan-protocol-servers-shared.md](plan-protocol-servers-shared.md); revised after checking against main and edge.
**Scope:** port main's Redis protocol server (`main/broker/src/main/kotlin/extensions/RedisServer.kt`, 1690
lines) to `internal/redisserver/`. Anything unmarked is a 1:1 port and cites the Kotlin line. Deliberate
differences are marked **[edge-only]** or **[fix: both]** inline and listed again under "Edge-only extensions
and fixes". Shared decisions D1-D6 and building blocks S1-S7 are defined in the shared plan and are only
referenced here.
**Companion plans:** [plan-winccoa-archive.md](plan-winccoa-archive.md) (WINCCOA stores,
`archive.Manager.ResolveGroupForTopic`, the S6 row limit), [plan-nats-server.md](plan-nats-server.md) (first
user of S1-S5).

Conventions in this plan:

- "RedisServer.kt:N" is shortened to ":N". The exception is where a file name heads the cell, item or
  wiring block: there ":N" means a line in that file. Other files are named in full.
- Error replies are given without the leading `-` and the trailing CRLF.
- `<n>` is the connection number, counted from 1 (:39, :59).

## Sign-off points

1. **New subsystem (D1).** The owner signed off on the Redis *server* on 2026-10-17. This overrides the
   "Redis off-limits" note in `AGENTS.md:21-28`, which Phase 6 updates. The Redis *client bridge* stays
   off-limits.
2. **GraphQL: copied from main, not a divergence.**

   > **IMPORTANT.** `redisDbNumber: Int` already exists in main:
   > - `ArchiveGroupInfo`: `schema-queries.graphqls:389`
   > - `CreateArchiveGroupInput`: `schema-mutations.graphqls:266`
   > - `UpdateArchiveGroupInput`: `schema-mutations.graphqls:287`
   >
   > Edge copies these three fields verbatim, after `databaseConnectionName` as main does, and stays a
   > strict subset of main's schema. Nothing else in the SDL changes. It is still an SDL edit, so it is
   > named here per `AGENTS.md:43-50`.

   `enabledFeatures` gains `"RedisServer"` (`Features.kt:10`).
3. **Storage.** The new column and field are `redis_db_number`, exactly as main has them today. Main
   needs no storage change.
4. **Main-side changes (D3).** The main bugs listed under "Main-side tasks" (issue monster-mq#203) are fixed in main in the same
   release.
5. **Native writes (D4).** Redis `SET`/`MSET` may write WinCC OA tag values through S2.
6. **No new production dependency.** The RESP parser is hand-rolled. `github.com/redis/go-redis/v9` is a
   test-only import.

## Context

- **Main** (paths under `main/broker/src/main/kotlin/`):
  - `extensions/RedisServer.kt`: the whole server.
  - `Monster.kt`:
    - `:1326-1332`: the server is created only when `RedisServer.Enabled` is set.
    - `:1627-1631`: it is deployed only when `Features.RedisServer` is enabled.
    - `:1563-1564`: features default to true.
    - `:1847-1849`: a failed deploy exits the broker.
  - `Features.kt:10`.
  - `handlers/ArchiveGroup.kt`:
    - `:52`: the `redisDbNumber` field.
    - `:210`: the getter `redisDbNumber ?: if (name == "Default") 0 else null`.
    - `:749`: config key `RedisDbNumber` or `redisDbNumber`.
  - `graphql/ArchiveGroupResolver.kt`: `:668` (create), `:936-940` (update), `:246` and `:349` (info).
  - Archive config stores under `stores/dbs/{sqlite,postgres,mongodb}/`.
  - Main has no Redis doc page. `main/doc/configuration.md:308-316` only mentions the keys.
- **Edge today:**
  - There is no Redis code.
  - `stores.ArchiveGroupConfig` (`internal/stores/interfaces.go:156-175`) has no `RedisDbNumber`.
  - The SDL (`internal/graphql/schema/schema.graphqls:226-281`) has no `redisDbNumber`.
  - The dashboard, including the copy bundled in `internal/dashboard/dist/js`, already shows and edits
    `redisDbNumber` when `enabledFeatures` contains `"RedisServer"`
    (`dashboard/src/js/archive-group-detail.js:71,89,155,370-371,479`; `archive-groups.js:29,39,273,304,351`).
- **Data flow in edge:**

  ```text
  Redis client -> redisserver.Conn -> archive.Manager.ByRedisDB / ResolveGroupForTopic
                                      -> Group.LastValue()   (reads; DEL deletes here)
                                   -> S1 ProtocolPublish     (SET, MSET, PUBLISH, typed writes)
                                      -> UNS hook / native hook (S2 for native value writes) / engine
                                   -> S3 pubsub.Bus          (SUBSCRIBE, PSUBSCRIBE; native interest)
  ```

## Parity rules

1. Same commands, arities, reply shapes and error strings as `RedisServer.kt`, except the marked items.
2. Same config keys and defaults as `:30-37` and main's `broker/yaml-json-schema.json:1127-1141`.
3. Same feature flag name and gating as `Monster.kt:1326-1332`, `:1563-1564` and `:1627-1631`.
4. Same storage layout: `redis_db_number` in SQLite, Postgres and MongoDB, and main's runtime getter.
5. Same GraphQL fields, copied from main.
6. Same session records (`redis-<n>`, the `Information` JSON) and the same publisher ID
   `redis-server-<n>`.
7. Zero CGO. No Redis library in the broker binary.

## Behaviour to port

### Config

```go
type RedisServerConfig struct {
	Enabled     bool   `yaml:"Enabled"`
	Host        string `yaml:"Host"`
	Port        int    `yaml:"Port"`
	RequireAuth *bool  `yaml:"RequireAuth,omitempty"`
	DefaultDb   int    `yaml:"DefaultDb"`
	SetRetained bool   `yaml:"SetRetained"`
	PublishQos  int    `yaml:"PublishQos"`
	KeysLimit   *int   `yaml:"KeysLimit,omitempty"`
}
```

| Key | Default and rule | Kotlin |
|---|---|---|
| `Enabled` | false | `Monster.kt:1327` |
| `Host` | `0.0.0.0` | :31 |
| `Port` | 6379 (0 in YAML -> 6379) | :32 |
| `RequireAuth` | true. Accessor `EffectiveRequireAuth()`. | :33 |
| `DefaultDb` | 0, not validated (an unmapped DB fails per command) | :34 |
| `SetRetained` | false | :35 |
| `PublishQos` | 0. `EffectivePublishQos()` clamps to 0..2 instead of rejecting. | :36 |
| `KeysLimit` | `EffectiveKeysLimit()`: nil -> 10000. Values below 1 become 1, matching `coerceAtLeast(1)` (not "<= 0 -> 10000"). | :37 |

- **Features flag.** `Features.RedisServer`.
  - Edge defaults it to false, like every edge feature (`config.go:541`).
  - The server starts only when `Features.RedisServer && RedisServer.Enabled`. Otherwise it logs at
    Info: `redis server disabled by Features.RedisServer or RedisServer.Enabled=false` (`Monster.kt:1630`).
  - `enabledFeatures` reports `"RedisServer"` whenever `Features.RedisServer` is true, whatever `Enabled`
    says (`Monster.kt:1563-1564`).

### Archive group DB mapping

- **Getter (1:1).** `func (c ArchiveGroupConfig) EffectiveRedisDbNumber() *int` returns `RedisDbNumber` when
  it is set, else 0 when `Name == "Default"`, else nil (`ArchiveGroup.kt:210`). It is used everywhere main
  calls `getRedisDbNumber()`:
  - every store `Save` (main persists the getter: SQLite.kt:213, Postgres.kt:345, MongoDB.kt:362);
  - GraphQL `ArchiveGroupInfo` (`ArchiveGroupResolver.kt:246,349`; `ArchiveHandler.kt:831,869`);
  - the startup check, the per-command lookup and `INFO`;
  - `configsEqual` (`internal/archive/manager.go:572-600`), which compares effective values, so a running
    group always holds its current number.
- **Startup check (:47-55).** The server collects the effective numbers of the running groups
  (`archives.Snapshot()`, `manager.go:624`).
  - On a duplicate, `Start` fails with `Duplicate RedisDbNumber mapping: {<db>=[<group>, <group>]}`.
  - `Serve` returns that error and the broker exits, as main does (`Monster.kt:1847-1849`). A listen error
    is handled the same way (:61-69).
- **Per-command lookup (:1342-1344).** New `archive.Manager.ByRedisDB(db int) *Group` returns the running
  group whose effective number is `db`, live on every command.
  - **[edge-only]** If duplicates appear after start (only possible through an external config edit,
    since F10 rejects them in GraphQL), the lowest group name wins and a warning is logged once per DB.
    Main takes the first match in hash-map order.
- **Errors.**
  - `SELECT` of an unmapped DB: `ERR DB index is not mapped to an archive group` (:475-477).
  - Any other command while the selected DB is unmapped: `ERR selected DB is not mapped to an archive group`
    (:1336-1340).

### Storage (`redis_db_number`)

| Backend | Main | Edge change |
|---|---|---|
| SQLite | <ul><li>`redis_db_number INTEGER` in CREATE (`ArchiveConfigStoreSQLite.kt:48`).</li><li>Migration `ALTER TABLE ... ADD COLUMN redis_db_number INTEGER` when `PRAGMA table_info` lacks the column (:94-95).</li><li>Read: `row.getInteger` (:350).</li></ul> | `internal/stores/sqlite/archive_config_store.go`: <ul><li>Add the column to CREATE (:24-47).</li><li>Add the ALTER, tolerated by `isDuplicateColumnErr` like the existing migrations (:49-72).</li><li>Add it to both SELECT lists (:176, :196).</li><li>`rowToArchive` (:92) reads it through `sql.NullInt64`.</li><li>`Save` (:200) writes `EffectiveRedisDbNumber()`; nil becomes NULL.</li></ul> |
| Postgres | <ul><li>`redis_db_number INTEGER` in CREATE (`ArchiveConfigStorePostgres.kt:66`).</li><li>`ALTER TABLE ... ADD COLUMN IF NOT EXISTS redis_db_number INTEGER` (:80).</li><li>Read: `getObject(...) as? Int` (:170, :257).</li></ul> | `internal/stores/postgres/postgres.go`: <ul><li>`EnsureTable` (:1326-1388): same CREATE column and ALTER.</li><li>SELECTs (:1391, :1410).</li><li>`scanArchiveCfg` (:1413), nullable.</li><li>`Save` (:1482-1525) writes the effective value.</li></ul> |
| MongoDB | <ul><li>Field `redis_db_number`, read with `getInteger` (`ArchiveConfigStoreMongoDB.kt:340`).</li><li>Always written, as null when unset (:362).</li></ul> | `internal/stores/mongodb/mongodb.go`: <ul><li>`docToArchive` (:1437) reads it with a new nullable helper next to `getInt` (:150), accepting int32, int64 and double.</li><li>`Save` (:1471-1489) writes `"redis_db_number": cfg.EffectiveRedisDbNumber()`; a nil `*int` encodes as BSON null.</li></ul> |
| oastore (`MMQConfigs`, edge-only store) | none. The Java broker does not read the OA layout (`winccoa/doc/spec-winccoa-native.md:17`). | <ul><li>`internal/stores/oastore/stores.go:181-221` marshals the untagged `ArchiveGroupConfig`, so the existing convention gives Go field names. The key is `RedisDbNumber`, next to `DatabaseConnectionName`. No JSON tag is added.</li><li>`Save` (:211-213) stores the effective value.</li></ul> |

Same column name, type and migration as main, so either broker can open the same database.

### GraphQL

- **SDL.** In `internal/graphql/schema/schema.graphqls`, add `redisDbNumber: Int` after
  `databaseConnectionName` in `ArchiveGroupInfo` (:235), `CreateArchiveGroupInput` (:260) and
  `UpdateArchiveGroupInput` (:280). Then run `make gen`.
- **Resolvers** in `internal/graphql/resolvers/resolver.go`:
  - `archiveGroupInfoTo` (:1424) sets `RedisDbNumber: c.EffectiveRedisDbNumber()`, so Default shows 0.
  - `Create` (:2102) copies `input.RedisDbNumber`. `Update` (:2147) copies it when non-nil.
  - **[fix: both] F10.** Create and Update reject a number that is already the effective number of another
    configured group, with the message
    `RedisDbNumber <n> is already used by archive group [<name>]`.
  - `enabledFeatures` (:117-156) appends `"RedisServer"` when `Features.RedisServer` is true.

### Connection, auth and session

| Item | Behaviour | Kotlin |
|---|---|---|
| IDs | Session client ID `redis-<n>`. Publisher and ACL client ID `redis-server-<n>`. | :84, :1299, :1327 |
| Initial state | Authenticated when `!EffectiveRequireAuth() \|\| !UserManagement.Enabled`; the session is then registered at connect. `SelectedDb = DefaultDb`. | :80-81, :158-160 |
| Before auth | Only `AUTH`, `HELLO`, `PING`, `QUIT` and `CLIENT` run. Anything else: `NOAUTH Authentication required.` | :197-202 |
| `AUTH` | <ul><li>`AUTH <pass>` authenticates as user `default`; `AUTH <user> <pass>` as the named user.</li><li>Other arity: `ERR wrong number of arguments for 'auth' command`.</li></ul> | :289-298 |
| Credential check | <ul><li>`authCache.Validate(user, pass)` (`internal/auth/auth.go:179`): the synchronous, cached bcrypt check that MQTT CONNECT uses (`internal/broker/hook_auth.go:163`).</li><li>An empty username fails without calling it, because `Validate("")` means anonymous (`auth.go:180-187`). Main's store finds no user `""`.</li></ul> | :302 |
| `default` -> `Admin` fallback | <ul><li>When the user is `default` and the check fails, the same password is checked as `Admin`. This applies to AUTH and to HELLO.</li><li>"Fails" means invalid credentials. Main's stores return null alike for an unknown user, a wrong password and a disabled user (`UserStoreSqlite.kt:186-193`, `UserStorePostgres.kt:336`, `UserStoreMongoDB.kt:274`), so the `user.enabled` branches (:306, :314-317, :326, :334-336) never run. Edge's `Validate` folds disabled into false too (`auth.go:190-193`). So any failure triggers the fallback.</li></ul> | :320-348, :395-423 |
| Auth result | <ul><li>Success: username = the name that passed, reply `+OK` (HELLO sends its map instead), register the session.</li><li>Failure: `WRONGPASS invalid username-password pair or user is disabled.`</li><li>Passwords are never logged.</li></ul> | :303-312, :350, :180-195 |
| `HELLO [proto] [... AUTH u p ...]` | <ul><li>An integer proto above 2 gives `NOPROTO sorry, this protocol version is not supported.`; a non-integer proto is ignored.</li><li>`AUTH` is found anywhere, case-insensitively, and needs two arguments after it.</li><li>Without AUTH it replies and registers the session if already authenticated.</li></ul> | :362-442 |
| HELLO reply | A 14-element array: `server` `monstermq` `version` `<version.Version>` `proto` `:2` `id` `:<n>` `mode` `standalone` `role` `master` `modules` `*0` | :444-463 |
| ACL | <ul><li>User management off: allow.</li><li>Otherwise `authCache.AllowClient(username, "redis-server-<n>", topic, write)` plus the canonical alias check, as `hook_auth.go:184-192`. Plain `Allow` would ignore `%c` (`auth.go:237-254`).</li><li>An empty username is Anonymous (`auth.go:288-296`); main uses `"Anonymous"` (:1326).</li></ul> | :1324-1334 |
| Session record (S5) | <ul><li>`SetClient`: ClientID `redis-<n>`, NodeID, CleanSession true, Connected true, ClientAddress.</li><li>`Information` JSON: `RemoteAddress`, `LocalAddress`, `ProtocolVersion` `"REDIS"`, `SSL` false, `User` (only when set), `ClientName` (only when set), `SelectedDb`.</li><li>Then `SetConnected`. `SessionInfo.ProtocolVersion` is handled as S5 says.</li></ul> | :110-128 |
| Session update | Rewritten on `SELECT` and `CLIENT SETNAME`. | :130-142, :481, :493 |
| Close | <ul><li>Unsubscribe all bus subscriptions (S3.4) and `DelClient` if the session was registered.</li><li>S1 `Release("redis-server-<n>")` and S5 `Unregister`.</li><li>Dashboard removal goes through S5 `Register(id, closeFn)`, main's disconnect consumer (:162-169).</li></ul> | :1548-1562 |

### Connection commands

| Command | Behaviour | Kotlin |
|---|---|---|
| `PING [msg]` | <ul><li>More than 2 args: `ERR wrong number of arguments for 'ping' command`.</li><li>With msg: bulk msg. Without: `+PONG`.</li></ul> | :279-287 |
| `ECHO msg` | Exactly 2 args, else `ERR wrong number of arguments for 'echo' command`. | :211 |
| `QUIT` | `+OK`, flush, close. | :212-215 |
| `SELECT db` | <ul><li>Not 2 args: `ERR wrong number of arguments for 'select' command`.</li><li>Non-integer or below 0: `ERR invalid DB index`.</li><li>Unmapped: `ERR DB index is not mapped to an archive group`.</li><li>Otherwise `+OK` and a session update.</li></ul> | :465-482 |
| `CLIENT` | <ul><li>Fewer than 2 args: `ERR wrong number of arguments for 'client' command`.</li><li>`SETNAME [name]`: `+OK`; a missing name clears it.</li><li>`GETNAME`: bulk or nil.</li><li>`SETINFO`: `+OK`. `ID`: `:<n>`.</li><li>Anything else: `ERR unsupported CLIENT subcommand`.</li></ul> | :484-500 |
| `COMMAND` | `COUNT` -> `:30`; `INFO a b ...` -> one nil bulk per name; anything else -> `*0`. | :502-510 |
| `INFO` | A bulk string of CRLF lines: `# Server`, `redis_version:7.0.0`, `monstermq_version:<version.Version>`, `redis_mode:standalone`, `# Clients`, `connected_clients:1`, `# Keyspace`, then `db<n>:keys=0,expires=0,avg_ttl=0` per effective number of the running groups, sorted. | :512-525 |
| Unknown or empty | <ul><li>Unknown: `ERR unknown command '<NAME>'` (upper-cased).</li><li>No arguments: `ERR empty command`.</li><li>A handler failure: `ERR <message>`.</li></ul> | :267, :173-176, :272-276 |

### Keys and strings

A wrong arity gives `ERR wrong number of arguments for '<cmd>' command`, where `<cmd>` is the lower-case
command name. Store failures give `ERR <error text>`; main falls back to `<cmd> failed` (:571, :599, :1125,
:1145, :1168, :1196, :1221). The one exception is `GET`, which replies nil on a store failure (:542-545).

| Command | Behaviour | Kotlin |
|---|---|---|
| `GET k` | <ul><li>Arity 2. Denied read: `NOPERM no permission to read topic`.</li><li>Reply: the bulk payload or `$-1`.</li></ul> | :527-546 |
| `GETRANGE` / `SUBSTR k s e` | <ul><li>Arity 4. Non-integer bounds: `ERR value is not an integer or out of range`. Read check as GET.</li><li>Redis slice rules with negative indexes; a missing key gives an empty bulk.</li></ul> | :548-576, :1394-1406 |
| `MGET k...` | <ul><li>At least 2 args. Any denied read: `NOPERM no permission to read one or more topics`.</li><li>Reply: an array of bulk or nil.</li></ul> | :578-602 |
| `SET k v [...]` | At least 3 args; extra arguments are ignored. Write checks (below), publish, `+OK`. | :604-612 |
| `MSET k v ...` | Odd count, at least 3. Write checks per pair, publish, `+OK`. F8 changes the order. | :614-632 |
| `DEL k...` | <ul><li>At least 2 args. Any denied write: `NOPERM no permission to delete one or more topics`. Then the group lookup.</li><li>Counts the existing keys, calls `DelAll`, replies `:<count>`. Nothing is published.</li></ul> | :1105-1127 |
| `EXISTS k...` | At least 2 args. Read check as MGET. Reply `:<count>`. | :1129-1147 |
| `TTL` / `PTTL k` | Arity 2. Read check as GET. Reply `:-1` if the key exists, else `:-2`. | :1149-1171 |
| `SCAN cursor [MATCH p] [COUNT n]` | <ul><li>Options are read in pairs from index 2; MATCH defaults to `#`. The cursor is not validated.</li><li>Reply: `["0", [keys...]]`. F2 changes what COUNT does.</li></ul> | :1173-1199, :1346-1362 |
| `KEYS p` | Arity 2. Reply: an array of keys, stopping at KeysLimit. | :1201-1223 |
| `TYPE k` | Arity 2. Read check as GET. Reply: a simple string with the type, or `none`. | :1408-1421 |

Every key returned by `KEYS`/`SCAN` passes `canRead`; denied keys are skipped silently (:1185, :1215). The
pattern itself is not ACL-checked.

### Typed values (hash, set, list, zset, json)

- **Decode (:1476-1490).** A payload that is a JSON object with a string `type` and a `value` key is read as
  `(type, value)`. Anything else, including WinCC OA `{"time":...,"value":...}`, is type `string`.
- **Encode (:1492-1498).** Compact `{"type":"<t>","value":<v>}`, with keys in that order.
- **Missing key.** Reads use the default: hash and zset `{}`, set and list `[]`, json null (:1500-1507).
- **Type mismatch.** `WRONGTYPE Operation against a key holding the wrong kind of value` (:44, :1431,
  :1449-1451).
- **Read check.** Typed reads use `NOPERM no permission to read topic` (:1423-1427).
- **Writes (:1437-1459).** Write checks, read the current value, mutate, publish the envelope, reply.
  Serialized per key (F6).

| Commands | Behaviour | Kotlin |
|---|---|---|
| `HSET k f v [f v]...` | At least 4 args, even count. Values are stored as strings. Reply `:<added>`. | :634-651 |
| `HGET`, `HMGET`, `HGETALL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS` | <ul><li>Arities: HGET 3, HMGET at least 3, HGETALL 2, HEXISTS 3, HLEN 2, HKEYS 2, HVALS 2.</li><li>HGETALL returns a flat field/value array; HEXISTS returns `:1`/`:0`.</li></ul> | :653-746 |
| `HDEL k f...` | At least 3 args. Reply `:<removed>`. | :690-707 |
| `HSCAN k cursor [MATCH p] [COUNT n]` | <ul><li>At least 3 args. A non-integer or negative cursor: `ERR invalid cursor`.</li><li>Options are read from index 3. Fields are filtered by glob (F1), the first `cursor` are skipped, and `count` are taken (default: all).</li><li>The next cursor is `0` when the scan is done, else the new offset.</li></ul> | :748-780 |
| `SADD`, `SREM` | At least 3 args. Insertion order is kept. Reply `:<added>` / `:<removed>`. | :782-810 |
| `SMEMBERS`, `SISMEMBER`, `SCARD` | Arity 2, 3 and 2. | :812-838 |
| `LPUSH` / `RPUSH k v...` | At least 3 args. LPUSH inserts each value at the head, so `LPUSH k a b` gives `[b, a]`. Reply `:<len>`. | :840-851 |
| `LPOP` / `RPOP k` | Exactly 2 args. Reply: bulk or nil. | :853-863 |
| `LRANGE k s e`, `LLEN`, `LINDEX k i` | Non-integer bounds: `ERR value is not an integer or out of range`. Redis slice rules (:1513-1522); a negative index counts from the end. | :865-905 |
| `ZADD k score m [score m]...` | <ul><li>At least 4 args, even count. Every score is validated first: `ERR value is not a valid float`.</li><li>Reply `:<added>`.</li></ul> | :907-933 |
| `ZRANGE k s e [WITHSCORES]` | Sorted by score, then member. Integral scores print without `.0`. | :935-957, :1524-1532 |
| `ZREM`, `ZSCORE`, `ZCARD` | ZSCORE returns the formatted score as bulk, or nil. | :959-995 |
| `JSON.SET k path json` | <ul><li>Arity 4. A path other than `$` or `.`: `ERR only root JSON path '$' or '.' is supported`.</li><li>Invalid JSON: `ERR invalid JSON`. Reply: bulk `OK`.</li></ul> | :997-1015 |
| `JSON.GET k [path]`, `JSON.TYPE k [path]` | <ul><li>2-3 args, same path rule.</li><li>Reply: nil, WRONGTYPE, compact JSON, or a type name (`null`, `object`, `array`, `string`, `number`, `boolean`).</li></ul> | :1017-1038, :1082-1103, :1536-1546 |
| `JSON.DEL k [path]` | <ul><li>Denied write: `NOPERM no permission to delete topic`.</li><li>Missing key `:0`; a non-json value gives WRONGTYPE; otherwise `DelAll` and `:1`.</li></ul> | :1040-1080 |

### Write checks

Order for `SET`, `MSET`, `PUBLISH` and typed writes (`validateWrite`, :1314-1322):

| Step | Check | Error |
|---|---|---|
| 1 | Topic contains `+` or `#` (:1315) | `ERR topic must not contain wildcard characters` |
| 2 | **[edge-only] Native rule.** <ul><li>`SET`/`MSET` on a writable native value topic take the S2 path. S2 checks the ACL on the `/set` topic, skips steps 3-6 and forces retain to false.</li><li>Typed writes on any native-namespace topic are rejected (E4).</li></ul> | `ERR '<cmd>' is not supported on WinCC OA topics, use SET or MSET` |
| 3 | DB group (:1316) | `ERR selected DB is not mapped to an archive group` |
| 4 | Topic filter, through S4 `MatchesFilter`. An empty filter list matches everything (:1317-1319). | `ERR topic is outside selected archive group topic filter` |
| 5 | **[fix: both] F7.** The last-value store is read-only. Skipped for `PUBLISH`. | `ERR last value store of archive group [<name>] is read-only` |
| 6 | Publish ACL (:1320) | `NOPERM no permission to publish topic` |

`DEL` and `JSON.DEL` run the step-2 native rule first, then main's own order (ACL, then group;
:1110-1115, :1049-1054), then step 5.

"Read-only" means `Config().LastValReadOnly`, or a `LastValue()` of type `stores.MessageStoreWinCCOA`
(E10). A nil `LastValue()` (type NONE) is not read-only.

**Publish.** Implements `publishTopicUnchecked` (:1298-1312).

- Call S1 `ProtocolPublish("redis-server-<n>", msg)` with:
  - `TopicName` and `Payload`
  - `QoS = EffectivePublishQos()`
  - `IsRetain = SetRetained` (false on the S2 path)
  - `ClientID = "redis-server-<n>"`
  - `Time = now`
- **[edge-only] E5.** A returned error becomes `ERR <reason>`, with CR/LF replaced by spaces. Main publishes
  fire-and-forget.
- **[fix: both] F11.** Each accepted publish calls `collector.IncClientIn("redis-<n>")` (S5).
- UNS Modeler `.../set` topics are consumed by S1 step 1, unchanged.

### Nil `LastValue()` guards

A group with `LastValType: NONE` has no last-value store (`internal/archive/manager.go:445-446`). Main's
replies in that case:

| Command | Reply | Kotlin |
|---|---|---|
| `GET` | `$-1` | :538-541 |
| `GETRANGE` | empty bulk | :565-567 |
| `MGET` | an array of nil | :589-591 |
| `DEL`, `EXISTS`, `JSON.DEL` | `:0` | :1116-1118, :1140-1142, :1055-1057 |
| `TTL` | `:-2` | :1160-1162 |
| `SCAN` | `["0", []]` | :1177-1179 |
| `KEYS` | `*0` | :1208-1210 |
| Typed reads, `TYPE`, `JSON.GET` | treated as a missing key | :1463-1465 |
| Typed writes | computed from the default value and published | :1448-1457 |

### Pub/Sub

| Command | Behaviour | Kotlin |
|---|---|---|
| `PUBLISH ch msg` | Arity 3. Write checks without step 5, publish, `:0`. | :1225-1233 |
| `SUBSCRIBE ch...` / `PSUBSCRIBE p...` | <ul><li>At least 2 args.</li><li>Every filter is ACL-checked first; PSUBSCRIBE checks the converted filter. Denied: `NOPERM no permission to subscribe to one or more topics`.</li><li>A SUBSCRIBE channel is used verbatim as an MQTT filter.</li><li>Reply per filter: `[subscribe\|psubscribe, <requested>, :<total subscriptions>]`.</li><li>Delivery: `[message, topic, payload]` or `[pmessage, <requested pattern>, topic, payload]`.</li></ul> | :1235-1272 |
| `UNSUBSCRIBE` / `PUNSUBSCRIBE` | Main drops every subscription and replies `[<cmd>, nil, :0]`. Fixed by F3. | :1274-1286 |

Edge mechanics (S3):

- **Validate first.** `bus.Validate` runs on all filters after the ACL step. An error gives
  `ERR <bus error>`, e.g. `ERR subscription rejected for <filter>: <verdict>: <why>`
  (`internal/broker/native_bus.go:38-50`).
- **Subscribe.** `bus.SubscribeWithOverflow([]string{filter}, 1024)`, one subscription per filter, as main
  registers one listener per filter (:1245-1249).
- **Ordering.** The connection holds its write lock while it subscribes, starts the reader goroutine and
  writes the confirmation. Initial values (S3.3) therefore follow the confirmation.
- **Delivery ACL (F12, S7).** For a non-admin user while user management is on, each message is checked on
  its concrete topic before it is written. A denied message is dropped silently.
- **Counters.** Each delivered message calls `collector.IncClientOut("redis-<n>")` (F11). Messages dropped
  by F12 are not counted.
- **Overflow.** Logged. The server writes `ERR slow subscriber, messages dropped, closing connection` and
  closes (S3.2, E7).

### RESP framing

- **Parser (:1625-1689).**
  - A first byte `*` starts a multibulk: `*<n>` followed by n binary-safe `$<len>` bulk strings
    (:1644-1668).
  - Anything else is an inline command, split on whitespace with no quoting (:1670-1677).
- **Writers (:1564-1602).** Simple string, error, integer, bulk, null bulk `$-1` and arrays.
- **Protocol errors and size limits.** Changed by F9 and E9.
- **Connection model (F5, E9).**
  - One goroutine per connection reads, executes and replies in request order.
  - Replies go to a `bufio.Writer`, flushed when the reader has no buffered input left or 64 KiB are
    pending.
  - Subscription goroutines share the per-connection write mutex.
  - `TCP_NODELAY` is Go's default (main sets it explicitly, :57).

## Edge-only extensions and fixes

| ID | Change | Main today | Edge and decision | Tag |
|---|---|---|---|---|
| F1 | **Pattern conversion and glob.** See "Pattern algorithm" below. Used by `KEYS`, `SCAN`, `PSUBSCRIBE`, and `HSCAN MATCH` (glob only). | `"*"` -> `#`; keep the pattern if it contains `+`/`#`; else replace every `*` with `#` (:1373-1377). So `a/*/b` becomes `a/#/b` and `pump*` becomes `pump#`, both invalid filters, and `?`/`[...]` are taken literally. `globMatches` knows only `*` and `?` (:1379-1392). | Bug: valid Redis patterns return nothing or garbage. | [fix: both] |
| F2 | **SCAN COUNT is a hint, not a cap.** Reply all matches up to KeysLimit, cursor `0`. | Result capped at `min(COUNT, KeysLimit)` with cursor `0` (:1175, :1186). RedisInsight (COUNT 500) sees at most 500 keys and never asks for more. | Bug. KEYS has no COUNT; its KeysLimit cap is an accepted limitation (below). | [fix: both] |
| F3 | **UNSUBSCRIBE/PUNSUBSCRIBE honour their arguments**, as Redis does. <ul><li>`UNSUBSCRIBE` removes the named channel subscriptions, or all channel subscriptions without arguments; `PUNSUBSCRIBE` does the same for patterns.</li><li>Reply per name: `[unsubscribe\|punsubscribe, <name>, :<remaining total>]`, including names that were not subscribed.</li><li>With nothing to remove: one `[<cmd>, nil, :<remaining total>]`.</li></ul> | Both remove everything and reply `[<cmd>, nil, :0]` (:1274-1286). | Bug: clients that unsubscribe one channel lose all of them. | [fix: both] |
| F4 | **No duplicate subscriptions.** A channel or pattern that is already subscribed (same kind and name) is not added again; the confirmation repeats the unchanged count. | A new listener each time, so messages are delivered twice (:1245-1249). | Bug. | [fix: both] |
| F5 | **Replies in request order** for pipelined commands. | `handle` completes the queue promise right after dispatch (:269), before the async replies (:542, :569, :593, :1467, ...) are written. Pipelined replies can come back out of order. | Bug. | [fix: both] |
| F6 | **Per-key serialization of store writes.** <ul><li>`redisserver.Server` holds `[256]sync.Mutex`. The stripe is `fnv32a(group + "\x00" + topic) % 256`.</li><li>`SET`, `MSET`, typed writes, `DEL` and `JSON.DEL` lock the distinct stripes of their keys in ascending order, from the first read to the end of the publish or delete.</li><li>The reply is written after unlock.</li><li>Edge publishing is synchronous down to `LastValue().AddAll` (`mqtt/server.go:1089-1091` -> `hook_storage.go:432-435` -> `archive/group.go:153-165`), so the next locked reader sees the new value.</li></ul> | Read-modify-write without any lock (:1437-1459). The publish reaches the store asynchronously, so even two pipelined `HSET`s on one connection can lose an update. | Bug. | [fix: both] |
| F7 | **Read-only last-value store.** `SET`, `MSET`, typed writes, `DEL` and `JSON.DEL` are rejected with `ERR last value store of archive group [<name>] is read-only`. The text is from main's NATS KV put (`NatsJetStreamKv.kt:345-348`). `PUBLISH` is unaffected (D4). | No check. DEL and JSON.DEL delete from a read-only store, and typed writes publish values that are never stored (`MessageHandler.kt:372`). | Bug for DEL and typed writes. For SET and MSET the owner decided on 2026-10-17 to reject too (shared plan D6): a SET followed by a GET that returns the old value is the worse surprise. | [fix: both] |
| F8 | **MSET validates every pair before publishing any.** Publish errors (E5) can still stop it part-way; earlier pairs stay published. | Validates and publishes pair by pair (:619-630). | Bug. | [fix: both] |
| F9 | **Protocol errors.** <ul><li>Replies use Redis's texts: `ERR Protocol error: invalid multibulk length`, `ERR Protocol error: expected '$', got '<c>'`, `ERR Protocol error: invalid bulk length`, `ERR Protocol error: too big inline request`.</li><li>The connection is closed after the reply.</li><li>A negative bulk length inside a request is `invalid bulk length`.</li></ul> | <ul><li>The parser throws `IllegalArgumentException` inside the socket handler (:145-151, :1646-1662). The handler does not catch it, so the bad bytes stay in the buffer and every later read throws again.</li><li>`$-1` becomes an empty argument (:1655-1656).</li></ul> | Bug. | [fix: both] |
| F10 | **GraphQL rejects a duplicate redisDbNumber** on create and update: `RedisDbNumber <n> is already used by archive group [<name>]`. | Accepted silently. The next start fails the deploy and the broker exits (:51-54, `Monster.kt:1847-1849`). | Bug. | [fix: both] |
| F11 | **Per-session message counters (S5).** In: each accepted publish. Out: each delivered pub/sub message. | Nothing is counted. | Bug (S5). | [fix: both] |
| F12 | **Delivery-time read ACL (S7).** <ul><li>Applies whenever user management is on and the session user is not an admin, whatever `AclCheckOnSubscription` says.</li><li>Every SUBSCRIBE/PSUBSCRIBE delivery is checked on its concrete topic with the same check as `canRead`: `AllowClient(user, "redis-server-<n>", topic, false)` plus the canonical alias rule.</li><li>A denied message is dropped silently and not counted as out.</li></ul> | <ul><li>The subscribe check admits a wildcard filter that only partly overlaps a deny rule (`AclCache.kt:194-195`), relying on a delivery re-check that only `MqttClient.kt:1328-1337` does.</li><li>The Redis listener (:1249-1265) never re-checks, so `PSUBSCRIBE` leaks denied topics.</li><li>Edge with `AclCheckOnSubscription: false` admits wildcard filters without any rule check (`auth.go:233-236`), which would widen the leak.</li></ul> | Bug (S7). | [fix: both] |
| E1 | **WinCCOA group gets DB 1.** The archive plan provisions the `WinCCOA` group in `Manager.Load`, building the row with `archive.WinCCOAGroupConfig(root)` (archive plan C7). Before saving that row, `Load` sets `RedisDbNumber: 1` unless another loaded config already has effective number 1. In that case the field stays nil and an Info log names the owner. The value is persisted, not a runtime getter. | No WINCCOA group. | | [edge-only] |
| E2 | **WinCC OA fallback on DefaultDb.** When the selected DB is `DefaultDb`, reads pass that DB's group name and the key (or the converted filter) to `archive.Manager.ResolveGroupForTopic` (archive plan, "Contract for other plans") and use the group it returns. <ul><li>Covers `GET`, `GETRANGE`, `MGET` (per key), `EXISTS` (per key), `TTL`, `TYPE`, typed reads, `JSON.GET`/`JSON.TYPE`, `KEYS` and `SCAN`.</li><li>Writes are never redirected.</li><li>`KEYS *` maps to `#`, which is not a WinCC OA topic, so it stays in the DB's own group.</li></ul> | | | [edge-only] |
| E3 | **Native value writes (D4).** <ul><li>`SET`/`MSET` on a topic for which S2 `NativeValueWrite` returns ok publish to its `/set` topic.</li><li>The ACL is checked on that topic; group checks are skipped; retain is false.</li><li>The command verdict comes back as `ERR <reason>`.</li></ul> | | | [edge-only] |
| E4 | **Typed writes on native topics rejected.** <ul><li>While the native namespace is on, `HSET`, `HDEL`, `SADD`, `SREM`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `ZADD`, `ZREM`, `JSON.SET`, `JSON.DEL` and `DEL` on a key with `Names.Classify(key) != KindOther` reply `ERR '<cmd>' is not supported on WinCC OA topics, use SET or MSET`.</li><li>Why: WinCC OA owns these values. An envelope write is denied for value topics (`winccoanative/service.go:1075-1077`) and reserved for status and CNS topics. For `<root>/topics/...` it round-trips through a datapoint asynchronously, so a read-modify-write cannot be atomic.</li><li>Typed reads stay as they are: WinCC OA payloads decode as `string`, so they give WRONGTYPE.</li></ul> | | | [edge-only] |
| E5 | Publish errors are returned as `ERR <reason>`. | Fire-and-forget (:1311). | | [edge-only] |
| E6 | **KEYS/SCAN on a WINCCOA store** call only `LastValue().FindMatchingTopics(ctx, filter, yield)`. The S6 row limit bounds the `OpDpQuery` itself; the yield loop stops at KeysLimit. Redis adds no scan of its own. | | | [edge-only] |
| E7 | (P)SUBSCRIBE validates native filters (S3.1) and closes slow subscribers (S3.2). | Listeners are unbounded. | | [edge-only] |
| E9 | **Bounded I/O.** <ul><li>10 s write deadline per flush; a timeout closes the connection.</li><li>Inline lines up to 64 KiB, multibulk counts up to 1,048,576 (Redis's limits).</li><li>Bulk length up to `MaxMessageSize` when that is above 0, else 512 MiB.</li></ul> | Unbounded buffers. | | [edge-only] |
| E10 | A `WINCCOA` last-value store counts as read-only whatever its flag says. This matches the archive plan's X10, where `effectiveConfig` forces `LastValReadOnly` on. | | | [edge-only] |
| E11 | Deterministic DB lookup when duplicates appear after start (see "Archive group DB mapping"). | Hash-map order. | | [edge-only] |

Unavoidable small differences: an envelope whose `value` has the wrong JSON shape gives WRONGTYPE (main throws
a `ClassCastException` and replies `ERR <java message>`). Non-string hash or list values written by other
MQTT clients are rendered as their JSON text (main's `getString` throws). Address strings follow Go's format
(`[::1]:6379`).

### Pattern algorithm (F1)

`redisPatternToMqtt(p) (filter string, post func(topic string) bool)`:

1. `p == "*"` -> `#`, no post-filter (:1374).
2. `p` contains `+` or `#` -> `p` verbatim; it is an MQTT filter (:1375).
3. `p` has no unescaped `*`, `?` or `[` -> the unescaped `p` is an exact topic.
4. Split `p` on `/`. If every segment containing a metacharacter is exactly `*`:
   - a non-final `*` becomes `+`;
   - a final `*` becomes `#`, with a post-filter that keeps only topics with at least as many levels as `p`,
     so `a/*` does not return `a`.
5. Otherwise:
   - `prefix` = the segments before the first segment containing a metacharacter;
   - filter = `prefix + "/#"`, or `#` when the prefix is empty;
   - post-filter = a Redis glob match of `p` against the whole topic: `*` matches any run including `/`,
     `?` one character, `[abc]`, `[^abc]` and `[a-z]` a class, `\x` a literal `x`.

| Pattern | Filter | Post-filter |
|---|---|---|
| `*` | `#` | none |
| `a/+/c`, `a/#` | verbatim | none |
| `a\*` | `a*` (exact) | none |
| `a/*` | `a/#` | at least 2 levels |
| `a/*/c`, `*/temp` | `a/+/c`, `+/temp` | none |
| `a/b*` | `a/#` | glob (matches `a/b1/x`) |
| `sensor*` | `#` | glob |
| `a/[xy]/c?` | `a/#` | glob |

- The post-filter runs before `canRead` and before counting toward KeysLimit, and on every PSUBSCRIBE
  delivery.
- `HSCAN MATCH` uses the glob of step 5 on field names, without conversion.

### Accepted limitations (parity kept)

| Item | Why it stays | Kotlin |
|---|---|---|
| `PUBLISH` returns `:0` | The receiver count spans MQTT, bus and PeerLink subscribers and is not known synchronously. Clients rarely use it. | :1231 |
| KEYS/SCAN stop at KeysLimit silently | Bounds memory and latency on edge devices. Documented. | :1186, :1216 |
| SCAN is single-pass (cursor always `0`) | Valid Redis behaviour once COUNT no longer caps (F2). | :1192 |
| `INFO` reports `connected_clients:1` and `keys=0` | Cosmetic. | :519-522 |
| `COMMAND` returns placeholders | Cosmetic. | :502-510 |
| `SET` options (EX, PX, NX, XX), LPOP/RPOP count, ZADD flags | Topics have no TTL. Unsupported flags fail the arity or float checks or are ignored, as in main. | :605, :854, :914 |
| `JSON.SET` replies bulk `OK`; `JSON.TYPE` replies bulk | Common clients decode bulk and simple strings alike. | :1013, :1100 |
| HELLO ignores SETNAME and a non-integer proto | Cosmetic. | :363-370 |
| All commands work in subscribed mode | Harmless superset of RESP2. | :197-266 |
| MQTT JSON payloads with `type` and `value` keys are read as typed values | This is the envelope format itself. | :1476-1490 |

## Main-side tasks

The main side is tracked in [vogler75/monster-mq#203](https://github.com/vogler75/monster-mq/issues/203). That issue holds the fix plan for F1-F12, with the `RedisServer.kt` and
`ArchiveGroupResolver.kt` lines and the matching tests. It ships in the same release as this plan (D3). The E items
are edge-only and are not ported back. Main's `getRedisDbNumber` and its storage already match.

## Package layout

`internal/redisserver/`:

- **`server.go`.**
  - `Server`, `New(Deps)`, `Start()` (startup check, listen) and `Close()` (listener and all connections).
  - The connection registry and the F6 stripes.
- **`deps.go`.**

  ```go
  type Deps struct {
      Cfg              *config.Config
      Archives         *archive.Manager
      Bus              *pubsub.Bus
      Sessions         stores.SessionStore
      Auth             *auth.Cache
      Publish          func(clientID string, msg stores.BrokerMessage) error // S1 ProtocolPublish
      Release          func(clientID string)                                  // S1 InlinePublisher.Release
      Registry         interface{ Register(string, func()); Unregister(string) } // S5 ProtocolSessions
      NativeNames      func() winccoanative.Names                             // nil when the native namespace is off
      NativeValueWrite func(topic string) (string, bool)                      // S2, bound to NativeNames
      Collector        *metrics.Collector                                     // may be nil
      Logger           *slog.Logger
  }
  ```

  Plain function and interface fields keep `internal/broker` (which owns S1, S2 and S5) out of this
  package's imports, avoiding an import cycle.
- **`resp.go`.** Streaming parser (multibulk and inline, F9 and E9 limits) and reply writer.
- **`conn.go`.** Per-connection loop, write mutex and deadline, auth state, ACL helpers, session
  registration.
- **`groups.go`.** DB resolution, the E2 fallback, the write checks, the read-only rule and the native
  rule.
- **`glob.go`.** `redisPatternToMqtt` and the Redis glob matcher (F1).
- **Command files:**
  - `commands_conn.go`: AUTH, HELLO, PING, ECHO, QUIT, SELECT, CLIENT, COMMAND, INFO.
  - `commands_kv.go`: GET, GETRANGE/SUBSTR, MGET, SET, MSET, DEL, EXISTS, TTL/PTTL, SCAN, KEYS, TYPE.
  - `commands_struct.go`: H*, S*, L*, Z*, JSON.*, plus envelope encode/decode.
  - `commands_pubsub.go`: PUBLISH, SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE.

## Wiring

**`internal/config/config.go`**
- Add `RedisServer RedisServerConfig \`yaml:"RedisServer"\`` to `Config` (:416-474).
- Add `RedisServer bool \`yaml:"RedisServer"\`` to `FeaturesConfig` (:232-244), and `RedisServer: false`
  to the default literal (:541).
- Add Host and Port defaults in `Default()` (:499).
- Add the `Effective*` accessors from the config table.

**Config samples**
- `yaml-json-schema.json`:
  - Add a root `RedisServer` object copied from main's `broker/yaml-json-schema.json:1127-1141`.
  - Add `Features.RedisServer` (main :1180), with an edge default of false. The root object (:7) and
    Features (:263) have `additionalProperties: false`.
- `config.yaml.example`: add `RedisServer: false` to Features (:92-103) and a `RedisServer:` block with the
  defaults and `Enabled: false`.
- `scripts/deb/config.yaml`: add `RedisServer: false` to Features (:81-92).

**`internal/stores/interfaces.go`**
- Add `RedisDbNumber *int` and `EffectiveRedisDbNumber()` to `ArchiveGroupConfig` (:156-175).

**`internal/archive/manager.go`**
- Add `ByRedisDB(db int) *Group` next to `Snapshot`/`Get` (:624-638).
- Add the effective-number comparison to `configsEqual` (:572-600).
- E1: add `RedisDbNumber` to the archive plan's WinCCOA provisioning in `Load` (:98-132).

**`internal/broker/server.go`**
- Add a `redis *redisserver.Server` field to `Server` (:55-96) and to the struct literal (:745).
- In `build`, after S1 and S5 exist (next to `publishFn`, :562), create the server when
  `cfg.Features.RedisServer && cfg.RedisServer.Enabled`. Otherwise log the Info line from the config
  section.
- `NativeNames` follows the resolver wiring (:703-711):
  - `native.Names` (`winccoanative/service.go:1232`) when `native != nil`;
  - else `func() winccoanative.Names { return names }` (:192) when `nativeOn`;
  - else nil.
- `Serve` (:957): call `s.redis.Start()` after `startNative` (:958-962) and before the GraphQL start
  (:1053). On error return `fmt.Errorf("redis server start: %w", err)`.
- `stopPublishers` (:1125): call `s.redis.Close()` first, because it publishes through inline clients.

**GraphQL.** The SDL and resolver edits listed under "Behaviour to port, GraphQL".

## Phases

0. **Prerequisites.**
   - S1, S3, S4, S5 and S7 land with NATS phase 1; S2 lands with NATS phase 2.
   - If Redis is built first, it lands them exactly as the shared plan specifies, with no Redis-specific
     variants.
1. **Config, storage and GraphQL.**
   - Files: `config.go`, `yaml-json-schema.json`, `config.yaml.example`, `scripts/deb/config.yaml`,
     `stores/interfaces.go`, `sqlite/archive_config_store.go`, `postgres/postgres.go`, `mongodb/mongodb.go`,
     `oastore/stores.go`, `archive/manager.go` (`ByRedisDB`, `configsEqual`), `schema/schema.graphqls`,
     `resolvers/resolver.go` (info, create, update, F10, `enabledFeatures`).
   - Then run `make gen`.
2. **RESP core and connections.**
   - Files: `redisserver/{server.go,deps.go,resp.go,conn.go,commands_conn.go}` and the
     `broker/server.go` wiring.
   - Covers auth, sessions (S5), F5, F9 and E9.
3. **Keys and typed values.**
   - Files: `redisserver/{commands_kv.go,commands_struct.go,glob.go,groups.go}`.
   - Covers the write checks (S4, F7, F8), nil guards, F1, F2, F6 and F11-in.
4. **Pub/Sub.**
   - File: `redisserver/commands_pubsub.go`.
   - Covers S3, S7 (F12), F3, F4, E7 and F11-out.
5. **WinCC OA.** Starts after archive M2/M3 and S6 have landed.
   - Files: `redisserver/groups.go` (E2, E4), `redisserver/commands_kv.go` (E3, E6), `archive/manager.go`
     (E1).
6. **Docs and release.**
   - `doc/redis-server.md`: new; main has no page, so write config, commands, DB mapping, WinCC OA and an
     "Edge-only extensions and fixes" section.
   - `README.md`: add the feature line under "Feature flags" (:141).
   - `AGENTS.md`:
     - Add the Redis server to the subsystem list (:21-28). Keep the Redis client bridge off-limits.
     - Add `redisserver/` to the repository layout (:88-128).
   - Config samples.

The main-side fixes (monster-mq#203) run in parallel in the main repo and ship in the same release.

## Tests

All tests live in `test/integration/` and drive the broker through its real listeners (AGENTS.md:146-148).
`github.com/redis/go-redis/v9` is the client, with raw TCP where framing matters. Each test sets
`cfg.Features.RedisServer = true`, `cfg.RedisServer.Enabled = true` and a free port.

**`redis_test.go`**

- **Startup.**
  - Two groups with `RedisDbNumber` 0 (Default by the getter plus one explicit) make `Serve` fail with
    `Duplicate RedisDbNumber mapping`.
  - With the feature off, nothing listens and `enabledFeatures` lacks `RedisServer`. With the feature on,
    it is reported even when `Enabled` is false.
- **Auth.**
  - `GET` before AUTH gives `NOAUTH Authentication required.`. `PING` and `CLIENT ID` work before auth;
    `ECHO` does not.
  - `AUTH <adminpass>` gives `OK` through the Admin fallback. `AUTH default wrong`, a disabled user, and
    `AUTH "" <pass>` with anonymous enabled all give WRONGPASS.
  - `HELLO 3` gives NOPROTO. `HELLO 2 AUTH u p` returns the 14 elements.
  - With RequireAuth false, no AUTH is needed.
- **Session.**
  - After AUTH, `SessionStore` holds `redis-1` with the Information keys. `SELECT` and `CLIENT SETNAME`
    update them.
  - Closing deletes the record. GraphQL `removeSessions(["redis-1"])` closes the TCP connection.
  - Per-client in and out counters increase.
- **SELECT.**
  - An unmapped DB gives `ERR DB index is not mapped to an archive group`; `abc` gives `ERR invalid DB index`.
  - DB 0 reaches Default while its stored `redis_db_number` is NULL.
- **Strings.**
  - SET then GET round-trips, and an MQTT subscriber receives the message with client ID `redis-server-1`.
  - MGET returns nil for missing keys. GETRANGE handles negative indexes.
  - EXISTS works; TTL returns -1 and -2. DEL returns the count and removes the key.
  - TYPE returns `string`, `hash` and `none`.
  - Errors: SET with a wildcard, SET outside the group filter, and NOPERM for reads and writes. An ACL rule
    `c/%c/#` allows `c/redis-server-1/x`.
- **Read-only (F7).** On a group with `LastValReadOnly`, DEL, HSET and SET return the read-only error;
  PUBLISH works.
- **Typed values.**
  - HSET/HGET/HGETALL/HDEL/HSCAN with `MATCH f[12]`.
  - SADD keeps order. `LPUSH k a b` gives `[b a]`. LRANGE with negative indexes.
  - ZRANGE WITHSCORES ordering, with `1` and `1.5` formatting.
  - JSON.SET/GET/TYPE/DEL.
  - HGET on a string key gives WRONGTYPE.
  - An MQTT subscriber sees exactly `{"type":"hash","value":{...}}`.
- **Order and races.**
  - 1000 pipelined mixed commands get their replies in order (F5).
  - 20 connections times 50 `HSET k f<i> v` on one key give `HLEN == 1000` under `-race` (F6).
- **Patterns (F1, F2).**
  - KEYS `a/*` (without `a`), `a/*/c`, `a/b?`, `sensor*` (crosses levels), `a/[xy]/c`, `a\*` and `a/#`.
  - `SCAN 0 COUNT 10` returns all keys with cursor `0`.
  - KeysLimit 5 returns 5 keys; a configured 0 behaves as 1.
- **Pub/Sub.**
  - SUBSCRIBE receives MQTT publishes. PSUBSCRIBE `s/*` and `s/b?` receive pmessage with the requested
    pattern.
  - A duplicate SUBSCRIBE leaves the count unchanged and delivers once (F4).
  - `UNSUBSCRIBE a` keeps the pattern subscription, with the F3 reply counts. UNSUBSCRIBE without
    arguments and PUNSUBSCRIBE work as F3 describes.
  - PUBLISH returns `:0` and reaches MQTT.
  - 5000 messages to a client that never reads close the connection (E7).
  - F12: a non-admin user allowed `s/#` with a deny rule on `s/secret/#` subscribes with `PSUBSCRIBE s/*`.
    A publish to `s/a` is delivered; a publish to `s/secret/b` is not, and does not count as out. The
    test runs with `AclCheckOnSubscription` both true and false. An admin user receives both.
- **Protocol (raw TCP).**
  - Inline `PING`; `QUIT`.
  - `*1\r\n$-1\r\n` gives `ERR Protocol error: invalid bulk length` and a close. A bad bulk terminator and
    a 70 KiB inline line also close the connection.
- **Storage and GraphQL.**
  - The SQLite `archiveconfigs` table has `redis_db_number INTEGER`.
  - `redisDbNumber` round-trips nil, 0 and 3 through create, update and `archiveGroups`. Default shows 0
    and is persisted as 0 after a save.
  - A duplicate is rejected with the F10 message.
  - The same checks run on Postgres and MongoDB when `EDGE_TEST_POSTGRES_DSN` / `EDGE_TEST_MONGODB_URI`
    are set, as `internal/stores/*/kafka_metrics_test.go` gates them.
- **Config.** `config_schema_test.go` passes with the new keys.

**`winccoa_redis_test.go`** (simhost through `startNative`, `winccoa_native_test.go:83`)

- **DB 1.** The WinCCOA group gets DB 1 (E1); `SELECT 1` works.
- **Reads.**
  - `GET winccoa/tags/Pump1/speed` returns `{"time":...,"value":...}` on DB 1, and on DB 0 through the
    fallback (E2).
  - MGET mixes native and plain keys.
  - `TYPE` returns `string`; `HGET` gives WRONGTYPE.
- **KEYS and SCAN.** `KEYS winccoa/tags/Pump1/*` returns the DPE topics. `KEYS *` on DB 1 with more DPEs
  than the S6 row limit returns at most that limit (E6).
- **SET (E3).**
  - `SET winccoa/tags/Pump1/speed 42.5` sets 42.5 in simhost, also with `SetRetained: true`.
  - An unknown DPE gives `ERR <verdict reason>`; an ACL that denies `.../set` gives NOPERM.
- **PUBLISH.**
  - `PUBLISH winccoa/tags/Pump1/speed/set 43` returns `:0` and sets 43.
  - `PUBLISH winccoa/tags/Pump1/speed 1` gives `ERR native value topics are broker-owned; publish to .../set`.
- **Rejected writes (E4).** `HSET` and `DEL` on `winccoa/tags/Pump1/speed` give
  `ERR 'hset' is not supported on WinCC OA topics, use SET or MSET` (and `'del'`). DEL of a non-native key
  on DB 1 gives the read-only error.
- **Subscriptions.**
  - `SUBSCRIBE winccoa/tags/Pump1/speed` gets the initial value after the confirmation, then a live
    change.
  - `PSUBSCRIBE winccoa/tags/Pump1/*` works.
  - An invalid native filter gives `ERR subscription rejected for ...`.
  - UNSUBSCRIBE removes the simhost connection.

## Acceptance criteria

- **Clients.** redis-cli, go-redis and RedisInsight can connect, AUTH, SELECT, browse with SCAN, and read
  and write keys and typed values.
- **Shared database.** One SQLite, Postgres or MongoDB database opened by main and by edge shows the same
  `redisDbNumber` values. Default shows 0 in both.
- **Dashboard.** On edge with `Features.RedisServer: true` it shows the Redis DB field and saves it.
- **Schema.** Edge's SDL is still a strict subset of main's.
- **Tests.** Every case above passes. `go vet` and `make test-race` are clean. `config_schema_test.go`
  passes.
- **Main.** monster-mq#203 is merged in the same release. For F1-F12 both brokers give identical
  replies.

## Risks

- **Concurrent MQTT writes.** MQTT publishers writing the same topic are not covered by the F6 stripes.
  Accepted: only Redis read-modify-write is serialized.
- **S6 before post-filter.** The S6 row limit applies before the glob post-filter, so a selective glob over
  a broad WinCC OA prefix can miss keys. Documented.
- **Store blocking.** WinCC OA `DpGet`/`DpQuery` calls block only their own connection. Each command runs
  under a context that is cancelled when the connection closes.
- **Group restarts.** Changing `redisDbNumber` restarts the group, like any other config change
  (`configsEqual`), so a MEMORY last-value store loses its values.
- **Clearing the number.** Edge update inputs are pointers, so `redisDbNumber: null` cannot clear the
  number. The dashboard never sends null either (`archive-group-detail.js:479`). Clearing needs delete and
  recreate.
- **Unstored typed writes.** Typed writes on a RetainedOnly group with `SetRetained: false` are never
  stored, as in main. Typed writes to UNS Modeler `.../set` topics are consumed by S1 step 1 and not stored.
- **Security.** Plain TCP with passwords in clear text and no TLS, as in main. The doc recommends
  `Host: 127.0.0.1` or a firewall.
- **Port clash.** Port 6379 clashes with a local Redis. The broker then exits at start, as main does.

