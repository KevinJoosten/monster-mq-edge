# Redis Protocol Server

The Redis protocol server exposes an in-memory key-value and pub/sub interface
using the Redis RESP2 protocol. It is the Go port of MonsterMQ Core's
`RedisServer.kt`, allowing standard Redis clients, dashboards, and CLIs to interact
with the broker without an MQTT client library.

Enable it under `Features`:

```yaml
Features:
  RedisServer: true

RedisServer:
  Enabled: true
  Host: "0.0.0.0"
  Port: 6379
  RequireAuth: false
  DefaultDb: 0
  SetRetained: false
  PublishQos: 0
  KeysLimit: 10000
```

`enabledFeatures` reports `"RedisServer"` whenever `Features.RedisServer` is true.

## Configuration

| Key | Default | Description |
|---|---|---|
| `Enabled` | `false` | Enables the Redis protocol listener when `Features.RedisServer` is active. |
| `Host` | `"0.0.0.0"` | Listen address for Redis client connections. |
| `Port` | `6379` | TCP listen port. |
| `RequireAuth` | `false` | If true, clients must authenticate via `AUTH` or `HELLO` before executing commands. |
| `DefaultDb` | `0` | Default database index mapped at connection start. |
| `SetRetained` | `false` | Sets MQTT retain flag on published messages. |
| `PublishQos` | `0` | QoS level for published messages (0, 1, or 2). |
| `KeysLimit` | `10000` | Maximum number of keys returned by `KEYS` and `SCAN`. |

## Archive group DB mapping

Redis databases (numeric DB indexes 0, 1, 2, ...) map to configured `ArchiveGroup`s:

- Each archive group has an optional `RedisDbNumber` (GraphQL: `redisDbNumber: Int`).
- The `Default` archive group defaults to effective DB number `0` when unset.
- When an embedded WinCC OA native namespace is present, group `WinCCOA` is provisioned with DB number `1` unless already claimed.
- `SELECT <db>` switches the active database. Attempting to select an unmapped database returns `ERR DB index is not mapped to an archive group`.
- Duplicate DB numbers across archive groups are rejected at startup and via GraphQL validation.

## Authentication and sessions

- **Initial state:** Authenticated if `RequireAuth` is false or user management is disabled.
- **Commands allowed before auth:** `AUTH`, `HELLO`, `PING`, `QUIT`, `CLIENT`. All other commands return `NOAUTH Authentication required.`.
- **AUTH:** Supports `AUTH <password>` (authenticates as user `default`) and `AUTH <username> <password>`.
- **Admin fallback:** If user is `default` and validation fails, the password is automatically verified against `Admin`.
- **HELLO:** Supports RESP2 `HELLO [proto] [AUTH user pass] [SETNAME name]`.
- **Sessions:** Registered in `SessionStore` as client ID `redis-<n>` with JSON metadata (`RemoteAddress`, `LocalAddress`, `ProtocolVersion: "REDIS"`, `User`, `ClientName`, `SelectedDb`). Terminating the session via GraphQL `removeSessions(["redis-<n>"])` closes the TCP connection.

## Supported commands

### Connection commands
- `AUTH [user] pass`
- `HELLO [proto] [...]`
- `PING [message]`
- `ECHO message`
- `QUIT`
- `SELECT db`
- `CLIENT SETNAME name`, `CLIENT GETNAME`, `CLIENT SETINFO`, `CLIENT ID`
- `COMMAND`, `COMMAND COUNT`, `COMMAND INFO`
- `INFO`

### Keys and strings
- `GET key`
- `GETRANGE key start end`, `SUBSTR key start end`
- `MGET key...`
- `SET key value`
- `MSET key value [key value...]`
- `DEL key...`
- `EXISTS key...`
- `TTL key`, `PTTL key`
- `SCAN cursor [MATCH pattern] [COUNT hint]`
- `KEYS pattern`
- `TYPE key`

### Typed values (Envelope format)
Values for hashes, sets, lists, sorted sets, and JSON are stored in compact JSON envelope format: `{"type":"<t>","value":<v>}`:
- **Hash:** `HSET`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HSCAN`
- **Set:** `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`
- **List:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`
- **Sorted Set:** `ZADD`, `ZRANGE [WITHSCORES]`, `ZREM`, `ZSCORE`, `ZCARD`
- **JSON:** `JSON.SET key path json`, `JSON.GET key [path]`, `JSON.TYPE key [path]`, `JSON.DEL key [path]` (root paths `$` and `.` supported)

### Pub/Sub
- `PUBLISH channel message`
- `SUBSCRIBE channel...`
- `PSUBSCRIBE pattern...`
- `UNSUBSCRIBE [channel...]`
- `PUNSUBSCRIBE [pattern...]`

## WinCC OA integration

When the WinCC OA native namespace is active:
- **Value writes:** `SET` and `MSET` to a tag value topic (e.g. `winccoa/tags/<dp>/<elem>`) automatically route to the command topic `.../set` with retain forced to false.
- **Typed write rejection:** Typed writes (`HSET`, `HDEL`, `SADD`, `SREM`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `ZADD`, `ZREM`, `JSON.SET`, `JSON.DEL`) and `DEL` on native topics return `ERR '<cmd>' is not supported on WinCC OA topics, use SET or MSET`.
- **DefaultDb fallback:** Reads on `DefaultDb` (DB 0) dynamically resolve to the appropriate group (including WinCC OA tag values) via `ResolveGroupForTopic`.

## Edge-only extensions and fixes

- **F1 (Pattern conversion & glob):** Accurate translation between Redis glob patterns and MQTT wildcard subscriptions (`*` -> `#`, segment wildcard `+`, character classes and escaping).
- **F2 (SCAN COUNT hint):** `SCAN` treats `COUNT` as a hint rather than a cap and returns all matches up to `KeysLimit` with cursor `0`.
- **F3 (UNSUBSCRIBE arguments):** `UNSUBSCRIBE` and `PUNSUBSCRIBE` honor target arguments, removing only specified subscriptions and reporting remaining counts.
- **F4 (No duplicate subscriptions):** Subscribing to the same channel or pattern multiple times preserves subscription state and does not create duplicate listeners.
- **F5 (Pipelined request ordering):** Sequential processing per connection ensures replies always match request order.
- **F6 (Store write stripe locking):** Writes use 256 hash stripes (`fnv32a(group + "\x00" + topic) % 256`) locked in ascending order to prevent race conditions during read-modify-write cycles.
- **F7 (Read-only group protection):** `SET`, `MSET`, `DEL`, and typed writes to read-only archive groups are rejected with an explicit error.
- **F8 (MSET atomicity):** Validates all topic-value pairs before publishing any.
- **F10 (Duplicate DB rejection):** GraphQL group creation and update reject duplicate `redisDbNumber` values.
- **F11 (Client message metrics):** Publishes and pub/sub deliveries increment per-client metrics counters (`collector.IncClientIn` and `IncClientOut`).
- **F12 (Delivery-time ACL checks):** Pub/sub deliveries re-verify read permissions on concrete topic names for non-admin users.
