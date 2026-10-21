# Plan: in-memory rolling-window archive in Apache Arrow layout

**Status:** proposed (2026-10-21), awaiting owner sign-off.
**Package:** `internal/stores/arrowmem/`. **[edge-first]**, Core needs the enum value (see below).
**Depends on:** nothing. Uses the existing archive group pipeline (`internal/archive/`).

> **GraphQL interface change required: flagged up front.**
> The dashboard picks an archive type from the `MessageArchiveType` enum
> (`internal/graphql/schema/schema.graphqls:16`, Core `schema-types.graphqls:38`). A new archive type
> can't be selected without a new enum value. The proposal is to add **one enum value, `MEMORY`**, to
> `MessageArchiveType` in both repos. Nothing else in the schema changes: the window reuses
> `archiveRetention: String`, reads go through the existing `archivedMessages` and `aggregatedMessages`
> queries. Alternatives are under "GraphQL and parity" below. No SDL or resolver is touched until the
> owner signs off on decision A1.

## Goal

An archive group can keep its history **in RAM only**, as a rolling window: every message older than
`now - window` drops out. Typical windows are 10s, 30s, 1m and 5m for high-frequency data, and longer
(15m, 1h, ...) for slower topics. The write path must be as fast as possible: it runs inline in
`OnPublished` for every matching message, so the target is a few tens of nanoseconds per message,
zero allocations in steady state and no lock contention between topics.

The data is held in the Apache Arrow columnar layout, so reads and aggregations run over flat
columns, and a whole window can be exported as an Arrow IPC stream with no copy, ready for pyarrow,
pandas, Polars, DuckDB or any other Arrow consumer.

## Non-goals (v1)

- Durability. A restart starts with an empty window (decision A5 covers an optional snapshot).
- Cluster-wide reads. Each broker answers from its own memory, like the `MEMORY` last-value store.
- Arrow Flight / gRPC. The export is a plain HTTP response (decision A4).
- Replacing the persistent archives. A group still has one archive type; to keep a short hot window
  *and* a long history, configure two groups with the same topic filters.

## Why not use the Arrow builders on the write path

`arrow-go` builders (`array.BinaryBuilder`, `array.Int64Builder`, ...) cost per call: interface
dispatch, reference counting, buffer growth checks and an allocator round trip. For the hot path we
write directly into plain Go slices that **already have the Arrow memory layout** (int64 values,
int32 offsets plus a byte data buffer for binary, uint8 for QoS). When a chunk is sealed those slices
are wrapped as Arrow buffers with `memory.NewBufferBytes` and `array.NewData`, which copies nothing.
The `arrow-go` dependency is only imported by the export code, never by the append path.

## Data model

### Series and chunks

```
Store
  shards [64]                      // FNV-1a of topic, same scheme as memory.MessageStore
    map[topic]*series
series                             // one per topic
  mu      sync.Mutex               // only writers of the same topic contend
  sealed  []*chunk                 // ring, oldest first, immutable
  active  *chunk                   // being appended to
  clients []string                 // small per-series dictionary for client IDs
chunk                              // Arrow layout, fixed capacity
  ts      []int64                  // unix nanos, UTC
  off     []int32                  // len n+1, binary offsets
  data    []byte                   // payload bytes
  qos     []uint8
  client  []int32                  // index into series.clients
  n       int
  minTs, maxTs int64
  sorted  bool                     // false once an out-of-order ts arrives
  refs    atomic.Int32             // readers holding the chunk
  derived sync.Map                 // lazily built value columns, see "Value columns"
```

Columns per row: `time` (timestamp[ns, UTC]), `payload` (binary), `qos` (uint8), `client_id`
(dictionary<int32, utf8>). In the exported record batch `topic` is added as a dictionary column, so a
multi-topic export is one stream.

MQTT 5 properties (content type, user properties, correlation data) are **not** kept in v1: they are
not part of `ArchivedMessage` and none of the read APIs returns them. Decision A6 if they are needed.

### Chunk sealing

The active chunk is sealed and a new one started when either:

- it holds `ChunkRows` rows (default 4096), or
- its time span exceeds `window / 16`.

The time rule keeps eviction granularity at about 6% of the window, so a 10s window holds 10.0 to
10.6 seconds of data, never more. Reads always clip to `[now - window, now]`, so the result is exact
regardless of granularity.

### Eviction (the rolling window)

- **On append:** after writing, if `sealed[0].maxTs < now - window`, the oldest chunk is dropped. This
  is one compare per message and keeps busy topics trimmed without any background work.
- **Janitor:** one goroutine per store, ticking every `max(window / 16, 100ms)`, trims series that
  stopped publishing and removes series that became empty, so topic churn doesn't grow the maps.
- `PurgeOlderThan` drops chunks by `maxTs` and is kept for the interface. `RunRetention`
  (`internal/archive/retention.go`) skips `MEMORY` archives: the window is enforced by the store.

### Recycling, zero allocations

Dropped chunks go back to a `sync.Pool` per capacity class once `refs` is zero. The payload `data`
buffer starts at `ChunkRows * 32` bytes and grows by doubling; a recycled chunk keeps its grown
buffer. In steady state an append copies the payload into a buffer that already exists and allocates
nothing. A chunk whose `data` grew above 4x the class size is not pooled, so one burst of large
payloads doesn't pin memory forever.

### Memory budget

Edge devices have little RAM, so the store has a hard budget across all `MEMORY` archive groups:

- `MaxMemory` (default 256 MB): bytes held by all chunks (accounted on chunk allocation and growth with
  one atomic add, not per row).
- When a new chunk would exceed the budget, the writer drops the oldest chunk **of its own series** and
  reuses it. That topic's effective window gets shorter; other topics are unaffected. A counter
  `droppedRows` and a rate-limited warning (once per minute per group) report it.
- `MaxTopics` (default 100000) per group: new topics past the limit are not archived, with a counter.
- `MaxPayloadBytes` (default 0 = no limit): larger payloads are skipped, with a counter.

Sizing guide for the docs (24-byte payload, about 37 bytes per row with offsets and columns):

| Rate per topic | Window | Rows per topic | RAM per topic |
|---|---|---|---|
| 1 kHz | 10 s | 10,000 | ~0.4 MB |
| 1 kHz | 1 m | 60,000 | ~2.2 MB |
| 10 kHz | 30 s | 300,000 | ~11 MB |
| 10 kHz | 5 m | 3,000,000 | ~110 MB |
| 1 Hz | 1 h | 3,600 | ~0.13 MB |

## Write path

Today `Group.Submit` (`internal/archive/group.go`) appends to `pending` under the group mutex, a
ticker flushes every 250 ms through `AddHistory`, or via the store-and-forward queue. For `MEMORY` this
is pure overhead, so:

1. `arrowmem.Archive` implements a fast interface, mirroring `fastSingleMessageStore`:
   ```go
   type fastArchive interface{ AppendOne(m stores.BrokerMessage) }
   ```
2. `NewGroup` detects it. `Submit` calls `AppendOne` directly and returns: no `pending` slice, no
   group mutex, no flush goroutine wake-ups. `AddHistory` still works (loops `AppendOne`) for callers
   that batch.
3. `QueueType` is ignored for `MEMORY` (there is nothing to forward to); a set value is logged once at
   group start.
4. `AppendOne` steps: shard lookup under `RLock` (create under `Lock` on first message of a topic),
   `series.mu.Lock`, write ts/qos/client index, copy payload into `data`, append offset, seal if needed,
   evict head if expired, unlock. The payload must be copied: the caller's slice can point into a
   connection read buffer (see the comment on `memory.MessageStore.PutOne`).
5. Client ID: compare with the last index used by the series first (one publisher per topic is the
   common case), then the small dictionary; clone the string only when it is new.
6. Timestamp: `msg.Time` when set, else `time.Now()`. Replicas from PeerLink can arrive out of order;
   that clears `sorted` on the chunk, and readers then filter linearly instead of binary-searching.

## Read path (`stores.MessageArchive`)

| Method | Implementation |
|---|---|
| `GetHistory(topicFilter, from, to, limit)` | Topic filter may contain `+`/`#` (`archivedMessages` takes a `topicFilter`): exact topic is one shard lookup, wildcards scan the series keys with the existing `matchTopic`. Per series: under the lock, take refs on the sealed chunks in range and copy the active chunk's rows `[0:n)` in range; unlock; binary-search `ts` in sorted chunks; build `ArchivedMessage` rows newest first, the same order the SQL archives return. |
| `GetAggregatedHistory` | Columnar loop per chunk: bucket index `(ts - start) / interval`, AVG/MIN/MAX/COUNT on a value column (below). Same `AggregatedResult` shape and `stores.AggregatedColumnName` naming as the SQL archives. |
| `GetArchiveStats` | `minTimestamp` = oldest `minTs`; daily counts from row counts per chunk (a window rarely spans midnight). |
| `PurgeOlderThan` | Drops chunks, returns row count. |
| `EnsureTable`, `Close` | No-op; `Close` stops the janitor and frees the chunks. |

### Value columns

Aggregation needs numbers, the payload is bytes. Parsing on every append would put `strconv` on the
hot path, so value columns are **derived lazily per sealed chunk** and cached in `chunk.derived`:

- key `""`: payload parsed as a plain number (`strconv.ParseFloat` on trimmed bytes), null if not
  numeric;
- key `<field>`: the JSON field path from the `fields` argument, parsed with the same path rules the
  other archives use;
- built once with a `sync.Once` per key; sealed chunks never change, so the cache stays valid until
  the chunk is dropped. The active chunk is parsed on each read (it is at most `ChunkRows` rows).

### Read concurrency

Writers never wait for readers: sealed chunks are immutable and protected by `refs`, so a long export
holds references while new data keeps flowing into other chunks. The only shared lock is the series
mutex, held by a reader only to take references and copy the active rows.

## Arrow IPC export (new REST endpoint, not GraphQL)

```
GET /api/v1/arrow/{group}?topicFilter=sensors/%23&start=...&end=...&fields=value,temp
Accept: application/vnd.apache.arrow.stream
```

- Registered in `internal/restapi/handler.go` inside the authenticated group, with the same auth and
  subscribe-ACL check as the history read on `GET /api/v1/topics/*`; listed in `openapi.yaml`.
- Writes one Arrow IPC stream: schema `topic`, `time`, `payload`, `qos`, `client_id`, plus one
  nullable `float64` column per requested field (`value` = plain-number payload). Each sealed chunk
  becomes one record batch built from the chunk's buffers without copying; the active chunk is copied
  once. References are released after the batch is written.
- Optional `compression=zstd|lz4` (IPC body compression) for slow links; off by default because it
  costs CPU on the edge device.
- Only for groups with `archiveType: MEMORY` in v1 (404 otherwise). Exporting the SQL archives as Arrow
  is a possible follow-up.

Consumer example for the docs:

```python
import pyarrow.ipc as ipc, requests
r = requests.get("http://edge:4000/api/v1/arrow/Hot?topicFilter=line1/%23", stream=True, headers=auth)
df = ipc.open_stream(r.raw).read_pandas()
```

## Configuration

Per group (existing fields, no new GraphQL or DB columns):

| Field | Meaning for `archiveType: MEMORY` |
|---|---|
| `archiveRetention` | The rolling window: `10s`, `30s`, `1m`, `5m`, `1h`, ... (parsed by `parseDuration`). Empty = `MemoryArchive.DefaultWindow`. Must be between 1s and `MemoryArchive.MaxWindow`. |
| `topicFilters`, `retainedOnly`, `lastValType`, `payloadFormat` | Unchanged. `lastValType: MEMORY` + `archiveType: MEMORY` is the all-RAM group. |
| `queueType`, `queueSize`, `bulkSize`, `bulkTimeoutMs`, `databaseConnectionName` | Ignored. |

Global tuning in YAML (not per group, so the schema and the config tables stay as they are):

```yaml
MemoryArchive:
  DefaultWindow: 1m
  MaxWindow: 24h
  MaxMemory: 256MB        # all MEMORY archive groups together
  MaxTopics: 100000       # per group
  MaxPayloadBytes: 0      # 0 = no limit
  ChunkRows: 4096
```

Checklist: `internal/config/config.go`, `yaml-json-schema.json`, `config.yaml.example`,
`scripts/deb/config.yaml` (commented example only, the feature is off unless a group uses it).

## GraphQL and parity

Options evaluated for selecting the new archive type:

1. **Add `MEMORY` to `MessageArchiveType` in Core and edge (recommended).** Mirrors the existing
   `MessageStoreType.MEMORY` for last values. Impact:
   - edge: SDL enum value, `make gen`, the mapping in `internal/graphql/resolvers/resolver.go:420`,
     `stores.ArchiveMemory`, `RequiredDatabaseConnectionTypes` (needs none).
   - Core: enum value in `schema-types.graphqls` and `stores/IMessageArchive.kt`. This is needed even
     if Core never implements the store: Core reads archive configs with
     `MessageArchiveType.valueOf(...)` (`ArchiveGroupResolver.kt:714`, `ArchiveGroup.kt:719`, the config
     stores), which throws on an unknown value, so a shared config database with a `MEMORY` group
     would break Core. Core can implement it later or fail that one group at deploy with a clear error.
   - dashboard: one more entry in the archive type dropdown and a hint that `archiveRetention` is the
     window. An older dashboard shows the value as unknown but keeps working for other groups.
2. **YAML-only groups**, outside the config store and GraphQL. No schema change, but the groups are
   invisible to the dashboard, can't be created or edited there, and `archivedMessages` with
   `archiveGroup: "<name>"` would need a special case. Rejected: it splits archive management in two.
3. **Reuse an existing value** (for example `NONE` plus a magic retention string). Rejected: it changes
   the meaning of a shared value and would confuse Core.

Storage parity: no new tables or columns. The `archive_type` column stores the string `MEMORY`.

Known limit of the shared API: `aggregatedMessages` takes `AggregationInterval`, whose smallest value is
`ONE_MINUTE`. For a 10s window that yields one bucket. Sub-minute aggregation is available through the
Arrow export (client-side) and the Prometheus range API (`internal/prometheus/api.go`, which reads raw
history for short ranges). Adding `ONE_SECOND`/`TEN_SECONDS` to the enum is a separate GraphQL change
and not part of this plan (decision A7).

## Metrics

Exposed on the Prometheus `/metrics` endpoint (new metric names, no GraphQL change), per group:
`monstermq_memarchive_rows`, `_bytes`, `_topics`, `_appended_total`, `_dropped_rows_total{reason=budget|topics|payload}`,
`_evicted_rows_total`. The existing `ArchiveGroupMetrics` keeps `messagesOut`; `bufferSize` stays 0
since nothing is buffered.

## Performance targets and how they are checked

Benchmarks in `internal/stores/arrowmem/*_test.go` (no listeners, plain `go test -bench`):

| Benchmark | Target (x86 desktop) | Target (Pi 5) |
|---|---|---|
| `AppendOne`, 1 topic, 24-byte payload | <= 40 ns/op, 0 allocs/op | <= 150 ns/op |
| `AppendOne`, 1000 topics, `RunParallel` | >= 20 M msg/s aggregate | >= 3 M msg/s |
| `Group.Submit` with `MEMORY` vs `NONE` | <= +50 ns/op | <= +200 ns/op |
| `GetHistory`, 1 topic, 100k rows in window, limit 1000 | <= 100 us | <= 500 us |
| `GetAggregatedHistory`, 1M rows, numeric payload (cold / cached) | <= 30 ms / <= 5 ms | <= 150 ms / <= 25 ms |
| Arrow export, 1M rows | >= 1 GB/s to `io.Discard` | >= 200 MB/s |

The numbers are targets, to be confirmed by the first benchmark run; the write-path targets gate the
wiring step. A `pprof` profile of the end-to-end publish path (MQTT client -> `OnPublished` ->
`AppendOne`) must show the archive below 10% of CPU at 100k msg/s.

## Tasks

1. **Store core** (`internal/stores/arrowmem/store.go`, `chunk.go`): shards, series, chunk layout,
   append, sealing, eviction, pool, budget, janitor. Unit tests and the write benchmarks. Stop and
   review if the targets are missed.
2. **MessageArchive**: `GetHistory`, `GetAggregatedHistory`, value columns, `GetArchiveStats`,
   `PurgeOlderThan`. Unit tests against the same cases the SQL archives are tested with.
3. **Group wiring**: `fastArchive` path in `Group.Submit`, queue bypass, `buildArchiveStore` case,
   `RunRetention` skip, window validation, `stores.ArchiveMemory`. Config block and schema files.
4. **GraphQL enum** (only after A1 sign-off): SDL value, `make gen`, resolver mapping; Core enum in
   lockstep (separate PR in `monster-mq`); dashboard dropdown (separate PR in `monster-mq-dashboard`).
5. **Arrow export**: add `github.com/apache/arrow-go/v18` (only `arrow`, `arrow/array`, `arrow/memory`,
   `arrow/ipc`; never `arrow/cdata`, which needs CGO), the REST route, `openapi.yaml`,
   `THIRD_PARTY_NOTICES.txt`. Measure binary growth for `make build-arm64` before and after.
6. **Metrics**: Prometheus collectors.
7. **Tests and docs**: integration tests, `doc/memory-archive.md` (window semantics, sizing table,
   Python/DuckDB examples), README archive section, AGENTS.md repository layout.

## Tests

Integration (`test/integration/memory_archive_test.go`, run with `test-integration.ps1` on Windows):

- Group with `archiveType: MEMORY`, `archiveRetention: 2s`: publish over MQTT, read back through
  GraphQL `archivedMessages`, REST history and the Arrow endpoint; same rows in all three.
- Window: after 3 s without publishing, `archivedMessages` returns nothing and the metrics show 0 rows
  (janitor path); with continuous publishing the oldest row is never older than the window plus one
  chunk span (append path).
- Wildcard `topicFilter`, `limit`, `start`/`end` bounds, newest-first order.
- `aggregatedMessages` with `fields` on JSON payloads and on plain numeric payloads.
- Budget: small `MaxMemory`, two topics, one flooded: the flooded topic's window shrinks, the other
  keeps its full window, `dropped_rows_total{reason="budget"}` increases.
- Group restart via GraphQL update: window starts empty, no error.
- ACL: a user without subscribe permission on the filter gets 403 from the Arrow endpoint.
- Arrow stream decodes with `arrow-go`'s `ipc.NewReader` in the test (schema, row count, values).

Unit: race test (`make test-race`) with parallel writers and readers on the same topic; out-of-order
timestamps; chunk recycling under held references; payload buffer detachment (mutating the caller's
slice after `AppendOne` doesn't change stored data).

## Acceptance criteria

- Write-path benchmarks meet the targets on x86 and on a Pi 5; 0 allocs/op in steady state.
- A 10 kHz publisher on one topic with a 30s window runs for 1 hour on a Pi 5 with flat RSS (within
  the sizing table plus 20%) and no growth in goroutines.
- `archivedMessages`, `aggregatedMessages`, the REST history read, the Prometheus API and MCP history
  tools work on a `MEMORY` group with no code changes in those callers.
- pyarrow reads the Arrow endpoint into a DataFrame.
- Binary growth from `arrow-go` measured and accepted (A3).
- No CGO; `make build-arm64` and `make build-armv7` still pure Go.

## Effort

About 6 to 8 days: store core and benchmarks 2-3, read path 1-2, wiring and config 1, Arrow export 1,
tests and docs 1-2. Core and dashboard PRs are small but need their own review.

## Decisions needed

- [ ] Sign-off: add the in-memory rolling-window archive.
- [ ] A1. **GraphQL:** add one value to `MessageArchiveType` in Core and edge (recommended), or keep
      the groups YAML-only with no dashboard support.
- [ ] A2. Enum value name: `MEMORY` (recommended, matches `MessageStoreType.MEMORY`; Arrow is the
      internal layout) or `ARROW`.
- [ ] A3. Dependency: `arrow-go` for the IPC export (recommended; accept the measured binary growth),
      or a hand-written IPC stream encoder (about 500 lines of flatbuffers framing, no new dependency).
- [ ] A4. Export transport: plain HTTP Arrow IPC stream (recommended) or also Arrow Flight (gRPC,
      larger binary, useful only for Flight-native clients).
- [ ] A5. Restart behaviour: start empty (recommended) or write an Arrow IPC snapshot on graceful
      shutdown and reload the part still inside the window.
- [ ] A6. Keep MQTT 5 properties (content type, user properties) as extra columns: no in v1
      (recommended, saves memory and time on the hot path) or yes.
- [ ] A7. Sub-minute `AggregationInterval` values in GraphQL: out of scope here (recommended), or a
      separate GraphQL change in both repos.
