# Plan: WinCC OA archive group (`WINCCOA` last-value store and `dpGetPeriod` history)

**Status:** approved 2026-10-17, owner decisions in [plan-protocol-servers-shared.md](plan-protocol-servers-shared.md);
revised after checking against main, edge and the WinCC OA 3.21 headers.
**Scope:** when edge runs embedded in `WCCOAmmq` with the native namespace on, an ArchiveGroup can read current
values (`dpGet`, one-shot `dpQuery`) and archived history (`dpGetPeriod`) straight from WinCC OA. Shared decisions
D1-D6 and building blocks S1-S7 are referenced by ID and not repeated here.
**Repos:** `edge/` (Go and `winccoa/manager/` C++), `main/` (D2 enum values and deploy error), `dashboard/` (type
options).
**Companion plans:** [plan-nats-server.md](plan-nats-server.md), [plan-redis-server.md](plan-redis-server.md),
[plan-prometheus-server.md](plan-prometheus-server.md) (they use the contract below),
[plan-uns-modeler-archive.md](../../winccoa/dev/plans/plan-uns-modeler-archive.md) (writes the `_archive` configs
this plan reads).

## Sign-off points

> [!IMPORTANT]
> **GraphQL interface change, committed by the owner (D2, 2026-10-17).** `WINCCOA` is appended as the last value of
> `enum MessageStoreType` and of `enum MessageArchiveType`, in both repos in the same release:
> - main: `broker/src/main/resources/schema-types.graphqls:27-35` and `:37-44`
> - edge: `internal/graphql/schema/schema.graphqls:15-16`
>
> Neither repo has the value today, so edge must not ship it before main does (AGENTS.md:36-50, strict subset).
> No other type, field, argument or default changes.
>
> Impact:
> - Dashboard: it gets a new option value (issue monster-mq#205).
> - Main: it accepts the value, runs no such group, and reports a clear error.
> - Edge: only edge embedded in `WCCOAmmq` runs such a group.
>
> Alternative considered and rejected: keep the enums and report WINCCOA groups as `NONE`, which
> `toMessageStoreType` already does as its fallback (`resolver.go:361-373`). The dashboard topic browser would then
> hide the group, because it lists `archiveGroups(lastValTypeNotEquals: NONE)` (`dashboard/src/js/topic-browser.js:546`).
> A dashboard save would also rewrite the type to `NONE` (`fromMessageStoreType`, `resolver.go:391-403`).

1. **D2:** the GraphQL enum values above.
2. **D4, writes:** the `WINCCOA` stores never write. NATS KV put and Redis `SET` write WinCC OA tags through S2 (the
   native command path). The read-only flags of the WINCCOA side are forced on. `LastValStore.AddAll` is a no-op.
3. **ABI extension.** This is the internal contract between Go and the C++ manager, so no external client sees it:
   - ops 17-20, tag 24, flag bits 9-10;
   - `winccoa/doc/spec-winccoa-native.md` §3.1 and §7 are updated in M1.
4. **New config key (S6):** `WinCCOaNative.QueryMaxRows`, default 10000 (§3.6).
5. **Edge-only behaviour** is listed under Deviations. The single **[fix: both]** item is X9.

## Context

Edge today:
- `stores.MessageStoreWinCCOA` exists (`types.go:68`), but only the WINCCOA retained store uses it
  (`oastore/retained.go:81`). There is no archive type (`types.go:73-80`). `stores/oastore/` holds the
  MMQConfigs, MMQSessions, MMQRetained and MMQUsers stores only.
- Groups are built by type in `buildLastValStore` / `buildArchiveStore` (`manager.go:413-526`). `Load` provisions
  "Default" when it is missing (`manager.go:98-132`). Only `Load` does this, so a deleted Default comes back at the
  next start, never on `Reload` (`manager.go:528-570`).
- `Group.Matches` is false when both stores are read-only (`group.go:136-138`). In that case `Dispatch` never calls
  `Submit` (`manager.go:603-611`).
- Free ABI numbers:
  - Ops 1-12 (`ops.go:16-27`) and 13-16 (`browse.go:12-15`) are used; the C++ enum is at `MmqTlv.hxx:36-41`.
  - Tags go up to `TagAttr` 23 (`browse.go:239`, `MmqTlv.hxx:19-23`).
  - Flag bits 0-8 are used (`ops.go:32-45`, `browse.go:216`, `browse.go:223`, `MmqTlv.hxx:43-50`).
  - An unknown op is answered with `MMQ_E_INVALID "unknown operation N"` (`MmqManager.cxx:462`).
- Query tables only travel as events today:
  - `sendTable` sends chunks of at most `MMQ_MAX_MESSAGE/2` = 512 KiB (`MmqManager.cxx:284-294`;
    `monstermq_types.h:47`).
  - The event queue is bounded and drops when full (`client.go:357-377`, spec §7). Completions are never dropped
    (`client.go:319-339`).
  - `onQueryAnswer` completes the request *before* it streams the rows (`MmqManager.cxx:877-887`).
  - So events are not usable for one-shot reads.
- There is no one-shot `dpQuery`. The one-shot `dpGet` uses `RequestWait` (`MmqManager.cxx:1120-1146`).
- Native helpers:
  - `Names.Classify` (`namespace.go:132-174`), `Parse` (`:224`), `ParseWildcard` (`wildcard.go:37`).
  - `WildTarget.Pattern` / `Query` / `RowTarget` (`wildcard.go:122-200`). `WildTarget.Remote` is set only by the
    service (`service_wild.go:30`). The service looks up the DP type per row for `types/+` filters
    (`service_wild.go:186-191`, `:247-255`).
  - `ValuePayload` (`values.go:171-196`).
  - Native value publishes use QoS 1, are not retained and carry a `ValuePayload` (`service.go:861`,
    `service_wild.go:201`, `:238`).
- Group selection on reads:
  - GraphQL selects the group by name, defaulting to `"Default"` (`resolver.go:1028-1052`).
  - REST uses the `group` parameter (`handler.go:284-292`); MCP uses `getArchiveGroup` (`tools.go:57-63`).
  - All six GraphQL read queries declare `archiveGroup: String = "Default"` (edge `schema.graphqls:725-761`; main
    `schema-queries.graphqls:14`, `28`, `68`, `98`, `142`, `157`). A resolver therefore cannot tell an omitted
    argument from an explicit `"Default"`.
- `SearchTopics` and `BrowseTopics` always scan `FindMatchingTopics(ctx, "#")` (`resolver.go:1167`, `:1247`).
- `InterestFilters` adds the filters of every group except retained-only ones, including groups that never archive
  (`manager.go:152-164`).

WinCC OA 3.21 API. These are the headers under `<API_ROOT>/include` (`API_ROOT` per `CMakeLists.txt:4-12`), checked
against the local installation:
- `dpGetPeriod(const TimeVar &start, const TimeVar &stop, PVSSushort number, const DpIdentList &, PVSSulong &reqId,
  WaitForAnswer *, bool del = true)` (`Manager/Manager.hxx:1174-1175`):
  - `number` is the count of "additional values outside the request period" (`:1095-1098`). It is a bonus count, not
    a row limit.
  - There is one answer group per DPE (`:1126-1127`), and the variant allows multiple answers.
- `dpQuery(const CharString &, PVSSulong &reqId, WaitForAnswer *, bool del = true)` (`:2037`) and
  `abortRequest(PVSSulong reqId)` (`:2266`). After an abort, no further callbacks come, and the wait object is
  deleted when `del` is true.
- Answer accessors:
  - `DpMsgAnswer::isFinalAnswer()` (`Messages/DpMsgAnswer.hxx:154`)
  - `AnswerItem::getTime()` (`Messages/AnswerItem.hxx:91`)
  - `DpVCItem::getDpIdentifier()` / `getValuePtr()` (`Basics/DpBasics/DpVCItem.hxx:94`, `:106`)
  - `AnswerGroup::wasOk()` (`Messages/AnswerGroup.hxx:87`)
- CTRL reference: `dpGetPeriod(t1, t2, 1, dpe + ":_offline.._value", ...)` (`scripts/libs/aes.ctl:8682`).
- History through `dpQuery` needs `TIMERANGE("start","stop",1,0)` (`scripts/libs/ac.ctl:3261`), not a `WHERE _TIME`
  clause. This plan does not use `dpQuery` for history.

Main:
- Main has no WINCCOA anywhere. The Kotlin enums are at `stores/IMessageStore.kt:7-15` and
  `stores/IMessageArchive.kt:11-18`.
- Today a config row with an unknown type is skipped with a parse warning (`ArchiveConfigStoreSQLite.kt:157-166`).
- `MonsterOA.kt:32-63` runs main inside `WCCOAJavaManager` through oa4j. Main's only WinCC OA integration is
  `Oa4jBridge`: `!OA/<ns>/dps/<name>` topics, started at `Monster.kt:1666-1684`. It has no `winccoa/...` namespace
  and makes no `dpGetPeriod`/`dpQuery` calls.
- So main cannot run a WINCCOA group, even under MonsterOA. It reports a deploy error (D2). An oa4j-based
  implementation would be possible later, but is out of scope.

## Design

### 1. ABI (oahost <-> WCCOAmmq)

#### 1.1 New constants (Go `internal/oahost/ops.go`, C++ `winccoa/manager/MmqTlv.hxx`)

| Kind | Name (spec name) | Value | Meaning |
|---|---|---|---|
| op | `OpDpGetPeriod` (`DP_GET_PERIOD`) | 17 | history of 1-100 DPEs |
| op | `OpDpQuery` (`DP_QUERY`) | 18 | one-shot `dpQuery` |
| op | `OpCursorNext` (`CURSOR_NEXT`) | 19 | next page of a read |
| op | `OpCursorClose` (`CURSOR_CLOSE`) | 20 | drop a read |
| tag | `TagIndex` | 24 | u32, index into the request's repeated `TagName` list |
| flag | `FlagNewest` | `1<<9` | `DP_GET_PERIOD` request: keep the newest samples |
| flag | `FlagTruncated` | `1<<10` | read answer: the row or sample limit cut the result |

`FlagMore` (`1<<3`, `ops.go:35`, `MmqTlv.hxx:45`) is reused in read answers. There it means that another page is
waiting on the cursor.

#### 1.2 One design: paging through completions

Every read returns its first page in the completion of its request. Further pages are pulled with `CURSOR_NEXT`.
Rows never travel as events, so a full event queue cannot lose them, and a page cannot race the completion. The host
buffers the finished read and Go pulls pages, so memory is bounded on both sides (§1.6).

Read answer (a "page"), shared by ops 17, 18 and 19:

| Field | When | Content |
|---|---|---|
| `TagFlags` | always | `FlagMore` if another page follows; `FlagTruncated` if the limit cut the result |
| `TagRef` | `FlagMore` set | u64 cursor id, allocated by the host, never 0 |
| `TagCount` | first page | u32 total data rows (query) or samples (period) over all pages |
| `TagTime` | period, oldest mode, truncated | i64 ms resume time (§1.4) |
| `TagRow` ... | | query: the header row first, then data rows, cells as `TagValue`; this is the layout of the query events, so `QueryRows` (`ops.go:443-467`) decodes a page. Period: one row per sample `{TagIndex, TagTime, TagValue}`; on the first page also one row per failed name `{TagIndex, TagError}` |

A page is at most 512 KiB, the `sendTable` split size. Every query page repeats the header row.

#### 1.3 `DP_QUERY` (18)

Request:

| Field | Content |
|---|---|
| `TagQuery` | `SELECT '<attr>'[, ...] FROM '<pattern>' [REMOTE '<sys>'] [WHERE _DPT = "<dpt>"]` |
| `TagCount` | u32 maximum data rows, 1..1,000,000; anything else is `MMQ_E_INVALID` |

Host:
1. Calls `dpQuery(CharString(q), dmReqId, wait, true)`.
2. Collects the data rows of every answer. It keeps the first answer's header and skips the header of later answers.
3. When the count would exceed the maximum, it sets truncated. Unless the answer is final (`isFinalAnswer`), it calls
   `abortRequest(dmReqId)`. Then it finishes.
4. A non-empty `answerError` (`MmqManager.cxx:296-309`) becomes `MMQ_E_OA` with the text. If `dpQuery` returns
   false, that is `MMQ_E_OA "dpQuery message not sent"`.

#### 1.4 `DP_GET_PERIOD` (17)

Request:

| Field | Content |
|---|---|
| `TagTime` x2 | start, end: i64 ms, both inclusive. Exactly two; the host reads them with `Message::all(TagTime)`, simhost with `Message.All(oahost.TagTime)`. `Message.U64` would only see the first one (`tlv.go:147-153`) |
| `TagCount` | u32 maximum samples: 1..100,000 with `FlagNewest`, 1..50,000 without |
| `TagFlags` | `FlagNewest` keeps the newest samples (history). Without it the host keeps the oldest samples and reports a resume time (aggregation); this mode takes exactly one name |
| `TagName` x1..100 | `Sys:DP.el:_offline.._value`, all on one system |

Host:
1. `getId` for each name. An unknown name is `MMQ_E_NOT_FOUND "datapoint element not found: <n>"`, as in `opDpGet`
   (`MmqManager.cxx:1124-1132`). Mixed systems, more than 100 names, or oldest mode with more than one name are
   `MMQ_E_INVALID`.
2. Calls `dpGetPeriod(start, stop, 0, list, dmReqId, wait, true)`. The bonus is 0 because history and aggregation
   only want values inside `[start, end]`, as SQL's `time >= ? AND time <= ?`. The host still drops any item outside
   the range.
3. For each group:
   - `!wasOk()` records the DPE as failed, with its error text. This covers a DPE without an archive config, if
     probe P2 shows that this is an error.
   - For each item: the index comes from `getDpIdentifier()` through a map built from the request ids (fallback:
     group order, probe P1), the time from `getTime()`, and the value from `getValuePtr()`, encoded with the existing
     value writer.
4. Newest mode keeps a bounded min-heap of `TagCount` samples ordered by (time, index). Any eviction sets truncated.
   The pages are then sorted by time descending, then by index.
5. Oldest mode keeps the first `TagCount` samples in answer order, which is ascending time for one DPE (probe P3).
   On overflow:
   - it sets truncated and calls `abortRequest`;
   - it removes the trailing samples that share the last kept millisecond and reports that millisecond as the
     resume `TagTime`, so the next request starting there loses and repeats nothing;
   - if every kept sample shares one millisecond, the read fails with `MMQ_E_TOO_LARGE`.

#### 1.5 `CURSOR_NEXT` (19), `CURSOR_CLOSE` (20)

- `CURSOR_NEXT {TagRef}` returns the next page with the fields of §1.2. An unknown or expired cursor is
  `MMQ_E_NOT_FOUND`. The read is freed after its last page.
- `CURSOR_CLOSE {TagRef}` frees the read and always answers `MMQ_OK`, also for unknown refs. Stores use
  `CURSOR_CLOSE` with ref 0 as a capability probe (§2.4).

#### 1.6 Host state, cancellation and limits (`MmqManager.hxx/.cxx`)

- **Wait objects.** `ReadWait : WaitForAnswer` holds only `(mgr, readId)` and is created with `del=true`. The
  framework deletes it after the final answer or on `abortRequest`. The read state lives in `std::map<uint64_t, Read>
  reads`, and a callback for an unknown `readId` is ignored. This is the guard `onQueryAnswer` uses
  (`MmqManager.cxx:866-868`).
- **Read state.** A `Read` holds:
  - `reqId`, the request deadline (`Pending.deadline`, `MmqManager.hxx:164-168`) and `dmReqId`;
  - kind, limits, rows or samples and the finished pages;
  - `lastUsed`.

  The cursor id is the read id.
- **Timeouts.** A new `expireReads()` runs in the main loop after `drainRequests()` (`MmqManager.cxx:1790-1793`).
  - A read still waiting on WinCC OA after its deadline gets `abortRequest(dmReqId)` and
    `complete(reqId, MMQ_E_TIMEOUT, "WinCC OA read timed out")`, and is erased.
  - A finished read idle for more than 30 s is erased.
- **Go gave up.** When `complete()` for the first page returns `MMQ_E_NOT_FOUND`, Go has already timed out and the
  read is erased at once. This is the same pattern as `MmqManager.cxx:877`.
- **Budget.** At most 64 reads (waiting or paging) and at most 64 MiB of buffered rows in total.
  - A new read above the limit gets `MMQ_E_OVERLOAD`.
  - A waiting read that crosses the byte budget is aborted with `MMQ_E_OVERLOAD`.
  - These numbers go into spec §7.
- **Host stats** (`mmq_stats`, `MmqManager.cxx:1811-1843`): `archiveReads`, `archiveReadsAborted`, `archiveReadsOpen`,
  `archiveReadBytes`.

#### 1.7 Go API (`internal/oahost/read.go`, new)

```go
const MaxPeriodNames = 100

type QueryPage struct {
	Header []Value
	Rows   [][]Value
}

// DpQuery runs a one-shot dpQuery limited to maxRows data rows and calls page
// for every page in order. timeout bounds the whole read.
func (a API) DpQuery(ctx context.Context, query string, maxRows int, timeout time.Duration,
	page func(QueryPage) error) (truncated bool, err error)

type PeriodRequest struct {
	Names      []string // Sys:DP.el:_offline.._value, one system, 1..MaxPeriodNames
	Start, End time.Time
	Max        int  // samples: 1..100000 (Newest), 1..50000 (oldest)
	Newest     bool // keep the newest Max; false: the oldest Max plus Resume (one name only)
}
type PeriodSample struct {
	Index int
	Time  time.Time
	Value Value
}
type PeriodResult struct {
	Total     int
	Truncated bool
	Resume    time.Time      // oldest mode, truncated: start of the next request
	Failed    map[int]string // name index -> WinCC OA error
}

func (a API) DpGetPeriod(ctx context.Context, req PeriodRequest, timeout time.Duration,
	page func([]PeriodSample) error) (PeriodResult, error)
```

- The deadline is the earlier of the ctx deadline and now+timeout. Each `Call` (`client.go:248`) gets the time that
  is left.
- A page callback error, a ctx cancel or a timeout ends the read. Go then sends `CURSOR_CLOSE` for an open cursor
  in a goroutine with a 1 s timeout. The host's deadline and TTL clean up any close that is lost.
- The first page's `TagCount` is checked against the rows actually delivered. A mismatch is `ErrMalformed`.
- Requests are validated before they are sent: wrong name count, `Max` out of range, or `!Newest` with several names
  is `ErrInvalid`.
- One `logCall` DEBUG line per read (`ops.go:77-90`): `"dpQuery"` / `"dpGetPeriod"`, with the pages, rows and
  truncated flag.

#### 1.8 simhost (`internal/oahost/simhost/simhost.go`, `history.go` new)

- `DP_QUERY` reuses `queryRe`, `oaPatternRe` and `queryTable` (`simhost.go:896-1000`).
- `DP_GET_PERIOD` reads a per-DPE history ring of 10000 samples, recorded on `Set`, `SetTimed` (`simhost.go:338-347`)
  and the `dpSet` op.
- Value elements are archived by default. `SetArchived(dpe string, on bool)` simulates a DPE without an archive
  config, which behaves as probe P2 records.
- The cursor protocol of §1.5, with test knobs:
  - `PageBytes` (default 512 KiB);
  - `ReadDelay` (delays the answer, for timeouts);
  - `DisableArchiveOps` (acts like an older host and answers "unknown operation");
  - `OpenReads()` and `Calls(op)`, for leak and call-count checks.

#### 1.9 Probe (first task of M1, `MmqProbe.cxx`, live project `Test321`)

The existing probe covers only `dpQueryConnectSingle` (`MmqProbe.cxx:1-2`, `:166`). It is extended to record:

| # | Question | Plan assumption if confirmed |
|---|---|---|
| P1 | Does every `dpGetPeriod` answer group map to one requested DPE, and does `getDpIdentifier()` equal the request id (including config)? | index by identifier, fallback group order |
| P2 | What does a DPE without `_archive` return: group error, empty group, or the online value? | error -> failed index; empty -> no rows; online value -> treat as no archive (drop items whose time is before start) |
| P3 | Are items of one DPE in ascending time? Is the item time the source time? | oldest mode and resume as specified |
| P4 | Does `number = 0` return no bonus values? | the host filters anyway |
| P5 | Do the reqId variants deliver several answers for large results, end with `isFinalAnswer()`, and stop calling back after `abortRequest` with `del=true`? | §1.3, §1.6 |
| P6 | Does `dpGetPeriod` work for a DPE of a connected remote system (dist)? | if not: remote history returns `oahost.ErrUnavailable`, documented |

The results are written into spec §3.1 before the M1 host code is merged. Any assumption the probe refutes is
corrected in this plan first.

### 2. Stores (`internal/stores/oastore/`)

#### 2.1 Construction (`read.go`)

```go
type ReadOptions struct {
	API     oahost.API
	Names   func() winccoanative.Names // late-bound; Local may be "" before the native service started
	MaxRows int                        // WinCCOaNative.QueryMaxRows
	Logger  *slog.Logger
}

func NewLastValStore(name string, o ReadOptions) *LastValStore
func NewMessageArchive(name string, o ReadOptions) *MessageArchive
```

- Local system: `Names().Local` once it is set. Before that, `API.SysInfo` (`ops.go:131`) is called once and cached.
- Timeouts, used when the ctx has no earlier deadline: 5 s for `Get`, 10 s for queries and history (spec §7 "store
  operations"), 30 s for aggregation.

#### 2.2 Exact topics (v1)

| Topic | Result |
|---|---|
| `<root>/tags/Pump1/speed` | `Resolve(Sys:Pump1.speed:_online.._value)`, then `DpGet` of that and `:_online.._stime` |
| `<root>/tags/ScalarTag` | DP root `Sys:ScalarTag.`, which must have `ElemType != 0` |
| `<root>/tags/Pump1/speed/_online.._status` | the explicit attribute plus `_online.._stime` |
| `<root>/systems/SubstationA/tags/Feeder1/voltage` | remote system; unavailable -> error wrapping `oahost.ErrUnavailable` |
| `<root>/types/AnalogDrive/Pump1/speed` | `Resolution.TypeName` must equal `AnalogDrive` |
| `.../set`, `Protected` DP (`service.go:373-376`), struct node (`ElemType` 0), `ElemUnsupported`, wrong type, missing DPE | `nil, nil` |
| `<root>/topics/...` (`KindTopics`) | `nil, nil`, excluded in v1 |
| `<root>`, `<root>/systems/<sys>` (`KindStatus`) | `nil, nil`; the status JSON is a retained broker message, not WinCC OA data |
| anything else (`KindOther`) | `nil, nil` |

Why the `topics` branch is excluded in v1:
- The native service publishes it as ordinary MQTT messages (`topics.go:645`), so Default and SQL groups already
  hold those values.
- Reading them back would need the `TopicDpNames` naming (`topics.go:79-84`) and raw blob values (`topics.go:32-35`)
  with last-value storage off (`topics.go:38`).
- `ResolveGroupForTopic` never routes these topics to a WINCCOA group (§3.3).

#### 2.3 Wildcards and broad filters (`expand.go`)

A filter F (after `SplitShared`, `namespace.go:211`) becomes a list of read targets:

1. **No wildcard.** If it is `KindNative`, it is an exact read (§2.2); otherwise nothing.
2. **Scoped native filter** (`Classify == KindNative`). Use `ParseWildcard(F)`, then set
   `w.Remote = w.System != "" && w.System != local`, as the service does (`service_wild.go:30`). A parse error gives
   nothing.
3. **Broad filter** (anything else). Let L be F's levels, R the root levels, T = `Tags`, S = `Systems`, and i the
   index of F's first wildcard.
   - **(a) Wildcard at a root level or the branch level** (`i <= len(R)`). Prefix P = `R+[T]`, the local tags in
     shortcut form, or `R+[S, local, T]` when `NoShortcut`.
   - **(b) Wildcard at the system level** (`L[:len(R)+1] == R+[S]` and `i == len(R)+1`). One prefix
     `R+[S, s, T]` per available system s: the local system first, then the connected remote systems
     (`Client.Systems`, `client.go:443`), in explicit form.
   - Match L against each prefix P: `#` matches the rest; `+` or an equal literal continues; anything else fails.
     The match also fails if L ends inside P or exactly at its end (there would be no DP level).
   - A match rewrites F to `P + L[len(P):]`, which then goes through step 1 or 2.
   - No match means no rows and no WinCC OA call.
   - The `types` branch, the `topics` branch and status topics are never reached through broad filters.
4. All targets of one call share one row budget, `QueryMaxRows`. Every row is also checked with
   `topic.MatchFilter(F, topic)` (`topic/tree.go:339`).

| Filter (root `winccoa`) | Targets | Rows |
|---|---|---|
| `#`, `winccoa/#`, `+/#` | `winccoa/tags/#` (`'*.**'`) | local tags, shortcut form |
| `+/tags/Pump1/#` | `winccoa/tags/Pump1/#` | |
| `winccoa/+/+` | `winccoa/tags/+` (`'*.'`) | scalar DP roots |
| `winccoa/+`, `winccoa/systems/+`, `sensors/#` | none | 0 rows, no host call |
| `winccoa/systems/+/tags/#`, `winccoa/systems/#` | `winccoa/systems/<s>/tags/#` per available system | explicit form |

Rows to topics:
- `row[0]` (`Sys:DP.el`) goes through `w.RowTarget(row[0].Str, typeName).Topic()` (`wildcard.go:182-200`), which
  keeps the filter's form (shortcut, explicit or types).
- Which type name is used for the `types` form:
  - **Fixed type** (`types/<dpt>/...`): the query has `WHERE _DPT` (`wildcard.go:162-164`), and
    `typeName = w.TypeName`.
  - **`types/+/...` and `types/#`:** the store reads the types of the distinct DPs of a page with
    `API.ElementMeta` on `Sys:DP.` names, 200 per call (`oahost/browse.go:148`; the batch size of `metaBatch`,
    `winccoabrowse/browse.go:27`). The results are cached for the call. Rows of protected types (`ProtectedType`,
    `service.go:368`) are dropped.

Query text comes from a new `WildTarget.QueryAttrs(attrs ...string) string` in `wildcard.go`. `Query()` becomes
`QueryAttrs("_online.._value", "_online.._stime")`, so `Key()` does not change.

#### 2.4 `LastValStore` (`lastval.go`), implements `stores.MessageStore` (`interfaces.go:8-20`)

| Method | Behaviour |
|---|---|
| `Name()` / `Type()` | `LastValName(group)` (`manager.go:238`) / `MessageStoreWinCCOA` |
| `Get` | §2.2 |
| `FindMatchingMessages` | Exact filter -> `Get`. Otherwise the §2.3 targets with `QueryAttrs("_online.._value","_online.._stime")`. Each row yields `BrokerMessage{TopicName, Payload: ValuePayload(row[1], row[2].Time), QoS: 1, Time: row[2].Time}`, where `row[2]` is `_online.._stime`. When `yield` returns false, the read stops and its cursor is closed. |
| `FindMatchingTopics` | Same targets with `QueryAttrs("_online.._stime")`; yields the topic |
| `AddAll`, `DelAll` | no-op, `nil` (D4) |
| `PurgeOlderThan`, `PurgeExpired` | `PurgeResult{}`, `nil` |
| `EnsureTable` | capability probe: `CURSOR_CLOSE` with ref 0. "unknown operation" -> error `"WCCOAmmq does not support archive reads (unknown operation 20); update the manager"`, which becomes the group's deploy error |
| `Close` | `nil` |
| `BrowseLevel` | §2.7 |

If the row limit cuts a call, it ends normally and the store writes a WARN line, at most once per minute per store:
`"WinCC OA query truncated at <n> rows"` with the query text.

#### 2.5 `MessageArchive.GetHistory` (`history.go`)

1. **Bounds.**
   - `to` = now if nil; `from` = `to - 24h` if nil **[edge-only, X4]**; `from > to` gives an empty result.
   - `limit <= 0` becomes 100 (as SQL, `sqlite/message_archive.go:106-108`). Values above 100,000 are clamped (the
     REST cap, `handler.go:394-403`).
2. **Targets.**
   - An exact `KindNative` topic qualifies if it is not a command, not protected, and has the default attribute or
     an explicit `_online.._value`. It is checked with `Resolve` (`ElemType != 0`, plus the type for the `types`
     form) and read as `t.DPE(sys) + ":_offline.._value"`, under the requested topic.
   - A wildcard or broad filter expands as in §2.3, using `QueryAttrs("_online.._stime")`, to at most 1000 DPEs
     (`DP_QUERY` `TagCount = min(QueryMaxRows, 1000)`) **[edge-only, X5]**. A cut writes a WARN line.
   - Anything else gives `[]ArchivedMessage{}`.
3. **Batching.** DPEs are grouped by system into batches of at most 100. That is `MaxPeriodNames`, the same 100 as
   `MaxGroupItems` (`MmqTlv.hxx:51`) and the connect batch in spec §7. Each batch runs
   `DpGetPeriod(Newest: true, Max: limit)`. The batch results are merged into one newest-`limit` set; this is exact,
   because the global newest N are a subset of the union of the per-batch newest N.
4. **Failed DPEs.** DPEs without history are skipped and logged at DEBUG.
5. **Rows.** Each sample becomes `ArchivedMessage{Topic, Timestamp: sample time, Payload: ValuePayload(value, time),
   QoS: 1}`, sorted by `Timestamp` descending, then topic ascending. That is SQL's `ORDER BY time DESC LIMIT ?`
   order (`sqlite/message_archive.go:121`, `postgres.go:372`).

#### 2.6 `MessageArchive.GetAggregatedHistory` (`aggregate.go`)

Decision: Go aggregates page by page and never holds the raw series. A cap on the result size bounds the
accumulators.

1. **Defaults.** `intervalMinutes <= 0` becomes 5; empty functions become `AVG`; empty fields become `[""]`, as
   SQLite does (`sqlite/message_archive.go:251-256`, `:279-282`).
2. **Size guard.** `buckets = floor((end-start)/interval) + 1`. If `buckets * columns > 1,000,000`, the call fails
   with `"WinCC OA aggregation would produce <b> buckets x <c> columns (limit 1000000); narrow the range or use a
   longer interval"` **[edge-only, X6]**.
3. **Topics.** Each topic, in order, must be an exact `KindNative` topic, resolved as in §2.5. Wildcards and other
   topics get `nil` columns, as SQL's `topic IN (...)` treats them.
4. **Paging per DPE.** Starting with `resume = start`:
   `DpGetPeriod(Names: [dpe], Start: resume, End: end, Max: 50000, Newest: false)`. The page callback feeds the
   accumulators. If the result is truncated, `resume = Resume` and the request repeats.
5. **Accumulators.** One `{count, sum, min, max}` per (bucket, column). The bucket key is
   `ts.UTC().Truncate(interval).Format("2006-01-02T15:04:00Z")`.
6. **Numeric value.**
   - For field `""` or `"value"`:
     - Float, Int, Uint and Bit32 give their number; Bool gives 1/0.
     - A String gives its JSON number, or `$.value` of a JSON object.
     - Other kinds give no value.
   - For any other field: a String DPE holding JSON gives the value at the dotted path; anything else gives no value.
7. **Result.**
   - Columns: `"timestamp"`, then per topic x field x function `topic + fieldAlias + "_" + lower(fn)`. `fieldAlias`
     is `""` or `"." + ReplaceAll(field, ".", "_")`.
   - Rows: one per bucket that has at least one sample of any topic, in ascending order.
   - `AVG`/`MIN`/`MAX`/`SUM` are `nil` for a column with no numeric value in that bucket; `COUNT` is 0.
   - `Interval` is `"%d"` minutes, `StartTime`/`EndTime` are RFC 3339, plus `TopicCount` and `RowCount`.

What matches SQL exactly:
- Column names follow `sqlite/message_archive.go:283-303` and main `MessageArchiveSQLite.kt:297`, `:310`.
- Bucket strings follow the five bucket expressions at `sqlite/message_archive.go:258-272`.
- Rows come in ascending order (`:341`).
- COUNT is 0 where SQL's `COUNT(CASE ...)` gives 0.
- This holds for the five intervals callers can request: 1, 5, 15, 60 and 1440 minutes (`resolver.go:1121-1134`,
  `tools.go:420-433`).

Values can differ from SQL: see X6.

#### 2.7 `LevelBrowser` for `BrowseTopics` (`browse.go`)

New optional interface in `internal/stores/interfaces.go`:

```go
// LevelBrowser is an optional MessageStore extension for BrowseTopics: it lists
// the names one level below pattern (literal levels plus one trailing "+").
// handled is false when the store cannot answer the pattern directly.
type LevelBrowser interface {
	BrowseLevel(ctx context.Context, pattern string, yield func(name string, leaf bool) bool) (handled bool, err error)
}
```

`LastValStore` implements it with `winccoabrowse.New(api.C)` (`winccoabrowse/browse.go:116`). Here `[sys]` is the
explicit system part, if any:

| Pattern | Source | Names, leaf |
|---|---|---|
| a proper prefix of the root, plus `+` | names | the next root level, not a leaf |
| `<root>/+` | names | `tags`, `types` (with the shortcut), `systems`; not leaves |
| `<root>/systems/+` | `Systems` (`:143`) | system names |
| `<root>/systems/<sys>/+` | names | `tags`, `types` |
| `<root>[sys]/tags/+` | `Datapoints(sys, "*", "", 1000, offset)` pages (`:266`); `TypeStructure` (`:218`) once per type | DP names; leaf = the type root is a value element |
| `<root>[sys]/types/+` | `Types(sys, "*")` (`:195`) | type names, not leaves |
| `<root>[sys]/types/<dpt>/+` | `Datapoints(sys, "*", dpt, ...)` | DP names, leaf as above |
| `.../<dp>[/<el>...]/+` (tags, or types after a `Resolve` type check) | `Elements(sys:dp)` (`:334`) | children of the addressed path; leaf = value element with no children |

- At most `QueryMaxRows` names per call.
- Names are encoded with `EncodeName(s, leaf)` (`namespace.go:374`), so a struct node is never encoded as a last
  segment.
- Patterns of any other shape return `handled = false`, and so do `topics` and status levels.

### 3. Manager wiring and API surfaces

#### 3.1 Server wiring (`internal/broker/server.go`)

- Move `var native *winccoanative.Service` from `:333` to above `archive.NewManager` (`:266`).
- Between `NewManager` (`:266`) and `Load` (`:267`), only when `nativeOn && cfg.WinCCOaNative.Namespace`
  (`:191`, `:335`):

  ```go
  archives.SetWinCCOA(oastore.ReadOptions{
  	API: oahost.API{C: opts.OA},
  	Names: func() winccoanative.Names {
  		if native != nil {
  			return native.Names() // Local is set after native.Start (server.go:936; service.go:1232-1235)
  		}
  		return names
  	},
  	MaxRows: cfg.WinCCOaNative.QueryMaxRows,
  	Logger:  logger,
  })
  ```

  This is the same late binding as `resolver.NativeNames` (`server.go:706-710`).

#### 3.2 `archive.Manager` and `Group` (`internal/archive/manager.go`, `group.go`)

- **New API:** `SetWinCCOA(oastore.ReadOptions)`, `WinCCOAEnabled() bool`,
  `const WinCCOAGroupName = "WinCCOA"`, and `func WinCCOAGroupConfig(root string) stores.ArchiveGroupConfig`:
  `{Name: "WinCCOA", Enabled: true, TopicFilters: [root + "/#"], LastValType: WINCCOA, ArchiveType: WINCCOA,
  PayloadFormat: DEFAULT, LastValReadOnly: true, ArchiveReadOnly: true}`.
- **Provisioning in `Load`.** After the Default block (`manager.go:110-123`), when the wiring is set and no config
  is named `WinCCOA`, save `WinCCOAGroupConfig(names.Root)`.
  - Like Default, only `Load` creates it. A deleted group comes back at the next start, and `Reload` never recreates
    it.
  - `Disable` persists. A user group already named `WinCCOA` is left as it is.
  - Nothing is provisioned without the wiring (standalone, or `Namespace` off).
- **Factory.** `buildLastValStore` and `buildArchiveStore` get a `WINCCOA` case.
  - Without wiring, they return `"group %s: WINCCOA stores need MonsterMQ Edge embedded in the WinCC OA manager
    (WCCOAmmq) with WinCCOaNative.Namespace enabled"`. This becomes the group's deploy error
    (`manager.go:166-174`).
  - With wiring, they call `oastore.NewLastValStore(LastValName(c.Name), *m.oa)` / `NewMessageArchive(...)`, then
    `EnsureTable`.
  - `RequiredDatabaseConnectionTypes` (`db_connections.go:93-128`) needs no change, and a `DatabaseConnectionName`
    on such a group keeps failing as today (`manager.go:291`).
- **Read-only forcing.** `effectiveConfig(c)` forces `LastValReadOnly` when `LastValType == WINCCOA` and
  `ArchiveReadOnly` when `ArchiveType == WINCCOA` **[edge-only, X10]**. It is applied in `startGroup` and to the
  wanted config before `configsEqual` in `Reload` (`manager.go:528-570`, `:572-601`). Without it, every `Reload` would restart
  the group.
  - A pure WINCCOA group is then both read-only, so `Matches` is false and MQTT publishes never reach it.
  - A mixed group (WINCCOA last value, SQL archive) keeps archiving into SQL.
- **`InterestFilters`** (`manager.go:152-164`) also skips groups whose effective config is read-only on both sides
  **[fix: both, X9]**.
- **`Group.MatchesFilter`** (S4) lands with whichever of NATS phase 1 and this plan's M3 comes first.

#### 3.3 `ResolveGroupForTopic` (smart fallback)

Signature and code are in the Contract (C1). Why this behaviour:
- **`""` and `"Default"` both mean "not chosen".** The SDL default makes them indistinguishable (Context), and REST
  and MCP already turn an empty value into `"Default"`.
- **Last value always moves to WinCC OA.** The online value is authoritative, and it is the same `ValuePayload` the
  native service publishes. Default's MEMORY copy only holds tags that somebody subscribed.
- **History moves only when Default has no archive covering the topic.** Default is provisioned with `ArchiveType
  NONE` (`manager.go:111-118`). A Default SQL archive that covers native topics was configured on purpose and can
  hold DPEs that have no WinCC OA archive config.
- **Only `KindNative` topics and filters are routed.** `#`, `<root>/#`, the `topics` branch and status topics stay on
  the requested group. Default holds them as ordinary messages, and a broad filter would otherwise lose them.

#### 3.4 API surfaces

GraphQL (`internal/graphql/resolvers/resolver.go`):
- **Store lookups.** `lastValueStore` / `archive` (`:1028-1052`) become `lastValueStoreFor(group, topic)` /
  `archiveFor(group, topic)` over `ResolveGroupForTopic`. They are used by:
  - `CurrentValue` (`:964`), `CurrentValues` (`:979`), `ArchivedMessages` (`:1054`);
  - `BrowseTopics` (`:1221`), with the pattern as topic.
- **`AggregatedMessages`** (`:1087`) resolves each topic.
  - If all topics resolve to one group, it makes one call as today.
  - Otherwise it makes one call per group with that group's topics, and combines them with
    `archive.MergeAggregated(topics, fields, functions, parts)` (`internal/archive/merge.go`, new):
    - columns are rebuilt in request order with the §2.6 naming;
    - rows are the union of the bucket strings, in ascending order;
    - every edge backend formats buckets as `YYYY-MM-DDTHH:MM:00Z` (`sqlite/message_archive.go:258-272`,
      `postgres.go:488-498`, `mongodb.go:739`);
    - a group without an archive contributes `nil` columns.
- **`BrowseTopics`.**
  - If the store is a `LevelBrowser` and `handled` is true, its names are the result, filtered by `allowTopic`.
  - If `handled` is false, the resolver calls `FindMatchingTopics` with the pattern's trailing `+` replaced by `#`,
    instead of `"#"`.
  - Other stores keep the current scan.
- **Unchanged:**
  - `SearchTopics` (`:1159`): the pattern is not a topic, so there is no fallback. On a WINCCOA group the `"#"` scan
    is bounded by S6.
  - `ArchiveStats` (`:1284`) and the GenAI lookups (`genai.go:344`, `:508`), which use an explicit group.
- **Enum mappers** (`:361-417`) get `WINCCOA`.
- **`archiveGroup.create` / `update`** (`:2102-2226`) reject `WINCCOA` unless `Archives.WinCCOAEnabled()`, with the
  §3.2 factory message.
- **Generated code:** run `make gen` after the SDL change (`schema.graphqls:15-16`).

REST (`internal/restapi/handler.go`):
- `group := h.archives.ResolveGroupForTopic(q.Get("group"), topic, hasStart || hasEnd)` replaces `:284-292`.
- `nil` keeps today's 404, with the requested name (`"Default"` if empty).
- `readHistory` (`:373`) takes the group instead of the name. Retained reads are unchanged.

MCP (`internal/mcp/tools.go`):
- `getArchiveGroupFor(name, topic, needArchive)` replaces `getArchiveGroup` (`:57-63`) in:
  - `get-topic-value`, per topic (`:241-251`);
  - `query-message-archive` (`:310`);
  - `query-message-archive-aggregated` (`:415`), per topic with `MergeAggregated`;
  - `find-topics-by-name` (`:102`), which uses `Namespace` as the hint.
- `query-message-archive-by-sql` is unchanged; it already rejects non-SQL types (`tools.go:380-382`).

#### 3.5 What the WINCCOA group never does

- It never writes to WinCC OA (D4):
  - MQTT publishes to native value topics are commands handled by the native service.
  - NATS KV put and Redis `SET` use S2.
  - `AddAll` and `AddHistory` are no-ops.
- It never deletes or purges: the read-only flags skip retention (`retention.go:34`, `:42`).

#### 3.6 Config key (S6)

`WinCCOaNative.QueryMaxRows` (int): the row limit of one `DP_QUERY` issued by a WINCCOA store, used for wildcard
reads, topic search and browse.
- Default 10000. 0 or absent means the default, and valid values are 100..1,000,000. Validation error:
  `"WinCCOaNative.QueryMaxRows %d must be between 100 and 1000000"`.
- Config checklist (shared plan):
  - `internal/config/config.go`: field in `WinCCOaNativeConfig` (`:249-271`), default and check in `validate()`
    (`:281-316`).
  - `yaml-json-schema.json`: `WinCCOaNative.properties` (`:402-417`, `additionalProperties: false`), as
    `{"type":"integer","minimum":100,"maximum":1000000,"default":10000}` with a description.
  - `winccoa/mmq.yaml.example` (`:33-43`): `QueryMaxRows: 10000`. `TestExampleConfigsValidate` validates this file
    (`config_schema_test.go:34`).
  - `config.yaml.example` has no `WinCCOaNative` block (the standalone binary ignores it), and
    `scripts/deb/config.yaml` is the standalone package. Neither changes.

The other bounds are constants of the oastore package, documented in `winccoa/doc/winccoa-archive.md`:

| Constant | Value |
|---|---|
| Wildcard history | 1000 DPEs |
| History limit | 100,000 |
| Aggregation page | 50,000 samples |
| Aggregation cells | 1,000,000 |
| Default history range | 24 h |

## Contract for other plans

The NATS, Redis and Prometheus plans build on these definitions and must not add their own full-project scans (S6).

**C1. Group selection** (`internal/archive/manager.go`):

```go
// ResolveGroupForTopic returns the running group that serves a read of topic
// (an exact topic or a filter), or nil. requested is the caller's group name;
// "" and "Default" both mean "not chosen". needArchive selects history reads.
func (m *Manager) ResolveGroupForTopic(requested, topic string, needArchive bool) *Group {
	if requested != "" && requested != "Default" {
		return m.Get(requested) // explicit choice, no fallback
	}
	def := m.Get("Default")
	if m.oa == nil {
		return def
	}
	t, _ := winccoanative.SplitShared(topic)
	if m.oa.Names().Classify(t) != winccoanative.KindNative {
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
```

The caller then uses `LastValue()` or `Archive()`. Either may be `nil` (for example, Default has no archive), and the
caller treats that as "no data", as `resolver.go:1058-1061` does. Redis calls it with the DB's group name and
Prometheus with `"Default"`. Both are covered by the rules above.

**C2. Bounds per call:**

| Call | Host ops | Bound |
|---|---|---|
| `LastValStore.Get` | `RESOLVE` + `DP_GET` | 1 DPE; 5 s |
| `Get` of a non-tag topic | none | `nil, nil` |
| `FindMatchingMessages` / `FindMatchingTopics`, scoped filter | 1 `DP_QUERY` (+ `ELEMENT_META` per 200 DPs for `types/+`) | <= `QueryMaxRows` rows; 10 s |
| same, broad filter, root or branch level | 1 `DP_QUERY` on the local system | <= `QueryMaxRows` rows |
| same, broad filter, system level | 1 `DP_QUERY` per available system | <= `QueryMaxRows` rows in total |
| same, filter that cannot match a tag | none | 0 rows |
| `BrowseLevel` | `DP_NAMES` / `ELEMENT_META` / `TYPE_STRUCTURE` / `TYPE_NAMES` | <= `QueryMaxRows` names |
| `MessageArchive.GetHistory`, exact topic | `RESOLVE` + 1 `DP_GET_PERIOD` (+ cursor pages) | newest `limit` (1..100,000); range defaults to the last 24 h; 10 s |
| `GetHistory`, wildcard or broad filter | 1 `DP_QUERY` (<= 1000 DPEs) + one `DP_GET_PERIOD` per 100 DPEs | newest `limit` overall |
| `GetAggregatedHistory` | `RESOLVE` per topic + `DP_GET_PERIOD` pages of 50,000 samples per DPE | <= 1,000,000 result cells; 30 s |
| `GetArchiveStats` | none | `nil, []DailyCount{}, nil` |

Go never holds more than one page of a read. The host's bounds are in §1.6.

**C3. Broad filters.** Defined in §2.3.
- **Root or branch-level wildcard** (`#`, `<root>/#`, `+/...`): only the local system's tags, in shortcut form
  (explicit form with `NoShortcut`).
- **System-level wildcard:** the tags of every available system, in explicit form.
- **Never included:** `types` duplicates, the `topics` branch, status topics.
- **No match:** a filter that cannot match a tag topic (`<root>/+`) returns nothing without a host call.
- **Truncation:** results cut at the row limit end without an error, and the store logs a WARN line.

**C4. Payload and message fields.**
- The payload is `winccoanative.ValuePayload`: `{"time":"<RFC 3339 UTC, ms>","value":<json>}` (`values.go:171-196`).
  It is byte-identical to a live native publish.
- `BrokerMessage`:
  - `TopicName` in the requested form (shortcut, explicit or types);
  - `QoS` 1, `IsRetain` false;
  - `Time` = `_online.._stime`;
  - no client ID or MQTT 5 properties.
- `ArchivedMessage`:
  - `Topic` in the requested form;
  - `Timestamp` = the archived source time;
  - `Payload` = `ValuePayload(value, Timestamp)`;
  - `QoS` 1, `ClientID` `""`.
- Numeric consumers (Prometheus) read `$.value`.

**C5. Writes.** They never go through these stores (D4). `AddAll`, `DelAll` and `AddHistory` return `nil` and do
nothing. Protocol servers write WinCC OA tags only through S2.

**C6. Errors.**
- Not a readable WinCC OA tag: empty result, `nil` error.
- Unknown or disconnected system: an error wrapping `oahost.ErrUnavailable`.
- Host timeout: `oahost.ErrTimeout`.
- Host overload: `oahost.ErrOverload`.
- Older `WCCOAmmq` without these ops: the group does not start, and its deploy error names the manager version
  problem (§2.4).

**C7. Provisioned group.** `archive.WinCCOAGroupConfig(root)` builds the auto-provisioned row. Plans that add
`ArchiveGroupConfig` fields (Redis `RedisDbNumber`) set their defaults there.

## Deviations

| # | Behaviour | Main | Marker |
|---|---|---|---|
| X1 | WINCCOA last-value store and archive run only in edge embedded in `WCCOAmmq` | the enum value exists (D2); the group fails to deploy with a clear message | [edge-only] |
| X2 | `ResolveGroupForTopic` sends native tag reads without a chosen group to the WINCCOA group | no WINCCOA group, so no fallback | [edge-only] |
| X3 | Broad filters on a WINCCOA store are limited to tags (C3) and to `QueryMaxRows` rows | n/a | [edge-only] |
| X4 | `GetHistory` without a start time reads the last 24 h | SQL archives have no lower bound | [edge-only] |
| X5 | Wildcard history covers at most 1000 DPEs | n/a | [edge-only] |
| X6 | Aggregation: field `""` means the DPE value; Bool counts as 1/0; result cap 1,000,000 cells; 30 s budget. SQL casts the stored payload instead: a `{"time":..,"value":..}` payload gives 0 in SQLite (`sqlite/message_archive.go:293`) and an error in Postgres (`postgres.go:520`), both 1:1 with main | fixed separately in monster-mq#207 | [edge-only] |
| X7 | `GetArchiveStats` returns no data for a WINCCOA archive | SQL counts rows | [edge-only] |
| X8 | History of explicit attributes other than `_online.._value`, and the `topics` branch and status topics, are not served by WINCCOA stores | n/a | [edge-only] |
| X9 | PeerLink interest leaves out groups whose two stores are read-only | main adds every deployed group's filters (`peerlink/PeerLinkManager.kt:257-262`) | [fix: both] |
| X10 | The read-only flag of a WINCCOA side is forced on | n/a | [edge-only] |

## Main-side tasks

The main and dashboard sides are tracked as issues. They land in the same release as edge M3 (D2, D3):

- [vogler75/monster-mq#205](https://github.com/vogler75/monster-mq/issues/205): the lockstep change for D2.
  - `WINCCOA` in main's SDL and Kotlin enums.
  - The deploy error, with message M: `"WINCCOA stores need MonsterMQ Edge embedded in the WinCC OA manager (WCCOAmmq)
    with WinCCOaNative.Namespace enabled"`.
  - GraphQL create/update rejection, the exhaustive `when` sites and the tests.
  - The dashboard type options (vogler75/monster-mq-dashboard).
- [vogler75/monster-mq#206](https://github.com/vogler75/monster-mq/issues/206): X9 [fix: both]. PeerLink static interest skips groups with both stores read-only.
- [vogler75/monster-mq#207](https://github.com/vogler75/monster-mq/issues/207): the SQL aggregation bug behind X6. Field `""` over JSON payloads returns 0 in SQLite and fails in
  Postgres, in both repos.

## Milestones

**M1: ABI, host, simhost.** Files:
- `winccoa/manager/MmqProbe.cxx`, `MmqTlv.hxx`, `MmqManager.hxx`, `MmqManager.cxx`
- `internal/oahost/ops.go`, `read.go` (new)
- `internal/oahost/simhost/simhost.go`, `history.go` (new)
- `winccoa/doc/spec-winccoa-native.md`

Tasks:
1. Run probe P1-P6 on `Test321` and record the results in spec §3.1. Correct this plan if an assumption fails.
2. Add the constants of §1.1 on both sides.
3. Host:
   - `ReadWait`, the `reads` map, `opDpQuery`, `opDpGetPeriod`, `opCursorNext`, `opCursorClose`;
   - `expireReads()` in the main loop (`MmqManager.cxx:1790-1793`);
   - the budgets and host stats of §1.6.
4. Go `DpQuery` / `DpGetPeriod` with paging, cursor close and the total check.
5. simhost ops, history ring and test knobs (§1.8).
6. Spec:
   - §3.1 tag table (`spec-winccoa-native.md:71-90`): add `index 24`.
   - §3.1 op list (`:93`): add `DP_GET_PERIOD 17`, `DP_QUERY 18`, `CURSOR_NEXT 19`, `CURSOR_CLOSE 20` with their
     fields, plus the flag bits.
   - §3 notes (`:62`): read answers are paged through completions, not events.
   - §7 (`:193-213`): read budgets and cursor TTL.

**M2: stores.** Files:
- `internal/winccoanative/wildcard.go` (`QueryAttrs`)
- `internal/stores/types.go` (`ArchiveWinCCOA MessageArchiveType = "WINCCOA"`)
- `internal/stores/interfaces.go` (`LevelBrowser`)
- `internal/stores/oastore/read.go`, `expand.go`, `lastval.go`, `history.go`, `aggregate.go`, `browse.go` (all new)

Tasks: §2.1-§2.7.

**M3: manager, surfaces, config, edge SDL.** Files:
- `internal/archive/manager.go`, `group.go` (S4 if not there yet), `merge.go` (new)
- `internal/broker/server.go`
- `internal/config/config.go`, `yaml-json-schema.json`, `winccoa/mmq.yaml.example`
- `internal/graphql/schema/schema.graphqls` plus `make gen` output, `internal/graphql/resolvers/resolver.go`
- `internal/restapi/handler.go`, `internal/mcp/tools.go`

Tasks: §3.1-§3.6. The SDL change merges only together with main's SDL change in monster-mq#205 (D2).

**M4: main and dashboard lockstep.** Issues monster-mq#205 and monster-mq#206 (Main-side tasks).

**M5: docs.** Files:
- `winccoa/doc/winccoa-archive.md` (new: group, fallback, bounds, deviations)
- `README.md` (feature line), `winccoa/README.md` (the `QueryMaxRows` line)
- `AGENTS.md`, as a task only:
  - `:24-27` lists MCP as off-limits although `internal/mcp/` exists (`tools.go`), so it needs rewording;
  - `:114` describes `stores/oastore/` as MMQConfigs/MMQSessions stores only, so it should name the WINCCOA
    last-value store and archive too;
  - the NATS and Redis entries are updated by their own plans (D1).

## Tests

All cases are in `test/integration/` on the simhost harness (`newSim` / `startNative`, `winccoa_native_test.go:38-120`)
unless they are marked live. The live cases are skipped unless `MMQ_LIVE_PORT` is set
(`winccoa_live_test.go:19-38`); the fixture `winccoa/scripts/mmqLiveFixture.ctl` gets `_archive` on one test DPE.

`winccoa_archive_abi_test.go` (`oahost.API` against simhost):
- `TestWinCCOaArchiveQueryPages`: a result larger than `PageBytes` arrives in several pages; header once per page;
  total matches.
- `TestWinCCOaArchiveQueryMaxRows`: `TagCount` 5 gives 5 rows and the truncated flag.
- `TestWinCCOaArchiveQueryEmpty`
- `TestWinCCOaArchivePeriodNewest`: newest N in descending order; truncated when more exist.
- `TestWinCCOaArchivePeriodOldestResume`: samples sharing a millisecond at a page boundary are delivered exactly once.
- `TestWinCCOaArchivePeriodNoArchive`: a failed index is reported.
- `TestWinCCOaArchivePeriodInvalid`: 0 names, 101 names, two systems, oldest mode with 2 names, two missing
  `TagTime`s.
- `TestWinCCOaArchiveTimeoutAbort`: `ReadDelay` longer than the timeout gives `ErrTimeout`; `OpenReads()` is 0.
- `TestWinCCOaArchiveCursorClosedOnStop`: a page callback error closes the cursor.

`winccoa_archive_test.go` (broker plus GraphQL, REST, MCP):
- **Provisioning and deploy:**
  - `TestWinCCOaArchiveGroupProvisioned`
  - `TestWinCCOaArchiveGroupNotProvisionedWithoutNamespace`
  - `TestWinCCOaArchiveGroupRecreatedOnRestart`
  - `TestWinCCOaArchiveGroupDisablePersists`
  - `TestWinCCOaArchiveStandaloneDeployError`: a WINCCOA row gives a deploy error; GraphQL create is rejected.
  - `TestWinCCOaArchiveOldHost`: with `DisableArchiveOps` the group fails to deploy with the version message.
- **Read-only and enum:**
  - `TestWinCCOaArchiveReadOnlyForced`: an update with read-only false still runs read-only, a `Reload` does not
    restart the group, and an MQTT publish does not change `sim.Sets()`.
  - `TestWinCCOaArchiveGraphQLEnum`: `archiveGroups` reports `WINCCOA`, and `lastValTypeNotEquals: NONE` includes
    the group.
- **Last value, exact topics:**
  - `TestWinCCOaArchiveCurrentValueExact`: tags, scalar root, explicit attribute, `systems/SubstationA`,
    `types/AnalogDrive`.
  - `TestWinCCOaArchiveCurrentValueRejected`: `set`, `_Users`, `MMQConfigs_k1`, `Pump1/nested`, wrong type, the
    `topics` branch, status, non-native.
  - `TestWinCCOaArchiveRemoteUnavailable`: `SubstationB` gives an error wrapping `ErrUnavailable`.
- **Last value, filters:**
  - `TestWinCCOaArchiveCurrentValuesScoped`: `Pump1/#`, `+/speed`, `types/AnalogDrive/#`, `types/+/Pump1/speed`
    (type looked up per DP), `systems/SubstationA/tags/#` (query text has `REMOTE 'SubstationA'`).
  - `TestWinCCOaArchiveBroadFilters`:
    - `#` and `winccoa/#` return local shortcut tags;
    - `winccoa/+` returns 0 rows with an unchanged `Calls(OpDpQuery)`;
    - `winccoa/+/+` returns scalar roots;
    - `winccoa/systems/+/tags/#` returns System1 and SubstationA.
  - `TestWinCCOaArchiveQueryMaxRows`: `QueryMaxRows` 100 over 300 DPEs gives 100 rows.
- **History:**
  - `TestWinCCOaArchiveHistoryExact`: order, payload, newest `limit`, 24 h default.
  - `TestWinCCOaArchiveHistoryBatches`: 150 DPEs make 2 `DP_GET_PERIOD` calls and give the global newest N.
  - `TestWinCCOaArchiveHistoryNoArchiveConfig`
- **Aggregation:**
  - `TestWinCCOaArchiveAggregatedMatchesSQLite`: the same samples, also published into a SQLite archive group; field
    `value` gives the same columns, bucket strings and values.
  - `TestWinCCOaArchiveAggregatedPaging`: a small page cap forces a resume; counts are correct.
  - `TestWinCCOaArchiveAggregatedTooLarge`
  - `TestWinCCOaArchiveAggregatedMixedGroups`: a native topic plus a SQLite topic are merged in request order.
- **Fallback:**
  - `TestWinCCOaArchiveFallbackGraphQL`: archiveGroup omitted, `"Default"` and explicit. A Default with a SQLite
    archive covering `winccoa/#` keeps serving history.
  - `TestWinCCOaArchiveFallbackREST`: `group` omitted, `Default`, `WinCCOA`, and unknown (404).
  - `TestWinCCOaArchiveFallbackMCP`: `get-topic-value`, `query-message-archive`, `query-message-archive-aggregated`.
- **Browse, search, interest:**
  - `TestWinCCOaArchiveBrowseLevels`: `+`, `winccoa/+`, `winccoa/systems/+`, `winccoa/tags/+` (ScalarTag is a leaf),
    `winccoa/tags/Pump1/+`, `winccoa/tags/Pump1/nested/+`, `winccoa/types/+`, `winccoa/types/AnalogDrive/+`.
  - `TestWinCCOaArchiveSearchTopicsBounded`
  - `TestWinCCOaArchiveInterestFilters`: PeerLink interest has no filter from a both-read-only group.

Live cases (`winccoa_live_test.go`):
- `TestLiveArchiveHistory`
- `TestLiveArchiveQueryPaging`
- `TestLiveArchiveAbort`

## Acceptance criteria

1. The probe results P1-P6 are in spec §3.1, and this plan matches them.
2. All cases above pass; the existing suite stays green; `CGO_ENABLED=0 make build` gains no dependency.
3. No read holds more than one page in Go or more than 64 MiB on the host. `TestWinCCOaArchiveTimeoutAbort` ends with
   no open reads and no late callbacks.
4. After M4, edge's `MessageStoreType` and `MessageArchiveType` equal main's.
5. With the shipped config, `currentValue(topic: "winccoa/tags/Pump1/speed")` without `archiveGroup` returns the
   WinCC OA value, and `archivedMessages` for that topic returns `dpGetPeriod` rows.
6. Main loads a WINCCOA row, logs a deploy error with M and starts the other groups. GraphQL create is rejected.
7. The dashboard edits a WINCCOA group without changing its type.
8. The live archive cases pass against `Test321`.

## Risks

- **Answer layout assumptions** (§1.9). The probe runs before the host code, and simhost follows the recorded
  behaviour.
- **Remote history** (P6). If dist does not forward `dpGetPeriod`, remote history fails with `ErrUnavailable`, and
  that is documented.
- **Search and browse truncation.** `SearchTopics` on a WINCCOA group only sees the first `QueryMaxRows` tags. The
  `QueryMaxRows` setting and the WARN line make this visible.
- **WinCC OA load.** A broad `dpQuery` still costs the data manager work up to the abort. The row limit bounds both
  the result and that work.
- **Host memory** inside the WinCC OA manager process: up to 64 MiB transient. The host stats expose it.
- **Changed results.** Native tag reads with archiveGroup `"Default"` now come from WinCC OA (more complete).
  History moves only when Default has no archive covering the topic.
- **Long aggregations.** These are sequential pages per DPE, bounded by the 30 s budget.

## Follow-ups

1. **SQL aggregation with field `""` over JSON object payloads** (X6). In both repos it gives 0 in SQLite and an
   error in Postgres. Tracked in [vogler75/monster-mq#207](https://github.com/vogler75/monster-mq/issues/207); not part of this plan.
2. **Reading the `topics` branch (MMQTopic) through WINCCOA stores.** A follow-up only if a user needs it; the Default
   group already holds those messages.
