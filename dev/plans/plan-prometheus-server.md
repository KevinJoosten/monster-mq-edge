# Plan: Prometheus metrics and PromQL API for the edge broker

**Status:** approved 2026-10-17, owner decisions in [plan-protocol-servers-shared.md](plan-protocol-servers-shared.md);
revised after checking against main and edge.
**Issue:** [vogler75/monster-mq-edge#39](https://github.com/vogler75/monster-mq-edge/issues/39). The QuestDB fix F5 is tracked separately in [vogler75/monster-mq-edge#35](https://github.com/vogler75/monster-mq-edge/issues/35).
**Scope:** port main's Prometheus-compatible server (`/metrics` scrape endpoint plus the small PromQL HTTP API used
by Grafana's Prometheus data source) to `internal/prometheus/`. Main bugs found on the way are fixed in both repos
(D3). WinCC OA groups are served through the archive plan's stores (S6).

## Context

- Main (paths under `main/broker/`):
  - `src/main/kotlin/extensions/PrometheusServer.kt` (947 lines). Cited below as `kt:<line>`.
  - `src/main/kotlin/Monster.kt:1300-1303`, `:1379-1385`: config read and server creation.
  - `yaml-json-schema.json:796-822`: the `Prometheus` config schema.
  - `main/doc/grafana.md`: user documentation.
- Main runs a separate Vert.x HTTP server (`kt:107-118`). It is read-only: it serves current values from archive
  group last-value stores, history from archive stores, and broker metrics from the metrics store.
- Edge today has none of this. The pieces it needs already exist:
  - `archive.Manager` (`Snapshot()`, `Get()`: `internal/archive/manager.go:624-638`) and `Group.LastValue()` /
    `Group.Archive()` (`group.go:101-102`, both may be nil).
  - `metrics.Collector.Latest()` / `Interval()` (`internal/metrics/collector.go:123-131`). The collector is nil when
    `Metrics.Enabled` is false (`internal/broker/server.go:393-396`).
  - `stores.MetricsStore.GetHistory` and `stores.MessageArchive.GetHistory` / `GetAggregatedHistory`
    (`internal/stores/interfaces.go:37-47`, `:226-232`). `storage.Metrics` is nil for `Metrics.StoreType: NONE`
    (`internal/broker/server.go:868-870`).
  - `auth.Cache.Authenticate` / `ValidateSession` (`internal/auth/auth.go:75-120`).
- No GraphQL change, no `enabledFeatures` change, no dashboard change. Main has no Prometheus feature flag
  (`Features.kt`), so edge adds none.

## Parity rules

1. **Config is 1:1 with main:** `Prometheus: {Enabled, Port, RawQueryLimit}` with main's defaults. No `Host` key
   (main binds `0.0.0.0`, `Monster.kt:1381`), no mount mode, no `Features.Prometheus`.
2. **Same HTTP surface:** paths, methods (plus F3), parameter sources, status codes and response bytes. That means
   the same JSON key order, Java `Double.toString` number strings and no HTML escaping (Vert.x/Jackson does not
   escape `<`, `>`, `&`, so edge encodes with `SetEscapeHTML(false)`).
3. **Same PromQL subset and regexes** (with F1). Nothing beyond main's selectors and four `*_over_time` functions.
4. **Same authentication outcomes and 401 bodies** as `validateAuthentication` (`kt:887-946`). Both repos add the
   per-topic read ACL of F7 (owner decision D5).
5. **Unmarked behaviour is a 1:1 port** and cites `kt:<line>`. Deviations are marked [edge-only] or [fix: both]
   and listed again under "Edge-only extensions and fixes".
6. **Zero CGO.** chi is already a dependency (`internal/graphql/server.go:112`).

## Behaviour to port

### Config

| Key | Default and rules | Main |
|---|---|---|
| `Prometheus.Enabled` | `false` | `Monster.kt:1302` |
| `Prometheus.Port` | `3001`, schema minimum 1, maximum 65535 | `Monster.kt:1303`, schema `:809-814` |
| `Prometheus.RawQueryLimit` | `10000`, schema minimum 1 (see F6) | `Monster.kt:1379`, schema `:815-821` |

The listener binds `0.0.0.0:<Port>` (`Monster.kt:1381`). It is independent of GraphQL: disabling GraphQL does not
disable Prometheus.

### HTTP surface

Middleware order is the same as main: CORS, then body parsing, then auth, then routing (`kt:72-88`). Auth therefore
also runs for unknown paths.

| Method | Path | Behaviour | Main |
|---|---|---|---|
| any | (all) | CORS: origin `*`; methods `GET`, `POST`, `OPTIONS`; headers `Content-Type`, `Authorization`, `Accept`. Preflight is answered before auth. | `kt:72-81` |
| GET | `/metrics` | text exposition | `kt:90` |
| GET, POST (F3) | `/api/v1/labels` | label names | `kt:92` |
| GET | `/api/v1/label/{name}/values` | label values | `kt:93` |
| GET (F3), POST | `/api/v1/series` | series | `kt:94` |
| GET, POST | `/api/v1/query_range` | range query | `kt:95-96` |
| GET, POST | `/api/v1/query` | instant query | `kt:97-98` |
| GET | `/` | `{"status":"ok","message":"MonsterMQ Prometheus API"}` | `kt:100-105` |

Parameters:
- `query`, `start`, `end`, `step` are read from the URL query first, then from the form body. A missing value is
  `""` (`kt:648-652`).
- `match[]`: only the first value is used, URL query first, then the form body (`kt:755-759`).

### Authentication (`kt:887-946`)

Every 401 has `Content-Type: application/json`. Bodies are byte-exact.

| Case | Result | Main |
|---|---|---|
| `UserManagement.Enabled` is false | allow | `kt:888` |
| No `Authorization` header, `AnonymousEnabled` | allow | `kt:892-896` |
| No header, edge `AllowAnonymousLocalhost` and `auth.IsLocalhostRequest(r)` [edge-only] | allow | E1 |
| No header, otherwise | 401, `WWW-Authenticate: Basic realm="MonsterMQ Prometheus"`, `{"error":"Authentication required"}` | `kt:897-901` |
| Prefix `Bearer ` (case-insensitive), token = rest after 7 chars, valid | allow | `kt:904-910` |
| Bearer, invalid | 401, `WWW-Authenticate: Bearer error="invalid_token"`, `{"error":"Invalid or expired token"}` | `kt:911-915` |
| Prefix `Basic ` (case-insensitive), base64 decodes, split on the first `:`, credentials valid and user enabled | allow | `kt:918-926` |
| Basic with two parts, credentials invalid | 401, Basic realm header, `{"error":"Invalid credentials"}` | `kt:927-932` |
| Basic that fails to decode or has no `:`, or any other scheme | 401, Basic realm header, `{"error":"Authentication required"}` | `kt:936-945` |

Edge validates Bearer with `authCache.ValidateSession(token)` (`auth.go:105`) and Basic with
`authCache.Authenticate(ctx, user, pass)` (`auth.go:75`, which rejects disabled users). It does not use
`auth.AuthenticateHeader`, because that returns one generic error for every scheme (`auth/http.go:44-76`) and the
401 bodies differ by case. Bearer tokens are edge session tokens, not JWTs (E2).

The middleware passes the identity to the handlers for F7. It stores the user with `auth.WithPrincipal`
(`internal/auth/context.go:12`):
- Basic: the user from `Authenticate`.
- Bearer: the user from `ValidateSession`.
- Requests let in by E1: `auth.LocalhostUser` (`auth/http.go:14-21`), as REST does (`restapi/handler.go:74-77`).
- Anonymous requests: no principal, which means the Anonymous user.

### Response format

| Item | Exact form | Main |
|---|---|---|
| JSON content type | `application/json` | `kt:224` and others |
| Label lists | `{"status":"success","data":[...]}` | `kt:225` |
| Vector | `{"status":"success","data":{"resultType":"vector","result":[...]}}` | `kt:684-693` |
| Matrix | `{"status":"success","data":{"resultType":"matrix","result":[...]}}` | `kt:673-682` |
| Topic sample | `{"metric":{"__name__":"topics","group":G,"topic":T,"field":F},"value":[<now s>,"<v>"]}` | `kt:629-637` |
| Metric sample | `{"metric":{"__name__":"metrics","node":N,"metric":M},"value":[<now s>,"<v>"]}` | `kt:598-605` |
| Topic series | `{"metric":{"__name__":"topics","group":G,"topic":T,"field":F},"values":[[<s>,"<v>"],...]}` | `kt:654-662` |
| Metric series | `{"metric":{"__name__":"metrics","node":N,"metric":M},"values":[...]}` | `kt:664-671` |
| Number strings | Java `Double.toString` (JDK 21, shortest digits): `42` -> `42.0`, `0.5` -> `0.5`, `1e7` -> `1.0E7`, `1e-4` -> `1.0E-4`, `NaN`, `Infinity` | `kt:604`, `:636`, `:496`, `:834` |
| Range errors | still `success` with an empty matrix | `kt:507-510` |

Edge builds these with ordered structs (Go maps would sort the keys) and a `javaDouble(float64) string` helper.

### PromQL subset (`kt:747-794`)

| Step | Rule | Main |
|---|---|---|
| Trim | `query.trim()` | `kt:762` |
| Function | `(?i)^(avg\|min\|max\|count)_over_time\((.+)\[(\w+)\]\s*\)$`. Group 1 uppercased is `Function`, group 2 trimmed is the inner selector, group 3 is `Window` (F1). The window is parsed but not used: the aggregation interval comes from `step`. | `kt:764-779` |
| Metric | `^(\w+)\s*(\{[^}]*\})?` on the inner selector. Group 1 is the metric, group 2 the label block. | `kt:781-784` |
| Labels | `(\w+)="([^"]*?)"` inside the label block. Only `=` matchers count; `=~`, `!=` and `!~` are ignored. The last duplicate wins. | `kt:786-791` |
| `match[]` labels | `(\w+)=~?"([^"]*?)"` over the whole selector; `=~` is treated as equality. | `kt:747-753` |

### `GET /metrics` (`kt:123-216`)

- Content-Type `text/plain; version=0.0.4; charset=utf-8` (`kt:150`). Broker metrics come first, then topic values
  (`kt:146-147`).
- Each family starts with `# HELP <name> <help>\n# TYPE <name> gauge\n` (`kt:205-208`):
  - `metrics`, help `MonsterMQ broker metric`
  - `topics`, help `Current value of MQTT topic`
- Line format: `name{k="v",...} <javaDouble>\n`. Label values escape `\` -> `\\`, `"` -> `\"` and newline -> `\n`
  (`kt:210-216`). NaN and infinite values are skipped (`kt:211`).
- Broker metrics: 7 lines for `node=<cfg.NodeID>`, in this order: `messages_in`, `messages_out`, `sessions`,
  `subscriptions`, `queue_depth`, `bus_in`, `bus_out` (`kt:157-165`). Values come from `collector.Latest()`; a nil
  collector gives zeros, as main renders 0.0 when the node request fails (`kt:136`).
- Topic values: for every group with a last-value store, `FindMatchingMessages("#")` (`kt:174-175`). Rules:
  - The payload is trimmed. A number gives one line with `field=""` (`kt:176-182`).
  - Otherwise, if it parses as a JSON object, each top-level entry in payload order gives one line with
    `field="<key>"` when the value is a JSON number or a numeric string (`kt:184-197`). Booleans, nested objects and
    other strings are skipped.
  - Anything else is skipped (`kt:198`).
  - Label order is `group`, `topic`, `field`. Vert.x keeps the payload's key order, so edge walks the object with
    `json.Decoder` tokens instead of decoding into a map.
- Topic lines are filtered by the read ACL (F7): each message topic is checked for the scraping user. A scrape
  with Basic auth therefore sees only that user's topics. Broker `metrics` lines are not filtered.
- Groups are visited in name order (E5). WINCCOA groups follow E4.

### Label names, label values and series

| Endpoint | Behaviour | Main |
|---|---|---|
| `/api/v1/labels` | `["__name__","group","topic","field","node","metric"]` | `kt:220-226` |
| `label/__name__/values` | `["topics","metrics"]` | `kt:241-245` |
| `label/group/values` | all running group names, sorted, **not** filtered by `match[]` | `kt:263-269` |
| `label/topic/values` | For each group selected by `match[]` `group` (all groups if absent): `FindMatchingMessages(topic matcher or "#")`, collecting topic names. Each group stops once 5000 are collected. Result is sorted. | `kt:246-262` |
| `label/field/values` | Same group and topic selection. A numeric payload adds `""`; a JSON object adds each top-level key whose value is a number or numeric string. Each group stops once 1000 are collected. Result is sorted. | `kt:270-296` |
| `label/metric/values` | all 19 `BROKER_METRIC_LABELS` in main's order | `kt:53-64`, `:297-302` |
| `label/node/values` | `[cfg.NodeID]` (E6) | `kt:303-309` |
| `label/<other>/values` | `[]` | `kt:310-315` |
| `/api/v1/series` | `topics` series when the metric is absent or `topics` (F4). Per message, fields are computed as for field values; if there are none, `""` is used. One `{"__name__":"topics","group","topic","field"}` object per field. A group stops after a message once the result holds 5000 or more entries. Then `metrics` series when the metric is absent or `metrics`: one `{"__name__":"metrics","node","metric"}` object for each of the 19 labels. | `kt:319-380` |

WINCCOA groups follow E4 in `label/topic/values`, `label/field/values` and `/api/v1/series`.

F7 applies to `label/topic/values`, `label/field/values` (fields come only from allowed topics) and the `topics`
series of `/api/v1/series`. Denied topics are skipped silently and do not count toward the 5000 / 1000 caps. The
`__name__`, `group`, `metric` and `node` lists and the `metrics` series are not topic data and stay unchecked.

### `/api/v1/query` (`kt:562-644`)

- An empty `query` gives an empty vector (`kt:568-571`). `now` is the request time in whole epoch seconds
  (`kt:564`).
- `metrics`:
  - `metric` label (default `""`) and `node` filter: a `node` label that is not `cfg.NodeID` gives an empty vector
    (`kt:577-586`).
  - Only the 7 scrape labels have instant values (`kt:874-883`), read from `collector.Latest()`. Any other label, or
    a nil collector, gives no sample (`kt:596-597`).
- `topics`:
  - `topic` (default `""`), `group` (default `"Default"`, E3) and `field` (default `""`) labels (`kt:617-619`).
  - An empty topic, a missing group or a nil `LastValue()` give an empty vector (`kt:623-625`).
  - Otherwise `FindMatchingMessages(topic)` runs. Each message whose topic passes the read ACL (F7) and whose
    trimmed payload passes `extractNumericValue(payload, field)` gives a sample. The `topic` label is the message
    topic; `group` and `field` are the query values (`kt:625-640`).

### `/api/v1/query_range` (`kt:382-560`)

Time and step:
- An empty `query` gives an empty matrix (`kt:390-393`).
- `start` / `end`: parsed as epoch seconds (a double, then `*1000` and truncated to ms), else as an RFC 3339
  instant. On failure they default to now-3600 s and now (`kt:395-396`, `:840-847`).
- `step`: `""` becomes `"60"`. It is parsed as a double and truncated toward zero; a non-number gives 60
  (`kt:386`, `:397`). Duration strings like `15s` therefore become 60.

`metrics` (`kt:513-560`, with F2):
- `metric` label (default `""`); a `node` label that is not `cfg.NodeID` gives an empty matrix (`kt:520-529`). A nil
  `storage.Metrics` gives an empty matrix (`kt:531-535`).
- Rows come from `storage.Metrics.GetHistory(ctx, stores.MetricBroker, cfg.NodeID, from, end, limit)`, with `from`
  and `limit` as defined by F2. Each row's JSON gives a value via the label-to-key table below. A missing key
  counts as 0, as main's `jsonToBrokerMetrics` defaults it (`MetricsStoreSQLite.kt:423-446`).
- The values go through `downsampleByStep(raw, start, end, step)` using the request's original `start`. One series
  per node is returned, also when it has no values (`kt:545-551`).

`topics` (`kt:407-511`):
- Labels: `topic` (default `""`), `group` (default `"Default"`, E3), `field` (default `""`) (`kt:409-411`). An empty
  topic or a nil `Archive()` gives an empty matrix (`kt:415-426`).
- F7: a topic without wildcards that the user may not read gives an empty matrix before any store call. The raw
  path also checks each row's `Topic`, so a wildcard topic only uses allowed rows. The aggregated path matches
  topics exactly (`topic IN (...)`, `internal/stores/sqlite/message_archive.go:339`), so the check on the query
  topic covers it.
- **Raw path** when `step < 60` and there is no function (`kt:431`):
  - `Archive().GetHistory(ctx, topic, &start, &end, RawQueryLimit)` (`kt:433-438`). The SQL stores return the newest
    `limit` rows (`ORDER BY time DESC LIMIT`, `internal/stores/sqlite/message_archive.go:121`), as main does
    (`MessageArchiveSQLite.kt:139`).
  - Each row gives `(timestamp ms / 1000, extractNumericValue(payload, field))`; rows without a value are skipped
    (`kt:441-456`).
  - `downsampleByStep` (`kt:458`). The series uses the query's `topic`, `group` and `field` (`kt:460`).
- **Aggregated path** otherwise (`kt:461-501`):
  - `fn = Function` or `"AVG"`. `interval = snapToValidIntervalMinutes(step)`. `fields = []` when `field == ""`,
    else `[field]`.
  - `Archive().GetAggregatedHistory(ctx, []string{topic}, start, end, interval, []string{fn}, fields)`.
  - Column name: `topic + ("" or "." + strings.ReplaceAll(field, ".", "_")) + "_" + lower(fn)` (`kt:478-479`). This
    is shared as `stores.AggregatedColumnName(topic, field, fn)` (F5).
  - A missing column gives an empty matrix with no series (`kt:482-483`).
  - Otherwise, for each row: `Rows[i][0]` is parsed as RFC 3339 to epoch seconds, and the column value must be
    numeric (any Go number type) or the row is skipped. The value is formatted with `javaDouble` (`kt:486-497`).

Helpers:
- `snapToValidIntervalMinutes` (`kt:800-809`): `m = max(step/60, 1)`; `m < 5` -> 1, `< 15` -> 5, `< 60` -> 15,
  `< 1440` -> 60, else 1440.
- `downsampleByStep` (`kt:816-838`):
  - `step = max(step, 1)`; `bucket = (ts - start) / step`. Go's integer division truncates toward zero like
    Kotlin's.
  - The bucket time is `start + bucket*step`, emitted only when `<= end`. The value is the bucket average as
    `javaDouble`, in bucket order.

### Payload values (`kt:695-723`)

| Function | Rule | Main |
|---|---|---|
| `extractNumericValue(p, "")` | Numbers only: `parseJavaDouble(p)`, else `p` must be a JSON number literal. No `"value"` extraction. | `kt:695-698` |
| `extractNumericValue(p, f)` | `extractJSONField(p, f)` | `kt:699-701` |
| `extractJSONField(p, f)` | `p` must be a JSON object. Split `f` on `.` and walk nested objects; any non-object on the way gives no value. The leaf must be a JSON number, or a string that passes `parseJavaDouble`. Booleans give no value. | `kt:704-723` |
| `parseJavaDouble(s)` | Matches Kotlin `String.toDoubleOrNull()` (Java `Double.valueOf` grammar): trims chars `<= 0x20`; accepts decimal and hex (`0x1p3`) forms, an optional `d`/`D`/`f`/`F` suffix, `NaN` and `[+-]Infinity`. Rejects forms only Go accepts (`Inf`, `inf`, other spellings of infinity or NaN, underscores). | `kt:178`, `:189`, `:697`, `:717` |

WinCC OA tags (payload `{"time":"<ISO>","value":<v>}`, `internal/winccoanative/values.go:171-194`) therefore need
`field="value"`. `/series` and `label/field/values` already offer `field="value"` for them; the ISO `time` string is
skipped by the numeric-string rule.

### Broker metric labels

| Label | Instant and scrape (`collector.Latest()`) | History JSON key | Main |
|---|---|---|---|
| `messages_in` | `MessagesIn` | `messagesIn` | `kt:851`, `:875` |
| `messages_out` | `MessagesOut` | `messagesOut` | `kt:852`, `:876` |
| `sessions` | `NodeSessionCount` | `nodeSessionCount` | `kt:853`, `:877` |
| `subscriptions` | `SubscriptionCount` | `subscriptionCount` | `kt:854`, `:878` |
| `queue_depth` | `QueuedMessages` | `queuedMessagesCount` | `kt:855`, `:879` |
| `bus_in` | `MessageBusIn` | `messageBusIn` | `kt:856`, `:880` |
| `bus_out` | `MessageBusOut` | `messageBusOut` | `kt:857`, `:881` |
| `mqtt_client_in`, `mqtt_client_out` | none | `mqttClientIn`, `mqttClientOut` | `kt:858-859` |
| `kafka_client_in`, `kafka_client_out` | none | `kafkaClientIn`, `kafkaClientOut` | `kt:860-861` |
| `opcua_client_in` | none | `opcUaClientIn` | `kt:862` |
| `winccoa_client_in`, `winccua_client_in` | none | `winCCOaClientIn`, `winCCUaClientIn` | `kt:863-864` |
| `nats_client_in`, `nats_client_out` | none | `natsClientIn`, `natsClientOut` | `kt:865-866` |
| `redis_client_in`, `redis_client_out` | none | `redisClientIn`, `redisClientOut` | `kt:867-868` |
| `neo4j_client_in` | none | `neo4jClientIn` | `kt:869` |

Edge's collector writes the first 11 history keys (`collector.go:40-52`); the others read as 0, as in main.

## Edge-only extensions and fixes

| # | Item | Main | Edge |
|---|---|---|---|
| E1 | [edge-only] Anonymous localhost | No such option | With no `Authorization` header, `AllowAnonymousLocalhost` and a localhost peer allow the request, as REST (`restapi/handler.go:75`) and GraphQL (`graphql/server.go:277`) do. |
| E2 | [edge-only] Bearer tokens | JWT checked with `JwtService` (`kt:904-916`) | Edge session token from GraphQL `login` (`resolver.go:584`) or REST `POST /api/v1/login` (`restapi/handler.go:123`). These tokens are process-local and expire after 24 h (`auth.go:86-103`), so the docs tell Grafana users to use Basic auth. |
| E3 | [edge-only] Group for a missing `group` label (`/query`, `/query_range`) | `"Default"` (`kt:410`, `:618`) | `archive.Manager.ResolveGroupForTopic` from the archive plan's "Contract for other plans", which routes WinCC OA topics to the WINCCOA group. The result's `group` label is the resolved group's name. An explicit label is passed through unchanged. Phase 6. |
| E4 | [edge-only] WINCCOA groups in scrape and enumeration | Main never deploys a WINCCOA group (D2) | See "WinCC OA groups" below. |
| E5 | [edge-only] Group order | `ConcurrentHashMap` order (`ArchiveHandler.kt:35`, `:1126-1128`) | Sorted by name. `Snapshot()` is map order (`manager.go:624-632`), so the package sorts it. |
| E6 | [edge-only] Single node | Cluster node IDs (`kt:124`, `:304`, `:364`, `:522`, `:579`) | `[cfg.NodeID]`. A `node` label that names another node gives an empty result, matching main's filter (`kt:521-522`, `:578-579`). |
| F1 | [fix: both] `*_over_time` crash | `funcPattern` has two groups but `groupValues[3]` is read (`kt:764`, `:774`), so every `avg/min/max/count_over_time` query fails with HTTP 500 | Capture the window: `\[(\w+)\]`. |
| F2 | [fix: both] Broker-metric history truncation | `getBrokerMetricsHistory(node, start, end, null)` (`kt:541`) uses the default limit 100 (`IMetricsStore.kt:98`) with `ORDER BY timestamp DESC` (`MetricsStoreSQLite.kt:194-195`): only the newest 100 samples | Window and limit, see below. Edge stores default `limit <= 0` to 1000 ascending (`sqlite/metrics_store.go:66-72`, same in postgres, mongodb, memory), so passing 0 would also truncate. |
| F3 | [fix: both] Missing HTTP methods | `/api/v1/series` POST only (`kt:94`); `/api/v1/labels` GET only (`kt:92`) | Both accept GET and POST. The Prometheus HTTP API defines both methods for these endpoints, and clients switch between them (Grafana's "HTTP method" setting). Vert.x answers the missing method with 405, so this is a bug, not parity. `label/{name}/values` stays GET only, as in the Prometheus API. |
| F4 | [fix: both] `/series` ignores the metric name | The metric is read only from `__name__` (`kt:324`), so `match[]=topics{...}` also returns all `metrics` series | The metric is `__name__` if present, else the leading name matched by `^(\w+)` (the parsePromQL metric pattern). A selector that names `topics` returns only topic series. This is a bug because the series returned do not match the selector. |
| F5 | [fix: edge] QuestDB aggregated column names | Main QuestDB: `"$topic$fieldAlias" + "_$funcLower"`, `.` in field replaced by `_` (`MessageArchiveQuestDB.kt:451`, `:460`) | Edge QuestDB uses `"%s%s (%s)"` without the replacement (`internal/stores/questdb/questdb.go:456-470`), so the Prometheus column lookup misses. Fix the edge store to use `stores.AggregatedColumnName`. This is not a deviation; it removes one. It also changes GraphQL `aggregatedMessages` column names for QuestDB groups (`resolver.go:1087-1150` passes `Columns` through), and they then match main. |
| F6 | [fix: both] `RawQueryLimit` 0 | `doc/grafana.md:57` says 0 means unlimited, but the schema requires >= 1 (`yaml-json-schema.json:815-821`) and `LIMIT 0` returns no rows (`MessageArchiveSQLite.kt:139-140`) | Both brokers treat a value < 1 as invalid: log a warning and use 10000. Main corrects the doc. |
| F7 | [fix: both] Read ACL on topic data (D5) | No ACL check (`kt:172-203`, `:246-296`, `:319-360`, `:407-511`, `:616-643`): any authenticated user, or anonymous, reads every topic of every group | Topic data is filtered per user, see below. REST and GraphQL reads already check ACLs (`restapi/handler.go:160-165`, `resolver.go:241-244`). |

**F2 window and limit (both repos).** `I` is the metrics collection interval in seconds, at least 1. Edge uses
`collector.Interval()`, or `cfg.Metrics.CollectionIntervalSeconds` when the collector is nil. Main uses the
`collectionInterval` from `Monster.kt:1360-1363`. `maxPoints = 50000`.

```text
from  = start
if end - from > maxPoints*I { from = end - maxPoints*I }   // keep the newest window
limit = (end - from)/I + 2                                  // every sample in [from, end], with slack
rows  = history(MetricBroker, node, from, end, limit), sorted ascending
values = downsampleByStep(rows, start, end, step)           // buckets still aligned to the request start
```

The limit covers the whole window, so both brokers return the same samples whatever their store order. Main sorts
the DESC list ascending before downsampling.

**F7 read ACL (both repos, D5).**

- **When it applies:** user management is on and the request's user is not an admin. Requests with no principal are
  checked as the Anonymous user, which `auth.Cache.Allow("", ...)` does: it uses the `Anonymous` record when one
  exists, and otherwise follows `AnonymousEnabled` (`internal/auth/auth.go:288-297`). Requests let in by E1 run as
  `auth.LocalhostUser`. That is an admin, so they read everything, the same edge-wide localhost convention as REST
  and MQTT (`hook_auth.go:180-182`).
- **The check:** a principal with `IsAdmin` passes at once. That covers `LocalhostUser`, which is synthetic and not in the cache, as in `hook_auth.go:180-182`. Every other request uses `authCache.Allow(username, topic, false)` (`auth.go:237-252`). It already passes admins
  (`auth.go:302-304`) and applies the canonical WinCC OA alias rule (`auth.go:243-251`). Every check uses the
  concrete topic of a message or archive row, never only the request's filter.
- **Checked endpoints:**
  - `/metrics` topic lines
  - `label/topic/values`
  - `label/field/values` (fields only from allowed topics)
  - the `topics` series of `/api/v1/series`
  - `topics` samples of `/api/v1/query` and `/api/v1/query_range`. A denied topic without wildcards gives an empty
    matrix.
- **Denied topics** are skipped silently and do not count toward the 5000 / 1000 caps.
- **Not checked:** broker `metrics{...}` in the scrape, `/query`, `/query_range` and `/series`. They are not topic
  data. The `group`, `metric`, `node` and `__name__` label lists are not checked either.
- **Who the user is:** see "Authentication". Handlers read the principal from the request context.

### WinCC OA groups (E3, E4), after archive M2/M3

WINCCOA groups are groups with `Config().LastValType == stores.MessageStoreWinCCOA` (`internal/stores/types.go:68`).
Their stores come from the archive plan (archive M2), and the provisioned group and `ResolveGroupForTopic` from
archive M3.

- **Scoped filter:** `Names.Classify(filter) == winccoanative.KindNative`, for example `winccoa/tags/Pump1/#`. Every
  other filter, such as `#`, `<Root>/#` or `+/#`, is **broad**. This is the S6 definition (archive plan C3).
- **Scrape:** a WINCCOA group is queried only with those entries of its own `Config().TopicFilters` that are scoped.
  The provisioned group (`<Root>/#`) is therefore skipped, and a user group with
  `TopicFilters: ["winccoa/tags/Pump1/#"]` is scraped.
- **Enumeration** (`/series`, `label/topic/values`, `label/field/values`): a WINCCOA group is queried only when
  `match[]` names it in `group`, or carries a scoped `topic` matcher. Otherwise it is skipped. A skip produces no
  host call.
- **Bounds:** every WINCCOA last-value call that is still made, including `/query` with a wildcard topic, is capped
  by the S6 row limit, in addition to the 5000 / 1000 caps above. Prometheus adds no scans of its own.
- **History:** the raw path calls the WINCCOA archive's `GetHistory` with `RawQueryLimit`. It relies on that store
  returning the newest `limit` samples, as the SQL stores do. The aggregated path relies on the archive plan's
  bounded aggregation and on its column names matching `stores.AggregatedColumnName` (`topic.value_avg` for
  `field="value"`).

**Smaller, unavoidable differences** (engine and library): chi's 404/405 bodies differ from Vert.x's; the CORS
preflight status code may differ; one node instead of a cluster (E6). They are listed in `doc/grafana.md`.

## Main-side tasks

The main side is tracked in [vogler75/monster-mq#204](https://github.com/vogler75/monster-mq/issues/204). That issue holds the fix plan for F1-F4, F6 and F7, with the
`PrometheusServer.kt` / `Monster.kt` lines, the F2 window algorithm and the matching tests. It ships in the same
release as this plan (D3). F5 is an edge-only store fix.

## Package layout

`internal/prometheus/`:
- `server.go`: `Deps`, `New(Deps) *Server`, `Handler() http.Handler` (chi router, CORS, auth, routes), and
  `Start()` / `Stop(ctx)` for the `http.Server` on `0.0.0.0:<Port>`.

  ```go
  type Deps struct {
      Cfg         *config.Config
      Archives    *archive.Manager
      Metrics     stores.MetricsStore          // may be nil
      Collector   *metrics.Collector           // may be nil
      Auth        *auth.Cache
      NativeNames func() winccoanative.Names   // nil when the native namespace is off
      Logger      *slog.Logger
  }
  ```

- `auth.go`: the authentication table with the exact 401 bodies; it stores the principal in the context. It also
  has `canRead(r *http.Request, topic string) bool` (F7), the one place handlers ask about topic access.
- `scrape.go`: `/metrics`.
- `api.go`: labels, label values, series, query, query_range, plus `groups()` (`Snapshot()` sorted by name) and
  nil checks for `LastValue()`, `Archive()`, `Metrics` and `Collector`.
- `promql.go`: `parsePromQL`, `extractLabelMatchers`, `seriesMetricName` (F4), `parsePrometheusTime`, `parseStep`,
  `snapToValidIntervalMinutes`, `downsampleByStep`, `metricsWindow` (F2).
- `values.go`: `extractNumericValue`, `extractJSONField`, `numericFields` (ordered top-level walk),
  `parseJavaDouble`, `javaDouble`, the broker-label tables.
- `winccoa.go`: `isWinCCOAGroup`, `isBroadFilter`, `scopedFilters`, the enumeration skip rule (E4).

`internal/stores/types.go`: `AggregatedColumnName(topic, field, fn string) string` (F5).

## Wiring

**`internal/config/config.go`**
- Add `PrometheusConfig{Enabled bool; Port int; RawQueryLimit int}` with yaml tags `Enabled`, `Port`,
  `RawQueryLimit`, and `Prometheus PrometheusConfig \`yaml:"Prometheus"\`` to `Config` (:416-474), next to `MCP`.
- Defaults in `Default()` (:499-574): `Prometheus: PrometheusConfig{Enabled: false, Port: 3001, RawQueryLimit: 10000}`.
- `Validate()` (:633), only when enabled: a `Port` outside 1-65535 is an error. `RawQueryLimit < 1` becomes 10000
  with a warning (F6).
- `FeaturesConfig` (:232-244) is unchanged.

**Config files** (shared config checklist)
- `yaml-json-schema.json`: add a `"Prometheus"` object after `"MCP"` (:256-262), with `additionalProperties: false`
  and main's titles, descriptions, defaults and bounds (main schema :796-822). The root has
  `additionalProperties: false` (:7), so this is required.
- `config.yaml.example`: a disabled `Prometheus:` block with all three keys after `MCP:` (:89-90).
- `scripts/deb/config.yaml`: the same disabled block after `MCP:` (:78-79), so operators find it.

**`internal/broker/server.go`**
- Add a `prom *prometheus.Server` field to `Server` (:55-94) and to the struct literal (:747).
- Create it after the archive manager (:266), the collector (:389-396), the auth cache (:247) and the native names
  are known, when `cfg.Prometheus.Enabled`. `NativeNames` uses the same choice as the resolver (:706-710).
- `Serve()`: start it in a goroutine next to the GraphQL start (:1053-1058) and log listen errors.
- `Close()`: stop it before `stopGraphQL()` (:1082) and `archives.Stop()` (:1083-1085), so no request uses a stopped
  group.

**Docs**
- `doc/grafana.md`, mirroring main's page:
  - Edge differences: E1-E6, Basic auth for Grafana, `field="value"` for WinCC OA tags.
  - The skip rule for WINCCOA groups.
  - The read ACL (F7), and that E1 localhost requests read everything, as with REST.
- A README "Highlights" line.
- `AGENTS.md` repository layout (:88-128): add `prometheus/`.

## Phases

1. **Config and server skeleton.**
   - Files: `internal/config/config.go`, `yaml-json-schema.json`, `config.yaml.example`, `scripts/deb/config.yaml`,
     `internal/prometheus/server.go`, `internal/prometheus/auth.go`, `internal/broker/server.go`.
   - Tasks: config, schema and samples; listener; CORS; auth table; principal in context and `canRead` (F7);
     `GET /`; start/stop wiring.
2. **Parsing and values.**
   - Files: `internal/prometheus/promql.go`, `internal/prometheus/values.go`.
   - Tasks: regexes with F1; time and step parsing; `snapToValidIntervalMinutes`; `downsampleByStep`;
     `parseJavaDouble`; `javaDouble`; ordered JSON field walk; label tables.
3. **Scrape, labels, series, instant query.**
   - Files: `internal/prometheus/scrape.go`, `internal/prometheus/api.go`.
   - Tasks: `/metrics` (7 broker lines, topic lines), labels (F3), label values, series (F3, F4), `/query`,
     sorted groups (E5), nil checks, read ACL on topic data (F7).
4. **Range query.**
   - Files: `internal/prometheus/api.go`, `internal/prometheus/promql.go`, `internal/stores/types.go`.
   - Tasks: metrics history with the F2 window; raw and aggregated topic paths with the F7 checks;
     `stores.AggregatedColumnName`.
5. **QuestDB column names (F5).**
   - Files: `internal/stores/questdb/questdb.go` (:456-470).
   - Tasks: use `stores.AggregatedColumnName`; the live test below.
6. **WinCC OA groups (after archive M2/M3).**
   - Files: `internal/prometheus/winccoa.go`, `internal/prometheus/api.go`, `internal/prometheus/scrape.go`.
   - Tasks: E3 via `archive.Manager.ResolveGroupForTopic`; E4 skip rule; S6 row limit; WinCC OA tests.
7. **Docs and main-side tasks.**
   - Files: `doc/grafana.md`, `README.md`, `AGENTS.md`; in main, `PrometheusServer.kt`, `Monster.kt`,
     `doc/grafana.md`.
   - Tasks: main-side fixes in monster-mq#204; same release.

Phases 1-5 do not depend on the archive plan. Phase 6 waits for archive M2 (stores) and M3 (provisioning and
`ResolveGroupForTopic`).

## Tests

All tests are in `test/integration/` and drive the broker through its real listeners (AGENTS.md:146-148).

**`test/integration/prometheus_test.go`**
- **Helper `startWithPrometheus(t, mqttPort, promPort, cfgFns...)`**, modelled on `startWithGraphQL`
  (`graphql_test.go:23-50`): enable Prometheus on `promPort` and wait for `GET /` to return 200. History cases add a
  group `Hist` with SQLite last-value and archive stores via `Storage().ArchiveConfig.Save` and
  `Archives().Reload` (as `archive_test.go:124-134` does).
- **Root:** `GET /` gives 200 and exactly `{"status":"ok","message":"MonsterMQ Prometheus API"}`.
- **Scrape:** publish `sensors/t1` = `21.5`, `sensors/n` = `42`, `sensors/j1` = `{"b":"2.5","a":1,"c":"x","d":true}`,
  and topic `q"x\y` = `1`. Check:
  - the exact Content-Type;
  - the HELP/TYPE lines;
  - the 7 `metrics{node="<id>",metric=...}` lines, in order;
  - `topics{group="Default",topic="sensors/t1",field=""} 21.5` and `... topic="sensors/n",field=""} 42.0`;
  - for `sensors/j1`, `field="b"} 2.5` before `field="a"} 1.0` (payload order), with no `c`/`d` lines;
  - the escaped topic label `topic="q\"x\\y"`.
- **Labels:** GET and POST `/api/v1/labels` (F3) both give the exact list.
- **Label values:**
  - `__name__` -> `["topics","metrics"]`;
  - `group` -> sorted names, unchanged by a `group` matcher;
  - `topic` with `match[]=topics{group="Default",topic="sensors/#"}` -> sorted topics;
  - `field` for `sensors/j1` -> `["a","b"]`, and for `sensors/t1` -> `[""]`;
  - `metric` -> the 19 labels in main's order;
  - `node` -> `[<NodeId>]`;
  - `foo` -> `[]`.
- **Series:** GET and POST (F3).
  - `match[]={__name__="topics",topic="sensors/j1"}` gives the `a` and `b` series.
  - `match[]=topics{topic="sensors/t1"}` gives only topic series (F4).
  - `match[]=metrics` gives 19 metric series.
  - With no `match[]`, both kinds are returned.
- **Instant query:**
  - `topics{group="Default",topic="sensors/t1"}` -> `"21.5"`.
  - `topics{topic="sensors/n"}` -> `"42.0"`.
  - `topics{topic="sensors/j1",field="b"}` -> `"2.5"`.
  - `topics{topic="sensors/j1"}` -> no samples (numbers only).
  - A nested field `{"m":{"rpm":1e7}}` with `field="m.rpm"` -> `"1.0E7"`.
  - `metrics{metric="sessions"}` -> one sample; `metrics{metric="nats_client_in"}` and
    `metrics{node="other",metric="sessions"}` -> no samples.
  - The same query by GET and by POST form gives the same result.
- **Range, raw:** publish 6 values into `Hist` 1 s apart and query `topics{group="Hist",topic="h/1"}` with step 2.
  Bucket times are `start + k*2`, values are bucket averages, and points after `end` are dropped. With
  `RawQueryLimit: 3`, only the newest 3 values remain.
- **Range, aggregated:**
  - Step 60 gives AVG over minute buckets.
  - `max_over_time(topics{group="Hist",topic="h/1"}[5m])` with step 15 gives MAX. Before F1 this was a 500.
  - `count_over_time` gives counts.
  - A missing column (topic not in the archive) gives `result: []`.
- **Range, metrics (F2):** with `Metrics.CollectionIntervalSeconds: 1`, insert 5000 broker rows over
  [now-5000 s, now] through `Storage().Metrics.StoreMetrics`. Query `metrics{metric="sessions"}` with step 10:
  one bucket per 10 s across the whole range, including the newest (before F2, edge stopped after 1000 rows). Insert one row at now-200000 s, query [now-200000 s, now] with step 3600:
  the old row is outside the clamped window and absent.
- **Nil stores:**
  - `Metrics.StoreType: NONE` -> metrics range gives an empty matrix.
  - `Metrics.Enabled: false` -> the scrape still has 7 zero lines and instant metrics are empty.
  - A group with `LastValType: NONE` -> empty vector.
  - `ArchiveType: NONE` -> empty matrix.
- **Auth:** with user management on and anonymous off, check:
  - no header -> 401 with the exact header and body;
  - a bad Bearer -> `Bearer error="invalid_token"` / `{"error":"Invalid or expired token"}`;
  - wrong Basic credentials -> `{"error":"Invalid credentials"}`;
  - Basic without `:` -> `{"error":"Authentication required"}`;
  - valid Basic -> 200;
  - a Bearer token from REST `POST /api/v1/login` -> 200;
  - `AllowAnonymousLocalhost` with no header -> 200 (E1);
  - an `OPTIONS` preflight succeeds without credentials and carries the CORS headers.
- **Read ACL (F7):** user management on. Publish numeric values to `allowed/a`, `allowed/secret/b` and `other/c` into
  `Hist`.
  - **User `reader`** (not admin) has an allow rule `allowed/#` and a deny rule `allowed/secret/#`. With Basic auth:
    - the scrape has `allowed/a` but no `allowed/secret/b` or `other/c` lines, and still has the 7 `metrics` lines;
    - `label/topic/values` with `match[]=topics{topic="allowed/#"}` -> `["allowed/a"]`;
    - `label/field/values` -> only fields of `allowed/a`;
    - `/series` -> only `allowed/a`, plus all 19 metric series;
    - `/query` `topics{group="Hist",topic="allowed/#"}` -> one sample, `allowed/a`;
    - `/query_range` on `allowed/secret/b` -> `result: []`, on both the raw and the aggregated path;
    - `/query_range` raw with `topic="allowed/#"` uses only `allowed/a` rows;
    - `metrics{metric="sessions"}` instant and range -> unchanged.
  - **Cap counting:** publish 5000 denied topics `allowed/secret/n<i>` first, then `allowed/z`.
    `label/topic/values` for `reader` still contains `allowed/z`, because denied topics do not use up the cap.
  - **Admin:** an admin user sees `allowed/a` and `allowed/secret/b` (and `other/c`) on every endpoint above.
  - **Bearer:** a REST `POST /api/v1/login` token for `reader` gives the same filtered results as Basic.
  - **Anonymous:** with `AnonymousEnabled` and an `Anonymous` record that allows only `other/#`, a request without
    credentials sees only `other/c`. With `AllowAnonymousLocalhost` and no header, a localhost request sees all
    three topics (E1 runs as `auth.LocalhostUser`, as REST does).
- **Config:**
  - `Enabled: false` -> nothing listens on the port.
  - `RawQueryLimit: 0` -> 10000 with a warning (F6).
  - `Port: 70000` -> `Validate` error.
- **Column names:** `stores.AggregatedColumnName("a/b", "x.y", "AVG") == "a/b.x_y_avg"`, and `"a/b", "", "max"` ->
  `"a/b_max"`.
- **QuestDB live (F5):** skipped unless `EDGE_TEST_QUESTDB_URL` is set. Build a QuestDB group, run an aggregated
  `/query_range`, and check that GraphQL `aggregatedMessages` returns `<topic>_avg`.

**`test/integration/winccoa_prometheus_test.go`** (after archive M2/M3; uses `newSim` / `startNative`,
`winccoa_native_test.go:37`, `:83`, and `sim.Calls(op)`, `simhost.go:175`)
- **Instant:** DP `Pump1.speed` = 42.5. `topics{topic="winccoa/tags/Pump1/speed",field="value"}` without `group`
  returns `"42.5"` with the WINCCOA group's name as `group` (E3). The same without `field` gives no samples.
- **Scrape skip:** with only the provisioned group (`<Root>/#`), `/metrics` has no `winccoa/` lines and
  `sim.Calls(oahost.OpDpQuery)` is unchanged.
- **Scrape scoped:** a user WINCCOA group with `["winccoa/tags/Pump1/#"]` gives
  `topics{group="<name>",topic="winccoa/tags/Pump1/speed",field="value"} 42.5`.
- **Enumeration:**
  - `/series` with `match[]=topics` -> no WINCCOA series and no `OpDpQuery`.
  - `match[]=topics{group="<name>"}` and `match[]=topics{topic="winccoa/tags/Pump1/#"}` -> `field="value"` series.
  - `label/field/values` for the Pump1 topic -> `["value"]`.
  - `label/topic/values` follows the same skip rule.
- **Range raw:** `sim.SetTimed` 10 samples 1 s apart; query with step 2 and `field="value"` -> bucket averages, and
  `sim.Calls(oahost.OpDpGetPeriod) >= 1`.
- **Range aggregated:** step 60 -> values from column `winccoa/tags/Pump1/speed.value_avg`; `max_over_time` -> MAX.
- **ACL alias (F7):** a user whose deny rule covers the canonical form of `winccoa/tags/Pump1/speed` (the
  `<Root>/<Systems>/<sys>/...` topic from `CanonicalOf`) gets no sample for the shortcut topic from `/query`,
  `/query_range` or a scoped scrape.

## Acceptance criteria

- `make test` and `make test-race` pass, including `config_schema_test.go` with the new block in
  `config.yaml.example`.
- No GraphQL SDL change, and `enabledFeatures` is unchanged.
- A Grafana Prometheus data source pointed at edge `:3001` (Basic auth when user management is on) can:
  - browse metrics, labels and series;
  - plot `topics{...}` and `metrics{...}` instant and range queries, including `*_over_time`;
  - plot the same dashboard against main after monster-mq#204.
- For the same stored data and users, edge and main give byte-identical JSON for the tested endpoints, except for
  E1-E6.
- A non-admin user never receives topic data their read ACL denies, on any endpoint, in either broker (F7).
- monster-mq#204 is merged in main in the same release.
- Zero CGO; `make build-arm64` succeeds.

## Risks

- **F2 clamp.** At edge's default 1 s interval, the newest 50,000 s (about 13.9 h) of broker metrics are returned for
  longer ranges; at main's default 10 s, about 5.8 days. Grafana shows a gap on the left for longer ranges. One
  request then holds at most about 50,000 JSON rows (roughly 20 MB).
- **Raw path truncation.** `RawQueryLimit` keeps the newest rows (main parity), so a long range with `step < 60`
  loses its oldest part silently.
- **WinCC OA cost.** The S6 row limit truncates WinCC OA enumeration. A scoped filter like `winccoa/tags/#` still
  costs a large, bounded `dpQuery` on every scrape. Long aggregated ranges cost what the archive plan's bounded
  aggregation allows.
- **Cost of the read ACL (F7).** Every enumerated topic costs one `Allow` call. In edge this is memoized until the
  next refresh (`auth.go:267-286`), so repeated scrapes cost a map lookup per topic. Main's `canSubscribe` goes
  through its `AclCache`. Denied topics are still read from the store and then dropped. Because they do not count
  toward the 5000 / 1000 caps, a user with a narrow ACL can make an enumeration scan the whole last-value store. The
  S6 row limit still bounds WINCCOA groups.
- **E1 reads everything.** A localhost request without credentials runs as the admin `LocalhostUser`, as REST does,
  so `AllowAnonymousLocalhost` bypasses the read ACL for local clients. This is the existing edge convention, and it
  is documented in `doc/grafana.md`.
- **Number formats.** `parseJavaDouble` and `javaDouble` must match Java exactly, or values differ between brokers.
  Both are covered by the instant-query cases.
