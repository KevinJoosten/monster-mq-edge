# Grafana Integration

MonsterMQ Edge exposes a Prometheus-compatible HTTP API for archive data and broker
metrics. Use Grafana's built-in **Prometheus** datasource.

## Enable the API

```yaml
Prometheus:
  Enabled: true
  Port: 3001
  RawQueryLimit: 10000
Metrics:
  Enabled: true
  CollectionIntervalSeconds: 1
```

The default port is 3001, listening on `0.0.0.0:3001` independently of the GraphQL
HTTP server. Set `Prometheus.Address` to bind a single interface, for example
`127.0.0.1` when Grafana runs on the same host.

Create a Prometheus datasource in Grafana pointing to `http://<broker-host>:3001`.
Use a hostname reachable from the Grafana server; `localhost` inside a Grafana
container refers to that container.

When `UserManagement.Enabled` is true:
- Configure **Basic authentication** in Grafana with a MonsterMQ account (recommended).
- `Authorization: Bearer <token>` is also accepted, using an edge session token
  obtained from GraphQL `login` or REST `POST /api/v1/login`. Because edge session
  tokens are process-local and expire after 24 hours, Basic authentication is
  recommended for Grafana datasources.
- When `UserManagement.AllowAnonymousLocalhost` is true, requests from localhost
  (`127.0.0.1`, `::1`, `localhost`) without an `Authorization` header are admitted
  as the synthetic admin `LocalhostUser` (same convention as REST and GraphQL).

## Queries

Select a numeric MQTT payload from an archive group's last-value store:

```promql
topics{group="Default",topic="sensors/temperature"}
```

For a JSON payload such as `{"data":{"temperature":23.5}}`, add a field path:

```promql
topics{group="Default",topic="sensors/data",field="data.temperature"}
```

When `field` is empty (`field=""` or omitted), only plain numeric payloads are
returned, unless the topic belongs to the `WinCCOA` archive group or is a native
WinCC OA tag, in which case the numeric `value` field from `{"time":"<ISO>","value":<v>}`
is automatically extracted. You can also specify `field="value"` explicitly:

```promql
topics{topic="winccoa/tags/Pump1/speed"}
# or explicitly:
topics{topic="winccoa/tags/Pump1/speed",field="value"}
```

When `group` is omitted or `"Default"`, `archive.Manager.ResolveGroupForTopic`
routes WinCC OA native tag topics to the `WinCCOA` archive group automatically
once configured.

Broker metrics use the `metrics` selector:

```promql
metrics{node="edge",metric="messages_in"}
```

Other metric labels include `messages_out`, `sessions`, `subscriptions`,
`queue_depth`, `bus_in`, `bus_out`, `winccoa_in` / `winccoa_out` (values/s
into and out of the native WinCC OA connection), and connector input/output
rates. Discover available label values through the API instead of assuming
all connectors have both directions.

Instant topic queries use the selected last-value store; range queries require
an archive backend implementing historical queries. Broker metric history
requires an enabled metrics store. In range queries, steps under 60 seconds
(without an `*_over_time` function) use raw topic history, capped by
`RawQueryLimit` (defaults to 10000; values under 1 log a warning and fall back
to 10000). Larger steps or `avg_over_time` / `min_over_time` / `max_over_time` /
`count_over_time` queries use supported archive aggregation intervals.

This is a limited compatibility API, not a full PromQL engine. Use the simple
selectors above. Do not assume arbitrary PromQL expressions, recording rules,
or alert-rule evaluation are implemented. A separate Prometheus deployment can
scrape `/metrics` when those capabilities are needed.

## Read ACL

When `UserManagement.Enabled` is true, topic data across `/metrics`,
`/api/v1/label/topic/values`, `/api/v1/label/field/values`, `/api/v1/series`,
`/api/v1/query`, and `/api/v1/query_range` is filtered by the requesting user's
subscribe (read) ACL:
- Non-admin users only see topics permitted by their ACL rules (including
  canonical WinCC OA alias checks). Denied topics are skipped silently and do not
  count toward the 5000 / 1000 enumeration caps.
- Anonymous requests (when `AnonymousEnabled` is true) are checked against the
  `Anonymous` user record.
- Unauthenticated localhost requests admitted via `AllowAnonymousLocalhost` run
  as `LocalhostUser` (admin) and can read all topics, matching the REST API.
- Broker `metrics{...}` series and metadata lists (`__name__`, `group`, `metric`,
  `node`) are not topic data and are not filtered by topic ACLs.

## WinCC OA Groups

Archive groups using the `WINCCOA` last-value store are protected against
unbounded full-project queries:
- **Scrape (`GET /metrics`):** a `WINCCOA` group is queried only for entries in
  its own `TopicFilters` that are scoped (`Names.Classify(filter) == KindNative`,
  such as `winccoa/tags/Pump1/#`). Broad filters (`#`, `<Root>/#`, `+/#`) are
  skipped during scraping.
- **Enumeration (`/api/v1/series`, `label/topic/values`, `label/field/values`):**
  a `WINCCOA` group is queried only when `match[]` explicitly names the group in
  `group="..."` or carries a scoped `topic="..."` matcher. Otherwise it is skipped
  without making a host call.

## Differences from MonsterMQ Core

MonsterMQ Edge's Prometheus API is byte-compatible with MonsterMQ Core, with the
following edge-specific behavior:
- **Anonymous localhost (E1):** `UserManagement.AllowAnonymousLocalhost` admits
  unauthenticated localhost requests as `LocalhostUser`.
- **Bearer tokens (E2):** Bearer authentication validates edge session tokens
  (from GraphQL `login` or REST `/api/v1/login`) rather than MonsterMQ Core JWTs.
- **WinCC OA group fallback (E3) and skip rules (E4):** see above.
- **Sorted groups (E5):** archive groups are visited in deterministic alphabetical
  order.
- **Single node (E6):** `label/node/values` returns `[<NodeId>]`.
- **HTTP server library:** 404/405 error bodies and `OPTIONS` preflight status
  codes come from `chi` rather than Vert.x.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/metrics` | Prometheus text exposition for scraping |
| GET, POST | `/api/v1/labels` | Discover label names |
| GET | `/api/v1/label/{name}/values` | Discover label values |
| GET, POST | `/api/v1/series` | Discover series matching a selector |
| GET, POST | `/api/v1/query` | Current values |
| GET, POST | `/api/v1/query_range` | Historical values |

```bash
curl -u Admin:Admin --get http://localhost:3001/api/v1/query \
  --data-urlencode 'query=topics{group="Default",topic="sensors/temperature"}'
```

If results are empty, check the datasource host/port, the archive group name,
whether its last-value/archive stores are enabled, whether the selected period
contains data, and whether the payload or selected JSON field is numeric.
