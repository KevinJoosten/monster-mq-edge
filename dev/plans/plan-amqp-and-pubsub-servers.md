# Plan: AMQP and other pub/sub protocol servers

**Status:** proposed (2026-10-21), awaiting owner sign-off (see "Decisions needed").
**Scope:** which pub/sub protocols edge should speak next, in what order, and how each one maps onto the
existing MQTT core. Reuses the shared building blocks S1-S7 from
[plan-protocol-servers-shared.md](plan-protocol-servers-shared.md); new shared pieces are P1-P4 below.

## Context

Edge already speaks MQTT 3.1.1/5 (native), NATS (`internal/nats/`) and Redis RESP2 Pub/Sub
(`internal/redisserver/`); a Kafka server is planned ([plan-kafka-server.md](plan-kafka-server.md)). All
protocol servers are in-process adapters over the MQTT core: they publish through `ProtocolPublish` (S1),
subscribe through `pubsub.Bus` (S3), register sessions in `SessionStore` (S5) and recheck read ACLs at
delivery (S7).

**Parity.** Main (`../main/broker/src/main/kotlin/extensions/`, read 2026-10-21) has none of the protocols in
this plan. Everything here is therefore **[edge-only]** until main ports it. The plan is designed so that:

- **No GraphQL change** is needed. Servers are configured by YAML like NATS and Redis; `enabledFeatures`
  is `[String!]!`, so new flag names are values, not schema changes.
- **No storage change** is needed. Durable queues reuse the existing session, subscription and V2
  queue tables (P2), so a database written by edge still opens in main.

Under AGENTS.md each protocol below still needs explicit owner sign-off before code lands.

## Candidate protocols

| Protocol | Where it is used | Fit with the MQTT core | Size (est.) | Recommendation |
|---|---|---|---|---|
| **AMQP 0-9-1** | RabbitMQ ecosystem, most enterprise/IT backends, Celery, Spring AMQP, MassTransit | Very good: topic exchange = MQTT topic tree, durable queues = persistent sessions | ~2,500 LoC | **Phase 1** |
| **STOMP 1.2** (+ over WebSocket) | Browsers, ActiveMQ/Artemis, RabbitMQ, Spring WebSocket, simple scripts | Very good, text protocol, same queue model as AMQP | ~800 LoC | **Phase 2** |
| **MQTT-SN 1.2** (UDP) | Battery/sensor nodes, Zigbee/6LoWPAN/LoRa gateways | Native: it *is* MQTT; each MQTT-SN client becomes an engine session | ~1,500 LoC | **Phase 3** |
| **AMQP 1.0** | Azure Service Bus/IoT Hub/Event Hubs, Artemis, Solace, Qpid, **OPC UA PubSub AMQP transport** | Good, but large type system and link/settlement model | ~4,000 LoC | **Phase 4** |
| **CoAP + Observe** (RFC 7252/7641, `draft-ietf-core-coap-pubsub`) | Constrained devices, LwM2M, Thread/Matter-adjacent | Good: PUT/POST = publish, GET+Observe = subscribe | ~1,500 LoC (no DTLS) | **Phase 5** |
| OPC UA PubSub (UADP/UDP) | Industrial field level | Needs an information model / DataSetMetaData layer, not just transport | large | Defer; separate plan |
| ZeroMQ (ZMTP 3.x PUB/SUB) | Brokerless, scientific/robotics | Prefix-match subscriptions only, no ack/durability | ~1,000 LoC | Not now |
| Apache Pulsar binary | Pulsar clusters | Protobuf, cursors, partitions; client bridge more useful than server | large | Not now |
| DDS / RTPS (ROS 2) | Robotics, defense | Discovery + QoS model is heavy; better as a bridge | very large | Not now |
| XMPP PubSub (XEP-0060) | Legacy | Low demand | medium | No |
| Cloud APIs (Google Pub/Sub, SNS/SQS emulation) | Cloud dev tooling | HTTP APIs; possible later via the REST server | medium | No |

Sparkplug B is a payload format on MQTT and stays off-limits per AGENTS.md. Kafka has its own plan.

## Shared building blocks (new)

These land with the first protocol that needs them. Names are binding.

### P1. Protocol server scaffold (`internal/protoserver/`), first used by AMQP

NATS and Redis each duplicate: TCP/TLS listener, accept loop with `TCP_NODELAY`, connection set,
graceful close, stale session cleanup by client ID prefix on start (NATS E9), anonymous-vs-user-management
auth, per-client in/out counters (S5) and the delivery-time ACL check (S7). Extract them:

```go
type Listener struct { /* net.Listener, conns, wg, closed */ }
func Listen(addr string, tlsCfg *tls.Config, logger *slog.Logger) (*Listener, error)
func (l *Listener) Serve(handle func(net.Conn))
func (l *Listener) Close() error

func CleanupStaleSessions(ss stores.SessionStore, prefix, nodeID string)
func Authenticate(cache *auth.Cache, userMgmt bool, user, pass string) (username string, ok bool)
func DeliveryAllowed(cache *auth.Cache, userMgmt bool, user, clientID, topic string) bool // S7
```

New servers use it from day one. Moving NATS and Redis onto it is an optional follow-up, not part of
this plan.

### P2. Durable queues as virtual persistent sessions, first used by AMQP

A durable AMQP queue, a STOMP `/queue/...` destination, an AMQP 1.0 durable link and an MQTT-SN
`clean=false` client all need "messages survive while nobody is connected, and survive a restart".
Edge already has this for MQTT: `QueueHook` (`internal/broker/hook_queue.go`) enqueues into the V2
queue store for **offline** persistent sessions, using `topic.SubscriptionIndex`.

Rule: a durable queue is a persistent session with client ID `<proto>-q-<vhost>-<queue>` (sanitised),
`CleanSession=false`, its bindings stored as that session's subscriptions in `SessionStore`, its
backlog in `QueueStore`. New `QueueHook` API:

```go
// AttachVirtual marks a virtual persistent session online; the caller then drains
// QueueStore and receives live messages via the returned bus subscription.
func (h *QueueHook) AttachVirtual(clientID string) error
// DetachVirtual marks it offline again; OnPublished resumes enqueueing for it.
func (h *QueueHook) DetachVirtual(clientID string)
func (h *QueueHook) RegisterVirtual(clientID string, filters []string, qos byte) error   // declare/bind
func (h *QueueHook) UnregisterVirtual(clientID string, purge bool) error              // delete
```

- **Hand-over without loss or duplicates.** Attach subscribes on the bus first, then drains the queue
  store, then switches the session to online; messages that arrive in both paths are deduplicated by
  `MessageUUID` within the drain window.
- **Ack.** A consumer ack maps to `QueueStore.Ack(clientID, uuid)`; nack/requeue and connection loss
  call `ResetVisibility`.
- **Limits.** `MaxQueueMessages` applies as for MQTT (`EnqueueMultiLimited`).
- **Dashboard.** These sessions appear in the sessions list with their prefix and queued count, like an
  offline MQTT persistent session. Removing one from the dashboard deletes the queue.
- **PeerLink.** Virtual sessions follow `WithPeerLink` ownership like MQTT sessions; no new rules.

Non-durable, exclusive or auto-delete queues do **not** use P2; they are plain bus subscriptions (S3).

### P3. Topic name mapping (`internal/protoserver/topicmap.go`)

Per protocol, a pure function pair, table-tested:

| Protocol | Name -> MQTT topic | Wildcards |
|---|---|---|
| AMQP 0-9-1 topic exchange | `.` -> `/`, `/` -> `.` (swap, as RabbitMQ's MQTT plugin) | `*` -> `+`, `#` -> `#` |
| STOMP | `/topic/<t>` -> `<t>` verbatim; `/queue/<q>` -> P2 queue | MQTT syntax in `selector`-free `SUBSCRIBE` |
| AMQP 1.0 | address `topic://<t>` or bare `<t>` -> `<t>` | MQTT syntax |
| MQTT-SN | topic ID registry per session; short topics; predefined IDs from config | MQTT syntax |
| CoAP | URI path `/ps/<t>` -> `<t>` | none; one observe per topic (`/ps/` + filter allowed as edge extension) |

The AMQP swap is the only lossy-looking one; it is a bijection, so round trips are exact.
`ProtocolPublish` and the ACL always see the MQTT topic.

### P4. Message property mapping

`stores.BrokerMessage` already carries what S1 needs. Common mapping:

| MQTT 5 | AMQP 0-9-1 | STOMP | AMQP 1.0 | CoAP |
|---|---|---|---|---|
| QoS 0 / 1 | `delivery-mode` 1 / 2 (config default) | `receipt` -> QoS 1 | settled / unsettled | NON / CON |
| retain | header `x-mqtt-retain` | header `retain:true` | annotation `x-opt-retain` | `PUT` (retain) vs `POST` |
| ResponseTopic | `reply-to` (mapped by P3) | `reply-to` | `reply-to` | - |
| CorrelationData | `correlation-id` | `correlation-id` | `correlation-id` | token |
| ContentType | `content-type` | `content-type` | `content-type` | Content-Format |
| MessageExpiry | `expiration` (ms) | `expires` | `ttl` | Max-Age |
| user properties | `headers` table (string values) | extra headers | application-properties | - |

This makes request/reply work across protocols (an AMQP RPC client can call a NATS or MQTT responder).

## Phase 1: AMQP 0-9-1 server (`internal/amqp/`)

**Goal:** stock clients (`rabbitmq/amqp091-go`, `pika`, `amqplib`, Spring AMQP, .NET `RabbitMQ.Client`)
publish and consume against edge; messages flow both ways with MQTT, NATS, Redis.

**Wire and connection**
- Hand-rolled codec, no protocol library in the broker (as Kafka plan rule 5). Frames: method, header,
  body, heartbeat; field tables; `frame-max` negotiation (default 128 KiB), `channel-max` 2047.
- `AMQP` protocol header 0-9-1 only; anything else gets the 0-9-1 header back and close.
- SASL `PLAIN` and `AMQPLAIN`; `EXTERNAL` when the TLS listener has client certs (mTLS via `tlsutil`).
- Heartbeats both ways; missed heartbeats close the connection.
- vhost `/` only by default. Decision A2: other vhosts either rejected or used as a topic prefix.
- Plain port 5672 and TLS port 5671 (`amqps`), each optional.

**Model**
- Exchanges: `amq.topic` (and `""` default exchange for direct-to-queue) are built in.
  `exchange.declare` type `topic` with any name is accepted and aliases the same MQTT topic space
  (decision A1: or maps the exchange name to a topic prefix). `direct`/`fanout`/`headers` exchanges
  return `503 command-invalid` in phase 1.
- Queues: `queue.declare` (incl. server-named `amq.gen-*`, `passive`, `exclusive`, `auto-delete`,
  `durable`), `queue.bind`/`unbind` with routing-key patterns via P3, `queue.purge`, `queue.delete`.
  Durable -> P2; otherwise in-memory bus subscription per queue (S3 rules: validate, overflow closes,
  1024 buffer, unsubscribe on close).
- Consumers: `basic.consume`/`cancel` with round-robin over a queue's consumers, `basic.qos`
  prefetch (count; size ignored), `basic.ack`/`nack`/`reject` (incl. `multiple`, `requeue`),
  `basic.recover`, `basic.get`.
- Publishing: `basic.publish` -> S1 with client ID `amqp-<uuid>`; `mandatory` returns via
  `basic.return` when no binding matches (decision A3, needs a cheap "has subscribers" check);
  `immediate` rejected as in RabbitMQ.
- Publisher confirms (`confirm.select`): `basic.ack` after `ProtocolPublish` returns, `basic.nack` on
  hook error.
- `tx.*`: `tx.select` accepted, `tx.commit` publishes the buffered batch (no rollback of side effects
  beyond discarding unsent messages).
- WinCC OA native writes: a publish to a native value topic goes through S2, as Redis `SET` does
  (decision A4).

**Not in phase 1:** dead-lettering, alternate exchanges, priority queues, queue TTL/length arguments other
than `x-max-length` (maps to `MaxQueueMessages`), exchange-to-exchange bindings, streams, federation,
management HTTP API.

**Sessions and ACL:** session per connection (`amqp-<uuid>`, `ProtocolVersion` `"AMQP"`), S5 counters,
publish ACL on the MQTT topic, subscribe ACL at `queue.bind` on the filter, S7 recheck at delivery.

**Config** (`AmqpServer:` block, Features flag `AmqpServer`, default off):

```yaml
Features:
  AmqpServer: false
AmqpServer:
  Enabled: true
  Host: 0.0.0.0
  Port: 5672          # 0 = off
  TlsPort: 0          # 5671 for amqps; uses the broker's TLS key pair
  RequireAuth: true   # only effective with user management on
  DefaultQos: 0       # QoS for delivery-mode 1; delivery-mode 2 is always QoS 1
  FrameMax: 131072
  Heartbeat: 60
```

Checklist: `config.go`, `yaml-json-schema.json`, `config.yaml.example` (and `scripts/deb/config.yaml` only
if enabled by default, which it is not).

**Tests** (`test/integration/amqp_*_test.go`, `amqp091-go` test-only): handshake and auth matrix;
topic mapping round trip; AMQP->MQTT and MQTT->AMQP; NATS/Redis interop; durable queue survives consumer
absence and broker restart (all three stores); prefetch and redelivery on nack/close; publisher confirms;
RPC via `reply-to` with an MQTT 5 responder; ACL deny on publish, bind and delivery; dashboard session
removal; overflow closes the connection.

**Docs:** README feature line, `doc/amqp.md`, AGENTS.md (subsystem list, repository layout).

## Phase 2: STOMP 1.2 (`internal/stomp/`)

- TCP port (default 61613) and STOMP over WebSocket on the existing HTTP server at `/stomp`
  (subprotocols `v12.stomp`, `v11.stomp`), so browsers can use `@stomp/stompjs` without extra ports.
- Frames `CONNECT/STOMP`, `SEND`, `SUBSCRIBE`, `UNSUBSCRIBE`, `ACK`, `NACK`, `BEGIN/COMMIT/ABORT`,
  `DISCONNECT` with `receipt`, heart-beating, `content-length`, header escaping.
- `/topic/<t>` -> bus subscription; `/queue/<q>` -> P2 virtual session (`stomp-q-<q>`), shared with
  nothing else (AMQP queues keep their own prefix).
- `ack: auto | client | client-individual`.
- Session ID `stomp-<uuid>`.
- Tests with `go-stomp/stomp` (test-only) and a WebSocket case.

## Phase 3: MQTT-SN 1.2 gateway (`internal/mqttsn/`)

- UDP listener (default 1884), optional multicast `ADVERTISE`/`SEARCHGW`/`GWINFO`.
- Transparent model: each MQTT-SN client gets an engine session with client ID `mqttsn-<clientId>`, so
  `clean=false`, will messages, retained messages and QoS 1/2 come from the MQTT core unchanged.
- Topic IDs: `REGISTER`/`REGACK` per session, short (2-char) topics, predefined topic IDs from config.
- QoS -1 (publish without connection) to predefined/short topics, gated by a config flag and the
  anonymous user's ACL.
- Sleeping clients (`DISCONNECT` with duration, `PINGREQ` with client ID) buffer messages in the session.
- No DTLS in this phase. Tests use a small hand-written MQTT-SN client in `test/integration/`
  (no maintained Go client exists).

## Phase 4: AMQP 1.0 (`internal/amqp10/`)

- Hand-rolled AMQP 1.0 type system codec, SASL (`ANONYMOUS`, `PLAIN`), sessions, links, flow control,
  settlement modes. Can share listener port 5672 with phase 1 by sniffing the protocol header
  (`AMQP\x00` 0-9-1 vs `AMQP\x00\x01\x00\x00` 1.0, plus SASL header `AMQP\x03`); decision A5.
- Sender link target address -> MQTT topic; receiver link source address -> MQTT filter (P3).
  Durable terminus (`durable=2`, `expiry-policy=never`) -> P2 with client ID `amqp10-q-<container>-<link>`.
- `rcv-settle-mode first` only; at-most-once (pre-settled) = QoS 0, at-least-once = QoS 1.
- Targets OPC UA PubSub over AMQP (JSON encoding) and Azure SDK clients pointed at edge for local testing.
- Tests with `Azure/go-amqp` (test-only).

## Phase 5: CoAP pub/sub (`internal/coap/`)

- UDP 5683, RFC 7252 messaging (CON/NON/ACK/RST, dedup, block-wise transfer RFC 7959 for payloads > 1 KiB).
- Broker resource `/ps`: `PUT /ps/<t>` publishes retained, `POST /ps/<t>` publishes non-retained,
  `GET /ps/<t>` returns the retained/last value from the last-value store, `GET` + Observe subscribes,
  `DELETE` clears the retained message, `GET /.well-known/core` lists discovery info.
- Auth: none on plain CoAP (anonymous user ACL applies); DTLS with PSK/certs is a later step and needs
  `pion/dtls` (pure Go) as a new dependency, decision A6.
- Tests with `plgd-dev/go-coap` (test-only).

## Cross-cutting

- **Footprint.** Each server is off by default behind its Features flag; measure binary growth and idle
  RSS on a Pi 4 per phase and record it in the doc page. Budget proposal: < 1 MiB binary growth per
  protocol, < 50 KiB RSS per idle connection.
- **Dashboard.** Sessions badge is chosen by client ID prefix (`dashboard/src/js/sessions.js`); the
  dashboard repo needs badges for `amqp-`, `stomp-`, `mqttsn-`, `amqp10-`, `coap-`. No GraphQL change.
  `sessionToGraphQL` returns null MQTT-only fields for these prefixes, as for NATS/Redis.
- **Metrics.** Per-protocol connection gauge and in/out counters in the Prometheus endpoint.
- **Main broker.** File one issue in vogler75/monster-mq per signed-off protocol so main can port it
  later; until then each doc page states "edge-only".
- **Security.** All TCP servers support TLS via `tlsutil`; none accepts unauthenticated connections when
  user management is on and anonymous is disabled.

## Order of work

```text
P1 scaffold ─┬─ P3/P4 mapping ─┬─ AMQP 0-9-1 (phase 1) ── STOMP (phase 2)
             │                 └───────────────────────── AMQP 1.0 (phase 4)
P2 virtual sessions ───────────┘
MQTT-SN (phase 3) needs only P1 + the engine; can run in parallel with phase 1/2.
CoAP (phase 5) needs P1 + P3 only.
```

## Decisions needed

- [ ] Sign-off per protocol (AGENTS.md off-limits rule): AMQP 0-9-1, STOMP, MQTT-SN, AMQP 1.0, CoAP.
- [ ] A1. AMQP named topic exchanges: alias the one MQTT topic space (recommended) or map exchange
      name to a topic prefix.
- [ ] A2. AMQP vhosts other than `/`: reject (recommended) or treat as topic prefix.
- [ ] A3. Support `mandatory` + `basic.return` (needs a subscriber-exists check on the publish path).
- [ ] A4. AMQP/STOMP publishes to WinCC OA native value topics go through S2 like Redis `SET`.
- [ ] A5. AMQP 1.0 shares port 5672 with 0-9-1 via header sniffing (recommended) or uses its own port.
- [ ] A6. Accept `pion/dtls` as a dependency for CoAP over DTLS (later step).
- [ ] P2 virtual persistent sessions: OK to show durable queues as sessions in the dashboard and in
      the session tables (no new tables).
- [ ] Whether main should get the same servers (one issue each) or they stay edge-only.
