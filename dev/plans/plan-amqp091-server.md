# Plan: AMQP 0-9-1 server

**Status:** proposed (2026-10-21), awaiting owner sign-off.
**Phase:** 1 of [plan-amqp-and-pubsub-servers.md](plan-amqp-and-pubsub-servers.md), which defines the shared
building blocks P1-P4 used here. S1-S7 refer to [plan-protocol-servers-shared.md](plan-protocol-servers-shared.md).
**Package:** `internal/amqp/`. **[edge-only]**: main has no AMQP server.

## Goal

Stock AMQP 0-9-1 clients (`rabbitmq/amqp091-go`, `pika`, `amqplib`, Spring AMQP, .NET `RabbitMQ.Client`,
Celery/kombu) publish and consume against edge. Messages flow both ways between AMQP, MQTT, NATS and Redis.
No GraphQL change, no new storage tables.

## Wire and connection

- Hand-rolled codec, no protocol library in the broker (as Kafka plan rule 5). `amqp091-go` is test-only.
- Frames: method, content header, body, heartbeat. Field tables and arrays with all 0-9-1 value types
  (RabbitMQ's variant of the type table, which every client uses).
- Protocol header `AMQP\x00\x00\x09\x01` only. Any other header gets the 0-9-1 header back and the socket
  closes (spec 4.2.2). AMQP 1.0 headers are left to phase 4 (decision A5 there).
- `connection.tune`: `channel-max` 2047, `frame-max` from config (default 128 KiB, min 4096), heartbeat
  from config. Missed heartbeats (2 intervals) close the connection.
- SASL mechanisms: `PLAIN`, `AMQPLAIN`; `EXTERNAL` on the TLS port when the client presents a certificate
  (user name from the certificate CN, as the MQTT mTLS path does).
- vhost `/` only (decision A2).
- Plain port (default 5672) and TLS port (5671), each 0 = off. TLS uses the broker key pair via `tlsutil`.
- Server properties: `product` `MonsterMQ Edge`, `version` from `internal/version`, capabilities
  `publisher_confirms`, `basic.nack`, `consumer_cancel_notify`, `connection.blocked` (false),
  `per_consumer_qos`.

## Topic and exchange model

- Routing keys map to MQTT topics with P3: `.` <-> `/` swapped, `*` -> `+`, `#` -> `#`. Example:
  routing key `plant1.line2.temp` is MQTT topic `plant1/line2/temp`; binding `plant1.*.temp` is filter
  `plant1/+/temp`.
- Built-in exchanges: `amq.topic` (the MQTT topic space) and `""` (default exchange: routing key = queue
  name, delivers directly to that queue, does not reach MQTT).
- `exchange.declare` type `topic` with any name: aliases `amq.topic` (decision A1). Types `direct`,
  `fanout`, `headers`: `503 command-invalid`. `passive` succeeds for built-ins and declared names.
  `exchange.delete` of a user name is accepted; of a built-in: `403 access-refused`.

## Queues

| Kind | Backing | Client ID |
|---|---|---|
| `durable=true`, not exclusive | P2 virtual persistent session: bindings as subscriptions in `SessionStore`, backlog in `QueueStore` | `amqp-q-<queue>` (sanitised) |
| non-durable, exclusive or auto-delete | in-memory queue fed by one bus subscription with all its bindings (S3) | owned by the connection |

- `queue.declare`: named and server-named (`amq.gen-<base64>`), `passive`, `exclusive`, `auto-delete`,
  `durable`. Re-declare with different flags: `406 precondition-failed`, as RabbitMQ.
- Arguments: `x-max-length` (caps at `MaxQueueMessages`), `x-message-ttl` (sets message expiry on enqueue).
  Unknown `x-` arguments are ignored and logged once.
- `queue.bind`/`unbind` on `amq.topic` or a user topic exchange; routing key mapped by P3. Bind runs the
  subscribe ACL on the MQTT filter and S3 `Validate`.
- `queue.purge`: in-memory clear, or `QueueStore.PurgeForClient` for durable queues.
- `queue.delete` with `if-unused`/`if-empty`; durable delete calls `UnregisterVirtual(purge=true)`.
- In-memory queues: bounded at 10,000 messages. Overflow drops the oldest and logs once per minute
  (decision Q1; S3 alternative is closing the connection).

## Consumers and delivery

- `basic.consume` (incl. `exclusive`, `no-ack`, server-generated tags), `basic.cancel`; server-initiated
  `basic.cancel` when a queue is deleted (consumer_cancel_notify).
- Several consumers on one queue get messages round-robin.
- `basic.qos` prefetch count, per consumer or per channel (`global`); prefetch size ignored.
- `basic.ack`/`nack`/`reject` with `multiple` and `requeue`. Requeued messages get `redelivered=true`.
  Durable: ack -> `QueueStore.Ack(uuid)`; requeue or channel close -> `ResetVisibility`.
- `basic.get` with `get-empty`. `basic.recover` requeues all unacked messages of the channel.
- Delivery-time read ACL recheck (S7) on the concrete MQTT topic; a denied message is acked and dropped.
- Delivered messages: exchange `amq.topic`, routing key = mapped MQTT topic.

## Publishing

- `basic.publish` -> S1 `ProtocolPublish` with client ID `amqp-<uuid>` of the connection.
- Property mapping by P4: `delivery-mode` 2 -> QoS 1, else `DefaultQos`; `reply-to`, `correlation-id`,
  `content-type`, `expiration`, `headers` (string-convertible values) -> MQTT 5 properties. Header
  `x-mqtt-retain: true` sets retain.
- Publish to the default exchange delivers only to the named queue, not to MQTT.
- `mandatory`: when no subscriber matches, `basic.return` 312 `NO_ROUTE` (decision A3).
  `immediate`: `540 not-implemented`, as RabbitMQ.
- Publish ACL on the MQTT topic; denied -> `403 access-refused` channel close.
- Publisher confirms (`confirm.select`): `basic.ack` with the delivery tag after `ProtocolPublish`
  returns; `basic.nack` on a hook error (e.g. WinCC OA command verdict).
- `tx.select`/`commit`/`rollback`: publishes and acks buffered per channel, applied on commit, discarded
  on rollback.
- WinCC OA native value topics go through S2 (decision A4).

## Sessions, metrics, dashboard

- Session per connection, `amqp-<uuid>`, `Information` JSON with `ProtocolVersion` `"AMQP"`,
  `RemoteAddress`, `LocalAddress`, `SSL`, plus `vhost`, `channels`. Durable queues appear as offline
  persistent sessions `amqp-q-...` with their queued count.
- Dashboard removal: closes a connection (`connection.close` 320) or deletes a durable queue.
- S5 counters per publish and delivery. Prometheus: `monstermq_amqp_connections`,
  `monstermq_amqp_channels`.
- Stale `amqp-` connection sessions of this node are deleted on start (P1); `amqp-q-` sessions are kept.

## Configuration

```yaml
Features:
  AmqpServer: false
AmqpServer:
  Enabled: true
  Host: 0.0.0.0
  Port: 5672          # 0 = off
  TlsPort: 0          # 5671 for amqps
  RequireAuth: true   # only effective with user management on
  DefaultQos: 0       # QoS for delivery-mode 1; delivery-mode 2 is always QoS 1
  FrameMax: 131072
  Heartbeat: 60       # seconds, 0 = off
```

Checklist: `config.go` (struct, defaults, validation), `yaml-json-schema.json`, `config.yaml.example`.
`enabledFeatures` reports `AmqpServer`.

## Tasks

1. P1 scaffold, P3 AMQP mapping, P4 property mapping (if not landed yet).
2. `internal/amqp/wire/`: frame reader/writer, field table codec, method structs for classes connection,
   channel, exchange, queue, basic, confirm, tx.
3. Connection state machine: header, start/start-ok, tune, open, heartbeats, close.
4. Channel state: flow, close, delivery tags, unacked map, prefetch.
5. Exchange and queue registry (in-memory + P2 durable).
6. Publish path, confirms, `mandatory`/return, tx.
7. Consumers, round-robin dispatch, ack/nack/reject/recover/get.
8. P2 `QueueHook` virtual session API if not landed.
9. Wiring in `internal/broker/server.go`, config, schema, example.
10. Integration tests, docs (`doc/amqp.md`, README, AGENTS.md).

## Tests (`test/integration/amqp_*_test.go`)

- Handshake: plain, TLS, wrong header, auth matrix (anonymous, user, wrong password, EXTERNAL).
- Mapping round trip: AMQP publish -> MQTT subscribe and back, wildcards, routing keys with `/`.
- Interop with NATS `SUB` and Redis `SUBSCRIBE`.
- Durable queue: messages kept with no consumer, across broker restart, on SQLite, Postgres and MongoDB.
- Prefetch, redelivery on nack and on channel close, `basic.get`, round-robin over two consumers.
- Publisher confirms ack and nack; tx commit/rollback.
- RPC: AMQP client with `reply-to` calls an MQTT 5 responder and vice versa.
- ACL: deny on publish, on bind, and at delivery for a wildcard binding.
- Dashboard removal of a connection and of a durable queue.
- Heartbeat timeout closes.

## Acceptance criteria

- `amqp091-go` and `pika` example programs (hello world, work queues, topics, RPC tutorials) run
  unchanged against edge, apart from exchange types other than topic.
- Durable queues survive a restart without new tables; the database still opens in main.
- Binary growth and idle RSS per connection measured on a Pi 4 and recorded in `doc/amqp.md`.

## Decisions needed

- [ ] Sign-off: allow the AMQP 0-9-1 server on edge.
- [ ] A1. Named topic exchanges alias the one MQTT topic space (recommended) or map to a topic prefix.
- [ ] A2. vhosts other than `/`: reject (recommended) or topic prefix.
- [ ] A3. Support `mandatory` + `basic.return` (needs a subscriber-exists check on the publish path).
- [ ] A4. Publishes to WinCC OA native value topics go through S2, like Redis `SET`.
- [ ] Q1. In-memory queue overflow: drop oldest (recommended) or close the connection.
- [ ] Later: `direct`/`fanout` exchanges, dead-lettering, management HTTP API.
