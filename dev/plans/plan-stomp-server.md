# Plan: STOMP 1.2 server

**Status:** proposed (2026-10-21), awaiting owner sign-off.
**Phase:** 2 of [plan-amqp-and-pubsub-servers.md](plan-amqp-and-pubsub-servers.md) (P1-P4 there; S1-S7 in
[plan-protocol-servers-shared.md](plan-protocol-servers-shared.md)).
**Package:** `internal/stomp/`. **[edge-only]**.
**Depends on:** P1, P2 (durable queues), P3, P4. Lands after AMQP 0-9-1, which brings P2.

## Goal

STOMP clients (`@stomp/stompjs` in browsers, `go-stomp`, `stomp.py`, Spring WebSocket messaging,
ActiveMQ/RabbitMQ STOMP users) publish and subscribe against edge over TCP and over WebSocket on the
existing HTTP server.

## Transport

- TCP listener (default 61613) and TLS listener (61614), each 0 = off.
- STOMP over WebSocket on the broker's HTTP server at `/stomp`, subprotocols `v12.stomp`, `v11.stomp`,
  `v10.stomp`. One STOMP frame per WebSocket message, text or binary. Uses the existing `coder/websocket`
  dependency; no new port needed for browsers.
- Versions 1.0, 1.1, 1.2 negotiated by `accept-version`; header escaping per version.

## Frames

| Client frame | Behaviour |
|---|---|
| `CONNECT` / `STOMP` | `login`/`passcode` against `auth.Cache`; `host` ignored; `heart-beat` negotiated; reply `CONNECTED` with `version`, `session`, `server: MonsterMQ-Edge/<version>` |
| `SEND` | destination via P3, body as payload, `content-length` honoured (binary safe), P4 headers; publish through S1 |
| `SUBSCRIBE` | `id`, `destination`, `ack` = `auto` / `client` / `client-individual` |
| `UNSUBSCRIBE` | by `id` |
| `ACK` / `NACK` | by `id` (1.2) or `message-id` + `subscription` (1.1) |
| `BEGIN` / `COMMIT` / `ABORT` | per-transaction buffer of `SEND` and `ACK` frames |
| `DISCONNECT` | with `receipt`: sends `RECEIPT` after all prior frames are processed |

Any frame with `receipt` gets a `RECEIPT`. Errors send `ERROR` with `message` header and close, as the spec
requires.

## Destinations

| Destination | Mapping |
|---|---|
| `/topic/<t>` | MQTT topic `<t>` verbatim (slashes stay); subscribe filter may use MQTT `+` and `#` |
| `/queue/<q>` | P2 durable queue, client ID `stomp-q-<q>`. `SEND` to it enqueues directly (point-to-point, not on MQTT). Bindings: header `x-bind: <mqtt filter>[,<filter>]` on `SUBSCRIBE` (decision T2) |
| `/temp-queue/<q>` | connection-scoped in-memory queue, used as `reply-to` for request/reply |
| anything else | `ERROR` unknown destination (decision T1: or treat as `/topic/`) |

`/exchange/amq.topic/<routing key>` (RabbitMQ style) is accepted as an alias that uses the AMQP P3 mapping,
so STOMP and AMQP clients of RabbitMQ habits work.

## Delivery and ack

- `MESSAGE` frames carry `subscription`, `message-id` (MQTT `MessageUUID`), `destination`, `ack` (1.2),
  P4 headers, `content-type`, `content-length`.
- `ack:auto`: settled on send. `client`: cumulative. `client-individual`: per message.
  Unacked messages of a `/queue/` subscription return to the queue on disconnect (`ResetVisibility`).
- Flow control: max 1,000 unacked per subscription (header `activemq.prefetchSize` /
  `x-prefetch-count` override).
- S3 rules for `/topic/` subscriptions (validate, overflow closes with `ERROR`, unsubscribe on close),
  S7 recheck at delivery.

## QoS and properties

- `SEND` with `receipt` -> QoS 1 publish, receipt after S1 returns; without receipt -> `DefaultQos`.
- `retain:true` header sets retain. `persistent:true` (ActiveMQ) is treated as QoS 1.
- P4 for `reply-to`, `correlation-id`, `expires`, `content-type`, other headers -> user properties.

## Sessions, config

- Session `stomp-<uuid>`, `ProtocolVersion` `"STOMP"`, `Information` with `transport` `tcp`/`ws`.
- Dashboard removal sends `ERROR` and closes.

```yaml
Features:
  StompServer: false
StompServer:
  Enabled: true
  Host: 0.0.0.0
  Port: 61613       # 0 = off
  TlsPort: 0        # 61614
  WebSocket: true   # /stomp on the HTTP server
  RequireAuth: true
  DefaultQos: 0
  HeartBeatMs: 10000
```

Checklist: `config.go`, `yaml-json-schema.json`, `config.yaml.example`.

## Tasks

1. Frame codec (versions 1.0-1.2, escaping, `content-length`, NUL terminator, heart-beat EOLs).
2. Connection loop shared by TCP and WebSocket transports.
3. Destination mapping and `/queue/` on P2; `/temp-queue/`.
4. Subscriptions, ack modes, transactions, receipts.
5. Wiring, config, HTTP route registration.
6. Integration tests, docs (`doc/stomp.md`, README, AGENTS.md).

## Tests

- `go-stomp/stomp` over TCP and a raw WebSocket client: connect/auth matrix, send/subscribe, binary body.
- STOMP <-> MQTT and STOMP <-> AMQP interop on the same queue name space rules.
- `/queue/` survives disconnect and restart; `client-individual` ack and NACK redelivery.
- Transactions; receipts; heart-beat timeout; ACL deny on SEND, SUBSCRIBE and at delivery.

## Acceptance criteria

- `@stomp/stompjs` in a browser page served by the broker's HTTP server connects to `/stomp` and receives
  MQTT messages.
- No new tables; binary growth measured.

## Decisions needed

- [ ] Sign-off: allow the STOMP server on edge.
- [ ] T1. Unknown destination prefix: error (recommended) or treat as `/topic/`.
- [ ] T2. How a `/queue/` gets MQTT bindings: `x-bind` header on `SUBSCRIBE` (recommended), config list,
      or only point-to-point `SEND`.
- [ ] T3. Share durable queue names with AMQP (`amqp-q-` and `stomp-q-` one name space) or keep apart
      (recommended: apart).
