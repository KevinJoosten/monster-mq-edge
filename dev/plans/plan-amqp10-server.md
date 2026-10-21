# Plan: AMQP 1.0 server

**Status:** proposed (2026-10-21), awaiting owner sign-off.
**Phase:** 4 of [plan-amqp-and-pubsub-servers.md](plan-amqp-and-pubsub-servers.md).
**Package:** `internal/amqp10/`. **[edge-only]**.
**Depends on:** P1, P2, P3, P4; the AMQP 0-9-1 listener if ports are shared (decision B1).

## Goal

OASIS AMQP 1.0 clients (Azure SDKs and `Azure/go-amqp`, Qpid Proton, ActiveMQ Artemis clients, Solace,
`rhea`, .NET `AMQPNetLite`) and OPC UA PubSub publishers using the AMQP transport connect to edge.

## Transport and security layers

- Protocol headers: `AMQP\x03\x01\x00\x00` (SASL) then `AMQP\x00\x01\x00\x00` (AMQP); TLS on its own
  port, or `AMQP\x02...` TLS upgrade rejected.
- Port sharing (decision B1, recommended): the 0-9-1 listener reads the 8-byte header and hands the
  connection to `amqp10` for version 1.0 headers. Otherwise a separate port.
- SASL mechanisms: `ANONYMOUS` (only if anonymous is enabled), `PLAIN`, `EXTERNAL` with client certs.
- `open`: `max-frame-size` from config (default 64 KiB), `channel-max` 255, `idle-time-out` honoured
  both ways. `container-id` stored in the session `Information`.

## Codec

Hand-rolled AMQP 1.0 type system: all primitive encodings (fixed, variable, compound, array), described
types, the performatives `open`, `begin`, `attach`, `flow`, `transfer`, `disposition`, `detach`, `end`,
`close`, and the message sections (header, delivery-annotations, message-annotations, properties,
application-properties, data / amqp-value / amqp-sequence, footer). Multi-frame transfers reassembled up to
`MaxMessageSize`. `Azure/go-amqp` is test-only.

## Links and addressing

- **Sender link from client** (client publishes): target `address` -> MQTT topic by P3
  (`topic://<t>` or bare `<t>`). A link with no target address takes the `to` property per message
  (anonymous relay, needed by many clients).
- **Receiver link from client** (client subscribes): source `address` -> MQTT filter (`+`/`#` allowed).
  - Non-durable source: bus subscription (S3).
  - Durable source (`durable` = 2 and `expiry-policy` = `never`): P2 virtual session, client ID
    `amqp10-q-<container-id>-<link-name>` (sanitised); reattach with the same names resumes it.
  - `queue://<q>` source shares durable queues by name (decision B2).
- Dynamic nodes (`dynamic=true`): server creates a temporary address `temp://<uuid>` for request/reply.
- Source filters (`apache.org:selector-filter:string`) rejected in this phase.

## Flow and settlement

- Link credit drives delivery; no deliveries without credit. Session windows enforced.
- `snd-settle-mode settled` (pre-settled) = QoS 0; `unsettled` = QoS 1 (ack on `accepted` disposition).
- `rcv-settle-mode first` only; `second` rejected at attach.
- Outcomes from client on receiver links: `accepted` -> ack; `released` -> requeue;
  `modified` (delivery-failed) -> requeue with `delivery-count`+1; `rejected` -> drop and log.
- Inbound transfers on sender links: `accepted` after S1 returns, `rejected` with
  `amqp:unauthorized-access` on ACL deny or the hook error text otherwise.

## Property mapping (P4)

| AMQP 1.0 | MQTT 5 |
|---|---|
| `header.durable` / settlement | QoS (see above) |
| `properties.to` | topic (anonymous relay) |
| `properties.reply-to` | ResponseTopic (P3) |
| `properties.correlation-id` (string/binary/uuid) | CorrelationData |
| `properties.content-type` | ContentType |
| `header.ttl` / `properties.absolute-expiry-time` | MessageExpiry |
| `application-properties` (string-convertible values) | user properties |
| message annotation `x-opt-retain` | retain |
| body `data` | payload; `amqp-value` string/binary -> payload, other types -> JSON (decision B3) |

## OPC UA PubSub over AMQP

OPC UA Part 14 AMQP transport sends JSON NetworkMessages to an address (queue/topic name). With this
server they arrive as MQTT messages on the mapped topic and can be decoded by later work. Decoding of
UADP/JSON NetworkMessages is out of scope.

## Sessions, config

- Session per connection `amqp10-<uuid>`, `ProtocolVersion` `"AMQP1.0"`; durable links as offline
  persistent sessions.

```yaml
Features:
  Amqp10Server: false
Amqp10Server:
  Enabled: true
  SharePortWithAmqp091: true   # B1
  Host: 0.0.0.0
  Port: 0                      # own port when not shared, e.g. 5672
  TlsPort: 0
  RequireAuth: true
  MaxFrameSize: 65536
  MaxMessageSize: 1048576
  IdleTimeoutMs: 60000
```

Checklist: `config.go`, `yaml-json-schema.json`, `config.yaml.example`.

## Tasks

1. Type system codec with fuzz tests (`go test -fuzz`) on decoder input.
2. Frame layer, SASL layer, connection/session/link state machines.
3. Flow control and settlement.
4. Address mapping, anonymous relay, dynamic nodes, durable links on P2.
5. Header sniffing in the 0-9-1 listener (B1).
6. Config, wiring, docs (`doc/amqp-1.0.md`, README, AGENTS.md).

## Tests

- `Azure/go-amqp`: SASL matrix, send/receive both settlement modes, credit, reattach of a durable link
  after disconnect and restart, released/modified redelivery.
- Interop with MQTT and AMQP 0-9-1 on the same topics.
- Anonymous relay with `to`; request/reply with a dynamic node and an MQTT 5 responder.
- ACL deny on attach and on transfer, S7 at delivery.
- Port sharing: 0-9-1 and 1.0 clients on one port.

## Acceptance criteria

- Qpid Proton (Python) and `rhea` (Node) send/receive examples work unchanged.
- Codec survives fuzzing for a fixed budget in CI without panics.
- Binary growth and idle RSS measured.

## Decisions needed

- [ ] Sign-off: allow the AMQP 1.0 server on edge.
- [ ] B1. Share port 5672 with 0-9-1 via header sniffing (recommended) or own port.
- [ ] B2. `queue://<q>` addresses share durable queues with AMQP 0-9-1 `amqp-q-` (recommended: no, own space).
- [ ] B3. Non-binary `amqp-value` bodies: JSON-encode (recommended) or reject.
