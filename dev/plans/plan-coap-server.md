# Plan: CoAP pub/sub server

**Status:** proposed (2026-10-21), awaiting owner sign-off.
**Phase:** 5 of [plan-amqp-and-pubsub-servers.md](plan-amqp-and-pubsub-servers.md).
**Package:** `internal/coap/`. **[edge-only]**.
**Depends on:** P1 (UDP variant), P3, P4. No P2: CoAP has no durable subscriptions.

## Goal

Constrained devices speaking CoAP (RFC 7252) publish values and observe topics on edge, following the
broker model of `draft-ietf-core-coap-pubsub`, simplified to a fixed topic tree.

## Transport

- UDP 5683, `0` = off. CoAP over TCP/WebSocket (RFC 8323) not in this plan.
- Message layer: CON/NON/ACK/RST, message ID deduplication (`EXCHANGE_LIFETIME` 247 s, bounded cache),
  piggybacked and separate responses, retransmission with `ACK_TIMEOUT` 2 s and `MAX_RETRANSMIT` 4.
- Block-wise transfer (RFC 7959) Block1 and Block2 for payloads above 1,024 bytes, up to
  `MaxPayloadSize`.
- DTLS 1.2 with PSK and certificates later (decision C3); plain UDP only in this plan.

## Resources

| Request | Behaviour |
|---|---|
| `GET /.well-known/core` | link format listing `</ps>;rt="core.ps"` |
| `PUT /ps/<t>` | publish to MQTT topic `<t>` with retain=true; 2.04 Changed |
| `POST /ps/<t>` | publish with retain=false; 2.04 Changed |
| `GET /ps/<t>` | last value from the retained store or last-value store of the matching archive group; 2.05 Content or 4.04 |
| `GET /ps/<t>` + `Observe: 0` | register observer; notifications on every message for `<t>` |
| `GET /ps/<filter>` + `Observe: 0` with `+`/`#` in path segments | [edge-only extension] observe a filter; each notification carries the concrete topic in a `Location-Path` option (decision C1) |
| `GET /ps/<t>` + `Observe: 1` | deregister |
| `DELETE /ps/<t>` | clear retained message; 2.02 Deleted |
| `GET /ps/<t>?list` | child topic names (browse), bounded like other broad reads (S6) |

- **Notifications.** Observe sequence numbers per observer, NON by default, CON every 24 h or every
  `ConEveryN` notifications to detect dead observers (RFC 7641 4.5). An RST or failed CON removes the
  observer and its bus subscription.
- **QoS.** CON publish -> QoS 1; NON -> `DefaultQos`. Notifications use CON for QoS 1 messages.
- **Content-Format.** `Content-Format` option <-> MQTT 5 ContentType (numeric registry for
  text/plain 0, application/json 50, application/cbor 60, octet-stream 42; others as string).
- **Max-Age** <-> MessageExpiry.

## Auth

Plain CoAP has no credentials. All requests run as the anonymous user, so the anonymous ACL applies;
with user management on and anonymous disabled the server refuses to start unless `AllowAnonymous` is set
(decision C2). DTLS PSK identities map to users once DTLS lands.

## Sessions, config

- One "session" per remote endpoint with at least one observer: `coap-<host>-<port>`,
  `ProtocolVersion` `"COAP"`, removed after the last observer leaves or `SessionIdleSeconds`.

```yaml
Features:
  CoapServer: false
CoapServer:
  Enabled: true
  Host: 0.0.0.0
  Port: 5683
  RootPath: ps
  AllowAnonymous: false     # C2
  DefaultQos: 0
  MaxPayloadSize: 65536
  MaxObservers: 10000
  ConEveryN: 20
  SessionIdleSeconds: 300
```

Checklist: `config.go`, `yaml-json-schema.json`, `config.yaml.example`.

## Tasks

1. Message codec and options (incl. Observe, Block1/2, Content-Format, Uri-Path, Uri-Query).
2. Message layer: dedup, retransmission, separate responses.
3. Resource handlers; last-value read through archive groups.
4. Observe registry on the bus (S3), S7 recheck, CON keep-alive.
5. Block-wise transfer.
6. Config, wiring, docs (`doc/coap.md`, README, AGENTS.md).

## Tests

With `plgd-dev/go-coap` (test-only):

- PUT/POST -> MQTT subscriber; MQTT publish -> observer notification; retained GET.
- Observe on a filter (edge extension) with `Location-Path`.
- Deregistration by `Observe: 1`, RST and CON timeout.
- Block-wise PUT and GET of a 10 KiB payload.
- Duplicate CON is answered from cache, not published twice.
- Anonymous ACL deny returns 4.03.

## Acceptance criteria

- `coap-client` (libcoap) and Californium examples PUT and observe a topic.
- Bounded memory under `MaxObservers`; binary growth measured.

## Decisions needed

- [ ] Sign-off: allow the CoAP server on edge.
- [ ] C1. Allow observing MQTT wildcard filters (edge extension) or only exact topics.
- [ ] C2. Behaviour with user management on: refuse to start unless `AllowAnonymous` (recommended).
- [ ] C3. Add `pion/dtls` (pure Go) for DTLS in a follow-up.
