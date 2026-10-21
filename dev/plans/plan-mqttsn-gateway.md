# Plan: MQTT-SN 1.2 gateway

**Status:** proposed (2026-10-21), awaiting owner sign-off.
**Phase:** 3 of [plan-amqp-and-pubsub-servers.md](plan-amqp-and-pubsub-servers.md).
**Package:** `internal/mqttsn/`. **[edge-only]**.
**Depends on:** P1 (UDP variant) only. Can be built in parallel with AMQP/STOMP.

## Goal

Battery-powered and constrained sensor nodes (and gateways for Zigbee, 6LoWPAN, LoRa, BLE) that speak
MQTT-SN 1.2 over UDP connect straight to edge, without a separate gateway process.

## Model: transparent gateway

Each MQTT-SN client gets its own MQTT engine session with client ID `mqttsn-<clientId>` (decision N1). The
MQTT core then provides clean/persistent sessions, will messages, retained messages, QoS 1/2 flows, the
queue hook and ACLs unchanged. The gateway only translates packets and manages topic IDs.

This differs from NATS/Redis (which use the bus) on purpose: MQTT-SN semantics are MQTT semantics, so going
through the engine avoids re-implementing sessions. It needs an engine entry point for an "external
transport" client, see task 2.

## Transport

- UDP listener (default 1884, `0` = off), IPv4 and IPv6. Client identity is the remote address until
  `CONNECT`; the address -> session map is refreshed on every packet (NAT rebinding).
- Max datagram 1,600 bytes by default (config); 3-byte length form supported.
- Optional `ADVERTISE` broadcast every `AdvertiseSeconds` and answers to `SEARCHGW` with `GWINFO`
  (multicast group from config).
- Retransmission: `T_retry` 10 s, `N_retry` 5 for gateway-originated QoS 1/2 messages and
  `REGISTER`.
- No DTLS in this plan (decision N4).

## Packets

| Packet | Behaviour |
|---|---|
| `CONNECT` (flags clean, will) | opens the engine session; with will: `WILLTOPICREQ`/`WILLMSGREQ` exchange first; auth see below |
| `WILLTOPICUPD` / `WILLMSGUPD` | update the session's will |
| `REGISTER` / `REGACK` | client -> gateway: assign topic ID per session; gateway -> client before first delivery on a new topic |
| `PUBLISH` | topic ID types normal / predefined / short; QoS 0, 1, 2 and -1 |
| `SUBSCRIBE` / `UNSUBSCRIBE` | topic name (wildcards allowed), predefined ID or short topic; `SUBACK` returns the topic ID for non-wildcard names |
| `PINGREQ` / `PINGRESP` | keep-alive; `PINGREQ` with client ID wakes a sleeping client |
| `DISCONNECT` | without duration: normal; with duration: client enters **asleep** state |

- **Sleeping clients.** While asleep, the gateway holds deliveries (bounded per client, default 100,
  oldest dropped with a log) and flushes them on `PINGREQ`, then `PINGRESP`. Keep-alive timer uses the
  sleep duration. Durable data still goes through the queue hook for `clean=false`.
- **QoS -1.** Publish without a connection to predefined or short topics, only when
  `AllowQosMinusOne` is true; published as the anonymous user, so the anonymous ACL applies.
- **Topic IDs.** Per-session registry, 1..65535 excluding predefined IDs. Predefined IDs come from
  config and are the same for all clients. Registries of `clean=false` sessions are kept in memory only;
  after a gateway restart the client re-registers (spec allows this; decision N2).

## Auth

MQTT-SN 1.2 has no credentials in `CONNECT`. Options (decision N3):

1. Anonymous user ACL only (recommended default).
2. Per-client-ID mapping in config to a user name (`ClientUsers: {sensor-01: plantA}`), trusting the
   network.
3. MQTT-SN 2.0 `AUTH` packet later.

## Sessions, config

- Engine session shows as `mqttsn-<clientId>` with `ProtocolVersion` handled like NATS/Redis
  (null MQTT-only fields), `Information` with `transport: udp`, `asleep`.

```yaml
Features:
  MqttSnGateway: false
MqttSnGateway:
  Enabled: true
  Host: 0.0.0.0
  Port: 1884
  MaxPacketSize: 1600
  AllowQosMinusOne: false
  AdvertiseSeconds: 0        # 0 = no ADVERTISE
  MulticastGroup: ""         # e.g. 239.255.0.1:1884
  SleepBufferMessages: 100
  PredefinedTopics:          # id -> MQTT topic
    1: plant1/line2/temp
  ClientUsers: {}            # N3 option 2
```

Checklist: `config.go`, `yaml-json-schema.json`, `config.yaml.example`.

## Tasks

1. Packet codec for all MQTT-SN 1.2 messages (1- and 3-byte length forms).
2. Engine hook-up: an external-transport client in `internal/mqtt/` that the gateway feeds with
   synthesised MQTT packets and that hands outbound packets back through a callback instead of a socket.
   This is the riskiest task; check it does not slow the engine hot path.
3. UDP loop, address map, retransmission timers.
4. Topic ID registry, predefined and short topics.
5. Will exchange, sleeping clients, QoS -1.
6. Config, wiring, docs (`doc/mqtt-sn.md`, README, AGENTS.md).

## Tests

No maintained Go MQTT-SN client exists, so `test/integration/mqttsn_client_test.go` adds a small one.

- Connect, register, publish QoS 0/1/2, subscribe with names, predefined and short topics; MQTT
  subscriber sees the messages and vice versa.
- `clean=false`: messages queued while disconnected are delivered on reconnect, across broker restart.
- Sleep: deliveries held and flushed on `PINGREQ`.
- Will fires on keep-alive timeout.
- QoS -1 allowed and denied by config and ACL.
- Retransmission of an unacked QoS 1 delivery.

## Acceptance criteria

- Eclipse Paho MQTT-SN embedded C client (e.g. on an ESP32 or a Linux test build) works against edge.
- Hot path for normal MQTT clients unchanged (benchmark with `cmd/mmqload` before and after).

## Decisions needed

- [ ] Sign-off: allow the MQTT-SN gateway on edge.
- [ ] N1. Client ID prefix `mqttsn-` (recommended, for dashboard badge) or the device's client ID as is.
- [ ] N2. Topic ID registries in memory only (recommended) or persisted.
- [ ] N3. Auth model: anonymous ACL (recommended), config mapping, or wait for MQTT-SN 2.0.
- [ ] N4. DTLS later via `pion/dtls` (shared with CoAP decision C3).
