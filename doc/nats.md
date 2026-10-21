# NATS Protocol Server

MonsterMQ Edge includes a native **NATS protocol server** running directly on a configurable plain TCP port (default disabled, `NATS: 0`). Standard NATS clients (`nats` CLI, `nats.go`, `nats.py`, `nats.rs`) connect directly to the edge broker and publish, subscribe, and access Key-Value buckets as if communicating with a native NATS server — with all messages transparently routed through the broker's MQTT topic tree, archive groups, and WinCC OA native namespace.

---

## Configuration

Set `NATS` in `config.yaml` to the desired TCP port (`0` disables the listener):

```yaml
TCP:
  Port: 1883
  Enabled: true

NATS: 4222
```

The listener binds to all interfaces (`0.0.0.0`) by default. Set `NATSAddress` to restrict it, for example to
loopback only:

```yaml
NATS: 4222
NATSAddress: "127.0.0.1"
```

The port is reported in GraphQL at `brokerConfig.natsPort`.

---

## Topic Translation

NATS subjects translate deterministically to MQTT topics and vice versa:

| NATS | MQTT | Description |
|------|------|-------------|
| `.` | `/` | Hierarchy separator |
| `*` | `+` | Single-level wildcard |
| `>` | `#` | Multi-level wildcard |

Characters allowed in MQTT topics that cannot appear in NATS subjects (such as spaces) are translated: spaces in MQTT topics become underscores (`_`) in NATS subjects.

**Examples:**

| NATS subject | MQTT topic |
|---|---|
| `sensors.temperature.room1` | `sensors/temperature/room1` |
| `sensors.>` | `sensors/#` |
| `sensors.*.temperature` | `sensors/+/temperature` |
| `winccoa.tags.Pump101.speed` | `winccoa/tags/Pump101/speed` |

---

## Supported Commands

| Command | Description |
|---------|-------------|
| `CONNECT` | Connection handshake and authentication (JSON payload with `user`, `pass`, `headers`, `verbose`, etc.) |
| `PUB` | Publish message (binary-safe payload up to 1 MiB `max_payload`, optional reply-to subject) |
| `HPUB` | Publish message with NATS headers |
| `SUB` | Subscribe to subject filter (queue groups in `SUB <subject> [queue group] <sid>` are accepted for protocol compatibility; each subscriber receives all matching messages) |
| `UNSUB` | Unsubscribe: `UNSUB <sid> [max_msgs]` removes immediately or after delivering `max_msgs` messages |
| `PING` / `PONG` | Protocol heartbeats and keep-alive |

---

## Headers and Request/Reply

- The server advertises `headers: true` in the initial `INFO` string.
- Headers sent with `HPUB` become MQTT v5 user properties.
- MQTT v5 user properties are delivered as `HMSG` frames when `headers: true` is negotiated in `CONNECT`.
- Reply-to subjects in `PUB`/`HPUB` are mapped to MQTT v5 response topics, enabling cross-protocol request-reply (e.g. `nc.Request` answered by an MQTT v5 client on `_INBOX/...`).

---

## Key-Value Buckets (JetStream KV Facade)

Archive groups configured with a last-value store (`LastValType` != `NONE`) are automatically surfaced as JetStream Key-Value buckets.

| NATS KV | MonsterMQ Edge |
|---|---|
| Bucket `Plant` (stream `KV_Plant`) | Archive group `Plant` |
| Key `plant.line1.temp` | MQTT topic `plant/line1/temp` (`.` <-> `/`) |
| Value | Payload in the archive group's last-value store |
| `put` | Retained publish on topic; visible to MQTT, archives, and KV watchers |
| `del` / `purge` | Clears retained message and last-value entry |
| `get`, `watch`, `keys` | Read from last-value store; `watch` delivers snapshot then live MQTT updates |
| `add` / `rm` bucket | Managed via archive group configuration / GraphQL |

```bash
nats kv ls
nats kv put Plant plant.line1.temp 21.5
nats kv get Plant plant.line1.temp
nats kv watch Plant 'plant.line1.>'
nats kv del Plant plant.line1.temp
```

---

## WinCC OA Integration

When WinCC OA Native namespace is enabled (`WinCCOaNative.Enabled: true`, `Namespace: true`):
- `SUB winccoa.tags.<DP>.<elem>` automatically registers native interest with the WinCC OA manager and delivers the current value immediately followed by live updates.
- `PUB winccoa.tags.<DP>.<elem>.set` with JSON `{"value": ...}` writes the datapoint element.
- `nc.Request` on the `.set` subject receives the command confirmation or rejection reply.
- `PUB` directly to a value topic (without `.set`) is rejected with `-ERR 'Permissions Violation for Publish to "..."'`.
- Invalid native filter subjects return `-ERR 'Invalid Subject'`.
- JetStream KV put on a native value topic automatically routes through `/set` to update the DPE.

---

## Edge-Only Extensions and Fixes

MonsterMQ Edge applies several safety bounds and parity fixes over older broker implementations:

| # | Item | Behavior in Edge |
|---|---|---|
| **F1** | Rejected PUB/HPUB payload consumption | Payload bytes are always consumed on auth or ACL violation, preventing stream corruption and command injection. |
| **F2** | Re-SUB on active SID | A `SUB` reusing an existing SID atomically replaces that SID's filter and releases the previous subscription. |
| **F3** | Synchronous auth pipeline | Commands sent in the same TCP chunk immediately after `CONNECT` wait for synchronous authentication. |
| **F4** | Bounded UNSUB | `UNSUB <sid> <max>` tracks delivery count and removes the SID after reaching `max` messages. |
| **F5** | Session traffic metrics | NATS sessions record per-session metrics (`messagesIn`, `messagesOut`). |
| **F6** | Delivery-time ACL check | Every delivered message (core subscription and KV watch/snapshot) is checked against subscribe ACL rules on its concrete topic. |
| **F7** | Bounded inputs | Max payload bound to 1 MiB (`Maximum Payload Violation`); control line bound to 64 KiB (`Maximum Control Line Exceeded`). |
| **E1** | Native initial values | `SUB` on a WinCC OA native filter registers interest and delivers the current value first. |
| **E2** | Native filter validation | Syntactically or structurally invalid native subjects reject with `-ERR 'Invalid Subject'`. |
| **E3** | Bounded slow consumers | Internal pubsub bus overflows, KV buffer saturation, or socket writes blocked for > 10s disconnect with `-ERR 'Slow Consumer'`. |
| **E4** | Native publish rejection | Native value topic publishes return `-ERR 'Permissions Violation for Publish to "<subject>"'`. |
| **E5** | Native KV writes | KV `put` on a native value topic forwards to the `/set` command topic. |
| **E6** | No double delivery on watch | KV consumers filter initial snapshot values delivered via pubsub bus to avoid duplicate delivery on native tags. |
| **E7** | Anonymous authentication | Follows edge auth conventions (`UserManagement.AnonymousEnabled`). |
| **E8** | Edge ACL engine | Reuses `auth.Cache.Allow`, including WinCC OA aliases and wildcard subscribe checks. |
| **E9** | Leftover session cleanup | Stale `nats-` sessions from previous runs are cleaned up on broker startup. |
