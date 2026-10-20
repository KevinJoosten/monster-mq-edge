# MQTT hot-path fixes after the perf commits

Created: 2026-10-20. Rewritten the same day after checking every claim against the code.
Status: Proposed

Follow-up to `22c871e` (*perf(pubsub,redis,nats,mqtt): optimize pubsub routing, batched
writers, and overload state logging*) and `685188a` (*perf(mqtt): replace mochi topic trie
with sharded exact/wildcard index and optimize hot paths*). Locations are in
`internal/mqtt/` unless stated otherwise. Line numbers are as of `e1fe12e`.

## Direction: replace the mochi code, don't patch it

The engine under `internal/mqtt/` is still largely mochi-mqtt, and we intend to remove it.
About half of the problems below are in code inherited unchanged from mochi. The others
come from the perf commits adding a second, faster write path next to mochi's instead of
replacing it. The fixes therefore replace mochi constructs:

| Mochi construct | Replaced by |
|---|---|
| `Client.WritePacket` writing to the socket under `cl.Lock()` | Enqueue onto the connection's single writer (item 3) |
| `ClientConnection.outbuf`, `flushOutbuf` | Removed; the writer's batch buffer is the only write buffer |
| Alias block in `publishToClientPtr`, mutex-guarded `OutboundTopicAliases` | Alias state owned by the writer, assigned at encode time (item 5) |
| `ResendInflightMessages` writing directly | Resends go through the writer (items 3, 6) |
| Zero-copy decode for every packet type | Shared read buffer used only for PUBLISH (item 2) |

Not in scope: replacing `Inflight`, inbound alias handling, session takeover or the hook
system. Those belong to the wider mochi replacement (see "Noticed in passing").

## Summary

| # | Problem | Priority | Origin |
|---|---|---|---|
| 1 | Per-connection memory grew about 10× | P1 | `22c871e`, `685188a` |
| 2 | Small strings keep 64 KiB read buffers alive | P2 | `685188a`, on top of mochi's zero-copy decode |
| 3 | Two write paths: acks can stall, no common wire order | P2 | `22c871e`, on top of mochi's `WritePacket`/`outbuf` |
| 4 | Oversize QoS 1/2 packets leak inflight entries; session resume then fails | P2 | mochi |
| 5 | Outbound topic alias assigned before the packet is admitted | P1 | mochi |
| 6 | Inflight stores an empty topic name when an alias exists | P1 | mochi |
| 7 | A PUBLISH can reach a reconnecting client before its CONNACK | P3 | mochi |
| 8 | Ownership of zero-copy payloads is undocumented | P3 | `22c871e`, `685188a` |

Items 4–6 affect only MQTT 5 clients: 4 needs a client Maximum Packet Size, 5 and 6 need a
client Topic Alias Maximum > 0.

**Order of work.** Phase 1: items 1, 2 and 8 (independent and small). Phase 2: item 3 (the
writer), then items 4–7, which depend on it.

---

## 1. Per-connection memory grew about 10× (P1)

**Location:** `server.go:70` (`MaximumClientWritesPending`), `server.go:245–251` (buffer
defaults), `clients.go:199` (outbound channel), `clients.go:210` (read buffer),
`clients.go:260` (batch buffer), `clients.go:691` (`allocReadBuf`).

**Problem:** `22c871e` raised the per-client defaults and `685188a` added a shared read
buffer:

| Per connected client | Before `22c871e` | Now |
|---|---|---|
| Outbound queue (`chan *Packet`, allocated at connect) | 8,192 entries = 64 KiB | 65,536 entries = 512 KiB |
| `bufio.Reader` | 2 KiB | 64 KiB |
| Write buffers (batch buffer, `outbuf`), allocated on first use | 2 KiB | 64 KiB each |
| Shared read buffer (`readSlab`), allocated on the first packet with a body | none | 64 KiB |
| **Total** | **≈ 70 KiB** | **≈ 700–770 KiB** |

For 1,000 connections that is about 700 MiB instead of about 70 MiB, before any pinning
from item 2. Nothing in `internal/broker` or `internal/config` overrides these defaults.

The memory also outlives the connection:
- A client with a persistent session stays in `s.Clients` until the session expires
  (`server.go:530–537`). Nothing releases its buffers or its queue.
- Every inline client (`NewClient(nil, …)`, one per publisher ID in `InlinePublisher`) also
  gets the 512 KiB outbound channel. Inline clients never use it: `publishToClientPtr`
  returns before the send when `cl.Net.Conn == nil` (`server.go:1476`, `1536`).

**Fix:**
- Add an MQTT throughput benchmark; none exists in the repo. Cover N publishers × M
  subscribers, small payloads, QoS 0 and 1. Use it to pick defaults: compare
  8,192 / 16 KiB / 16 KiB (queue / read / write) with the current values, and keep a larger
  value only where its gain is measurable. Target: under about 100 KiB per idle connection.
- Expose the three values in the broker YAML config. This needs no GraphQL change.
- Don't allocate the outbound channel for inline clients (`cl.Net.Conn == nil`).
- Release buffers when a connection ends. Each goroutine releases what it owns: the reader
  (`attachClient`, after `cl.Read` returns) sets `readSlab` and `bconn` to nil, and the
  writer sets its batch buffer to nil before returning.
- The outbound channel can't be released safely while the `Client` stays in `s.Clients`.
  In the mochi replacement, keep an offline session as a small session record instead of a
  full `Client`.

## 2. Small strings keep 64 KiB read buffers alive (P2)

**Location:** `clients.go:691–705` (`allocReadBuf`), `packets/codec.go:46`
(`decodeString`), `packets/packets.go:752` (`PublishDecode` payload).

**Problem:** Since `685188a`, packet bodies under 32 KiB are cut from a per-connection
64 KiB buffer. Decoding was already zero-copy (inherited from mochi): `decodeString` returns
strings that point into the packet body, and `PublishDecode` sets `Payload` to a sub-slice.
Before `685188a` such a reference kept only its own packet's buffer alive. Now any
long-lived reference keeps the whole 64 KiB buffer alive.

Long-lived references today:

| What | Where it is kept |
|---|---|
| Subscription filters (SUBSCRIBE) | `cl.State.Subscriptions`, `TopicsIndex` |
| Client ID, username, will topic and payload (CONNECT) | `cl.ID` (map key in `s.Clients`, `TopicsIndex`, the queue hook, metrics), `cl.Properties` |
| Topic name and payload of every PUBLISH | `memory.MessageStore.PutOne` (`stores/memory/message_store.go:64`), through the Default archive group. That group is created automatically with filter `#` and an in-memory last-value store (`archive/manager.go:166`, `archive/group.go:180`). |
| Topic name of retained messages | In-memory retained store (`internal/broker/server.go:883`, `stores/sqlite/factory.go:44`). `retainedMessage` (`hook_storage.go:532`) copies the payload but not the topic. |
| Topic name of offline-queued messages | `hook_queue.go:182` into `stores/memory/queue_store.go`. The payload is copied, the topic is not. |
| Topic names in alias maps | Outbound alias keys (`topics.go:97`), inbound alias values (`topics.go:46`, called from `server.go:996` and `1165`) |

The worst case is many slowly changing topics mixed with a busy one: each slow topic's last
value can sit in a different buffer. For example, 10,000 such topics × 64 KiB ≈ 640 MiB.

Short-lived references need no change: the outbound queue, inflight messages until acked,
archive batches waiting for a flush, and bus subscriber channels.

**Fix:**
1. In `ReadPacket`, take the shared buffer only for PUBLISH (`fh.Type` is known before
   allocating). Every other packet type gets its own `make([]byte, n)`, as before
   `685188a`. This covers filters, client IDs, usernames and wills in one place, so
   `SubscribeDecode` needs no clones.
2. In `memory.MessageStore.PutOne`, copy the payload and detach the key:
   ```go
   func (s *MessageStore) PutOne(m stores.BrokerMessage) {
       sh := &s.shards[shardFor(m.TopicName)]
       m.Payload = append([]byte(nil), m.Payload...)
       sh.mu.Lock()
       if existing, ok := sh.data[m.TopicName]; ok {
           m.TopicName = existing.TopicName // reuse the stored key, no allocation
       } else {
           m.TopicName = strings.Clone(m.TopicName)
       }
       sh.data[m.TopicName] = m
       sh.mu.Unlock()
   }
   ```
   Reusing the stored key is required, not just cheaper. Go replaces a string map key on
   every assignment, so without it `sh.data[m.TopicName] = m` would keep the new buffer
   alive through the key even with the payload copied.
   This brings back one allocation per publish (the one `22c871e` removed from
   `OnPublished`), but only in the store that keeps the message. Retained writes copy the
   payload twice (once in `retainedMessage`). That is acceptable because retained writes are
   rare; drop the copy in `retainedMessage` only after confirming that every retained backend
   either copies or serializes.
3. At `hook_queue.go:182`, use `TopicName: strings.Clone(pk.TopicName)`. That is one clone
   per queued message, shared by all target clients.
4. In the alias maps, clone the topic when a new mapping is stored (once per topic per
   connection). For outbound aliases this happens in the writer (item 5).
5. Optional: measure a 16 KiB shared buffer instead of 64 KiB. It makes the worst case four
   times smaller, probably at little throughput cost.

## 3. Two write paths: acks can stall, no common wire order (P2)

**Location:** `clients.go:225–371` (`WriteLoop`, `writeOutboundBatch`), `clients.go:770–904`
(`WritePacket`, `flushOutbuf`).

**Problem:** Packets reach the socket in two ways:
- **Live publishes:** `cl.State.outbound` → `WriteLoop` → encoded into the batch buffer
  without the client lock → flushed under `cl.Lock()`.
- **Everything else** goes through `WritePacket`: CONNACK, acks, SUBACK/UNSUBACK, PINGRESP,
  AUTH, DISCONNECT, inflight resends, deferred QoS sends (`server.go:822`) and offline-queue
  replays (`internal/broker/hook_queue.go:336`, `359`). `WritePacket` writes directly when
  `outbound` is empty and otherwise parks the packet in `outbuf`.

`outbuf` is flushed only by `writeOutboundBatch`. At the end of a batch it is flushed only
if the batch buffer has data (`clients.go:352`). The batch buffer can end up empty when the
packets remaining in the queue are all dropped, either for exceeding the client's Maximum
Packet Size (`clients.go:314`) or by failing to encode. A parked ack then stays in `outbuf`
until some unrelated write happens. A QoS 1 publisher with a small Receive Maximum stalls,
and a delayed PINGRESP can trip the client's keepalive.

The two paths also have no common order: `outbuf` is always written before the batch
buffer, whichever packet was produced first. This doesn't matter for acks. It does break
anything where one packet depends on an earlier one: topic aliases (item 5) and CONNACK
first (item 7).

Two smaller points:
- Write errors are ignored (`_, _ = batchBuf.WriteTo(...)`), and the sent counters count
  bytes encoded, not bytes written.
- Not a bug: at the 256-packet limit with more packets queued, the batch is deliberately
  carried into the next call, and `WriteLoop` re-enters immediately. Packets are lost only
  while the client is closing, and then the queue is dropped as well.

**Fix: one writer per connection.**
- `WriteLoop` becomes the only code that encodes packets or writes to `cl.Net.Conn`.
  Remove `outbuf`, `flushOutbuf` and the `cl.Lock()` around socket writes.
- Add a control queue next to `outbound` for every packet that goes through `WritePacket`
  today. Keep it small (for example 256 entries) and never drop from it: senders block,
  with a select on `cl.State.open.Done()` so that a closing client releases them. Blocking
  the reader goroutine on a slow socket is the same back-pressure `WritePacket` gives today.
- The writer drains the control queue before `outbound`, so resends and replays aren't held
  behind a backlog of live messages.
- `WritePacket(pk)` becomes an enqueue onto the control queue and returns only
  `ErrConnectionClosed`. It stays a no-op for inline clients. Review the callers that act on
  its error: `processPubrel`, `ResendInflightMessages` and `hook_queue.go`.
- Enqueue DISCONNECT with a close-after-write flag: the writer flushes it, then calls
  `Stop`. Callers on other goroutines, such as session takeover in `inheritClientSession`,
  enqueue with a short timeout and then call `Stop` regardless, so a stuck socket can't
  block a takeover.
- Flush when both queues are empty or the buffer reaches `ClientNetWriteBufferSize`.
- On a write error, call `Stop` on the client with the error as the cause.
- Update `BytesSent`, `PacketsSent`, `MessagesSent` and `OnPublishBatchSent` after each
  successful flush. Keep `OnPacketSent` firing for the packet types that go through
  `WritePacket` today.
- For oversize packets, see item 4.

## 4. Oversize QoS 1/2 packets leak inflight entries; session resume then fails (P2)

**Location:** `clients.go:314` (drop in `writeOutboundBatch`), `clients.go:844`
(`WritePacket`), `server.go:502–507` (resend on connect).

**Problem:** A QoS 1/2 PUBLISH that exceeds the client's Maximum Packet Size is dropped at
encode time. Its `Inflight` entry and the send quota it consumed are kept, and it is never
acknowledged:
- The entry counts toward `MaximumInflight` for the rest of the session.
- If the client set a Receive Maximum, one unit of send quota is lost per drop. Once the
  quota is used up, every further QoS 1/2 message is deferred indefinitely.
- On reconnect with the session present, `ResendInflightMessages` tries the oversize packet
  again. If the client reconnects with the same limit, `WritePacket` returns
  `ErrPacketTooLarge` and `attachClient` fails the connection ("resend inflight"). The
  session can't be resumed until it expires.

The `WriteLoop` from before the perf commits ignored the same error, so this was inherited
from mochi.

**Fix:** In the writer, apply [MQTT-3.1.2-25]: discard the packet without sending it and
then behave as if sending it had completed. For a QoS 1/2 PUBLISH:
- delete the `Inflight` entry,
- return the send quota,
- decrement `Info.Inflight`,
- call `OnQosDropped`,
- log once per client at Warn level, with the packet size and the limit.

## 5. Outbound topic alias assigned before the packet is admitted (P1)

**Location:** `server.go:1464–1473` (`publishToClientPtr`), `topics.go:71–100`
(`OutboundTopicAliases`).

**Problem:** The alias is assigned before the packet is admitted. When the alias already
exists, `TopicName` is also cleared at that point. Both happen before:
- the inflight limit (`1493`),
- packet ID allocation (`1504`),
- send-quota deferral (`1518`),
- the non-blocking queue send (`1550`).

The first packet for a topic is the one that carries both the topic name and the alias. If
it is dropped, or deferred while a later QoS 0 packet on the same topic goes out, the client
receives an alias it was never told about. That is a Protocol Error (MQTT 5 §3.3.2.3.4), and
the client disconnects. Packets dropped at encode time for their size (item 4) leak the
alias in the same way.

The same code is in mochi's `publishToClient`, so this predates the perf commits.

**Fix** (needs item 3):
- Remove the alias block from `publishToClientPtr`. Packets in `outbound`, in the control
  queue and in `Inflight` always carry the full topic name and no alias.
- The writer assigns aliases while encoding. It is the only encoder, so its encode order is
  the wire order, and the first packet sent for a topic always carries the topic name.
- Alias state becomes a plain map owned by the writer, with no mutex and no atomics. It is
  created on the first PUBLISH from `cl.Properties.Props.TopicAliasMaximum`. Clone the topic
  string when storing a new alias (item 2).
- Add a new alias to the map only after the packet has been encoded and has passed the size
  check, so a dropped packet registers nothing.
- Set the alias fields only on the writer's own copy. Resetting the pooled packet clears
  them.

## 6. Inflight stores an empty topic name when an alias exists (P1)

**Location:** `server.go:1469–1471` (clears `TopicName`), `server.go:1518` (stores the packet
in `Inflight`), `clients.go:514–546` (`ResendInflightMessages`), `server.go:625` (inflight
cloned on takeover).

**Problem:** When the alias already exists, the packet stored in `Inflight` has
`TopicName == ""` and carries the old connection's alias. On session takeover or resume,
`ResendInflightMessages` runs the ACL check against `""` (`clients.go:521`). Depending on
the ACL hook, the message is either:
- dropped silently, or
- sent with an empty topic and an alias the new connection never set up. Aliases exist only
  per connection, and `ParseConnect` creates a fresh map (`clients.go:435`). The client
  disconnects with a protocol error; if it reconnects with MQTT 3.1.1, it receives a PUBLISH
  with a zero-length topic.

This was inherited from mochi.

**Fix:** Covered by item 5. `Inflight` keeps the full topic name, and resends go through the
writer, which assigns aliases for the new connection.

## 7. A PUBLISH can reach a reconnecting client before its CONNACK (P3)

**Location:** `server.go:455` (`go cl.WriteLoop()`), `server.go:492–495`.

**Problem:** The writer starts before CONNECT is read. On a resumed session,
`inheritClientSession` subscribes the new client again, and `s.Clients.Add(cl)` (493) runs
before `SendConnack` (495). Publish routing looks clients up by ID (`server.go:1297`,
`1373`). A message on a busy topic that arrives in that window is queued and can be flushed
before the CONNACK, although the server's first packet must be CONNACK. The probability is
low. This was inherited from mochi.

**Fix** (needs item 3): the writer doesn't drain `outbound` until it has written the
CONNACK. It drains the control queue from the start.

## 8. Ownership of zero-copy payloads is undocumented (P3)

**Location:** `internal/broker/hook_storage.go:406` (`OnPublished`), `server.go:1432–1433`
(`publishToClientPtr`).

**Problem:** Since the perf commits, neither `StorageHook.OnPublished` nor
`publishToClientPtr` copies the payload; mochi's per-subscriber `pk.Copy` was removed. The
last-value store, the bus, outbound queues and `Inflight` all share the caller's buffer. An
in-process publisher that reuses its buffer after `Publish` returns would change messages
that are already queued or stored.

Callers checked so far; none of them reuses its buffer:
- **Redis server:** `internal/redisserver/resp.go` allocates each argument.
- **NATS server:** `internal/nats/conn.go:655` allocates per message.
- **WinCC OA C ABI:** `embed/cabi/cabi.go:454` copies with `C.GoBytes`.
- **WinCC OA command replies:** `sendReply` in `hook_winccoa.go` uses the output of
  `json.Marshal`.
- **Kafka bridge:** franz-go, used without `kgo.WithPools`.

Not checked yet:
- the MQTT client bridge (`internal/bridge/mqttclient`),
- the WinCC Unified bridge (`internal/bridge/winccua`),
- the UNS modeler (`hook_unsmodeler.go`),
- the other callers of `publishFn` (`internal/broker/server.go:590–602`).

**Fix:** Don't add a copy. Document that the broker keeps references to the topic and the
payload, and that callers must not modify them afterwards. Put this on `Server.Publish`,
`PublishPacket`, `InjectPacket`, `InlinePublisher.Publish`/`PublishMessage` and
`ProtocolPublish`. Then audit the remaining callers.

---

## Verification

Each test below must fail on the current code.

- **Item 1:** measure heap in use per connection with 1,000 idle connections, before and
  after. Run the new throughput benchmark at the old and new defaults.
- **Item 2:** after a SUBSCRIBE or CONNECT is read, `cl.Net.readSlab` is untouched. After
  `PutOne`, changing the input payload doesn't change the stored message, and the stored key
  doesn't share memory with the input (`unsafe.StringData`).
- **Item 3:** use a client with a small Maximum Packet Size. Queue several oversize
  publishes, then have that client send a QoS 1 PUBLISH. Its PUBACK must arrive without any
  further traffic.
- **Item 4:** with the same client, an oversize QoS 1 message leaves no `Inflight` entry and
  the send quota unchanged, and reconnecting with the session present succeeds.
- **Item 5:** use an MQTT 5 client with Topic Alias Maximum > 0 and Receive Maximum 1. Test
  two cases:
  - a deferred QoS 1 message followed by a QoS 0 message on the same topic,
  - a full outbound queue (small `MaximumClientWritesPending` in the test server).

  A raw packet reader asserts that no alias is used before the packet that registers it.
- **Item 6:** resume a session with an aliased QoS 1 message in flight. The resent PUBLISH
  has the full topic and no stale alias, and the ACL hook sees the real topic.
- **Item 7:** in a writer unit test, a publish queued before the CONNACK is written after it.
- Run `go test ./internal/mqtt/... ./internal/broker/... ./internal/stores/memory/...`.
- Run `.\test-integration.ps1`.

## Noticed in passing (mochi code, for the replacement)

- `Inflight.GetAll` (`inflight.go:96`) and `ClaimNextImmediate` (`inflight.go:171`) order
  messages by `uint16(Created)`. `Created` is in Unix seconds, so the 16-bit value wraps
  about every 18 hours, and messages created just before a wrap are sorted after newer ones.

## Changes from the first draft

- The first draft's Issue 1 was mostly a false alarm:
  - The 256-packet carry-over in `writeOutboundBatch` is intentional coalescing, not a
    flush bug.
  - Byte accounting across batches is correct.
  - `Reset()` already runs after every write.

  The real flush gap is the parked ack in `outbuf` (item 3), which the old fix didn't
  cover.
- The old alias fix assumed that one goroutine or `cl.Lock()` serializes writes. Neither is
  true, so the fix would have reproduced the bug. The alias fix now depends on the single
  writer.
- The first draft's Issues 3 and 4 (now items 5 and 6) were inherited from mochi, not
  introduced by the perf commits.
- New items: per-connection memory (1), more pinned references (2), the oversize inflight
  leak (4) and CONNACK ordering (7).
- The old "more than 256 packets are flushed" test would already pass on the current code,
  so it was dropped.
