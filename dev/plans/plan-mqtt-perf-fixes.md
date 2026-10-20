# Plan: Fix MQTT Hot-Path Performance Edge Cases

**Date**: 2026-10-20  
**Status**: Proposed  
**Context**: Follow-up analysis of commits `22c871e` (*perf(pubsub,redis,nats,mqtt): optimize pubsub routing, batched writers, and overload state logging*) and `685188a` (*perf(mqtt): replace mochi topic trie with sharded exact/wildcard index and optimize hot paths*).

---

## Overview

The recent performance optimizations significantly improved MQTT publish/subscribe throughput and reduced heap allocations by introducing:
1. A sharded exact-topic + wildcard-snapshot `TopicsIndex` (`internal/mqtt/topics.go`).
2. Per-client `readSlab` allocation and zero-copy `PublishDecode` (`internal/mqtt/clients.go`, `internal/mqtt/packets/packets.go`).
3. Lock-free batched outbound packet encoding (`writeOutboundBatch` in `internal/mqtt/clients.go`) with `outboundPacketPool` (`internal/mqtt/server.go`).
4. Fast in-memory `PutOne`/`DelOne` last-value store updates (`internal/stores/memory/message_store.go`).

While QoS 1/2 handshakes, retained messages, and offline queued messages continue to work in normal flows, four edge cases were identified that can cause unflushed outbound batches, memory retention of 64 KB read slabs, or MQTT v5 Topic Alias desynchronization under load.

---

## Issue 1: Unflushed `batchBuf` and Ordering Between `writeOutboundBatch` and `WritePacket`

### Problem
In `internal/mqtt/clients.go` (`writeOutboundBatch`, lines 245–371):
1. `WriteLoop` encodes up to 256 packets from `cl.State.outbound` into `cl.Net.batchBuf` without holding `cl.Lock()`.
2. Inside the loop (line 330), `batchBuf` is only flushed if `batchBuf.Len() >= maxBuf` (default 64 KB).
3. After the loop exits (either because `cl.State.outbound` is empty or `dequeued >= 256`), line 352 checks:
   ```go
   if batchBuf.Len() > 0 && (len(cl.State.outbound) == 0 || batchBuf.Len() >= maxBuf) {
       cl.Lock()
       _ = cl.flushOutbuf()
       _, _ = batchBuf.WriteTo(cl.Net.Conn)
       batchBuf.Reset()
       cl.Unlock()
   }
   ```
4. **Bug A (Unflushed batch on 256-packet boundary)**: When `dequeued == 256` and `batchBuf.Len() < maxBuf` (e.g., 256 small packets totaling ~5 KB) while `len(cl.State.outbound) > 0`, `writeOutboundBatch` returns **without flushing `batchBuf`**, yet it immediately increments `BytesSent`, `PacketsSent`, `MessagesSent`, and calls `OnPublishBatchSent` (lines 361–370).
   - If the client disconnects or `cl.State.open` cancels before the next iteration flushes `batchBuf`, those already-counted packets are never written to `cl.Net.Conn`.
   - If `batchBuf` carries data across iterations of `writeOutboundBatch`, `totalBytes` on the next iteration only counts newly encoded `pktLen`, while `batchBuf.WriteTo(cl.Net.Conn)` flushes both old and new bytes.
5. **Bug B (Interleaving with `WritePacket`)**: While `writeOutboundBatch` is encoding packets into `batchBuf` without `cl.Lock()` (or if `batchBuf` is left unflushed across iterations), a concurrent call to `cl.WritePacket(pk)` on the reader goroutine (e.g. `PUBACK`, `PUBREC`, `PUBCOMP`, `SUBACK`, `PINGRESP`, or `QueueHook` replaying messages) acquires `cl.Lock()` and writes to `cl.Net.outbuf` or directly to `cl.Net.Conn`.
   - Even though `WritePacket` is always a complete MQTT packet frame (so TCP framing itself is not corrupted on the wire), `cl.Net.outbuf` is flushed *before* `batchBuf` in `writeOutboundBatch`, and `WritePacket` has no visibility into `batchBuf`.

### How to Fix
Always flush `batchBuf` unconditionally at the end of `writeOutboundBatch` whenever `batchBuf.Len() > 0`:
```go
if batchBuf.Len() > 0 {
    cl.Lock()
    _ = cl.flushOutbuf()
    _, _ = batchBuf.WriteTo(cl.Net.Conn)
    batchBuf.Reset()
    cl.Unlock()
}
```
Additionally, check the `WriteTo` error: if writing to `cl.Net.Conn` fails, `batchBuf.Reset()` should still be called so stale bytes do not accumulate, and metrics (`BytesSent`, `PacketsSent`, `MessagesSent`, `OnPublishBatchSent`) should only count successfully written batches (or stop the client on write error).

---

## Issue 2: 64 KB `readSlab` Backing Array Pinning & Zero-Copy Payload Mutation Risk

### Problem
1. **64 KB `readSlab` Pinning via Long-Lived Strings and Payloads**:
   - In `internal/mqtt/clients.go` (`allocReadBuf`, lines 691–705), incoming packets `< 32 KB` are sliced from a 64 KB `cl.Net.readSlab`.
   - In `internal/mqtt/packets/codec.go` (`decodeString`, line 46), strings (including `pk.TopicName` and `sub.Filter` in `SUBSCRIBE`) are created zero-copy via `unsafe.String` (`bytesToString`) pointing directly into `readSlab`.
   - When a client subscribes (`processSubscribe`), `sub.Filter` is stored long-term in `cl.State.Subscriptions`, `TopicsIndex`, and `topic.SubscriptionIndex`. That single ~20-byte `sub.Filter` string pins the entire 64 KB `readSlab` in heap memory for the lifetime of the subscription.
   - Similarly, in `internal/broker/hook_storage.go` (`OnPublished`, line 406) and `internal/stores/memory/message_store.go` (`PutOne`, line 61), `msg.TopicName` and `msg.Payload` (which point directly into `readSlab`) are stored directly in the `Default` archive group's in-memory last-value store (`sh.data[m.TopicName] = m`). Every unique topic's last value in memory pins the 64 KB `readSlab` it arrived in!
2. **In-Process Zero-Copy Payload Mutation**:
   - Because `StorageHook.OnPublished` no longer clones `pk.Payload`, any in-process caller (`Server.Publish`, `InlinePublisher`) that passes a caller-mutated buffer could mutate the stored slice in `memory.MessageStore` or `pubsub.Bus`.

### How to Fix
1. **In `SubscribeDecode` / `processSubscribe`**:
   - Clone `sub.Filter` (`strings.Clone(filter)`) when decoding `SUBSCRIBE` filters in `Packet.SubscribeDecode` (`internal/mqtt/packets/packets.go`), since `SUBSCRIBE` is a cold path and subscription filters are stored long-term in indexes.
2. **In `memory.MessageStore.PutOne` (`internal/stores/memory/message_store.go`)**:
   - When storing a message into the in-memory last-value table (`PutOne`), detach `m.TopicName` and `m.Payload` from the network `readSlab` so the 64 KB slab can be garbage-collected:
     ```go
     func (s *MessageStore) PutOne(m stores.BrokerMessage) {
         sh := &s.shards[shardFor(m.TopicName)]
         m.Payload = append([]byte(nil), m.Payload...)
         sh.mu.Lock()
         if existing, ok := sh.data[m.TopicName]; ok {
             m.TopicName = existing.TopicName // reuse existing cloned key string without allocating!
         } else {
             m.TopicName = strings.Clone(m.TopicName)
         }
         sh.data[m.TopicName] = m
         sh.mu.Unlock()
     }
     ```
   - Notice the optimization above: if the topic already exists in `sh.data` (which is true 99.9% of the time in steady state), reusing `existing.TopicName` avoids allocating a new topic string on every publish while still detaching `m` from `readSlab`!

---

## Issue 3: Outbound MQTT v5 Topic Alias Desynchronization on Dropped Packets

### Problem
In `internal/mqtt/server.go` (`publishToClientPtr`, lines 1464–1473):
```go
if cl.Properties.Props.TopicAliasMaximum > 0 {
    var aliasExists bool
    outPtr.Properties.TopicAlias, aliasExists = cl.State.TopicAliases.Outbound.Set(pk.TopicName)
    if outPtr.Properties.TopicAlias > 0 {
        outPtr.Properties.TopicAliasFlag = true
        if aliasExists {
            outPtr.TopicName = ""
        }
    }
}
```
This runs **before**:
- checking whether the client is offline/closed (lines 1476, 1536),
- checking `MaximumInflight` / `NextPacketID` quota (lines 1493, 1504),
- checking if `SetOutbound` defers the packet (`deferred == true`, line 1518), and
- attempting the non-blocking send `case cl.State.outbound <- outPtr:` (line 1550).

If the very first publish that assigns a new outbound Topic Alias (`aliasExists == false`, carrying both `TopicName` and `TopicAlias`) is **dropped** (e.g., because `cl.State.outbound` is full or `MaximumInflight` is exceeded) or **deferred** (while a subsequent QoS 0 publish on the same topic is not deferred and goes out immediately with `aliasExists == true` and `TopicName == ""`), the client receives a `PUBLISH` with `TopicAlias = N` and an empty `TopicName` before ever receiving the alias registration. Per MQTT v5 [MQTT-3.3.2-11], the client will treat this as a Protocol Error and disconnect.

### How to Fix
Move the outbound Topic Alias assignment (`cl.State.TopicAliases.Outbound.Set`) to **`writeOutboundBatch`** and **`WritePacket`** (or right before encoding `Publish`), **after** a packet has already passed queue admission and inflight deferral, in the exact order packets are serialized onto the wire:
- Because `WriteLoop` is a single goroutine per client (or `cl.Lock()` serializes writes), assigning outbound topic aliases at encode time guarantees that the first packet sent over the TCP connection for a topic always carries both `TopicName` and `TopicAlias`, and subsequent packets on that connection safely omit `TopicName`.
- Even better: if an inflight packet is resent or a deferred packet is later claimed, its `TopicName` is still intact in `cl.State.Inflight`! (Currently, if `aliasExists == true`, `outPtr.TopicName` is cleared to `""` *before* `cl.State.Inflight.SetOutbound(*outPtr)`, which means `cl.State.Inflight` stores an empty `TopicName`—breaking ACL checks in `ResendInflightMessages` at `clients.go:521` and breaking session takeover!).

---

## Issue 4: Outbound Inflight Storing Empty `TopicName` When Outbound Topic Alias Exists

### Problem (Related to Issue 3)
As noted above, in `internal/mqtt/server.go` (`publishToClientPtr`, lines 1464–1518):
1. Lines 1469–1471 clear `outPtr.TopicName = ""` when `aliasExists` is true.
2. Line 1518 then stores `*outPtr` into `cl.State.Inflight.SetOutbound(*outPtr)`.
3. If the client disconnects and reconnects (session resumption or takeover), `ResendInflightMessages` (`internal/mqtt/clients.go:521`) checks:
   ```go
   if !cl.ops.hooks.OnACLCheck(cl, tk.TopicName, false)
   ```
   With `tk.TopicName == ""`, the ACL check checks `""` instead of the actual topic, and the resent `PUBLISH` packet sent on the new connection has `TopicName == ""` even though Topic Aliases do **not** survive across connections in MQTT v5!

### How to Fix
Fixing Issue 3 by moving outbound Topic Alias resolution to the encode step (or keeping `outPtr.TopicName` populated in `Inflight` and only clearing `TopicName` on the wire copy / during `PublishEncode` when `TopicAlias` is already established) fixes both Issue 3 and Issue 4 simultaneously.

---

## Verification Plan

1. **Unit & Integration Tests**:
   - Add a unit test in `internal/mqtt` verifying that >256 small packets in `cl.State.outbound` are all flushed without waiting for a subsequent publish.
   - Add a test in `internal/mqtt` verifying MQTT v5 Outbound Topic Alias behavior when:
     - the first packet is dropped due to outbound queue overflow or inflight quota, and
     - an inflight QoS 1/2 packet with an active Topic Alias is resent after reconnect (verifying `TopicName` is preserved in `Inflight`).
   - Run `go test ./internal/mqtt/... ./internal/broker/... ./internal/stores/memory/...`.
   - Run integration tests via `.\test-integration.ps1`.
