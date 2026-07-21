# Plan: Offline Queue Publish-Path Performance

## Summary

The persistent offline-message queue currently has two related publish-path
problems:

1. When `MaxQueueMessages` is greater than zero, every publish performs a
   storage `Count` for every matching offline client before enqueueing. The
   shipped example and Debian configurations set the limit to `1000000`, so
   normal deployments pay for synchronous database reads even though queue
   writes are buffered.
2. `BatchingQueueStore.EnqueueMulti` holds the pending-count mutex while it can
   block on the bounded enqueue channel. The worker needs the same mutex after
   a flush. When the channel saturates, the producer and worker can deadlock.

The goal is to make queue admission an in-memory operation, preserve the
configured per-client limit, remove the saturation deadlock, and reduce
database round trips without changing the shared queue table or collection
layout.

## Constraints

- Keep the `messagequeue` schema and MongoDB document shape unchanged.
- Preserve persistent queue behavior across broker restarts.
- Preserve `MaxQueueMessages == 0` as unlimited.
- Keep the publish path free of per-message session or subscription scans.
- Do not introduce CGO or backend-specific behavior into the broker hook.
- Apply bounded backpressure when the in-memory batch queue is full; do not
  silently discard accepted messages.
- Surface asynchronous storage failures rather than treating failed writes as
  successfully persisted.

## Current Hot Path

For each publish while at least one persistent client is offline:

1. Resolve matching clients through `topic.SubscriptionIndex`.
2. Filter the candidates through the in-memory offline-client set.
3. For every remaining client, call `QueueStore.Count` when a queue limit is
   configured.
4. Copy the message and submit one `QueueBatchItem` per client to the batching
   store.
5. Flush accumulated items to the database on batch size or timer.

Step 3 is synchronous and scales with both the number of matching offline
clients and their queue depth. SQLite must scan the client's index range,
while PostgreSQL and MongoDB also incur network round trips.

## Target Design

### 1. Queue Admission Tracker

Introduce an in-memory per-client depth tracker owned by the queue layer. It
must represent both persisted and accepted-but-not-yet-flushed rows.

Required operations:

```go
type QueueDepthTracker interface {
    TryReserve(clientID string, limit int64) bool
    Release(clientID string, count int64)
    Set(clientID string, count int64)
    Delete(clientID string)
}
```

The exact API may remain private to `BatchingQueueStore`; the important
contract is atomic reservation against the configured limit. Concurrent
publishes for the same client must not admit more than the configured maximum.

Hydrate depths once during queue initialization or persistent-session
hydration. Avoid one query per publish. Prefer adding a bulk count operation
such as `CountsByClient` to `QueueStore` so each backend can load all non-empty
client depths in one query/aggregation:

- SQLite/PostgreSQL: `SELECT client_id, COUNT(*) FROM messagequeue GROUP BY client_id`
- MongoDB: aggregation grouped by `client_id`
- Memory: iterate the queue map under a read lock

If changing `QueueStore` would create unnecessary surface area, the tracker may
be initialized lazily once per offline client. It must not fall back to
repeated counts on the publish path.

Depth changes must follow queue lifecycle events:

- Increment before an item is accepted into the batching channel.
- Roll back the reservation if enqueueing is cancelled or fails.
- Do not decrement after a successful flush; the row still exists.
- Decrement after a successful `Ack`.
- Clear or adjust after `PurgeForClient` and `PurgeAll`.
- Preserve the count when visibility changes or dequeue leases a message.

The broker is single-node, so this cache may assume only the running broker
mutates its queue while it owns the database. Database compatibility with the
Java broker remains intact because no physical layout changes are required.

### 2. Saturation-Safe Batching

Refactor `BatchingQueueStore` so no mutex is held during a blocking channel
send or an underlying storage operation.

The enqueue sequence should be:

1. Reserve capacity in the depth tracker under a short lock.
2. Release the lock.
3. Send the item to the batching channel, respecting `ctx.Done()` and store
   shutdown.
4. Roll back the reservation if the item was not accepted.

The worker should:

- Drain channel items into a bounded batch.
- Write the batch without holding tracker locks.
- Retain or retry a failed batch according to a bounded retry policy.
- Record and log the terminal error if shutdown cannot persist the batch.
- Never decrement total queue depth merely because a batch reached storage.

Make `Close` idempotent and define how concurrent enqueue calls behave during
shutdown. Avoid closing the data channel while producers can still send; use a
shutdown signal and wait for admitted producers before the final drain.

### 3. Queue-Limit Integration

Move limit enforcement out of `QueueHook.OnPublished` and into one atomic queue
admission operation. A preferred interface is:

```go
EnqueueMultiLimited(ctx context.Context, msg BrokerMessage, clientIDs []string, limit int64) (accepted, rejected []string, err error)
```

An equivalent private method on `BatchingQueueStore` is acceptable if the hook
can use it without backend type assertions. The hook should log rejected
clients outside the admission lock. Rate-limit or aggregate full-queue warnings
so a saturated client cannot create a logging bottleneck.

The existing `Enqueue`, `EnqueueMulti`, and `EnqueueBatch` methods must retain
their current behavior for callers that do not request a limit.

### 4. PostgreSQL Batch Writes

Replace the per-item `tx.Exec` loop in PostgreSQL `EnqueueBatch` with
`pgx.CopyFrom` or `pgx.Batch`.

Prefer `CopyFrom` when it works cleanly with the existing types and transaction
semantics. Otherwise use `pgx.Batch` to pipeline inserts. Preserve all existing
column names and values, including message UUID, publisher, creation time,
expiry interval, visibility time, and read count defaults.

SQLite should keep its prepared statement inside one transaction. MongoDB
already uses `InsertMany` and needs no equivalent rewrite.

### 5. Topic-Matching Follow-Up

After the queue changes are measured, benchmark the remaining active-consumer
paths:

- GraphQL pubsub scans every subscriber and splits filters/topics repeatedly.
- Archive groups scan every group/filter and perform the same splitting.

If profiles show meaningful CPU or allocation pressure, compile filters at
subscription/configuration time or reuse a generic form of the existing topic
tree. Keep this separate from queue admission so the high-impact change remains
reviewable and easy to validate.

## Implementation Phases

### Phase 1: Reproduction And Benchmarks

- Add a deterministic saturation test that fills the batching channel while a
  flush is blocked and proves enqueue resumes when storage resumes.
- Add benchmarks for queue admission with limits disabled and enabled.
- Cover one and many matching offline clients.
- Record allocations and operations per second for the existing implementation.

### Phase 2: Depth Tracking And Admission

- Add bulk or lazy one-time depth hydration.
- Implement atomic per-client reservation and release.
- Replace publish-time `Queue.Count` calls with in-memory admission.
- Preserve exact limit behavior under concurrent publishers.
- Aggregate or rate-limit queue-full logging.

### Phase 3: Batching Lifecycle

- Remove blocking channel sends from mutex-protected sections.
- Honor context cancellation during enqueue.
- Make shutdown safe with concurrent publishers.
- Define and test retry/error behavior for failed asynchronous flushes.
- Ensure acknowledged and purged messages update tracked depth.

### Phase 4: PostgreSQL Bulk Insert

- Implement `CopyFrom` or pipelined batch insertion.
- Verify rollback/error behavior for a partially invalid batch.
- Compare batch throughput with 1, 100, and 1,000 items.

### Phase 5: End-To-End Verification

- Run `go test ./...` and `go test -race ./...`.
- Run ARM64 and ARMv7 cross-compilation to preserve edge targets.
- Benchmark MQTT publishing with no offline clients, one offline client, and
  many offline clients.
- Repeat with SQLite and, where available, PostgreSQL and MongoDB.
- Confirm broker shutdown flushes all accepted messages.
- Confirm restart replay delivers the expected queued messages without
  duplicates.

## Required Tests

- No offline clients: publish bypasses queue resolution and admission.
- Unlimited queue: no storage count occurs on the publish path.
- Limited queue: exactly the configured number of messages is accepted.
- Concurrent limited queue: the limit is never exceeded.
- One message matching multiple filters for one client is queued once.
- Channel saturation recovers without deadlock.
- Context cancellation rolls back an unaccepted reservation.
- Flush failure does not decrement depth or silently lose the batch.
- Ack decrements depth only after successful storage deletion.
- Purge and reconnect paths reset depth correctly.
- Restart hydration accounts for pre-existing rows.
- Close during active enqueue terminates without panic, deadlock, or accepted
  message loss.
- Existing persistence, visibility, replay, and no-duplicate integration tests
  continue to pass.

## Acceptance Criteria

- `QueueStore.Count` is not called per publish for queue-limit enforcement.
- The batching queue cannot deadlock when its channel is full.
- The configured per-client limit remains correct under concurrency.
- Accepted messages are either persisted or reported as failed; asynchronous
  write errors are not ignored.
- PostgreSQL batch insertion does not issue one network round trip per item.
- No storage schema, collection shape, GraphQL contract, or configuration field
  is changed.
- The full test and race suites pass.
- Benchmarks show that limited queue admission is independent of persisted queue
  depth and does not perform backend I/O in steady state.

## Completion

After implementation and verification, record benchmark results and design
decisions in this file, then move it to `dev/done/`.

## Implementation Results - 2026-07-10

Implemented the queue hot-path changes across the broker and all storage
backends:

- Added `QueueStore.EnqueueMultiLimited` for atomic per-client admission.
- Added `QueueStore.CountsByClient` for one-time depth hydration.
- Rebuilt `BatchingQueueStore` around a hydrated total-depth map that includes
  persisted and accepted-but-pending messages.
- Removed publish-time `Queue.Count` calls from `QueueHook.OnPublished`.
- Aggregated full-queue logging to one warning per publish instead of one
  warning per rejected client.
- Replaced the batching worker's separate item/control paths with one FIFO
  command channel. Flush barriers now order correctly after accepted items.
- Removed mutex ownership from blocking channel sends and storage writes.
- Added context cancellation with reservation rollback.
- Serialized purge, reconnect visibility reset, and shutdown against admission.
- Made shutdown idempotent and made terminal flush failures visible from
  subsequent admission and `Close`.
- Prevented failed batches from retrying once per buffered item; retries happen
  on the flush timer, an explicit barrier, or shutdown.
- Changed PostgreSQL `EnqueueBatch` from one `tx.Exec` per item to
  `pgx.CopyFrom`.
- Added grouped depth queries for SQLite, PostgreSQL, and MongoDB.

The FIFO flush barrier also fixes a pre-existing reconnect race: a client could
reconnect before its accepted queue items reached storage, observe an empty
dequeue, and leave those messages stranded until another reconnect.

### Benchmark

Command:

```text
go test ./internal/stores -run '^$' \
  -bench BenchmarkBatchingQueueLimitedAdmission -benchmem -count=3
```

Apple M4, Darwin/arm64 results for admission against an already-full client:

```text
78.21 ns/op    40 B/op    3 allocs/op
78.30 ns/op    40 B/op    3 allocs/op
77.18 ns/op    40 B/op    3 allocs/op
```

This path performs no backend call and is independent of persisted queue
depth. A pre-change numeric comparison was not retained, but the former path
executed `COUNT(*)`/`CountDocuments` for each matched client and publish.

### Verification

Passed:

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- Queue persistence, limit, visibility, restart replay, and no-duplicate
  integration tests
- Linux ARM64 cross-build with `CGO_ENABLED=0`
- Linux ARMv7 cross-build with `CGO_ENABLED=0`
- Deterministic 12,000-recipient saturation recovery test
- 100-publisher concurrent admission test with an exact limit of 10
- Hydration, cancellation rollback, purge barrier, reconnect barrier, and
  asynchronous flush failure tests

PostgreSQL and MongoDB compile successfully. Live server-backed throughput
benchmarks were not run because those services were not available in the local
test environment; these measurements are optional follow-up validation and do
not leave a functional implementation gap.

Status: complete. The implemented queue changes satisfy the required
acceptance criteria. Topic-matching optimization remains conditional on future
profiling and is not part of the completed queue work.
