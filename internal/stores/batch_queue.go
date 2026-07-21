package stores

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrQueueStoreClosed = errors.New("queue store closed")

type batchFlushRequest struct {
	ctx    context.Context
	result chan error
}

type batchCommand struct {
	item  QueueBatchItem
	flush *batchFlushRequest
}

type BatchingQueueStore struct {
	underlying    QueueStore
	ch            chan batchCommand
	closing       chan struct{}
	workerStop    chan struct{}
	workerDone    chan struct{}
	maxBatchSize  int
	flushInterval time.Duration

	// opsMu lets enqueues run concurrently but serializes purge and shutdown
	// against admission. The worker never acquires it.
	opsMu sync.RWMutex

	stateMu  sync.RWMutex
	depths   map[string]int64
	flushErr error
	closed   bool

	closeOnce sync.Once
	closeErr  error
}

func NewBatchingQueueStore(ctx context.Context, underlying QueueStore, maxBatchSize int, flushInterval time.Duration) (*BatchingQueueStore, error) {
	if maxBatchSize <= 0 {
		maxBatchSize = 1000
	}
	if flushInterval <= 0 {
		flushInterval = 50 * time.Millisecond
	}
	depths, err := underlying.CountsByClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("hydrate queue depths: %w", err)
	}
	b := &BatchingQueueStore{
		underlying:    underlying,
		ch:            make(chan batchCommand, 10000),
		closing:       make(chan struct{}),
		workerStop:    make(chan struct{}),
		workerDone:    make(chan struct{}),
		maxBatchSize:  maxBatchSize,
		flushInterval: flushInterval,
		depths:        depths,
	}
	go b.worker()
	return b, nil
}

func (b *BatchingQueueStore) Enqueue(ctx context.Context, clientID string, msg BrokerMessage) error {
	_, err := b.EnqueueMultiLimited(ctx, msg, []string{clientID}, 0)
	return err
}

func (b *BatchingQueueStore) EnqueueMulti(ctx context.Context, msg BrokerMessage, clientIDs []string) error {
	_, err := b.EnqueueMultiLimited(ctx, msg, clientIDs, 0)
	return err
}

func (b *BatchingQueueStore) EnqueueMultiLimited(ctx context.Context, msg BrokerMessage, clientIDs []string, limit int64) (QueueEnqueueResult, error) {
	if len(clientIDs) == 0 {
		return QueueEnqueueResult{}, nil
	}
	b.opsMu.RLock()
	defer b.opsMu.RUnlock()

	accepted, rejected, err := b.reserve(clientIDs, limit)
	if err != nil {
		return QueueEnqueueResult{Rejected: append([]string(nil), clientIDs...)}, err
	}
	result := QueueEnqueueResult{Accepted: accepted, Rejected: rejected}
	for i, clientID := range accepted {
		select {
		case b.ch <- batchCommand{item: QueueBatchItem{ClientID: clientID, Message: msg}}:
		case <-ctx.Done():
			b.release(accepted[i:])
			result.Accepted = accepted[:i]
			result.Rejected = append(result.Rejected, accepted[i:]...)
			return result, ctx.Err()
		case <-b.closing:
			b.release(accepted[i:])
			result.Accepted = accepted[:i]
			result.Rejected = append(result.Rejected, accepted[i:]...)
			return result, ErrQueueStoreClosed
		}
	}
	return result, nil
}

func (b *BatchingQueueStore) reserve(clientIDs []string, limit int64) (accepted, rejected []string, err error) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.closed {
		return nil, nil, ErrQueueStoreClosed
	}
	if b.flushErr != nil {
		return nil, nil, fmt.Errorf("queue batch flush failed: %w", b.flushErr)
	}
	accepted = make([]string, 0, len(clientIDs))
	for _, clientID := range clientIDs {
		if limit > 0 && b.depths[clientID] >= limit {
			rejected = append(rejected, clientID)
			continue
		}
		b.depths[clientID]++
		accepted = append(accepted, clientID)
	}
	return accepted, rejected, nil
}

func (b *BatchingQueueStore) release(clientIDs []string) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	for _, clientID := range clientIDs {
		b.releaseLocked(clientID, 1)
	}
}

func (b *BatchingQueueStore) releaseLocked(clientID string, count int64) {
	remaining := b.depths[clientID] - count
	if remaining <= 0 {
		delete(b.depths, clientID)
		return
	}
	b.depths[clientID] = remaining
}

func (b *BatchingQueueStore) EnqueueBatch(ctx context.Context, batch []QueueBatchItem) error {
	if len(batch) == 0 {
		return nil
	}
	b.opsMu.RLock()
	defer b.opsMu.RUnlock()
	if err := b.checkAvailable(); err != nil {
		return err
	}
	if err := b.underlying.EnqueueBatch(ctx, batch); err != nil {
		return err
	}
	b.stateMu.Lock()
	for _, item := range batch {
		b.depths[item.ClientID]++
	}
	b.stateMu.Unlock()
	return nil
}

func (b *BatchingQueueStore) checkAvailable() error {
	b.stateMu.RLock()
	defer b.stateMu.RUnlock()
	if b.closed {
		return ErrQueueStoreClosed
	}
	if b.flushErr != nil {
		return fmt.Errorf("queue batch flush failed: %w", b.flushErr)
	}
	return nil
}

func (b *BatchingQueueStore) worker() {
	defer close(b.workerDone)
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batch := make([]QueueBatchItem, 0, b.maxBatchSize)
	flushFailed := false
	flush := func(ctx context.Context) error {
		if len(batch) == 0 {
			flushFailed = false
			b.setFlushError(nil)
			return nil
		}
		err := b.underlying.EnqueueBatch(ctx, batch)
		flushFailed = err != nil
		b.setFlushError(err)
		if err == nil {
			batch = batch[:0]
		}
		return err
	}

	for {
		select {
		case cmd := <-b.ch:
			if cmd.flush != nil {
				cmd.flush.result <- flush(cmd.flush.ctx)
				continue
			}
			batch = append(batch, cmd.item)
			if len(batch) >= b.maxBatchSize && !flushFailed {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = flush(ctx)
				cancel()
			}
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = flush(ctx)
			cancel()
		case <-b.workerStop:
			for {
				select {
				case cmd := <-b.ch:
					if cmd.flush != nil {
						cmd.flush.result <- flush(cmd.flush.ctx)
						continue
					}
					batch = append(batch, cmd.item)
				default:
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = flush(ctx)
					cancel()
					return
				}
			}
		}
	}
}

func (b *BatchingQueueStore) setFlushError(err error) {
	b.stateMu.Lock()
	b.flushErr = err
	b.stateMu.Unlock()
}

func (b *BatchingQueueStore) flush(ctx context.Context) error {
	result := make(chan error, 1)
	req := batchFlushRequest{ctx: ctx, result: result}
	select {
	case b.ch <- batchCommand{flush: &req}:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.workerDone:
		return ErrQueueStoreClosed
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-b.workerDone:
		return ErrQueueStoreClosed
	}
}

func (b *BatchingQueueStore) Dequeue(ctx context.Context, clientID string, batchSize int) ([]BrokerMessage, error) {
	return b.underlying.Dequeue(ctx, clientID, batchSize)
}

func (b *BatchingQueueStore) Ack(ctx context.Context, clientID, messageUUID string) error {
	b.opsMu.RLock()
	defer b.opsMu.RUnlock()
	if err := b.underlying.Ack(ctx, clientID, messageUUID); err != nil {
		return err
	}
	b.stateMu.Lock()
	b.releaseLocked(clientID, 1)
	b.stateMu.Unlock()
	return nil
}

func (b *BatchingQueueStore) PurgeForClient(ctx context.Context, clientID string) (int64, error) {
	b.opsMu.Lock()
	defer b.opsMu.Unlock()
	if err := b.flush(ctx); err != nil {
		return 0, err
	}
	n, err := b.underlying.PurgeForClient(ctx, clientID)
	if err != nil {
		return 0, err
	}
	b.stateMu.Lock()
	delete(b.depths, clientID)
	b.stateMu.Unlock()
	return n, nil
}

func (b *BatchingQueueStore) PurgeAll(ctx context.Context) (int64, error) {
	b.opsMu.Lock()
	defer b.opsMu.Unlock()
	if err := b.flush(ctx); err != nil {
		return 0, err
	}
	n, err := b.underlying.PurgeAll(ctx)
	if err != nil {
		return 0, err
	}
	b.stateMu.Lock()
	b.depths = make(map[string]int64)
	b.stateMu.Unlock()
	return n, nil
}

func (b *BatchingQueueStore) ResetVisibility(ctx context.Context, clientID string) error {
	b.opsMu.Lock()
	defer b.opsMu.Unlock()
	if err := b.flush(ctx); err != nil {
		return err
	}
	return b.underlying.ResetVisibility(ctx, clientID)
}

func (b *BatchingQueueStore) Count(_ context.Context, clientID string) (int64, error) {
	b.stateMu.RLock()
	n := b.depths[clientID]
	b.stateMu.RUnlock()
	return n, nil
}

func (b *BatchingQueueStore) CountAll(_ context.Context) (int64, error) {
	b.stateMu.RLock()
	var total int64
	for _, n := range b.depths {
		total += n
	}
	b.stateMu.RUnlock()
	return total, nil
}

func (b *BatchingQueueStore) CountsByClient(_ context.Context) (map[string]int64, error) {
	b.stateMu.RLock()
	depths := make(map[string]int64, len(b.depths))
	for clientID, n := range b.depths {
		depths[clientID] = n
	}
	b.stateMu.RUnlock()
	return depths, nil
}

func (b *BatchingQueueStore) Close() error {
	b.closeOnce.Do(func() {
		b.stateMu.Lock()
		b.closed = true
		close(b.closing)
		b.stateMu.Unlock()

		b.opsMu.Lock()
		close(b.workerStop)
		<-b.workerDone
		b.stateMu.RLock()
		flushErr := b.flushErr
		b.stateMu.RUnlock()
		b.closeErr = errors.Join(flushErr, b.underlying.Close())
		b.opsMu.Unlock()
	})
	return b.closeErr
}
