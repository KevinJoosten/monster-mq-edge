package stores

import (
	"context"
	"sync"
	"time"
)

type BatchingQueueStore struct {
	underlying    QueueStore
	ch            chan QueueBatchItem
	done          chan struct{}
	wg            sync.WaitGroup
	maxBatchSize  int
	flushInterval time.Duration

	// In-memory pending counts to prevent limits race condition
	mu            sync.RWMutex
	pendingCounts map[string]int64
}

func NewBatchingQueueStore(underlying QueueStore, maxBatchSize int, flushInterval time.Duration) *BatchingQueueStore {
	b := &BatchingQueueStore{
		underlying:    underlying,
		ch:            make(chan QueueBatchItem, 10000),
		done:          make(chan struct{}),
		maxBatchSize:  maxBatchSize,
		flushInterval: flushInterval,
		pendingCounts: make(map[string]int64),
	}
	b.wg.Add(1)
	go b.worker()
	return b
}

func (b *BatchingQueueStore) Enqueue(ctx context.Context, clientID string, msg BrokerMessage) error {
	select {
	case b.ch <- QueueBatchItem{ClientID: clientID, Message: msg}:
		b.mu.Lock()
		b.pendingCounts[clientID]++
		b.mu.Unlock()
		return nil
	case <-b.done:
		return b.underlying.Enqueue(ctx, clientID, msg)
	}
}

func (b *BatchingQueueStore) EnqueueMulti(ctx context.Context, msg BrokerMessage, clientIDs []string) error {
	b.mu.Lock()
	for _, cid := range clientIDs {
		select {
		case b.ch <- QueueBatchItem{ClientID: cid, Message: msg}:
			b.pendingCounts[cid]++
		case <-b.done:
			b.mu.Unlock()
			return b.underlying.EnqueueMulti(ctx, msg, clientIDs)
		}
	}
	b.mu.Unlock()
	return nil
}

func (b *BatchingQueueStore) EnqueueBatch(ctx context.Context, batch []QueueBatchItem) error {
	return b.underlying.EnqueueBatch(ctx, batch)
}

func (b *BatchingQueueStore) worker() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	var batch []QueueBatchItem

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.underlying.EnqueueBatch(ctx, batch)
		cancel()

		b.mu.Lock()
		for _, item := range batch {
			b.pendingCounts[item.ClientID]--
			if b.pendingCounts[item.ClientID] <= 0 {
				delete(b.pendingCounts, item.ClientID)
			}
		}
		b.mu.Unlock()

		batch = nil
	}

	for {
		select {
		case item := <-b.ch:
			batch = append(batch, item)
			if len(batch) >= b.maxBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-b.done:
			for {
				select {
				case item := <-b.ch:
					batch = append(batch, item)
				default:
					goto doneDraining
				}
			}
		doneDraining:
			flush()
			return
		}
	}
}

func (b *BatchingQueueStore) Dequeue(ctx context.Context, clientID string, batchSize int) ([]BrokerMessage, error) {
	return b.underlying.Dequeue(ctx, clientID, batchSize)
}

func (b *BatchingQueueStore) Ack(ctx context.Context, clientID, messageUUID string) error {
	return b.underlying.Ack(ctx, clientID, messageUUID)
}

func (b *BatchingQueueStore) PurgeForClient(ctx context.Context, clientID string) (int64, error) {
	b.mu.Lock()
	delete(b.pendingCounts, clientID)
	b.mu.Unlock()
	return b.underlying.PurgeForClient(ctx, clientID)
}

func (b *BatchingQueueStore) PurgeAll(ctx context.Context) (int64, error) {
	b.mu.Lock()
	b.pendingCounts = make(map[string]int64)
	b.mu.Unlock()
	return b.underlying.PurgeAll(ctx)
}

func (b *BatchingQueueStore) ResetVisibility(ctx context.Context, clientID string) error {
	return b.underlying.ResetVisibility(ctx, clientID)
}

func (b *BatchingQueueStore) Count(ctx context.Context, clientID string) (int64, error) {
	underlyingCount, err := b.underlying.Count(ctx, clientID)
	if err != nil {
		return 0, err
	}
	b.mu.RLock()
	pending := b.pendingCounts[clientID]
	b.mu.RUnlock()
	return underlyingCount + pending, nil
}

func (b *BatchingQueueStore) CountAll(ctx context.Context) (int64, error) {
	underlyingCount, err := b.underlying.CountAll(ctx)
	if err != nil {
		return 0, err
	}
	b.mu.RLock()
	var pending int64
	for _, v := range b.pendingCounts {
		pending += v
	}
	b.mu.RUnlock()
	return underlyingCount + pending, nil
}

func (b *BatchingQueueStore) Close() error {
	close(b.done)
	b.wg.Wait()
	return b.underlying.Close()
}
