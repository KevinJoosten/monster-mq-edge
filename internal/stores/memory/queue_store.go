package memory

import (
	"context"
	"sync"
	"time"

	"monstermq.io/edge/internal/stores"
)

type queueItem struct {
	msg stores.BrokerMessage
	vt  int64
}

type QueueStore struct {
	mu                sync.RWMutex
	queues            map[string][]*queueItem
	visibilityTimeout time.Duration
}

func NewQueueStore(visibilityTimeout time.Duration) *QueueStore {
	return &QueueStore{
		queues:            make(map[string][]*queueItem),
		visibilityTimeout: visibilityTimeout,
	}
}

func (q *QueueStore) Enqueue(ctx context.Context, clientID string, msg stores.BrokerMessage) error {
	return q.EnqueueMulti(ctx, msg, []string{clientID})
}

func (q *QueueStore) EnqueueMulti(ctx context.Context, msg stores.BrokerMessage, clientIDs []string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	msgCopy := msg
	msgCopy.IsQueued = true

	for _, cid := range clientIDs {
		item := &queueItem{
			msg: msgCopy,
			vt:  0, // initially visible
		}
		q.queues[cid] = append(q.queues[cid], item)
	}
	return nil
}

func (q *QueueStore) EnqueueBatch(ctx context.Context, batch []stores.QueueBatchItem) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, item := range batch {
		msgCopy := item.Message
		msgCopy.IsQueued = true
		qItem := &queueItem{
			msg: msgCopy,
			vt:  0, // initially visible
		}
		q.queues[item.ClientID] = append(q.queues[item.ClientID], qItem)
	}
	return nil
}

func (q *QueueStore) Dequeue(ctx context.Context, clientID string, batchSize int) ([]stores.BrokerMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	queue, ok := q.queues[clientID]
	if !ok || len(queue) == 0 {
		return nil, nil
	}

	if batchSize <= 0 {
		batchSize = 10
	}

	now := time.Now().Unix()
	newVT := now + int64(q.visibilityTimeout.Seconds())

	var out []stores.BrokerMessage
	count := 0
	for _, item := range queue {
		if item.vt <= now {
			item.vt = newVT
			out = append(out, item.msg)
			count++
			if count >= batchSize {
				break
			}
		}
	}
	return out, nil
}

func (q *QueueStore) Ack(ctx context.Context, clientID, messageUUID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	queue, ok := q.queues[clientID]
	if !ok {
		return nil
	}

	for i, item := range queue {
		if item.msg.MessageUUID == messageUUID {
			// Remove from slice
			q.queues[clientID] = append(queue[:i], queue[i+1:]...)
			break
		}
	}
	return nil
}

func (q *QueueStore) PurgeForClient(ctx context.Context, clientID string) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	queue, ok := q.queues[clientID]
	if !ok {
		return 0, nil
	}
	n := int64(len(queue))
	delete(q.queues, clientID)
	return n, nil
}

func (q *QueueStore) PurgeAll(ctx context.Context) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var total int64
	for _, qSlice := range q.queues {
		total += int64(len(qSlice))
	}
	q.queues = make(map[string][]*queueItem)
	return total, nil
}

func (q *QueueStore) Count(ctx context.Context, clientID string) (int64, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return int64(len(q.queues[clientID])), nil
}

func (q *QueueStore) CountAll(ctx context.Context) (int64, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var total int64
	for _, qSlice := range q.queues {
		total += int64(len(qSlice))
	}
	return total, nil
}

func (q *QueueStore) ResetVisibility(ctx context.Context, clientID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	queue, ok := q.queues[clientID]
	if !ok {
		return nil
	}
	for _, item := range queue {
		item.vt = 0
	}
	return nil
}

func (q *QueueStore) Close() error {
	return nil
}
