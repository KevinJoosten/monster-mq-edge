package stores_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"monstermq.io/edge/internal/stores"
	storememory "monstermq.io/edge/internal/stores/memory"
)

type controlledQueueStore struct {
	*storememory.QueueStore
	started   chan struct{}
	release   chan struct{}
	failCount atomic.Int32
}

func newControlledQueueStore() *controlledQueueStore {
	return &controlledQueueStore{
		QueueStore: storememory.NewQueueStore(30 * time.Second),
		started:    make(chan struct{}, 1),
	}
}

func (q *controlledQueueStore) EnqueueBatch(ctx context.Context, batch []stores.QueueBatchItem) error {
	select {
	case q.started <- struct{}{}:
	default:
	}
	if q.release != nil {
		select {
		case <-q.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if q.failCount.Add(-1) >= 0 {
		return errors.New("injected batch failure")
	}
	return q.QueueStore.EnqueueBatch(ctx, batch)
}

func queueMessage(id string) stores.BrokerMessage {
	return stores.BrokerMessage{
		MessageUUID: id,
		TopicName:   "performance/test",
		Payload:     []byte("payload"),
		Time:        time.Now(),
	}
}

func TestBatchingQueueSaturationRecovers(t *testing.T) {
	underlying := newControlledQueueStore()
	underlying.release = make(chan struct{})
	q, err := stores.NewBatchingQueueStore(context.Background(), underlying, 1000, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	clientIDs := make([]string, 12_000)
	for i := range clientIDs {
		clientIDs[i] = fmt.Sprintf("client-%d", i)
	}
	done := make(chan error, 1)
	go func() {
		done <- q.EnqueueMulti(context.Background(), queueMessage("saturation"), clientIDs)
	}()
	<-underlying.started
	close(underlying.release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("enqueue did not recover after the batching channel saturated")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	count, err := underlying.CountAll(context.Background())
	if err != nil || count != int64(len(clientIDs)) {
		t.Fatalf("persisted count = %d, err = %v", count, err)
	}
}

func TestBatchingQueueConcurrentLimit(t *testing.T) {
	underlying := storememory.NewQueueStore(30 * time.Second)
	q, err := stores.NewBatchingQueueStore(context.Background(), underlying, 1000, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	const limit = int64(10)
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result, err := q.EnqueueMultiLimited(context.Background(), queueMessage(fmt.Sprintf("msg-%d", i)), []string{"client"}, limit)
			if err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
			accepted.Add(int64(len(result.Accepted)))
		}(i)
	}
	wg.Wait()
	if got := accepted.Load(); got != limit {
		t.Fatalf("accepted = %d, want %d", got, limit)
	}
	count, err := q.Count(context.Background(), "client")
	if err != nil || count != limit {
		t.Fatalf("tracked count = %d, err = %v", count, err)
	}
}

func TestBatchingQueueHydratesDepths(t *testing.T) {
	underlying := storememory.NewQueueStore(30 * time.Second)
	for i := 0; i < 3; i++ {
		if err := underlying.Enqueue(context.Background(), "client", queueMessage(fmt.Sprintf("existing-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	q, err := stores.NewBatchingQueueStore(context.Background(), underlying, 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	result, err := q.EnqueueMultiLimited(context.Background(), queueMessage("rejected"), []string{"client"}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Accepted) != 0 || len(result.Rejected) != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestBatchingQueuePurgeFlushesPending(t *testing.T) {
	underlying := storememory.NewQueueStore(30 * time.Second)
	q, err := stores.NewBatchingQueueStore(context.Background(), underlying, 1000, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	if err := q.Enqueue(context.Background(), "client", queueMessage("pending")); err != nil {
		t.Fatal(err)
	}
	purged, err := q.PurgeForClient(context.Background(), "client")
	if err != nil || purged != 1 {
		t.Fatalf("purged = %d, err = %v", purged, err)
	}
	count, _ := q.Count(context.Background(), "client")
	underlyingCount, _ := underlying.Count(context.Background(), "client")
	if count != 0 || underlyingCount != 0 {
		t.Fatalf("counts after purge: tracked=%d underlying=%d", count, underlyingCount)
	}
}

func TestBatchingQueueResetVisibilityFlushesPending(t *testing.T) {
	underlying := storememory.NewQueueStore(30 * time.Second)
	q, err := stores.NewBatchingQueueStore(context.Background(), underlying, 1000, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	if err := q.Enqueue(context.Background(), "client", queueMessage("pending")); err != nil {
		t.Fatal(err)
	}
	if err := q.ResetVisibility(context.Background(), "client"); err != nil {
		t.Fatal(err)
	}
	batch, err := q.Dequeue(context.Background(), "client", 10)
	if err != nil || len(batch) != 1 {
		t.Fatalf("dequeue after reset returned %d messages, err = %v", len(batch), err)
	}
}

func TestBatchingQueueCancellationRollsBackReservation(t *testing.T) {
	underlying := newControlledQueueStore()
	underlying.release = make(chan struct{})
	q, err := stores.NewBatchingQueueStore(context.Background(), underlying, 1, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(underlying.release)
		_ = q.Close()
	}()

	clientIDs := make([]string, 10_010)
	for i := range clientIDs {
		clientIDs[i] = "client"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result, err := q.EnqueueMultiLimited(ctx, queueMessage("cancel"), clientIDs, 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("enqueue error = %v", err)
	}
	count, countErr := q.Count(context.Background(), "client")
	if countErr != nil || count != int64(len(result.Accepted)) {
		t.Fatalf("tracked count = %d, accepted = %d, err = %v", count, len(result.Accepted), countErr)
	}
}

func TestBatchingQueueFlushFailureRejectsNewAdmission(t *testing.T) {
	underlying := newControlledQueueStore()
	underlying.failCount.Store(1)
	q, err := stores.NewBatchingQueueStore(context.Background(), underlying, 1, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	if err := q.Enqueue(context.Background(), "client", queueMessage("fails")); err != nil {
		t.Fatal(err)
	}
	<-underlying.started
	deadline := time.Now().Add(time.Second)
	for {
		_, err = q.EnqueueMultiLimited(context.Background(), queueMessage("rejected"), []string{"client"}, 0)
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("flush failure was not exposed to enqueue")
		}
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(err, context.DeadlineExceeded) && err.Error() != "queue batch flush failed: injected batch failure" {
		t.Fatalf("unexpected enqueue error: %v", err)
	}
}

func BenchmarkBatchingQueueLimitedAdmission(b *testing.B) {
	underlying := storememory.NewQueueStore(30 * time.Second)
	q, err := stores.NewBatchingQueueStore(context.Background(), underlying, 1000, time.Hour)
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	if _, err := q.EnqueueMultiLimited(context.Background(), queueMessage("seed"), []string{"client"}, 1); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := q.EnqueueMultiLimited(context.Background(), queueMessage("rejected"), []string{"client"}, 1); err != nil {
			b.Fatal(err)
		}
	}
}
