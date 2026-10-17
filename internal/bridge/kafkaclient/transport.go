package kafkaclient

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record is one Kafka record as the connector sees it. A nil Key is a null
// key; an empty, non-nil Key is the empty string.
type Record struct {
	Topic string
	Key   []byte
	Value []byte
}

// consumer and producer are the thin seams over franz-go that tests replace.
type consumer interface {
	// Poll blocks until records arrive, an error occurs or ctx ends.
	Poll(ctx context.Context) ([]Record, error)
	Close()
}

type producer interface {
	// Produce queues rec; done runs once the broker acknowledged or the
	// record failed. Produce blocks while the client's buffer is full.
	Produce(ctx context.Context, rec Record, done func(error))
	Close()
}

type consumerOptions struct {
	seeds          []string
	topic          string
	groupID        string
	clientID       string
	maxPollRecords int
	extra          []kgo.Opt
}

type producerOptions struct {
	seeds    []string
	clientID string
	extra    []kgo.Opt
}

func newKgoConsumer(o consumerOptions) (consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(o.seeds...),
		kgo.ClientID(o.clientID),
		kgo.ConsumerGroup(o.groupID),
		kgo.ConsumeTopics(o.topic),
		// Core's consumer defaults; extra options below override them.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	}
	opts = append(opts, o.extra...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &kgoConsumer{cl: cl, max: o.maxPollRecords}, nil
}

type kgoConsumer struct {
	cl  *kgo.Client
	max int
}

func (c *kgoConsumer) Poll(ctx context.Context) ([]Record, error) {
	var fetches kgo.Fetches
	if c.max > 0 {
		fetches = c.cl.PollRecords(ctx, c.max)
	} else {
		fetches = c.cl.PollFetches(ctx)
	}
	if fetches.IsClientClosed() {
		return nil, errClosed
	}
	var errs []error
	fetches.EachError(func(topic string, partition int32, err error) {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			errs = append(errs, err)
		}
	})
	var out []Record
	fetches.EachRecord(func(r *kgo.Record) {
		out = append(out, Record{Topic: r.Topic, Key: r.Key, Value: r.Value})
	})
	return out, errors.Join(errs...)
}

func (c *kgoConsumer) Close() { c.cl.Close() }

var errClosed = errors.New("kafka client closed")

// flushTimeout bounds how long Stop waits for buffered records while the
// cluster is unreachable.
const flushTimeout = 5 * time.Second

func newKgoProducer(o producerOptions) (producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(o.seeds...),
		kgo.ClientID(o.clientID),
	}
	opts = append(opts, o.extra...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &kgoProducer{cl: cl}, nil
}

type kgoProducer struct{ cl *kgo.Client }

func (p *kgoProducer) Produce(ctx context.Context, rec Record, done func(error)) {
	p.cl.Produce(ctx, &kgo.Record{Topic: rec.Topic, Key: rec.Key, Value: rec.Value}, func(_ *kgo.Record, err error) {
		done(err)
	})
}

func (p *kgoProducer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	_ = p.cl.Flush(ctx)
	cancel()
	p.cl.Close()
}
