// Package dlq provides a dead-letter queue writer backed by Kafka.
package dlq

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is the envelope written to the DLQ topic.
type Message struct {
	OriginalPayload []byte    `json:"original_payload"`
	Error           string    `json:"error"`
	Partition       int32     `json:"partition"`
	Offset          int64     `json:"offset"`
	Timestamp       time.Time `json:"timestamp"`
}

// producer abstracts kgo.Client for testing.
type producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// Writer sends failed records to a Kafka DLQ topic.
type Writer struct {
	client producer
	topic  string
}

// NewWriter creates a DLQ Writer using a dedicated Kafka producer client.
func NewWriter(brokers []string, topic string) (*Writer, error) {
	c, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return nil, err
	}
	return &Writer{client: c, topic: topic}, nil
}

// newWriterWithProducer is used in tests to inject a mock producer.
func newWriterWithProducer(p producer, topic string) *Writer {
	return &Writer{client: p, topic: topic}
}

// Send serialises the original payload and error reason into a DLQ Message and
// produces it synchronously. Errors are logged but never returned — the caller
// must not be blocked by DLQ failures.
func (w *Writer) Send(ctx context.Context, payload []byte, partition int32, offset int64, reason error) {
	msg := Message{
		OriginalPayload: payload,
		Partition:       partition,
		Offset:          offset,
		Timestamp:       time.Now().UTC(),
	}
	if reason != nil {
		msg.Error = reason.Error()
	}

	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("dlq: marshal message", "err", err)
		return
	}

	results := w.client.ProduceSync(ctx, &kgo.Record{
		Topic: w.topic,
		Value: data,
	})
	if err := results.FirstErr(); err != nil {
		slog.Error("dlq: produce failed", "err", err, "partition", partition, "offset", offset)
	}
}
