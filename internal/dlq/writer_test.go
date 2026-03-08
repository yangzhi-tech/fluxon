package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ---- mock producer ----

type mockProducer struct {
	produced []*kgo.Record
	failErr  error
}

func (m *mockProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	var results kgo.ProduceResults
	for _, r := range rs {
		m.produced = append(m.produced, r)
		results = append(results, kgo.ProduceResult{Record: r, Err: m.failErr})
	}
	return results
}

// ---- tests ----

func TestSendSerializesMessage(t *testing.T) {
	mp := &mockProducer{}
	w := newWriterWithProducer(mp, "dlq-topic")

	payload := []byte(`{"id":1}`)
	w.Send(context.Background(), payload, 2, 42, errors.New("handler failed"))

	if len(mp.produced) != 1 {
		t.Fatalf("expected 1 produced record, got %d", len(mp.produced))
	}
	rec := mp.produced[0]
	if rec.Topic != "dlq-topic" {
		t.Errorf("topic=%q", rec.Topic)
	}

	var msg Message
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(msg.OriginalPayload) != string(payload) {
		t.Errorf("payload=%q", msg.OriginalPayload)
	}
	if msg.Error != "handler failed" {
		t.Errorf("error=%q", msg.Error)
	}
	if msg.Partition != 2 {
		t.Errorf("partition=%d", msg.Partition)
	}
	if msg.Offset != 42 {
		t.Errorf("offset=%d", msg.Offset)
	}
	if msg.Timestamp.IsZero() {
		t.Error("timestamp should not be zero")
	}
}

func TestSendOnProduceFailureDoesNotPanic(t *testing.T) {
	mp := &mockProducer{failErr: errors.New("kafka down")}
	w := newWriterWithProducer(mp, "dlq-topic")
	// Should not panic or block
	w.Send(context.Background(), []byte("payload"), 0, 0, errors.New("reason"))
}

func TestSendNilError(t *testing.T) {
	mp := &mockProducer{}
	w := newWriterWithProducer(mp, "dlq-topic")
	w.Send(context.Background(), []byte("x"), 0, 0, nil)

	var msg Message
	json.Unmarshal(mp.produced[0].Value, &msg)
	if msg.Error != "" {
		t.Errorf("expected empty error field, got %q", msg.Error)
	}
}
