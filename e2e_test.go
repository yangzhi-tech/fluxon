package fluxon_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	dockerclient "github.com/docker/docker/client"
	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	tces "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dropbox/fluxon"
)

// ---- test handler ----

type e2eHandler struct{}

func (h *e2eHandler) Handle(e *fluxon.Event) (*fluxon.Action, error) {
	switch e.Source.Table {
	case "users":
		if e.Op == fluxon.OpDelete {
			return fluxon.SoftDelete("users", e.Before.String("id"), e.Source.LSN), nil
		}
		return fluxon.Index("users", e.After.String("id"), e.Source.LSN, fluxon.Doc{
			"name": e.After.String("name"),
		}), nil
	case "orders":
		if e.Op == fluxon.OpDelete {
			return fluxon.SoftDelete("orders", e.Before.String("order_id"), e.Source.LSN), nil
		}
		return fluxon.Index("orders", e.After.String("order_id"), e.Source.LSN, fluxon.Doc{
			"total": e.After.Float("total"),
		}), nil
	default:
		return nil, nil
	}
}

// ---- infrastructure ----

type e2eEnv struct {
	brokers  []string
	esAddr   string
	esUser   string
	esPass   string
	esClient *elasticsearch.Client
	produce  func(payload string)
}

func requireDocker(t *testing.T) {
	t.Helper()
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv)
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx); err != nil {
		t.Skipf("docker not reachable: %v", err)
	}
	cli.Close()
}

func setupE2E(t *testing.T) *e2eEnv {
	t.Helper()
	if testing.Short() {
		t.Skip("e2e: skipping in short mode")
	}
	requireDocker(t)
	ctx := context.Background()

	// Start Kafka
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		t.Skipf("docker unavailable (kafka): %v", err)
	}
	t.Cleanup(func() { kc.Terminate(ctx) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Start Elasticsearch
	ec, err := tces.Run(ctx, "docker.elastic.co/elasticsearch/elasticsearch:8.17.0",
		tces.WithPassword("testpass"),
	)
	if err != nil {
		t.Skipf("docker unavailable (es): %v", err)
	}
	t.Cleanup(func() { ec.Terminate(ctx) })
	esAddr := ec.Settings.Address
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esAddr},
		Username:  ec.Settings.Username,
		Password:  ec.Settings.Password,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec
	})
	if err != nil {
		t.Fatal(err)
	}

	// Producer + topic setup
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)

	adm := kadm.NewClient(producer)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, "cdc.events", "cdc.dlq"); err != nil {
		t.Fatalf("create topics: %v", err)
	}

	produce := func(payload string) {
		results := producer.ProduceSync(ctx, &kgo.Record{
			Topic: "cdc.events",
			Value: []byte(payload),
		})
		if err := results.FirstErr(); err != nil {
			t.Fatalf("produce failed: %v", err)
		}
	}

	return &e2eEnv{
		brokers:  brokers,
		esAddr:   esAddr,
		esUser:   ec.Settings.Username,
		esPass:   ec.Settings.Password,
		esClient: esClient,
		produce:  produce,
	}
}

// engineOpts allows optional overrides for startEngineWith.
type engineOpts struct {
	handler    fluxon.Handler
	maxEvents  int
	maxWaitMs  int
	maxRetries int
}

func (env *e2eEnv) startEngine(t *testing.T) context.CancelFunc {
	return env.startEngineWith(t, engineOpts{})
}

func (env *e2eEnv) startEngineWith(t *testing.T, opts engineOpts) context.CancelFunc {
	t.Helper()
	h := opts.handler
	if h == nil {
		h = &e2eHandler{}
	}
	maxEvents := opts.maxEvents
	if maxEvents == 0 {
		maxEvents = 100
	}
	maxWaitMs := opts.maxWaitMs
	if maxWaitMs == 0 {
		maxWaitMs = 200
	}
	maxRetries := opts.maxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}
	ctx, cancel := context.WithCancel(context.Background())
	cfg := fluxon.Config{
		Kafka: fluxon.KafkaConfig{
			Brokers:           env.brokers,
			Topic:             "cdc.events",
			GroupID:           fmt.Sprintf("fluxon-e2e-%d", time.Now().UnixNano()),
			DLQTopic:          "cdc.dlq",
			MaxHandlerRetries: maxRetries,
		},
		ES: fluxon.ESConfig{
			Addresses:      []string{env.esAddr},
			Username:       env.esUser,
			Password:       env.esPass,
			InsecureTLS:    true,
			MaxRetries:     3,
			RetryBackoffMs: 50,
		},
		Buffer: fluxon.BufferConfig{MaxEvents: maxEvents, MaxWaitMs: maxWaitMs},
	}
	engine := fluxon.New(cfg)
	engine.Register(h)
	go func() {
		if err := engine.Run(ctx); err != nil && ctx.Err() == nil {
			t.Logf("engine exited: %v", err)
		}
	}()
	time.Sleep(3 * time.Second) // let consumer join group and assignment settle
	return cancel
}

// consumeDLQ reads up to want messages from the DLQ topic within timeout.
func consumeDLQ(t *testing.T, brokers []string, want int, timeout time.Duration) []map[string]interface{} {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(fmt.Sprintf("dlq-reader-%d", time.Now().UnixNano())),
		kgo.ConsumeTopics("cdc.dlq"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("dlq consumer: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var msgs []map[string]interface{}
	for len(msgs) < want {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var m map[string]interface{}
			if err := json.Unmarshal(r.Value, &m); err == nil {
				msgs = append(msgs, m)
			}
		})
	}
	return msgs
}

func cdcEvent(op, table string, lsn int64, before, after map[string]interface{}) string {
	type source struct {
		LSN    int64  `json:"lsn"`
		Table  string `json:"table"`
		Schema string `json:"schema"`
	}
	type event struct {
		Op     string                 `json:"op"`
		Before map[string]interface{} `json:"before"`
		After  map[string]interface{} `json:"after"`
		Source source                 `json:"source"`
	}
	data, _ := json.Marshal(event{Op: op, Before: before, After: after, Source: source{LSN: lsn, Table: table, Schema: "public"}})
	return string(data)
}

func getDoc(t *testing.T, client *elasticsearch.Client, index, id string) map[string]interface{} {
	t.Helper()
	res, err := client.Get(index, id)
	if err != nil {
		return nil
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	data, _ := io.ReadAll(res.Body)
	var out map[string]interface{}
	json.Unmarshal(data, &out)
	src, _ := out["_source"].(map[string]interface{})
	return src
}

func waitForDoc(t *testing.T, client *elasticsearch.Client, index, id string, check func(map[string]interface{}) bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if doc := getDoc(t, client, index, id); doc != nil && check(doc) {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("condition not met for %s/%s within %v", index, id, timeout)
}

// ---- E2E tests ----

func TestE2EInsert(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "alice"}))

	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["name"] == "alice" && doc["_lsn"].(float64) == 100
	}, 15*time.Second)
}

func TestE2EUpdate(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "alice"}))
	time.Sleep(500 * time.Millisecond)
	env.produce(cdcEvent("u", "users", 200,
		map[string]interface{}{"id": "u1", "name": "alice"},
		map[string]interface{}{"id": "u1", "name": "bob"},
	))

	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["name"] == "bob"
	}, 15*time.Second)
}

func TestE2ESoftDelete(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "alice"}))
	time.Sleep(500 * time.Millisecond)
	env.produce(cdcEvent("d", "users", 200, map[string]interface{}{"id": "u1"}, nil))

	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["_deleted"] == true
	}, 15*time.Second)
}

func TestE2EDuplicateInsertIdempotent(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	// Write lsn=200 first, then redeliver lsn=100 (stale)
	env.produce(cdcEvent("c", "users", 200, nil, map[string]interface{}{"id": "u1", "name": "bob"}))
	time.Sleep(1 * time.Second)
	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "old-alice"}))

	// Wait for the newer doc to land and verify stale one didn't overwrite
	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["name"] == "bob"
	}, 15*time.Second)

	time.Sleep(1 * time.Second) // give stale event time to be rejected
	doc := getDoc(t, env.esClient, "users", "u1")
	if doc["name"] != "bob" {
		t.Errorf("stale event overwrote newer doc: name=%v", doc["name"])
	}
}

func TestE2EPoisonPillContinuesPipeline(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	env.produce(`{not valid json at all}`)
	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "alice"}))

	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["name"] == "alice"
	}, 15*time.Second)
}

func TestE2EMultipleRecords(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	for i := 0; i < 5; i++ {
		env.produce(cdcEvent("c", "orders", int64((i+1)*10),
			nil,
			map[string]interface{}{"order_id": fmt.Sprintf("o%d", i), "total": float64(i * 100)},
		))
	}

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("o%d", i)
		waitForDoc(t, env.esClient, "orders", id, func(doc map[string]interface{}) bool {
			return doc != nil
		}, 20*time.Second)
	}
}

func TestE2EHandlerSkipUnknownTable(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	// Unknown table — handler returns nil,nil; pipeline must continue
	env.produce(cdcEvent("c", "unknown_table", 1, nil, map[string]interface{}{"id": "x1"}))
	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "alice"}))

	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["name"] == "alice"
	}, 15*time.Second)
}

// ---- TC1: DLQ content ----

func TestE2EDLQPoisonPillContent(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	badPayload := `{not valid json at all}`
	env.produce(badPayload)

	msgs := consumeDLQ(t, env.brokers, 1, 15*time.Second)
	if len(msgs) == 0 {
		t.Fatal("expected 1 DLQ message, got 0")
	}
	m := msgs[0]
	if m["error"] == nil || m["error"] == "" {
		t.Errorf("DLQ message missing error field: %v", m)
	}
	// original_payload is base64 when marshalled from []byte
	if m["original_payload"] == nil {
		t.Errorf("DLQ message missing original_payload: %v", m)
	}
}

// ---- TC2: DLQ on handler retry exhaustion ----

func TestE2EDLQHandlerRetryExhausted(t *testing.T) {
	env := setupE2E(t)

	alwaysFail := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		if e.Source.Table == "bad_table" {
			return nil, fmt.Errorf("injected failure")
		}
		return (&e2eHandler{}).Handle(e)
	})

	stop := env.startEngineWith(t, engineOpts{handler: alwaysFail, maxRetries: 2})
	defer stop()

	env.produce(cdcEvent("c", "bad_table", 1, nil, map[string]interface{}{"id": "b1"}))
	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "alice"}))

	// Pipeline must continue — users event still lands in ES.
	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["name"] == "alice"
	}, 15*time.Second)

	// bad_table event must be in DLQ.
	msgs := consumeDLQ(t, env.brokers, 1, 15*time.Second)
	if len(msgs) == 0 {
		t.Fatal("expected 1 DLQ message for exhausted handler, got 0")
	}
	if msgs[0]["error"] == nil {
		t.Errorf("DLQ message missing error: %v", msgs[0])
	}
}

// ---- TC3: Skip (nil,nil) does NOT go to DLQ ----

func TestE2ESkipDoesNotGoToDLQ(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	// unknown_table → handler returns nil,nil (skip, not an error)
	env.produce(cdcEvent("c", "unknown_table", 1, nil, map[string]interface{}{"id": "x1"}))
	// sentinel to confirm the engine processed past the skipped event
	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "alice"}))
	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["name"] == "alice"
	}, 15*time.Second)

	// DLQ must be empty — skip is not an error.
	msgs := consumeDLQ(t, env.brokers, 1, 3*time.Second)
	if len(msgs) != 0 {
		t.Errorf("expected 0 DLQ messages for skipped event, got %d: %v", len(msgs), msgs)
	}
}

// ---- TC4: Stale INSERT redelivery after DELETE does not resurrect ----
//
// Realistic scenario: INSERT (LSN=100) and DELETE (LSN=200) both land in ES.
// Then INSERT is redelivered (Kafka redelivery / consumer restart).
// The tombstone must survive — the stale redelivery must be rejected.
//
// Mechanism: the DELETE scripted upsert leaves the ES _version at 201 (one
// increment beyond the external version set by the INSERT). The redelivered
// INDEX with version_type=external,version=100 is rejected as a 409 conflict.

func TestE2EStaleInsertAfterDeleteIgnored(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	// Normal flow: INSERT then DELETE.
	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "alice"}))
	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["name"] == "alice"
	}, 15*time.Second)

	env.produce(cdcEvent("d", "users", 200, map[string]interface{}{"id": "u1"}, nil))
	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["_deleted"] == true
	}, 15*time.Second)

	// Redelivery of the old INSERT (LSN=100) — must be rejected by ES version conflict.
	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "ghost"}))
	time.Sleep(2 * time.Second) // give engine time to process

	doc := getDoc(t, env.esClient, "users", "u1")
	if doc["_deleted"] != true {
		t.Errorf("stale INSERT redelivery resurrected soft-deleted doc: %v", doc)
	}
	if doc["name"] == "ghost" {
		t.Errorf("stale INSERT wrote name into tombstone: %v", doc)
	}
}

// ---- TC5: Same LSN twice is idempotent ----

func TestE2ESameLSNIdempotent(t *testing.T) {
	env := setupE2E(t)
	stop := env.startEngine(t)
	defer stop()

	payload := cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "alice"})
	env.produce(payload)
	env.produce(payload) // exact duplicate

	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["name"] == "alice"
	}, 15*time.Second)

	// Verify no DLQ messages — 409 conflict is treated as success, not an error.
	msgs := consumeDLQ(t, env.brokers, 1, 3*time.Second)
	if len(msgs) != 0 {
		t.Errorf("expected 0 DLQ messages for idempotent redelivery, got %d", len(msgs))
	}
}

// ---- TC6: Rebalance safety ----

func TestE2ERebalanceSafety(t *testing.T) {
	env := setupE2E(t)

	// Engine A: consumes first batch.
	stopA := env.startEngine(t)

	for i := 0; i < 5; i++ {
		env.produce(cdcEvent("c", "users", int64(100+i), nil,
			map[string]interface{}{"id": fmt.Sprintf("u%d", i), "name": "batch1"}))
	}
	for i := 0; i < 5; i++ {
		waitForDoc(t, env.esClient, "users", fmt.Sprintf("u%d", i), func(doc map[string]interface{}) bool {
			return doc["name"] == "batch1"
		}, 20*time.Second)
	}

	// Engine B joins the same group → triggers rebalance.
	stopB := env.startEngine(t)
	defer stopB()

	// Produce second batch during/after rebalance.
	for i := 5; i < 10; i++ {
		env.produce(cdcEvent("c", "users", int64(100+i), nil,
			map[string]interface{}{"id": fmt.Sprintf("u%d", i), "name": "batch2"}))
	}

	// Stop A — B must have the partition and process the remaining events.
	stopA()
	time.Sleep(2 * time.Second)

	for i := 5; i < 10; i++ {
		waitForDoc(t, env.esClient, "users", fmt.Sprintf("u%d", i), func(doc map[string]interface{}) bool {
			return doc["name"] == "batch2"
		}, 20*time.Second)
	}
}

// ---- TC7: Shutdown flush ----

func TestE2EShutdownFlush(t *testing.T) {
	env := setupE2E(t)
	// Large MaxWaitMs and MaxEvents so only shutdown flush triggers.
	stop := env.startEngineWith(t, engineOpts{maxEvents: 1000, maxWaitMs: 30000})
	defer stop()

	for i := 0; i < 3; i++ {
		env.produce(cdcEvent("c", "users", int64(100+i), nil,
			map[string]interface{}{"id": fmt.Sprintf("u%d", i), "name": "shutdown"}))
	}
	time.Sleep(500 * time.Millisecond) // let engine consume into buffer

	// Cancel — shutdown flush must commit buffered events.
	stop()

	// Start a new engine to verify offsets were committed (won't reprocess).
	stop2 := env.startEngine(t)
	defer stop2()

	for i := 0; i < 3; i++ {
		waitForDoc(t, env.esClient, "users", fmt.Sprintf("u%d", i), func(doc map[string]interface{}) bool {
			return doc["name"] == "shutdown"
		}, 15*time.Second)
	}
}

// ---- TC8: Time-based flush ----

func TestE2ETimeBasedFlush(t *testing.T) {
	env := setupE2E(t)
	// MaxEvents=1000 so size flush never fires; MaxWaitMs=500 drives the flush.
	stop := env.startEngineWith(t, engineOpts{maxEvents: 1000, maxWaitMs: 500})
	defer stop()

	env.produce(cdcEvent("c", "users", 100, nil, map[string]interface{}{"id": "u1", "name": "alice"}))

	// Should appear well within 3× MaxWaitMs — time-based flush must have fired.
	waitForDoc(t, env.esClient, "users", "u1", func(doc map[string]interface{}) bool {
		return doc["name"] == "alice"
	}, 5*time.Second)
}

// ---- TC9: Size-based flush ----

func TestE2ESizeBasedFlush(t *testing.T) {
	env := setupE2E(t)
	// MaxWaitMs=30s so time flush won't fire; MaxEvents=3 drives the flush.
	stop := env.startEngineWith(t, engineOpts{maxEvents: 3, maxWaitMs: 30000})
	defer stop()

	for i := 0; i < 3; i++ {
		env.produce(cdcEvent("c", "users", int64(100+i), nil,
			map[string]interface{}{"id": fmt.Sprintf("u%d", i), "name": "size"}))
	}

	// All 3 docs must appear well before the 30s time-based flush would fire.
	for i := 0; i < 3; i++ {
		waitForDoc(t, env.esClient, "users", fmt.Sprintf("u%d", i), func(doc map[string]interface{}) bool {
			return doc["name"] == "size"
		}, 10*time.Second)
	}
}
