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

func (env *e2eEnv) startEngine(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := fluxon.Config{
		Kafka: fluxon.KafkaConfig{
			Brokers:           env.brokers,
			Topic:             "cdc.events",
			GroupID:           fmt.Sprintf("fluxon-e2e-%d", time.Now().UnixNano()),
			DLQTopic:          "cdc.dlq",
			MaxHandlerRetries: 3,
		},
		ES: fluxon.ESConfig{
			Addresses:      []string{env.esAddr},
			Username:       env.esUser,
			Password:       env.esPass,
			InsecureTLS:    true,
			MaxRetries:     3,
			RetryBackoffMs: 50,
		},
		Buffer: fluxon.BufferConfig{MaxEvents: 100, MaxWaitMs: 200},
	}
	engine := fluxon.New(cfg)
	engine.Register(&e2eHandler{})
	go func() {
		if err := engine.Run(ctx); err != nil && ctx.Err() == nil {
			t.Logf("engine exited: %v", err)
		}
	}()
	time.Sleep(3 * time.Second) // let consumer join group and assignment settle
	return cancel
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
