package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"

	"github.com/dropbox/fluxon"
)

// ---- Unit tests: bulk body serialisation ----

func TestBuildBulkBodyIndex(t *testing.T) {
	actions := []*fluxon.Action{
		fluxon.Index("users", "u1", 100, fluxon.Doc{"email": "a@b.com"}),
	}
	body, err := buildBulkBody(actions)
	if err != nil {
		t.Fatal(err)
	}
	lines := ndjsonLines(body)
	if len(lines) != 2 {
		t.Fatalf("expected 2 NDJSON lines, got %d", len(lines))
	}

	// meta line
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(lines[0]), &meta); err != nil {
		t.Fatalf("meta line invalid JSON: %v", err)
	}
	idx := meta["index"].(map[string]interface{})
	if idx["_index"] != "users" {
		t.Errorf("_index=%v", idx["_index"])
	}
	if idx["_id"] != "u1" {
		t.Errorf("_id=%v", idx["_id"])
	}
	if idx["version_type"] != "external" {
		t.Errorf("version_type=%v", idx["version_type"])
	}
	if idx["version"].(float64) != 100 {
		t.Errorf("version=%v", idx["version"])
	}

	// doc line
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(lines[1]), &doc); err != nil {
		t.Fatalf("doc line invalid JSON: %v", err)
	}
	if doc["email"] != "a@b.com" {
		t.Errorf("email=%v", doc["email"])
	}
	if doc["_lsn"].(float64) != 100 {
		t.Errorf("_lsn=%v", doc["_lsn"])
	}
}

func TestBuildBulkBodySoftDelete(t *testing.T) {
	actions := []*fluxon.Action{
		fluxon.SoftDelete("orders", "o1", 200),
	}
	body, err := buildBulkBody(actions)
	if err != nil {
		t.Fatal(err)
	}
	lines := ndjsonLines(body)
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}

	var meta map[string]interface{}
	json.Unmarshal([]byte(lines[0]), &meta)
	upd := meta["update"].(map[string]interface{})
	if upd["_index"] != "orders" || upd["_id"] != "o1" {
		t.Errorf("unexpected meta: %v", upd)
	}

	var bdoc map[string]interface{}
	json.Unmarshal([]byte(lines[1]), &bdoc)
	if bdoc["scripted_upsert"] != true {
		t.Error("scripted_upsert not set")
	}
	script := bdoc["script"].(map[string]interface{})
	if script["lang"] != "painless" {
		t.Errorf("lang=%v", script["lang"])
	}
	if !strings.Contains(script["source"].(string), "_deleted") {
		t.Error("script missing _deleted reference")
	}
	params := script["params"].(map[string]interface{})
	if params["lsn"].(float64) != 200 {
		t.Errorf("params.lsn=%v", params["lsn"])
	}
	upsert := bdoc["upsert"].(map[string]interface{})
	if upsert["_deleted"] != true {
		t.Error("upsert._deleted not true")
	}
}

func TestBuildBulkBodyMixed(t *testing.T) {
	actions := []*fluxon.Action{
		fluxon.Index("users", "u1", 1, fluxon.Doc{"x": 1}),
		fluxon.SoftDelete("orders", "o1", 2),
		fluxon.Index("users", "u2", 3, fluxon.Doc{"x": 2}),
	}
	body, err := buildBulkBody(actions)
	if err != nil {
		t.Fatal(err)
	}
	lines := ndjsonLines(body)
	if len(lines) != 6 { // 2 lines per action
		t.Errorf("expected 6 lines, got %d", len(lines))
	}
}

func TestBuildBulkBodyEmpty(t *testing.T) {
	// Flush with nil actions should return nil immediately (tested via Flush)
	w := &Writer{maxRetries: 1, backoff: time.Millisecond}
	if err := w.Flush(context.Background(), nil); err != nil {
		t.Errorf("expected nil for empty flush, got %v", err)
	}
}

// ---- Integration tests: fake HTTP server ----

// esHandler wraps a handler and injects the required Elastic product header.
func esHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("X-Elastic-Product", "Elasticsearch")
		next.ServeHTTP(rw, r)
	})
}

func newTestWriter(t *testing.T, handler http.Handler) *Writer {
	t.Helper()
	srv := httptest.NewServer(esHandler(handler))
	t.Cleanup(srv.Close)
	c, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:    []string{srv.URL},
		DisableRetry: true, // our Writer owns all retry logic
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Writer{client: c, maxRetries: 3, backoff: time.Millisecond}
}

func bulkSuccessResponse() []byte {
	return []byte(`{"errors":false,"items":[{"index":{"status":200}}]}`)
}

func TestFlushSuccess(t *testing.T) {
	w := newTestWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.Write(bulkSuccessResponse())
	}))
	err := w.Flush(context.Background(), []*fluxon.Action{
		fluxon.Index("i", "1", 1, fluxon.Doc{}),
	})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestFlush409TreatedAsSuccess(t *testing.T) {
	resp := `{"errors":true,"items":[{"index":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"version conflict"}}}]}`
	w := newTestWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(resp))
	}))
	err := w.Flush(context.Background(), []*fluxon.Action{
		fluxon.Index("i", "1", 1, fluxon.Doc{}),
	})
	if err != nil {
		t.Errorf("409 should be success, got %v", err)
	}
}

func TestFlushPerItemErrorRetries(t *testing.T) {
	calls := 0
	resp := `{"errors":true,"items":[{"index":{"status":500,"error":{"type":"internal","reason":"oops"}}}]}`
	w := newTestWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		calls++
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(resp))
	}))
	w.maxRetries = 2
	err := w.Flush(context.Background(), []*fluxon.Action{fluxon.Index("i", "1", 1, fluxon.Doc{})})
	if err == nil {
		t.Error("expected error")
	}
	if calls != 3 { // initial + 2 retries
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestFlush5xxRetries(t *testing.T) {
	calls := 0
	w := newTestWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		calls++
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("unavailable"))
	}))
	w.maxRetries = 2
	err := w.Flush(context.Background(), []*fluxon.Action{fluxon.Index("i", "1", 1, fluxon.Doc{})})
	if err == nil {
		t.Error("expected error after 5xx")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestFlushRetriesExhausted(t *testing.T) {
	w := newTestWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusInternalServerError)
	}))
	w.maxRetries = 1
	err := w.Flush(context.Background(), []*fluxon.Action{fluxon.Index("i", "1", 1, fluxon.Doc{})})
	if err == nil {
		t.Error("expected error when retries exhausted")
	}
}

func TestFlushContextCancellation(t *testing.T) {
	// Server that always returns 503 to force retries
	w := newTestWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusServiceUnavailable)
	}))
	w.maxRetries = 100
	w.backoff = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	err := w.Flush(ctx, []*fluxon.Action{fluxon.Index("i", "1", 1, fluxon.Doc{})})
	if err == nil {
		t.Error("expected error on context cancellation")
	}
}

// ---- helpers ----

func ndjsonLines(data []byte) []string {
	var lines []string
	for _, line := range strings.Split(string(bytes.TrimRight(data, "\n")), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// Ensure Writer satisfies the esWriter interface used by worker (compile-time check).
type esWriterIface interface {
	Flush(ctx context.Context, actions []*fluxon.Action) error
}

var _ esWriterIface = (*Writer)(nil)

// dummy to satisfy unused import
var _ = fmt.Sprintf
var _ = io.Discard
