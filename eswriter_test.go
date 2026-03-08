package fluxon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
)

// esHandler wraps a handler and injects the required Elastic product header.
func esTestHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("X-Elastic-Product", "Elasticsearch")
		next.ServeHTTP(rw, r)
	})
}

func newTestESWriter(t *testing.T, handler http.Handler) *esWriter {
	t.Helper()
	srv := httptest.NewServer(esTestHandler(handler))
	t.Cleanup(srv.Close)
	c, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:    []string{srv.URL},
		DisableRetry: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &esWriter{client: c, maxRetries: 3, backoff: time.Millisecond}
}

func ndjsonLines(data []byte) []string {
	var lines []string
	for _, line := range strings.Split(string(bytes.TrimRight(data, "\n")), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestBuildBulkBodyIndex(t *testing.T) {
	actions := []*Action{Index("users", "u1", 100, Doc{"email": "a@b.com"})}
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
	idx := meta["index"].(map[string]interface{})
	if idx["_index"] != "users" || idx["_id"] != "u1" {
		t.Errorf("meta mismatch: %v", idx)
	}
	if idx["version_type"] != "external" {
		t.Errorf("version_type=%v", idx["version_type"])
	}
	var doc map[string]interface{}
	json.Unmarshal([]byte(lines[1]), &doc)
	if doc["email"] != "a@b.com" || doc["_lsn"].(float64) != 100 {
		t.Errorf("doc mismatch: %v", doc)
	}
}

func TestBuildBulkBodySoftDelete(t *testing.T) {
	actions := []*Action{SoftDelete("orders", "o1", 200)}
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
		t.Errorf("meta mismatch: %v", upd)
	}
	var bdoc map[string]interface{}
	json.Unmarshal([]byte(lines[1]), &bdoc)
	if bdoc["scripted_upsert"] != true {
		t.Error("scripted_upsert not set")
	}
	script := bdoc["script"].(map[string]interface{})
	if !strings.Contains(script["source"].(string), "_deleted") {
		t.Error("script missing _deleted")
	}
}

func TestBuildBulkBodyMixed(t *testing.T) {
	actions := []*Action{
		Index("users", "u1", 1, Doc{}),
		SoftDelete("orders", "o1", 2),
		Index("users", "u2", 3, Doc{}),
	}
	body, _ := buildBulkBody(actions)
	if len(ndjsonLines(body)) != 6 {
		t.Errorf("expected 6 lines, got %d", len(ndjsonLines(body)))
	}
}

func TestBuildBulkBodyEmpty(t *testing.T) {
	w := &esWriter{maxRetries: 1, backoff: time.Millisecond}
	if err := w.flush(context.Background(), nil); err != nil {
		t.Errorf("expected nil for empty flush, got %v", err)
	}
}

func TestESFlushSuccess(t *testing.T) {
	w := newTestESWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(`{"errors":false,"items":[{"index":{"status":200}}]}`))
	}))
	if err := w.flush(context.Background(), []*Action{Index("i", "1", 1, Doc{})}); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestESFlush409TreatedAsSuccess(t *testing.T) {
	resp := `{"errors":true,"items":[{"index":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"conflict"}}}]}`
	w := newTestESWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(resp))
	}))
	if err := w.flush(context.Background(), []*Action{Index("i", "1", 1, Doc{})}); err != nil {
		t.Errorf("409 should be success, got %v", err)
	}
}

func TestESFlushPerItemErrorRetries(t *testing.T) {
	calls := 0
	resp := `{"errors":true,"items":[{"index":{"status":500,"error":{"type":"internal","reason":"oops"}}}]}`
	w := newTestESWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		calls++
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(resp))
	}))
	w.maxRetries = 2
	err := w.flush(context.Background(), []*Action{Index("i", "1", 1, Doc{})})
	if err == nil {
		t.Error("expected error")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestESFlush5xxRetries(t *testing.T) {
	calls := 0
	w := newTestESWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		calls++
		rw.WriteHeader(http.StatusServiceUnavailable)
	}))
	w.maxRetries = 2
	err := w.flush(context.Background(), []*Action{Index("i", "1", 1, Doc{})})
	if err == nil {
		t.Error("expected error after 5xx")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestESFlushContextCancellation(t *testing.T) {
	w := newTestESWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusServiceUnavailable)
	}))
	w.maxRetries = 100
	w.backoff = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	err := w.flush(ctx, []*Action{Index("i", "1", 1, Doc{})})
	if err == nil {
		t.Error("expected error on context cancellation")
	}
}
