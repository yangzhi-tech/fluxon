package es

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

	"github.com/dropbox/fluxon/pkg/types"
)

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
		Addresses: []string{srv.URL}, DisableRetry: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Writer{Client: c, MaxRetries: 3, Backoff: time.Millisecond}
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
	body, err := BuildBulkBody([]*types.Action{types.Index("users", "u1", 100, types.Doc{"email": "a@b.com"})})
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
	if idx["_index"] != "users" || idx["_id"] != "u1" || idx["version_type"] != "external" {
		t.Errorf("meta mismatch: %v", idx)
	}
	var doc map[string]interface{}
	json.Unmarshal([]byte(lines[1]), &doc)
	if doc["email"] != "a@b.com" || doc["_lsn"].(float64) != 100 {
		t.Errorf("doc mismatch: %v", doc)
	}
}

func TestBuildBulkBodySoftDelete(t *testing.T) {
	body, err := BuildBulkBody([]*types.Action{types.SoftDelete("orders", "o1", 200)})
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
	actions := []*types.Action{
		types.Index("users", "u1", 1, types.Doc{}),
		types.SoftDelete("orders", "o1", 2),
		types.Index("users", "u2", 3, types.Doc{}),
	}
	body, _ := BuildBulkBody(actions)
	if len(ndjsonLines(body)) != 6 {
		t.Errorf("expected 6 lines, got %d", len(ndjsonLines(body)))
	}
}

func TestFlushEmpty(t *testing.T) {
	w := &Writer{MaxRetries: 1, Backoff: time.Millisecond}
	if err := w.Flush(context.Background(), nil); err != nil {
		t.Errorf("expected nil for empty flush, got %v", err)
	}
}

func TestFlushSuccess(t *testing.T) {
	w := newTestWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(`{"errors":false,"items":[{"index":{"status":200}}]}`))
	}))
	if err := w.Flush(context.Background(), []*types.Action{types.Index("i", "1", 1, types.Doc{})}); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestFlush409TreatedAsSuccess(t *testing.T) {
	resp := `{"errors":true,"items":[{"index":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"conflict"}}}]}`
	w := newTestWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(resp))
	}))
	if err := w.Flush(context.Background(), []*types.Action{types.Index("i", "1", 1, types.Doc{})}); err != nil {
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
	w.MaxRetries = 2
	if err := w.Flush(context.Background(), []*types.Action{types.Index("i", "1", 1, types.Doc{})}); err == nil {
		t.Error("expected error")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestFlush5xxRetries(t *testing.T) {
	calls := 0
	w := newTestWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		calls++
		rw.WriteHeader(http.StatusServiceUnavailable)
	}))
	w.MaxRetries = 2
	if err := w.Flush(context.Background(), []*types.Action{types.Index("i", "1", 1, types.Doc{})}); err == nil {
		t.Error("expected error after 5xx")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestFlushContextCancellation(t *testing.T) {
	w := newTestWriter(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusServiceUnavailable)
	}))
	w.MaxRetries = 100
	w.Backoff = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if err := w.Flush(ctx, []*types.Action{types.Index("i", "1", 1, types.Doc{})}); err == nil {
		t.Error("expected error on context cancellation")
	}
}
