package fluxon

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
)

func newTestSweeper(t *testing.T, handler http.Handler, safeHorizon int64) (*sweeper, *httptest.Server) {
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
	s := &sweeper{
		client:      c,
		index:       "test-index",
		interval:    10 * time.Millisecond,
		safeHorizon: safeHorizon,
	}
	return s, srv
}

func TestSweeperSweepCallsDeleteByQuery(t *testing.T) {
	called := false
	var gotBody map[string]interface{}

	srv := httptest.NewServer(esTestHandler(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		called = true
		data, _ := io.ReadAll(r.Body)
		json.Unmarshal(data, &gotBody)
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(`{"deleted":1,"failures":[]}`))
	})))
	t.Cleanup(srv.Close)

	c, _ := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{srv.URL}, DisableRetry: true,
	})
	s := &sweeper{client: c, index: "idx", interval: time.Hour, safeHorizon: 500}
	if err := s.sweep(context.Background()); err != nil {
		t.Fatalf("sweep error: %v", err)
	}
	if !called {
		t.Error("delete_by_query not called")
	}
}

func TestSweeperRunFiresOnTicker(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(esTestHandler(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		calls++
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(`{"deleted":0,"failures":[]}`))
	})))
	t.Cleanup(srv.Close)

	c, _ := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{srv.URL}, DisableRetry: true,
	})
	s := &sweeper{client: c, index: "idx", interval: 30 * time.Millisecond, safeHorizon: 100}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	s.run(ctx)

	if calls < 2 {
		t.Errorf("expected at least 2 sweep calls, got %d", calls)
	}
}

func TestSweeperNotBeforeInterval(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(esTestHandler(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		calls++
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(`{"deleted":0,"failures":[]}`))
	})))
	t.Cleanup(srv.Close)

	c, _ := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{srv.URL}, DisableRetry: true,
	})
	s := &sweeper{client: c, index: "idx", interval: time.Hour, safeHorizon: 100}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	s.run(ctx)

	if calls != 0 {
		t.Errorf("sweep should not run before interval, got %d calls", calls)
	}
}

func TestSweeperErrorDoesNotStop(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(esTestHandler(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		calls++
		rw.WriteHeader(http.StatusInternalServerError)
	})))
	t.Cleanup(srv.Close)

	c, _ := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{srv.URL}, DisableRetry: true,
	})
	s := &sweeper{client: c, index: "idx", interval: 30 * time.Millisecond, safeHorizon: 100}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	s.run(ctx) // should not panic or stop early

	if calls < 2 {
		t.Errorf("expected multiple sweep attempts despite errors, got %d", calls)
	}
}
