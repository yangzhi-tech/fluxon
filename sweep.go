package fluxon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// sweeper runs delete_by_query on a ticker to purge soft-deleted documents
// whose LSN is below the configured safe horizon.
type sweeper struct {
	client      *elasticsearch.Client
	index       string
	interval    time.Duration
	safeHorizon int64
}

func newSweeper(client *elasticsearch.Client, index string, cfg SweepConfig) *sweeper {
	interval := cfg.IntervalS
	if interval == 0 {
		interval = 300
	}
	return &sweeper{
		client:      client,
		index:       index,
		interval:    time.Duration(interval) * time.Second,
		safeHorizon: cfg.SafeHorizon,
	}
}

// run starts the sweep ticker and blocks until ctx is cancelled.
func (s *sweeper) run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.sweep(ctx); err != nil {
				slog.Error("sweep failed", "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// sweep issues a delete_by_query for soft-deleted docs below the safe horizon.
func (s *sweeper) sweep(ctx context.Context) error {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{"term": map[string]interface{}{"_deleted": true}},
					map[string]interface{}{"range": map[string]interface{}{"_lsn": map[string]interface{}{"lt": s.safeHorizon}}},
				},
			},
		},
	}
	body, err := json.Marshal(query)
	if err != nil {
		return fmt.Errorf("sweep: marshal query: %w", err)
	}

	req := esapi.DeleteByQueryRequest{
		Index: []string{s.index},
		Body:  bytes.NewReader(body),
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return fmt.Errorf("sweep: delete_by_query: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("sweep: delete_by_query status %s", res.Status())
	}

	slog.Info("sweep complete", "index", s.index, "safeHorizon", s.safeHorizon)
	return nil
}
