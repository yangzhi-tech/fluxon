package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/dropbox/fluxon/pkg/types"
)

// Sweeper runs delete_by_query on a ticker to purge soft-deleted documents
// whose LSN is below the configured safe horizon.
type Sweeper struct {
	Client      *elasticsearch.Client
	Index       string
	Interval    time.Duration
	SafeHorizon int64
}

// NewSweeper creates a Sweeper from SweepConfig.
func NewSweeper(client *elasticsearch.Client, index string, cfg types.SweepConfig) *Sweeper {
	interval := cfg.IntervalS
	if interval == 0 {
		interval = 300
	}
	return &Sweeper{
		Client:      client,
		Index:       index,
		Interval:    time.Duration(interval) * time.Second,
		SafeHorizon: cfg.SafeHorizon,
	}
}

// Run starts the sweep ticker and blocks until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.Sweep(ctx); err != nil {
				slog.Error("sweep failed", "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// Sweep issues a delete_by_query for soft-deleted docs below the safe horizon.
func (s *Sweeper) Sweep(ctx context.Context) error {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{"term": map[string]interface{}{"_deleted": true}},
					map[string]interface{}{"range": map[string]interface{}{"_lsn": map[string]interface{}{"lt": s.SafeHorizon}}},
				},
			},
		},
	}
	body, err := json.Marshal(query)
	if err != nil {
		return fmt.Errorf("sweep: marshal query: %w", err)
	}
	req := esapi.DeleteByQueryRequest{Index: []string{s.Index}, Body: bytes.NewReader(body)}
	res, err := req.Do(ctx, s.Client)
	if err != nil {
		return fmt.Errorf("sweep: delete_by_query: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("sweep: delete_by_query status %s", res.Status())
	}
	slog.Info("sweep complete", "index", s.Index, "safeHorizon", s.SafeHorizon)
	return nil
}
