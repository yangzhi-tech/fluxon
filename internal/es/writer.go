// Package es provides an Elasticsearch bulk writer and soft-delete sweeper.
package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/dropbox/fluxon/pkg/types"
)

const softDeleteScript = `if (ctx._source._lsn == null || params.lsn > ctx._source._lsn) { ctx._source._deleted = true; ctx._source._lsn = params.lsn; } else { ctx.op = 'none'; }`

// Writer sends bulk actions to Elasticsearch with exponential-backoff retry.
type Writer struct {
	Client     *elasticsearch.Client
	MaxRetries int
	Backoff    time.Duration
}

// NewWriter creates a Writer from ESConfig.
func NewWriter(cfg types.ESConfig) (*Writer, error) {
	c, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: cfg.Addresses})
	if err != nil {
		return nil, err
	}
	maxRetries := cfg.MaxRetries
	if maxRetries == 0 {
		maxRetries = 5
	}
	backoffMs := cfg.RetryBackoffMs
	if backoffMs == 0 {
		backoffMs = 200
	}
	return &Writer{
		Client:     c,
		MaxRetries: maxRetries,
		Backoff:    time.Duration(backoffMs) * time.Millisecond,
	}, nil
}

// Flush sends actions as a single _bulk request, retrying on failure.
func (w *Writer) Flush(ctx context.Context, actions []*types.Action) error {
	if len(actions) == 0 {
		return nil
	}
	body, err := BuildBulkBody(actions)
	if err != nil {
		return fmt.Errorf("es: build bulk body: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= w.MaxRetries; attempt++ {
		if attempt > 0 {
			wait := w.Backoff * (1 << (attempt - 1))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		req := esapi.BulkRequest{Body: bytes.NewReader(body)}
		res, err := req.Do(ctx, w.Client)
		if err != nil {
			lastErr = fmt.Errorf("es: bulk request: %w", err)
			slog.Warn("es bulk request failed, retrying", "attempt", attempt, "err", err)
			continue
		}
		defer res.Body.Close()

		if res.StatusCode >= 500 {
			b, _ := io.ReadAll(res.Body)
			lastErr = fmt.Errorf("es: server error %d: %s", res.StatusCode, string(b))
			slog.Warn("es bulk 5xx, retrying", "attempt", attempt, "status", res.StatusCode)
			continue
		}

		if err := parseBulkResponse(res.Body); err != nil {
			lastErr = err
			slog.Warn("es bulk per-item error, retrying", "attempt", attempt, "err", err)
			continue
		}
		return nil
	}
	return fmt.Errorf("es: retries exhausted: %w", lastErr)
}

// BuildBulkBody serialises actions into NDJSON bulk format.
func BuildBulkBody(actions []*types.Action) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)

	for _, a := range actions {
		switch a.OpType {
		case "index":
			meta := map[string]interface{}{
				"index": map[string]interface{}{
					"_index": a.Index, "_id": a.ID,
					"version": a.LSN, "version_type": "external",
				},
			}
			if err := enc.Encode(meta); err != nil {
				return nil, err
			}
			doc := make(map[string]interface{}, len(a.Doc)+1)
			for k, v := range a.Doc {
				doc[k] = v
			}
			doc["_lsn"] = a.LSN
			if err := enc.Encode(doc); err != nil {
				return nil, err
			}

		case "delete":
			meta := map[string]interface{}{
				"update": map[string]interface{}{"_index": a.Index, "_id": a.ID},
			}
			if err := enc.Encode(meta); err != nil {
				return nil, err
			}
			body := map[string]interface{}{
				"scripted_upsert": true,
				"script": map[string]interface{}{
					"source": softDeleteScript, "lang": "painless",
					"params": map[string]interface{}{"lsn": a.LSN},
				},
				"upsert": map[string]interface{}{"_lsn": a.LSN, "_deleted": true},
			}
			if err := enc.Encode(body); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

type bulkResponse struct {
	Errors bool                   `json:"errors"`
	Items  []map[string]bulkItem `json:"items"`
}

type bulkItem struct {
	Status int `json:"status"`
	Error  *struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

func parseBulkResponse(body io.Reader) error {
	var resp bulkResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return fmt.Errorf("es: decode bulk response: %w", err)
	}
	if !resp.Errors {
		return nil
	}
	var msgs []string
	for _, item := range resp.Items {
		for _, bi := range item {
			if bi.Status == http.StatusConflict {
				continue
			}
			if bi.Error != nil {
				msgs = append(msgs, fmt.Sprintf("[%d] %s: %s", bi.Status, bi.Error.Type, bi.Error.Reason))
			}
		}
	}
	if len(msgs) > 0 {
		return fmt.Errorf("es: bulk item errors: %s", strings.Join(msgs, "; "))
	}
	return nil
}
