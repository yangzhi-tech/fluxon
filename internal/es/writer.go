// Package es provides an Elasticsearch bulk writer with retry logic.
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

	"github.com/dropbox/fluxon"
)

const softDeleteScript = `if (ctx._source._lsn == null || params.lsn > ctx._source._lsn) { ctx._source._deleted = true; ctx._source._lsn = params.lsn; } else { ctx.op = 'none'; }`

// Writer sends bulk actions to Elasticsearch with exponential-backoff retry.
type Writer struct {
	client     *elasticsearch.Client
	maxRetries int
	backoff    time.Duration
}

// NewWriter creates a Writer from the given config.
func NewWriter(cfg fluxon.ESConfig) (*Writer, error) {
	c, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: cfg.Addresses,
	})
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
		client:     c,
		maxRetries: maxRetries,
		backoff:    time.Duration(backoffMs) * time.Millisecond,
	}, nil
}

// NewWriterWithClient creates a Writer using a pre-built ES client (useful in tests).
func NewWriterWithClient(client *elasticsearch.Client, maxRetries int, backoffMs int) *Writer {
	if maxRetries == 0 {
		maxRetries = 5
	}
	if backoffMs == 0 {
		backoffMs = 200
	}
	return &Writer{
		client:     client,
		maxRetries: maxRetries,
		backoff:    time.Duration(backoffMs) * time.Millisecond,
	}
}

// Flush sends actions to ES as a single _bulk request, retrying on failure.
func (w *Writer) Flush(ctx context.Context, actions []*fluxon.Action) error {
	if len(actions) == 0 {
		return nil
	}

	body, err := buildBulkBody(actions)
	if err != nil {
		return fmt.Errorf("es: build bulk body: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= w.maxRetries; attempt++ {
		if attempt > 0 {
			wait := w.backoff * (1 << (attempt - 1))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		req := esapi.BulkRequest{Body: bytes.NewReader(body)}
		res, err := req.Do(ctx, w.client)
		if err != nil {
			lastErr = fmt.Errorf("es: bulk request: %w", err)
			slog.Warn("es bulk request failed, retrying", "attempt", attempt, "err", err)
			continue
		}
		defer res.Body.Close()

		if res.StatusCode >= 500 {
			bodyBytes, _ := io.ReadAll(res.Body)
			lastErr = fmt.Errorf("es: server error %d: %s", res.StatusCode, string(bodyBytes))
			slog.Warn("es bulk 5xx, retrying", "attempt", attempt, "status", res.StatusCode)
			continue
		}

		// Parse bulk response to check per-item errors.
		if err := parseBulkResponse(res.Body); err != nil {
			lastErr = err
			slog.Warn("es bulk per-item error, retrying", "attempt", attempt, "err", err)
			continue
		}

		return nil // success
	}

	return fmt.Errorf("es: retries exhausted: %w", lastErr)
}

// buildBulkBody serialises actions into NDJSON bulk format.
func buildBulkBody(actions []*fluxon.Action) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)

	for _, a := range actions {
		switch a.OpType {
		case "index":
			meta := map[string]interface{}{
				"index": map[string]interface{}{
					"_index":       a.Index,
					"_id":          a.ID,
					"version":      a.LSN,
					"version_type": "external",
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
				"update": map[string]interface{}{
					"_index": a.Index,
					"_id":    a.ID,
				},
			}
			if err := enc.Encode(meta); err != nil {
				return nil, err
			}
			body := map[string]interface{}{
				"scripted_upsert": true,
				"script": map[string]interface{}{
					"source": softDeleteScript,
					"lang":   "painless",
					"params": map[string]interface{}{"lsn": a.LSN},
				},
				"upsert": map[string]interface{}{
					"_lsn":     a.LSN,
					"_deleted": true,
				},
			}
			if err := enc.Encode(body); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

// bulkResponse is the top-level ES _bulk response.
type bulkResponse struct {
	Errors bool `json:"errors"`
	Items  []map[string]bulkItem `json:"items"`
}

type bulkItem struct {
	Status int `json:"status"`
	Error  *struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

// parseBulkResponse reads the ES bulk response and returns an error if any
// non-409 item error is present.
func parseBulkResponse(body io.Reader) error {
	var resp bulkResponse
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return fmt.Errorf("es: decode bulk response: %w", err)
	}
	if !resp.Errors {
		return nil
	}

	var errMsgs []string
	for _, item := range resp.Items {
		for _, bi := range item {
			if bi.Status == http.StatusConflict { // 409 — version conflict, treat as success
				continue
			}
			if bi.Error != nil {
				errMsgs = append(errMsgs, fmt.Sprintf("[%d] %s: %s", bi.Status, bi.Error.Type, bi.Error.Reason))
			}
		}
	}
	if len(errMsgs) > 0 {
		return fmt.Errorf("es: bulk item errors: %s", strings.Join(errMsgs, "; "))
	}
	return nil
}
