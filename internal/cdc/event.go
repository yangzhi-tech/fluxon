// Package cdc parses Debezium CDC wire-format JSON into types.Event values.
package cdc

import (
	"encoding/json"
	"fmt"

	"github.com/dropbox/fluxon/pkg/types"
)

type rawEvent struct {
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
	Source struct {
		LSN    int64  `json:"lsn"`
		Table  string `json:"table"`
		Schema string `json:"schema"`
	} `json:"source"`
	Op string `json:"op"`
}

// Parse deserialises a Debezium JSON payload into a *types.Event.
func Parse(data []byte) (*types.Event, error) {
	var raw rawEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("cdc: unmarshal: %w", err)
	}
	if raw.Op == "" {
		return nil, fmt.Errorf("cdc: missing op field")
	}
	e := &types.Event{
		Op: types.Op(raw.Op),
		Source: types.Source{
			LSN:    raw.Source.LSN,
			Table:  raw.Source.Table,
			Schema: raw.Source.Schema,
		},
	}
	if raw.Before != nil {
		e.Before = types.Row(raw.Before)
	}
	if raw.After != nil {
		e.After = types.Row(raw.After)
	}
	return e, nil
}
