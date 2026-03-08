package fluxon

import (
	"encoding/json"
	"fmt"
)

// rawEvent mirrors the Debezium JSON envelope (never exposed to users).
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

// parseCDC deserialises a Debezium JSON payload into an *Event.
func parseCDC(data []byte) (*Event, error) {
	var raw rawEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("cdc: unmarshal: %w", err)
	}
	if raw.Op == "" {
		return nil, fmt.Errorf("cdc: missing op field")
	}

	e := &Event{
		Op: Op(raw.Op),
		Source: Source{
			LSN:    raw.Source.LSN,
			Table:  raw.Source.Table,
			Schema: raw.Source.Schema,
		},
	}
	if raw.Before != nil {
		e.Before = Row(raw.Before)
	}
	if raw.After != nil {
		e.After = Row(raw.After)
	}
	return e, nil
}
