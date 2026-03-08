package fluxon

import (
	"testing"
)

func TestParseCDCInsert(t *testing.T) {
	payload := []byte(`{
		"before": null,
		"after": {"id": 1, "name": "alice"},
		"source": {"lsn": 12345, "table": "users", "schema": "public"},
		"op": "c"
	}`)
	e, err := parseCDC(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != OpInsert {
		t.Errorf("op=%q", e.Op)
	}
	if e.Before != nil {
		t.Error("Before should be nil for insert")
	}
	if e.After == nil {
		t.Fatal("After should not be nil for insert")
	}
	if e.After.String("name") != "alice" {
		t.Errorf("name=%q", e.After.String("name"))
	}
	if e.Source.LSN != 12345 {
		t.Errorf("lsn=%d", e.Source.LSN)
	}
}

func TestParseCDCUpdate(t *testing.T) {
	payload := []byte(`{
		"before": {"id": 1, "name": "alice"},
		"after":  {"id": 1, "name": "bob"},
		"source": {"lsn": 99, "table": "users", "schema": "public"},
		"op": "u"
	}`)
	e, err := parseCDC(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != OpUpdate {
		t.Errorf("op=%q", e.Op)
	}
	if e.Before == nil || e.After == nil {
		t.Error("both Before and After should be set for update")
	}
}

func TestParseCDCDelete(t *testing.T) {
	payload := []byte(`{
		"before": {"id": 2},
		"after": null,
		"source": {"lsn": 500, "table": "orders", "schema": "public"},
		"op": "d"
	}`)
	e, err := parseCDC(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != OpDelete {
		t.Errorf("op=%q", e.Op)
	}
	if e.After != nil {
		t.Error("After should be nil for delete")
	}
}

func TestParseCDCSnapshot(t *testing.T) {
	payload := []byte(`{
		"before": null,
		"after": {"id": 3},
		"source": {"lsn": 1, "table": "users", "schema": "public"},
		"op": "r"
	}`)
	e, err := parseCDC(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != OpRead {
		t.Errorf("op=%q", e.Op)
	}
}

func TestParseCDCMalformedJSON(t *testing.T) {
	_, err := parseCDC([]byte(`{bad json`))
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func TestParseCDCMissingOp(t *testing.T) {
	payload := []byte(`{"before": null, "after": {}, "source": {"lsn": 1, "table": "t", "schema": "s"}}`)
	_, err := parseCDC(payload)
	if err == nil {
		t.Error("expected error for missing op")
	}
}
