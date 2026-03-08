package cdc

import (
	"testing"

	"github.com/dropbox/fluxon"
)

func TestParseInsert(t *testing.T) {
	payload := []byte(`{
		"before": null,
		"after": {"id": 1, "name": "alice"},
		"source": {"lsn": 12345, "table": "users", "schema": "public"},
		"op": "c"
	}`)
	e, err := Parse(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != fluxon.OpInsert {
		t.Errorf("op=%q, want %q", e.Op, fluxon.OpInsert)
	}
	if e.Before != nil {
		t.Error("Before should be nil for insert")
	}
	if e.After == nil {
		t.Fatal("After should not be nil for insert")
	}
	if e.After.String("name") != "alice" {
		t.Errorf("unexpected name: %s", e.After.String("name"))
	}
	if e.Source.LSN != 12345 {
		t.Errorf("lsn=%d, want 12345", e.Source.LSN)
	}
	if e.Source.Table != "users" {
		t.Errorf("table=%q", e.Source.Table)
	}
}

func TestParseUpdate(t *testing.T) {
	payload := []byte(`{
		"before": {"id": 1, "name": "alice"},
		"after":  {"id": 1, "name": "bob"},
		"source": {"lsn": 99, "table": "users", "schema": "public"},
		"op": "u"
	}`)
	e, err := Parse(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != fluxon.OpUpdate {
		t.Errorf("op=%q", e.Op)
	}
	if e.Before == nil || e.After == nil {
		t.Error("both Before and After should be set for update")
	}
	if e.Before.String("name") != "alice" {
		t.Errorf("before name=%q", e.Before.String("name"))
	}
	if e.After.String("name") != "bob" {
		t.Errorf("after name=%q", e.After.String("name"))
	}
}

func TestParseDelete(t *testing.T) {
	payload := []byte(`{
		"before": {"id": 2, "name": "charlie"},
		"after": null,
		"source": {"lsn": 500, "table": "orders", "schema": "public"},
		"op": "d"
	}`)
	e, err := Parse(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != fluxon.OpDelete {
		t.Errorf("op=%q", e.Op)
	}
	if e.Before == nil {
		t.Error("Before should be set for delete")
	}
	if e.After != nil {
		t.Error("After should be nil for delete")
	}
}

func TestParseSnapshot(t *testing.T) {
	payload := []byte(`{
		"before": null,
		"after": {"id": 3},
		"source": {"lsn": 1, "table": "users", "schema": "public"},
		"op": "r"
	}`)
	e, err := Parse(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != fluxon.OpRead {
		t.Errorf("op=%q", e.Op)
	}
	if e.After == nil {
		t.Error("After should be set for snapshot")
	}
}

func TestParseLSN(t *testing.T) {
	payload := []byte(`{
		"before": null,
		"after": {},
		"source": {"lsn": 9999999999, "table": "t", "schema": "s"},
		"op": "c"
	}`)
	e, err := Parse(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Source.LSN != 9999999999 {
		t.Errorf("lsn=%d", e.Source.LSN)
	}
}

func TestParseMalformedJSON(t *testing.T) {
	_, err := Parse([]byte(`{bad json`))
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func TestParseMissingOp(t *testing.T) {
	payload := []byte(`{
		"before": null,
		"after": {},
		"source": {"lsn": 1, "table": "t", "schema": "s"}
	}`)
	_, err := Parse(payload)
	if err == nil {
		t.Error("expected error for missing op")
	}
}
