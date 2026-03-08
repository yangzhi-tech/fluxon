package cdc

import (
	"testing"

	"github.com/dropbox/fluxon/pkg/types"
)

func TestParseInsert(t *testing.T) {
	payload := []byte(`{"before":null,"after":{"id":1,"name":"alice"},"source":{"lsn":12345,"table":"users","schema":"public"},"op":"c"}`)
	e, err := Parse(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != types.OpInsert {
		t.Errorf("op=%q", e.Op)
	}
	if e.Before != nil {
		t.Error("Before should be nil for insert")
	}
	if e.After == nil {
		t.Fatal("After should not be nil")
	}
	if e.After.String("name") != "alice" {
		t.Errorf("name=%q", e.After.String("name"))
	}
	if e.Source.LSN != 12345 {
		t.Errorf("lsn=%d", e.Source.LSN)
	}
}

func TestParseUpdate(t *testing.T) {
	payload := []byte(`{"before":{"id":1,"name":"alice"},"after":{"id":1,"name":"bob"},"source":{"lsn":99,"table":"users","schema":"public"},"op":"u"}`)
	e, err := Parse(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != types.OpUpdate {
		t.Errorf("op=%q", e.Op)
	}
	if e.Before == nil || e.After == nil {
		t.Error("both Before and After should be set for update")
	}
}

func TestParseDelete(t *testing.T) {
	payload := []byte(`{"before":{"id":2},"after":null,"source":{"lsn":500,"table":"orders","schema":"public"},"op":"d"}`)
	e, err := Parse(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != types.OpDelete {
		t.Errorf("op=%q", e.Op)
	}
	if e.After != nil {
		t.Error("After should be nil for delete")
	}
}

func TestParseSnapshot(t *testing.T) {
	payload := []byte(`{"before":null,"after":{"id":3},"source":{"lsn":1,"table":"users","schema":"public"},"op":"r"}`)
	e, err := Parse(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Op != types.OpRead {
		t.Errorf("op=%q", e.Op)
	}
}

func TestParseMalformedJSON(t *testing.T) {
	if _, err := Parse([]byte(`{bad`)); err == nil {
		t.Error("expected error")
	}
}

func TestParseMissingOp(t *testing.T) {
	payload := []byte(`{"before":null,"after":{},"source":{"lsn":1,"table":"t","schema":"s"}}`)
	if _, err := Parse(payload); err == nil {
		t.Error("expected error for missing op")
	}
}
