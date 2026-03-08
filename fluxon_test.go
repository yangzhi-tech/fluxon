package fluxon

import (
	"encoding/json"
	"testing"
	"time"
)

// ---- Row tests ----

func TestRowStringPresent(t *testing.T) {
	r := Row{"name": "alice"}
	if got := r.String("name"); got != "alice" {
		t.Errorf("got %q, want %q", got, "alice")
	}
}

func TestRowStringMissing(t *testing.T) {
	r := Row{}
	if got := r.String("missing"); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestRowIntFromFloat64(t *testing.T) {
	r := Row{"n": float64(42)}
	if got := r.Int("n"); got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestRowIntMissing(t *testing.T) {
	r := Row{}
	if got := r.Int("x"); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}

func TestRowBoolPresent(t *testing.T) {
	r := Row{"active": true}
	if !r.Bool("active") {
		t.Error("expected true")
	}
}

func TestRowBoolMissing(t *testing.T) {
	r := Row{}
	if r.Bool("missing") {
		t.Error("expected false")
	}
}

func TestRowTimeEpochMs(t *testing.T) {
	ms := int64(1700000000000)
	r := Row{"ts": float64(ms)}
	got := r.Time("ts")
	want := time.UnixMilli(ms).UTC()
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRowJSONValidJSONB(t *testing.T) {
	r := Row{"profile": `{"age":30,"city":"NYC"}`}
	got := r.JSON("profile")
	m, ok := got.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	if m["city"] != "NYC" {
		t.Errorf("unexpected value: %v", m["city"])
	}
}

func TestRowJSONPlainString(t *testing.T) {
	r := Row{"name": "not-json"}
	got := r.JSON("name")
	if got != "not-json" {
		t.Errorf("expected plain string, got %v", got)
	}
}

func TestRowRawNil(t *testing.T) {
	r := Row{"x": nil}
	if r.Raw("x") != nil {
		t.Error("expected nil")
	}
}

func TestRowRawMissing(t *testing.T) {
	r := Row{}
	if r.Raw("x") != nil {
		t.Error("expected nil for missing key")
	}
}

// ---- Action tests ----

func TestIndex(t *testing.T) {
	doc := Doc{"email": "a@b.com"}
	a := Index("users", "u1", 100, doc)
	if a.OpType != "index" {
		t.Errorf("opType=%q", a.OpType)
	}
	if a.Index != "users" || a.ID != "u1" || a.LSN != 100 {
		t.Error("field mismatch")
	}
	if a.Doc["email"] != "a@b.com" {
		t.Error("doc mismatch")
	}
}

func TestSoftDelete(t *testing.T) {
	a := SoftDelete("orders", "o1", 200)
	if a.OpType != "delete" {
		t.Errorf("opType=%q", a.OpType)
	}
	if a.Index != "orders" || a.ID != "o1" || a.LSN != 200 {
		t.Error("field mismatch")
	}
	if a.Doc != nil {
		t.Error("SoftDelete doc should be nil")
	}
}

// ---- HandlerFunc adapter ----

func TestHandlerFunc(t *testing.T) {
	called := false
	var h Handler = HandlerFunc(func(e *Event) (*Action, error) {
		called = true
		return nil, nil
	})
	h.Handle(&Event{})
	if !called {
		t.Error("handler was not called")
	}
}

// ---- Row.Float ----

func TestRowFloat(t *testing.T) {
	r := Row{"total": float64(3.14)}
	if got := r.Float("total"); got != 3.14 {
		t.Errorf("got %f", got)
	}
}

// ---- Row.Int from json.Number ----

func TestRowIntFromJSONNumber(t *testing.T) {
	r := Row{"n": json.Number("99")}
	if got := r.Int("n"); got != 99 {
		t.Errorf("got %d, want 99", got)
	}
}
