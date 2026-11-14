package fakedb

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"rowcache/internal/db"
	"rowcache/internal/schema"
)

var _ db.DB = (*DB)(nil)

func TestSelect(t *testing.T) {
	d := New()
	ctx := context.Background()
	if _, err := d.Select(ctx, "k"); !errors.Is(err, db.ErrNoRows) {
		t.Fatalf("Select of missing key: err = %v, want ErrNoRows", err)
	}
	d.Put(&schema.Row{Key: "k", Gen: 5, Ver: 2, Vals: []any{int64(1), "a"}})
	r, err := d.Select(ctx, "k")
	want := &schema.Row{Key: "k", Gen: 5, Ver: 2, Vals: []any{int64(1), "a"}}
	if err != nil || !reflect.DeepEqual(r, want) {
		t.Fatalf("Select = %+v, %v; want %+v", r, err, want)
	}
	r.Vals[1] = "changed"
	if r2 := d.Row("k"); r2.Vals[1] != "a" {
		t.Fatalf("modifying a returned row changed the stored row: %v", r2.Vals[1])
	}
	if n := d.Selects(); n != 2 {
		t.Fatalf("Selects = %d, want 2", n)
	}
}

func TestSelectHook(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1})
	boom := errors.New("boom")
	var seen Stmt
	d.Hook = func(_ context.Context, st *Stmt) error { seen = *st; return boom }
	if _, err := d.Select(context.Background(), "k"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if seen != (Stmt{Kind: Select, Key: "k"}) {
		t.Fatalf("hook saw %+v", seen)
	}
	if n := d.Selects(); n != 1 {
		t.Fatalf("Selects = %d, want 1", n)
	}
}

func TestSelectWithDoneContext(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := d.Select(ctx, "k")
	if !errors.Is(err, db.ErrNotExecuted) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want ErrNotExecuted wrapping context.Canceled", err)
	}
}
