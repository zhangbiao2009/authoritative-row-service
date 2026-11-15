package fakedb

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
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

func row(gen int64, name string) *schema.Row {
	return &schema.Row{Key: "k", Gen: gen, Ver: 4, Vals: []any{int64(1), name, int64(10)}}
}

func TestUpdateIsConditional(t *testing.T) {
	d := New()
	ctx := context.Background()
	if _, err := d.Update(ctx, row(5, "x"), []int{1}); !errors.Is(err, db.ErrNoRows) {
		t.Fatalf("Update of a missing row: err = %v, want ErrNoRows", err)
	}
	d.Put(row(5, "a"))
	if _, err := d.Update(ctx, row(6, "x"), []int{1}); !errors.Is(err, db.ErrNoRows) {
		t.Fatalf("Update of another gen: err = %v, want ErrNoRows", err)
	}
	w := row(5, "x")
	w.Ver = 99            // ignored: the DB sets ver
	w.Vals[2] = int64(77) // not in cols: not written
	got, err := d.Update(ctx, w, []int{1})
	want := &schema.Row{Key: "k", Gen: 5, Ver: 5, Vals: []any{int64(1), "x", int64(10)}}
	if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(d.Row("k"), want) {
		t.Fatalf("Update = %+v, %v; stored %+v; want %+v", got, err, d.Row("k"), want)
	}
	got.Vals[1] = "changed"
	if d.Row("k").Vals[1] != "x" {
		t.Fatal("modifying the returned row changed the stored row")
	}
	if n := d.Updates(); n != 3 {
		t.Fatalf("Updates = %d, want 3", n)
	}
}

func TestUpdateNormalizes(t *testing.T) {
	d := New()
	d.Normalize = func(col int, v any) any {
		if s, ok := v.(string); ok {
			return strings.ToUpper(s)
		}
		return v
	}
	d.Put(row(5, "a"))
	got, err := d.Update(context.Background(), row(5, "x"), []int{1})
	if err != nil || got.Vals[1] != "X" || d.Row("k").Vals[1] != "X" {
		t.Fatalf("Update = %+v, %v; want the normalized value X returned and stored", got, err)
	}
}

func TestUpdateHookAndDoneContext(t *testing.T) {
	d := New()
	d.Put(row(5, "a"))
	boom := errors.New("boom")
	var seen Stmt
	d.Hook = func(_ context.Context, st *Stmt) error { seen = *st; return boom }
	if _, err := d.Update(context.Background(), row(5, "x"), []int{1}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if seen != (Stmt{Kind: Update, Key: "k", Gen: 5}) {
		t.Fatalf("hook saw %+v", seen)
	}
	d.Hook = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Update(ctx, row(5, "x"), []int{1}); !errors.Is(err, db.ErrNotExecuted) {
		t.Fatalf("done ctx: err = %v, want ErrNotExecuted", err)
	}
	if d.Row("k").Vals[1] != "a" || d.Row("k").Ver != 4 {
		t.Fatalf("a statement that was not executed changed the row: %+v", d.Row("k"))
	}
}

func TestOneWriterPerKeyIsAsserted(t *testing.T) {
	d := New()
	d.Put(row(5, "a"))
	inside, release := make(chan struct{}), make(chan struct{})
	d.Hook = func(context.Context, *Stmt) error { inside <- struct{}{}; <-release; return nil }
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { d.Update(context.Background(), row(5, "x"), []int{1}) })
	}
	<-inside
	<-inside // two Update calls of one key are in progress at once
	close(release)
	wg.Wait()
	if d.Violation() == nil {
		t.Fatal("two concurrent writes of one key were not reported")
	}
}
