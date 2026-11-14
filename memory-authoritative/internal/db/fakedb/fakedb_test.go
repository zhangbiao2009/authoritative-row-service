package fakedb

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"rowsvc/internal/db"
	"rowsvc/internal/schema"
)

var _ db.DB = (*DB)(nil)

func TestSelect(t *testing.T) {
	d := New()
	ctx := context.Background()
	if _, err := d.Select(ctx, "k"); !errors.Is(err, db.ErrNoRows) {
		t.Fatalf("Select of missing key: err = %v, want ErrNoRows", err)
	}
	want := &schema.Row{Key: "k", Gen: 5, Ver: 2, Vals: []any{int64(1), "a"}}
	d.Put(want)
	r, err := d.Select(ctx, "k")
	if err != nil || !reflect.DeepEqual(r, want) {
		t.Fatalf("Select = %+v, %v; want %+v", r, err, want)
	}
	r.Vals[1] = "changed"
	if r2, _ := d.Select(ctx, "k"); r2.Vals[1] != "a" {
		t.Fatalf("modifying a returned row changed the stored row: %v", r2.Vals[1])
	}
	if n := d.Selects(); n != 3 {
		t.Fatalf("Selects = %d, want 3", n)
	}
}

func TestSelectHook(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1})
	boom := errors.New("boom")
	d.SelectHook = func(context.Context, schema.Key) error { return boom }
	if _, err := d.Select(context.Background(), "k"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if n := d.Selects(); n != 1 {
		t.Fatalf("Selects = %d, want 1", n)
	}
}

func TestUpdateIsConditional(t *testing.T) {
	ctx := context.Background()
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 5, Ver: 2, Vals: []any{int64(1), "a", int64(10)}})
	write := func(gen, ver int64, mask uint64, vals ...any) *schema.Row {
		t.Helper()
		if err := d.Update(ctx, &schema.Row{Key: "k", Gen: gen, Ver: ver, Vals: vals}, mask); err != nil {
			t.Fatal(err)
		}
		return d.Row("k")
	}
	// only the masked column and ver change
	got := write(5, 3, schema.ColBit(2), int64(1), "zzz", int64(11))
	want := &schema.Row{Key: "k", Gen: 5, Ver: 3, Vals: []any{int64(1), "a", int64(11)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after update: %+v, want %+v", got, want)
	}
	// another gen, an equal ver, an older ver: no effect
	for _, c := range []struct{ gen, ver int64 }{{6, 9}, {5, 3}, {5, 2}} {
		if got := write(c.gen, c.ver, schema.ColBit(2), int64(1), "a", int64(99)); !reflect.DeepEqual(got, want) {
			t.Fatalf("gen %d ver %d: %+v, want unchanged %+v", c.gen, c.ver, got, want)
		}
	}
	// a missing row is not inserted
	if err := d.Update(ctx, &schema.Row{Key: "other", Gen: 1, Ver: 1, Vals: []any{int64(2), "b", int64(0)}}, 1); err != nil || d.Row("other") != nil {
		t.Fatalf("update of a missing row: err %v, row %+v; want nil, nil", err, d.Row("other"))
	}
	if d.Updates() != 5 || d.Violation() != nil {
		t.Fatalf("Updates = %d, Violation = %v; want 5, nil", d.Updates(), d.Violation())
	}
}

func TestUpdateHookLateExec(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1, Ver: 1, Vals: []any{int64(0)}})
	var late func()
	d.UpdateHook = func(_ context.Context, _ schema.Key, exec func()) error {
		late = exec
		return context.DeadlineExceeded
	}
	err := d.Update(context.Background(), &schema.Row{Key: "k", Gen: 1, Ver: 2, Vals: []any{int64(7)}}, 1)
	if !errors.Is(err, context.DeadlineExceeded) || d.Row("k").Ver != 1 {
		t.Fatalf("err %v, ver %d; want DeadlineExceeded and the row unchanged", err, d.Row("k").Ver)
	}
	late()
	if r := d.Row("k"); r.Ver != 2 || r.Vals[0] != int64(7) {
		t.Fatalf("after the late exec: %+v", r)
	}
}

func TestOneWriterPerKeyIsAsserted(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1, Ver: 1, Vals: []any{int64(0)}})
	entered, release := make(chan struct{}), make(chan struct{})
	d.UpdateHook = func(_ context.Context, _ schema.Key, exec func()) error {
		entered <- struct{}{}
		<-release
		exec()
		return nil
	}
	var wg sync.WaitGroup
	for v := int64(2); v <= 3; v++ {
		wg.Go(func() { d.Update(context.Background(), &schema.Row{Key: "k", Gen: 1, Ver: v, Vals: []any{v}}, 1) })
	}
	<-entered
	<-entered // both calls are in progress at once
	close(release)
	wg.Wait()
	if d.Violation() == nil {
		t.Fatal("two concurrent writes of one key were not reported")
	}
}
