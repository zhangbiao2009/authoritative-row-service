package fakedb

import (
	"context"
	"errors"
	"fmt"
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
	d.Hook = func(context.Context, *Stmt) error { return boom }
	if _, err := d.Select(context.Background(), "k"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if n := d.Selects(); n != 1 {
		t.Fatalf("Selects = %d, want 1", n)
	}
}

func TestHookSeesEveryStatement(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 5, Ver: 2, Vals: []any{int64(0)}})
	var seen []Stmt
	d.Hook = func(_ context.Context, st *Stmt) error {
		seen = append(seen, Stmt{Kind: st.Kind, Key: st.Key, Gen: st.Gen, Ver: st.Ver, Session: st.Session})
		return nil
	}
	ctx := context.Background()
	if r, err := d.Select(ctx, "k"); err != nil || r.Ver != 2 {
		t.Fatalf("Select = %+v, %v", r, err)
	}
	if err := d.Update(ctx, &schema.Row{Key: "k", Gen: 5, Ver: 3, Vals: []any{int64(9)}}, 1); err != nil || d.Row("k").Ver != 3 {
		t.Fatalf("Update: err %v, row %+v", err, d.Row("k"))
	}
	if len(seen) != 2 {
		t.Fatalf("seen %+v, want 2 statements", seen)
	}
	s1, s2 := seen[0].Session, seen[1].Session
	want := []Stmt{{Kind: Select, Key: "k", Session: s1}, {Kind: Update, Key: "k", Gen: 5, Ver: 3, Session: s2}}
	if !reflect.DeepEqual(seen, want) || s1 == 0 || s2 == 0 || s1 == s2 {
		t.Fatalf("seen %+v, want %+v with distinct non-zero sessions", seen, want)
	}
}

func TestHookErrorWithoutExecDoesNotExecute(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1, Ver: 1, Vals: []any{int64(0)}})
	d.Hook = func(context.Context, *Stmt) error { return fmt.Errorf("%w", db.ErrNotExecuted) }
	err := d.Update(context.Background(), &schema.Row{Key: "k", Gen: 1, Ver: 2, Vals: []any{int64(7)}}, 1)
	if !errors.Is(err, db.ErrNotExecuted) || d.Row("k").Ver != 1 {
		t.Fatalf("err %v, row %+v; want ErrNotExecuted and the row unchanged", err, d.Row("k"))
	}
}

func TestNoHookCancelledCtxIsNotExecuted(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1, Ver: 1, Vals: []any{int64(0)}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := d.Update(ctx, &schema.Row{Key: "k", Gen: 1, Ver: 2, Vals: []any{int64(7)}}, 1)
	if !db.NotExecuted(err) || !errors.Is(err, context.Canceled) || d.Row("k").Ver != 1 {
		t.Fatalf("err %v, row %+v; want not executed, Canceled, the row unchanged", err, d.Row("k"))
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
	d.Hook = func(_ context.Context, st *Stmt) error {
		late = st.Exec
		return Ambiguous(st, context.DeadlineExceeded)
	}
	err := d.Update(context.Background(), &schema.Row{Key: "k", Gen: 1, Ver: 2, Vals: []any{int64(7)}}, 1)
	if !db.IsAmbiguous(err) || !errors.Is(err, context.DeadlineExceeded) || d.Row("k").Ver != 1 {
		t.Fatalf("err %v, ver %d; want an ambiguous DeadlineExceeded and the row unchanged", err, d.Row("k").Ver)
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
	d.Hook = func(_ context.Context, st *Stmt) error {
		entered <- struct{}{}
		<-release
		st.Exec()
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
