package fakedb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
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

func row(gen int64, name string) *schema.Row {
	return &schema.Row{Key: "k", Gen: gen, Vals: []any{int64(1), name}}
}

// lateInsert sends an Insert of row(gen, "a") whose caller gives up; it returns the statement's Exec.
func lateInsert(t *testing.T, d *DB, gen int64) (exec func(), session db.SessionID) {
	t.Helper()
	d.Hook = func(_ context.Context, st *Stmt) error {
		exec, session = st.Exec, st.Session
		return Ambiguous(st, context.DeadlineExceeded)
	}
	defer func() { d.Hook = nil }()
	if _, err := d.InsertDedup(context.Background(), row(gen, "a")); !db.IsAmbiguous(err) {
		t.Fatalf("late insert: err %v, want ambiguous", err)
	}
	return exec, session
}

func TestInsertDedup(t *testing.T) {
	ctx := context.Background()
	d := New()
	got, err := d.InsertDedup(ctx, row(100, "a"))
	if err != nil || !reflect.DeepEqual(got, row(100, "a")) || !reflect.DeepEqual(d.Row("k"), got) || !d.Claimed("k", 100) {
		t.Fatalf("insert = %+v, %v; stored %+v; claimed %v", got, err, d.Row("k"), d.Claimed("k", 100))
	}
	if _, err := d.InsertDedup(ctx, row(100, "b")); !errors.Is(err, db.ErrGenClaimed) || d.Row("k").Vals[1] != "a" {
		t.Fatalf("same gen again: err %v, row %+v; want ErrGenClaimed, unchanged", err, d.Row("k"))
	}
	if _, err := d.InsertDedup(ctx, row(200, "b")); !errors.Is(err, db.ErrDuplicate) || !d.Claimed("k", 200) || d.Row("k").Gen != 100 {
		t.Fatalf("other gen on an existing row: err %v, claimed %v, row %+v; want ErrDuplicate, the gen burned, row gen 100",
			err, d.Claimed("k", 200), d.Row("k"))
	}
	if s := d.Stats(); s.GenClaimed != 1 {
		t.Fatalf("Stats %+v, want GenClaimed 1", s)
	}
}

func TestDeleteKeepsDedupAndChecksGen(t *testing.T) {
	ctx := context.Background()
	d := New()
	if _, err := d.InsertDedup(ctx, row(100, "a")); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete(ctx, "k", 999); err != nil || d.Row("k") == nil {
		t.Fatalf("delete of another gen: err %v, row %+v; want nil and the row kept", err, d.Row("k"))
	}
	if err := d.Delete(ctx, "k", 100); err != nil || d.Row("k") != nil {
		t.Fatalf("delete: err %v, row %+v; want nil, no row", err, d.Row("k"))
	}
	if !d.Claimed("k", 100) {
		t.Fatal("Delete removed the dedup record (I14)")
	}
	if _, err := d.InsertDedup(ctx, row(100, "a")); !errors.Is(err, db.ErrGenClaimed) || d.Row("k") != nil {
		t.Fatalf("re-insert of a deleted gen: err %v, row %+v; want ErrGenClaimed, no row", err, d.Row("k"))
	}
	if err := d.Violation(); err != nil {
		t.Fatal(err)
	}
}

func TestCrossRowSwitch(t *testing.T) {
	ctx := context.Background()
	d := New()
	d.SetCrossRow("k", true)
	if _, err := d.InsertDedup(ctx, row(100, "a")); !db.IsCrossRow(err) || d.Row("k") != nil || d.Claimed("k", 100) {
		t.Fatalf("insert while blocked: err %v, row %+v, claimed %v", err, d.Row("k"), d.Claimed("k", 100))
	}
	if s := d.Stats(); s.CrossRow != 1 {
		t.Fatalf("Stats %+v, want CrossRow 1", s)
	}
	exec, _ := lateInsert(t, d, 200)
	exec() // still blocked: rejected
	if d.Row("k") != nil || d.Stats().LateEffective != 0 {
		t.Fatalf("late insert while blocked: row %+v, Stats %+v", d.Row("k"), d.Stats())
	}
	d.SetCrossRow("k", false)
	exec, _ = lateInsert(t, d, 300)
	exec()
	if r := d.Row("k"); r == nil || r.Gen != 300 {
		t.Fatalf("late insert after the switch: row %+v, want gen 300", r)
	}
	if s := d.Stats(); s.Late != 2 || s.LateEffective != 1 {
		t.Fatalf("Stats %+v, want Late 2, LateEffective 1", s)
	}
}

func TestFencedSessionNeverExecutes(t *testing.T) {
	ctx := context.Background()
	d := New()
	exec, session := lateInsert(t, d, 100)
	if err := d.FenceSessions(ctx, []db.SessionID{session}); err != nil {
		t.Fatal(err)
	}
	exec()
	if d.Row("k") != nil || d.Claimed("k", 100) || d.Stats().Fenced != 1 {
		t.Fatalf("after the fence: row %+v, claimed %v, Stats %+v", d.Row("k"), d.Claimed("k", 100), d.Stats())
	}
	exec, session = lateInsert(t, d, 200)
	boom := errors.New("not confirmed")
	d.Hook = func(context.Context, *Stmt) error { return boom }
	if err := d.FenceSessions(ctx, []db.SessionID{session}); !errors.Is(err, boom) {
		t.Fatalf("fence: err %v, want boom", err)
	}
	d.Hook = nil
	exec()
	if r := d.Row("k"); r == nil || r.Gen != 200 {
		t.Fatalf("an unconfirmed fence must fence nothing: row %+v", r)
	}
}

func TestOneWriterPerKeyAcrossKinds(t *testing.T) {
	d := New()
	entered, release := make(chan struct{}), make(chan struct{})
	d.Hook = func(_ context.Context, st *Stmt) error {
		if st.Kind == Insert {
			entered <- struct{}{}
			<-release
		}
		return nil
	}
	var wg sync.WaitGroup
	wg.Go(func() { d.InsertDedup(context.Background(), row(100, "a")) })
	<-entered
	d.Delete(context.Background(), "k", 100) // while the Insert is in progress
	close(release)
	wg.Wait()
	if d.Violation() == nil {
		t.Fatal("an Insert and a Delete of one key in progress at once were not reported")
	}
}

func TestNoResurrectionIsAsserted(t *testing.T) {
	ctx := context.Background()
	d := New()
	d.InsertDedup(ctx, row(100, "a"))
	d.Delete(ctx, "k", 100)
	d.mu.Lock()
	delete(d.dedup, dedupKey{"k", 100}) // what mutation DedupDeletedWithRow does
	d.mu.Unlock()
	d.InsertDedup(ctx, row(100, "a"))
	if err := d.Violation(); err == nil || !strings.Contains(err.Error(), "NoResurrection") {
		t.Fatalf("Violation = %v, want NoResurrection", err)
	}
}

func TestRowCheck(t *testing.T) {
	d := New()
	d.RowCheck = func(r *schema.Row) error {
		if r.Vals[1] == "bad" {
			return errors.New("bad value")
		}
		return nil
	}
	d.InsertDedup(context.Background(), row(100, "good"))
	if err := d.Violation(); err != nil {
		t.Fatal(err)
	}
	d.InsertDedup(context.Background(), row(200, "bad")) // ErrDuplicate: no row written, nothing to check
	d.Delete(context.Background(), "k", 100)
	d.InsertDedup(context.Background(), row(300, "bad"))
	if d.Violation() == nil {
		t.Fatal("RowCheck failure not reported")
	}
}
