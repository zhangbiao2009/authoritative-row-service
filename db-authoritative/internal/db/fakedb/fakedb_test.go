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
	if seen.Kind != Select || seen.Key != "k" {
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
	if seen.Kind != Update || seen.Key != "k" || seen.Gen != 5 {
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

func ins(gen int64, name string) *schema.Row {
	return &schema.Row{Key: "k", Gen: gen, Ver: 9, Vals: []any{int64(1), name, int64(10)}}
}

func TestHookSeesEveryStatement(t *testing.T) {
	d := New()
	ctx := context.Background()
	var seen []Stmt
	d.Hook = func(_ context.Context, st *Stmt) error { seen = append(seen, *st); return nil }
	d.Select(ctx, "k")
	d.InsertDedup(ctx, ins(5, "a"))
	d.GenClaimed(ctx, "k", 5)
	d.Update(ctx, ins(5, "b"), []int{1})
	d.Delete(ctx, "k", 5)
	d.FenceSession(ctx, 77)
	want := []struct {
		kind Kind
		key  schema.Key
		gen  int64
	}{{Select, "k", 0}, {Insert, "k", 5}, {GenClaimed, "k", 5}, {Update, "k", 5}, {Delete, "k", 5}, {Fence, "", 0}}
	if len(seen) != len(want) {
		t.Fatalf("hook saw %d statements, want %d", len(seen), len(want))
	}
	sessions := map[db.SessionID]bool{}
	for i, w := range want {
		st := seen[i]
		if st.Kind != w.kind || st.Key != w.key || st.Gen != w.gen || st.Session == 0 || st.Exec == nil {
			t.Fatalf("statement %d = %+v, want %+v", i, st, w)
		}
		if i < 5 {
			if sessions[st.Session] {
				t.Fatalf("session %d reused", st.Session)
			}
			sessions[st.Session] = true
		}
	}
	if seen[5].Session != 77 {
		t.Fatalf("Fence session = %d, want 77", seen[5].Session)
	}
}

func TestHookErrorWithoutExecDoesNotExecute(t *testing.T) {
	d := New()
	d.Put(row(5, "a"))
	boom := errors.New("boom")
	d.Hook = func(context.Context, *Stmt) error { return boom }
	if _, err := d.Update(context.Background(), row(5, "x"), []int{1}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if d.Row("k").Vals[1] != "a" || d.Pending() != 0 {
		t.Fatalf("row %+v, pending %d", d.Row("k"), d.Pending())
	}
}

func TestLostAnswerExecutes(t *testing.T) {
	d := New()
	d.Put(row(5, "a"))
	var sess db.SessionID
	d.Hook = func(_ context.Context, st *Stmt) error {
		st.Exec()
		sess = st.Session
		return Ambiguous(st, errors.New("conn lost"))
	}
	_, err := d.Update(context.Background(), row(5, "x"), []int{1})
	if !db.IsAmbiguous(err) || db.SessionOf(err) != sess || sess == 0 {
		t.Fatalf("err = %v, session %d", err, sess)
	}
	if d.Row("k").Vals[1] != "x" || d.Pending() != 0 {
		t.Fatalf("row %+v, pending %d", d.Row("k"), d.Pending())
	}
}

// keepAmbiguous makes the hook keep Exec of every statement and answer ambiguously.
func keepAmbiguous(d *DB, kept *func(), sess *db.SessionID) {
	d.Hook = func(_ context.Context, st *Stmt) error {
		if st.Kind == Fence {
			return nil
		}
		*kept, *sess = st.Exec, st.Session
		return Ambiguous(st, errors.New("timeout"))
	}
}

func TestLateExecAndPending(t *testing.T) {
	d := New()
	d.Put(row(5, "a"))
	var exec func()
	var sess db.SessionID
	keepAmbiguous(d, &exec, &sess)
	if _, err := d.Update(context.Background(), row(5, "x"), []int{1}); !db.IsAmbiguous(err) {
		t.Fatalf("err = %v", err)
	}
	if d.Pending() != 1 || d.Row("k").Vals[1] != "a" {
		t.Fatalf("pending %d, row %+v", d.Pending(), d.Row("k"))
	}
	exec()
	if d.Pending() != 0 || d.Row("k").Vals[1] != "x" || d.Row("k").Ver != 5 {
		t.Fatalf("pending %d, row %+v", d.Pending(), d.Row("k"))
	}
	exec()
	if d.Row("k").Ver != 5 {
		t.Fatalf("second Exec executed again: %+v", d.Row("k"))
	}
}

func TestFenceDropsPendingStatement(t *testing.T) {
	d := New()
	d.Put(row(5, "a"))
	var exec func()
	var sess db.SessionID
	keepAmbiguous(d, &exec, &sess)
	d.Update(context.Background(), row(5, "x"), []int{1})
	if err := d.FenceSession(context.Background(), sess); err != nil {
		t.Fatal(err)
	}
	if d.Pending() != 0 {
		t.Fatalf("pending = %d", d.Pending())
	}
	exec()
	if d.Row("k").Vals[1] != "a" {
		t.Fatalf("a fenced statement executed: %+v", d.Row("k"))
	}
}

func TestFenceNotConfirmed(t *testing.T) {
	d := New()
	d.Put(row(5, "a"))
	var exec func()
	var sess db.SessionID
	keepAmbiguous(d, &exec, &sess)
	d.Update(context.Background(), row(5, "x"), []int{1})
	boom := errors.New("boom")
	d.Hook = func(context.Context, *Stmt) error { return boom }
	if err := d.FenceSession(context.Background(), sess); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	exec()
	if d.Row("k").Vals[1] != "x" {
		t.Fatalf("an unconfirmed fence stopped the statement: %+v", d.Row("k"))
	}
}

func TestInsertDedup(t *testing.T) {
	d := New()
	ctx := context.Background()
	got, err := d.InsertDedup(ctx, ins(5, "a"))
	if err != nil || got.Ver != 0 || got.Gen != 5 || !d.Claimed("k", 5) || d.Row("k").Ver != 0 {
		t.Fatalf("InsertDedup = %+v, %v", got, err)
	}
	if _, err := d.InsertDedup(ctx, ins(5, "b")); !errors.Is(err, db.ErrGenClaimed) {
		t.Fatalf("same gen: err = %v, want ErrGenClaimed", err)
	}
	if _, err := d.InsertDedup(ctx, ins(6, "b")); !errors.Is(err, db.ErrDuplicate) || !d.Claimed("k", 6) {
		t.Fatalf("new gen over a row: err = %v, claimed %v", err, d.Claimed("k", 6))
	}
	if d.Row("k").Gen != 5 || d.Row("k").Vals[1] != "a" {
		t.Fatalf("row changed: %+v", d.Row("k"))
	}
}

func TestInsertNormalizes(t *testing.T) {
	d := New()
	d.Normalize = func(col int, v any) any {
		if s, ok := v.(string); ok {
			return strings.ToUpper(s)
		}
		return v
	}
	got, err := d.InsertDedup(context.Background(), ins(5, "a"))
	if err != nil || got.Vals[1] != "A" || d.Row("k").Vals[1] != "A" {
		t.Fatalf("InsertDedup = %+v, %v", got, err)
	}
}

func TestDeleteKeepsDedupAndChecksGen(t *testing.T) {
	d := New()
	ctx := context.Background()
	d.InsertDedup(ctx, ins(5, "a"))
	if err := d.Delete(ctx, "k", 6); !errors.Is(err, db.ErrNoRows) || d.Row("k") == nil {
		t.Fatalf("Delete of another gen: err = %v", err)
	}
	if err := d.Delete(ctx, "k", 5); err != nil {
		t.Fatal(err)
	}
	if d.Row("k") != nil || !d.Deleted("k", 5) || !d.Claimed("k", 5) {
		t.Fatalf("row %+v, deleted %v, claimed %v", d.Row("k"), d.Deleted("k", 5), d.Claimed("k", 5))
	}
	if err := d.Delete(ctx, "k", 5); !errors.Is(err, db.ErrNoRows) {
		t.Fatalf("second Delete: err = %v", err)
	}
}

func TestRejectingSwitch(t *testing.T) {
	d := New()
	ctx := context.Background()
	d.SetRejecting("k", true)
	if _, err := d.InsertDedup(ctx, ins(5, "a")); !errors.Is(err, db.ErrRejected) || d.Claimed("k", 5) || d.Row("k") != nil {
		t.Fatalf("Insert: err = %v, claimed %v", err, d.Claimed("k", 5))
	}
	d.SetRejecting("k", false)
	d.InsertDedup(ctx, ins(5, "a"))
	d.SetRejecting("k", true)
	if _, err := d.Update(ctx, ins(5, "x"), []int{1}); !errors.Is(err, db.ErrRejected) || d.Row("k").Vals[1] != "a" || d.Row("k").Ver != 0 {
		t.Fatalf("Update: err = %v, row %+v", err, d.Row("k"))
	}
	if _, err := d.Update(ctx, ins(6, "x"), []int{1}); !errors.Is(err, db.ErrNoRows) {
		t.Fatalf("Update of another gen: err = %v, want ErrNoRows", err)
	}
	if err := d.Delete(ctx, "k", 5); !errors.Is(err, db.ErrRejected) || d.Row("k") == nil || d.Deleted("k", 5) {
		t.Fatalf("Delete: err = %v", err)
	}
	if err := d.Delete(ctx, "k", 6); !errors.Is(err, db.ErrNoRows) {
		t.Fatalf("Delete of another gen: err = %v, want ErrNoRows", err)
	}
}

func TestNoResurrectionIsAsserted(t *testing.T) {
	d := New()
	ctx := context.Background()
	d.InsertDedup(ctx, ins(5, "a"))
	d.Delete(ctx, "k", 5)
	if d.Violation() != nil {
		t.Fatal(d.Violation())
	}
	d.mu.Lock()
	delete(d.dedup, dedupKey{"k", 5})
	d.mu.Unlock()
	d.InsertDedup(ctx, ins(5, "a"))
	if v := d.Violation(); v == nil || !strings.Contains(v.Error(), "NoResurrection") {
		t.Fatalf("Violation = %v, want NoResurrection", v)
	}
}

func TestRowCheck(t *testing.T) {
	bad := errors.New("bad row")
	for _, name := range []string{"insert", "update"} {
		d := New()
		ctx := context.Background()
		if name == "update" {
			d.Put(row(5, "a"))
		}
		d.RowCheck = func(r *schema.Row) error {
			if r.Vals[1] == "x" {
				return bad
			}
			return nil
		}
		if name == "insert" {
			d.InsertDedup(ctx, ins(5, "a"))
			if d.Violation() != nil {
				t.Fatal(d.Violation())
			}
			d.Delete(ctx, "k", 5)
			d.InsertDedup(ctx, ins(6, "x"))
		} else {
			d.Update(ctx, row(5, "x"), []int{1})
		}
		if d.Violation() == nil {
			t.Fatalf("%s: RowCheck rejection not reported", name)
		}
	}
}

func TestAppliedUpdates(t *testing.T) {
	d := New()
	ctx := context.Background()
	d.Put(row(5, "a"))
	d.Update(ctx, row(5, "x"), []int{1})
	d.Update(ctx, row(6, "nope"), []int{1})
	d.Update(ctx, row(5, "y"), []int{1})
	got := d.AppliedUpdates()
	want := []*schema.Row{
		{Key: "k", Gen: 5, Ver: 5, Vals: []any{int64(1), "x", int64(10)}},
		{Key: "k", Gen: 5, Ver: 6, Vals: []any{int64(1), "y", int64(10)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AppliedUpdates = %+v, want %+v", got, want)
	}
}

func TestOneWriterPerKeyAcrossKinds(t *testing.T) {
	d := New()
	d.Put(row(5, "a"))
	inside, release := make(chan struct{}), make(chan struct{})
	d.Hook = func(context.Context, *Stmt) error { inside <- struct{}{}; <-release; return nil }
	var wg sync.WaitGroup
	wg.Go(func() { d.Update(context.Background(), row(5, "x"), []int{1}) })
	wg.Go(func() { d.Delete(context.Background(), "k", 5) })
	<-inside
	<-inside
	close(release)
	wg.Wait()
	if d.Violation() == nil {
		t.Fatal("an Update and a Delete of one key in progress at once were not reported")
	}
}
