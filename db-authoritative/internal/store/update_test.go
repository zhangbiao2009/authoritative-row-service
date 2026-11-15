package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"rowcache/internal/db"
	"rowcache/internal/db/fakedb"
	"rowcache/internal/schema"
)

func set(col int, v any) ColChange    { return ColChange{Col: col, Op: OpSet, Val: v} }
func incr(col int, d int64) ColChange { return ColChange{Col: col, Op: OpIncr, Val: d} }

// blockUpdates makes every fakedb.Update statement wait until the returned channel is closed.
func blockUpdates(d *fakedb.DB) (release chan struct{}) {
	release = make(chan struct{})
	d.Hook = func(_ context.Context, st *fakedb.Stmt) error {
		if st.Kind == fakedb.Update {
			<-release
		}
		return nil
	}
	return release
}

func entryOf(s *Store, k schema.Key) *entry {
	sh := s.shardFor(k)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.m[k]
}

// opFree reports whether e.op is free, taking and releasing it if so (NoLockLeak).
func opFree(e *entry) bool {
	select {
	case e.op <- struct{}{}:
		<-e.op
		return true
	default:
		return false
	}
}

func TestUpdateSetAndIncr(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1)) // gen 101, ver 3, {1, "a", 10}
	got, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b"), incr(2, 5)}})
	want := &schema.Row{Key: k, Gen: 101, Ver: 4, Vals: []any{int64(1), "b", int64(15)}}
	if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(d.Row(k), want) {
		t.Fatalf("Update = %+v, %v; DB %+v; want %+v", got, err, d.Row(k), want)
	}
	if r, _ := s.Get(context.Background(), k); r != got {
		t.Fatalf("Get = %p, want the row Update returned %p", r, got)
	}
	if !opFree(entryOf(s, k)) {
		t.Fatal("op held after Update returned")
	}
}

func TestUpdateLoadsFirst(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	var kinds []fakedb.Kind
	d.Hook = func(_ context.Context, st *fakedb.Stmt) error {
		kinds = append(kinds, st.Kind)
		return nil
	}
	got, err := s.Update(context.Background(), k, Change{Cols: []ColChange{incr(2, 1)}})
	if err != nil || got.Vals[2] != int64(11) || got.Vals[1] != "a" {
		t.Fatalf("Update = %+v, %v", got, err)
	}
	if want := []fakedb.Kind{fakedb.Select, fakedb.Update}; !reflect.DeepEqual(kinds, want) {
		t.Fatalf("statements = %v, want %v", kinds, want)
	}
}

func TestUpdateMissingRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	for i := 0; i < 2; i++ {
		if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "x")}}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Update #%d: %v, want ErrNotFound", i, err)
		}
		if d.Updates() != 0 {
			t.Fatalf("Updates = %d, want 0", d.Updates())
		}
		if !opFree(entryOf(s, k)) {
			t.Fatal("op held")
		}
	}
}

func TestUpdateWithStaleGen(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1)) // gen 101
	if _, err := s.Update(context.Background(), k, Change{Gen: 999, Cols: []ColChange{set(1, "x")}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Update with another gen: %v, want ErrNotFound", err)
	}
	if d.Updates() != 0 {
		t.Fatalf("Updates = %d, want 0", d.Updates())
	}
	if r, err := s.Update(context.Background(), k, Change{Gen: 101, Cols: []ColChange{set(1, "x")}}); err != nil || r.Vals[1] != "x" {
		t.Fatalf("Update with the row's gen = %+v, %v", r, err)
	}
}

func TestUpdateRejectsMalformedChange(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	before, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatal(err)
	}
	for name, cols := range map[string][]ColChange{
		"no columns":     nil,
		"primary key":    {set(0, int64(9))}, // I11
		"out of range":   {set(3, "x")},
		"negative":       {set(-1, "x")},
		"same col twice": {set(1, "x"), set(1, "y")},
		"unknown op":     {{Col: 1, Op: 9, Val: "x"}},
	} {
		if _, err := s.Update(context.Background(), k, Change{Cols: cols}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	if d.Updates() != 0 {
		t.Fatalf("Updates = %d, want 0", d.Updates())
	}
	if after, _ := s.Get(context.Background(), k); after != before || !reflect.DeepEqual(d.Row(k), testRow(k, 1)) {
		t.Fatalf("row changed to %+v", after)
	}
}

func TestFailedIncrementSendsNothing(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	for name, c := range map[string]struct {
		score any
		cols  []ColChange
	}{
		"NULL":                  {nil, []ColChange{incr(2, 1)}},
		"overflow":              {int64(math.MaxInt64), []ColChange{incr(2, 1)}},
		"set then failing incr": {nil, []ColChange{set(1, "z"), incr(2, 1)}},
	} {
		id := int64(len(name))
		k := key(t, s, id)
		r := testRow(k, id)
		r.Vals[2] = c.score
		d.Put(r)
		if _, err := s.Update(context.Background(), k, Change{Cols: c.cols}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
		if got, _ := s.Get(context.Background(), k); got.Vals[1] != "a" || d.Row(k).Vals[1] != "a" {
			t.Errorf("%s: name changed: memory %+v, DB %+v", name, got, d.Row(k))
		}
	}
	if d.Updates() != 0 {
		t.Fatalf("Updates = %d, want 0", d.Updates())
	}
}

func TestUpdateLoadErrorSendsNothing(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	d.Hook = func(_ context.Context, st *fakedb.Stmt) error {
		if st.Kind == fakedb.Select {
			return errors.New("boom")
		}
		return nil
	}
	if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "x")}}); !errors.Is(err, db.ErrNotExecuted) {
		t.Fatalf("err = %v, want ErrNotExecuted", err)
	}
	if d.Updates() != 0 {
		t.Fatalf("Updates = %d, want 0", d.Updates())
	}
}

func TestUpdateCachesDBValue(t *testing.T) {
	d := fakedb.New()
	d.Normalize = func(_ int, v any) any {
		if s, ok := v.(string); ok {
			return strings.ToUpper(s)
		}
		return v
	}
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	got, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}}) // I7
	if err != nil || got.Vals[1] != "B" {
		t.Fatalf("Update = %+v, %v; want name B", got, err)
	}
	if r, _ := s.Get(context.Background(), k); r.Vals[1] != "B" || d.Row(k).Vals[1] != "B" {
		t.Fatalf("Get %+v, DB %+v; want name B", r, d.Row(k))
	}
}

func TestUpdateZeroRowsIsInternal(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	if _, err := s.Get(context.Background(), k); err != nil { // caches gen 101
		t.Fatal(err)
	}
	other := testRow(k, 1)
	other.Gen = 999
	d.Put(other) // behind the cache's back
	_, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "x")}})
	if !errors.Is(err, ErrInternal) {
		t.Fatalf("err = %v, want ErrInternal", err)
	}
	e := entryOf(s, k)
	if e.snap.Load() != nil || !opFree(e) {
		t.Fatal("snapshot not dropped, or op held")
	}
	if r, err := s.Get(context.Background(), k); err != nil || r.Gen != 999 {
		t.Fatalf("Get = %+v, %v; want gen 999", r, err)
	}
}

// The caller's ctx expires while its write is in flight (TLA: X_wait): ErrOutcomeUnknown, and the write
// still completes in the background.
func TestUpdateClientGivesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		release := blockUpdates(d)
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := s.Update(ctx, k, Change{Cols: []ColChange{set(1, "b")}}); !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("err = %v, want ErrOutcomeUnknown", err)
		}
		if opFree(entryOf(s, k)) {
			t.Fatal("op free while the write is still in flight (I5)")
		}
		close(release)
		s.inflight.Wait()
		if r, err := s.Get(context.Background(), k); err != nil || r.Vals[1] != "b" {
			t.Fatalf("Get = %+v, %v; want the background write applied", r, err)
		}
		checkInvariants(t, s, d)
	})
}

// The caller reuses its []ColChange after ErrOutcomeUnknown: the update still in flight must write what was
// sent, not what the caller wrote since.
func TestUpdateDoesNotKeepCallerChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		release := make(chan struct{})
		script(d, map[fakedb.Kind][]step{fakedb.Select: {blockOn(release)}})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		pk := d.Row(k).Vals[0]
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		cols := []ColChange{set(1, "b")}
		if _, err := s.Update(ctx, k, Change{Cols: cols}); !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("err = %v, want ErrOutcomeUnknown", err)
		}
		cols[0] = set(1, "other") // the caller reuses its slice
		close(release)
		s.inflight.Wait()
		if r := d.Row(k); r.Vals[1] != "b" || r.Vals[0] != pk {
			t.Fatalf("row = %+v, want column 1 \"b\" and the primary key %v", r, pk)
		}
		checkInvariants(t, s, d)
	})
}

// A request that times out while queued for op did nothing (TLA: X_qtimeout).
func TestUpdateTimesOutWhileQueued(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		release := blockUpdates(d)
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		go s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "first")}})
		synctest.Wait() // the first Update is blocked in the hook, holding op
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := s.Update(ctx, k, Change{Cols: []ColChange{set(1, "second")}}); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("err = %v, want context.DeadlineExceeded and not ErrOutcomeUnknown", err)
		}
		close(release)
		s.inflight.Wait()
		if d.Updates() != 1 {
			t.Fatalf("Updates = %d, want 1", d.Updates())
		}
		if r := d.Row(k); r.Vals[1] != "first" || r.Ver != 4 {
			t.Fatalf("DB row = %+v, want only the first change", r)
		}
		checkInvariants(t, s, d)
	})
}

// While a write is in flight a Get answers with the old row at once, and the next write waits for op.
func TestGetDuringWriteSeesOldRow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		release := blockUpdates(d)
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		go s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "first")}})
		synctest.Wait()
		if r, err := s.Get(context.Background(), k); err != nil || r.Vals[1] != "a" || r.Ver != 3 {
			t.Fatalf("Get during the write = %+v, %v; want the old row", r, err)
		}
		go s.Update(context.Background(), k, Change{Cols: []ColChange{incr(2, 1)}})
		synctest.Wait()
		if d.Updates() != 1 {
			t.Fatalf("Updates = %d while the first write is in flight, want 1", d.Updates())
		}
		close(release)
		synctest.Wait() // both Updates have returned
		r := d.Row(k)
		if d.Updates() != 2 || r.Vals[1] != "first" || r.Vals[2] != int64(11) || r.Ver != 5 {
			t.Fatalf("Updates = %d, DB row = %+v; want both applied in order", d.Updates(), r)
		}
		checkInvariants(t, s, d)
	})
}

func TestConcurrentIncrementsMatchDB(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := 0; i < 100; i++ {
				if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{incr(2, 1)}}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	r, err := s.Get(context.Background(), k)
	if err != nil || r.Vals[2] != int64(810) {
		t.Fatalf("memory = %+v, %v; want 810", r, err)
	}
	if dr := d.Row(k); dr.Vals[2] != int64(810) || dr.Ver != 803 {
		t.Fatalf("DB = %+v; want score 810, ver 803", dr)
	}
	checkInvariants(t, s, d)
}

// A request whose ctx is already done has not started: it must not take op or send a write.
func TestUpdateWithExpiredCtxSendsNothing(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	if _, err := s.Get(context.Background(), k); err != nil {
		t.Fatal(err)
	}
	before := d.Row(k)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 100; i++ {
		if _, err := s.Update(ctx, k, Change{Cols: []ColChange{incr(2, 1)}}); !errors.Is(err, context.Canceled) {
			t.Fatalf("call %d: Update = %v, want context.Canceled", i, err)
		}
	}
	if n := d.Updates(); n != 0 {
		t.Fatalf("Updates = %d, want 0", n)
	}
	if got := d.Row(k); !reflect.DeepEqual(got, before) {
		t.Fatalf("row changed: %+v, want %+v", got, before)
	}
}

// TLA: B_2 ambiguous, the statement executed (W_FenceThenApplied): OK with the DB's row.
func TestUpdateAmbiguousApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		rec := script(d, map[fakedb.Kind][]step{fakedb.Update: {lost}})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		got, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}})
		if err != nil || got.Ver != 4 || got.Vals[1] != "b" {
			t.Fatalf("Update = %+v, %v; want ver 4, b", got, err)
		}
		rec.wantFences(t, 1, fakedb.Update)
		checkInvariants(t, s, d)
	})
}

// TLA: B_2 ambiguous, the fence dropped the statement (W_FenceThenNotApplied): not executed, and the kept
// statement can never land (I5).
func TestUpdateAmbiguousNotApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		var late func()
		rec := script(d, map[fakedb.Kind][]step{fakedb.Update: {amb(&late)}})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}}); !errors.Is(err, db.ErrNotExecuted) {
			t.Fatalf("err = %v, want ErrNotExecuted", err)
		}
		rec.wantFences(t, 1, fakedb.Update)
		if n := d.Pending(); n != 0 { // before late(): only the fence can clear it
			t.Fatalf("Pending = %d, want 0", n)
		}
		late() // too late: the session is fenced
		if r := d.Row(k); r.Ver != 3 || r.Vals[1] != "a" {
			t.Fatalf("DB row %+v changed after the answer", r)
		}
		checkInvariants(t, s, d)
	})
}

// The statement executes while the fence is being confirmed: the fence step runs the kept statement.
func TestUpdateAmbiguousExecutesAtFence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		var late func()
		rec := script(d, map[fakedb.Kind][]step{
			fakedb.Update: {amb(&late)},
			fakedb.Fence:  {func(*fakedb.Stmt) error { late(); return nil }},
		})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		got, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}})
		if err != nil || got.Ver != 4 || got.Vals[1] != "b" {
			t.Fatalf("Update = %+v, %v; want ver 4, b", got, err)
		}
		rec.wantFences(t, 1, fakedb.Update)
		checkInvariants(t, s, d)
	})
}

// The fence is retried until confirmed; the statement lands between attempts, so op was rightly held.
func TestUpdateFenceNotConfirmed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		var late func()
		rec := script(d, map[fakedb.Kind][]step{
			fakedb.Update: {amb(&late)},
			fakedb.Fence: {
				fail(errors.New("not confirmed")),
				func(*fakedb.Stmt) error { late(); return fmt.Errorf("not confirmed again") },
			},
		})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		got, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}})
		if err != nil || got.Ver != 4 || got.Vals[1] != "b" {
			t.Fatalf("Update = %+v, %v; want ver 4, b", got, err)
		}
		rec.wantFences(t, 3, fakedb.Update)
		checkInvariants(t, s, d)
	})
}

// The read after the fence is retried until it succeeds.
func TestUpdateReadAfterFenceFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		var late func()
		rec := script(d, map[fakedb.Kind][]step{
			fakedb.Update: {amb(&late)},
			fakedb.Select: {nil, fail(db.ErrNotExecuted), fail(db.ErrNotExecuted)},
		})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}}); !errors.Is(err, db.ErrNotExecuted) {
			t.Fatalf("err = %v, want ErrNotExecuted", err)
		}
		rec.wantFences(t, 1, fakedb.Update)
		if n := d.Pending(); n != 0 { // the kept statement never runs: only the fence clears it
			t.Fatalf("Pending = %d, want 0", n)
		}
		if n := len(rec.of(fakedb.Select)); n != 4 { // the load, then three after the fence
			t.Fatalf("Selects = %d, want 4", n)
		}
		checkInvariants(t, s, d)
	})
}

func TestUpdateRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		rec := script(d, nil)
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		d.SetRejecting(k, true)
		if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}}); !errors.Is(err, db.ErrRejected) {
			t.Fatalf("err = %v, want ErrRejected", err)
		}
		if r := d.Row(k); r.Ver != 3 || r.Vals[1] != "a" {
			t.Fatalf("DB row %+v changed", r)
		}
		if n := len(rec.of(fakedb.Fence)); n != 0 {
			t.Fatalf("%d fences, want none", n)
		}
		checkInvariants(t, s, d)
	})
}
