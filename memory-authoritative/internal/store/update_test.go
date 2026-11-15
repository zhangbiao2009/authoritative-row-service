package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

// Columns of the test schema: 0 id (primary key), 1 name, 2 score.

func set(col int, v any) ColChange    { return ColChange{Col: col, Op: OpSet, Val: v} }
func incr(col int, d int64) ColChange { return ColChange{Col: col, Op: OpIncr, Val: d} }

func update(t *testing.T, s *Store, k schema.Key, cols ...ColChange) *schema.Row {
	t.Helper()
	r, err := s.Update(context.Background(), k, Change{Cols: cols})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func queueLen(s *Store) int {
	n := 0
	for _, sh := range s.shards {
		sh.qmu.Lock()
		n += len(sh.queue)
		sh.qmu.Unlock()
	}
	return n
}

func entryOf(s *Store, k schema.Key) *entry {
	sh := s.shardFor(k)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.m[k]
}

// clean is the bookkeeping of a store whose only entries are clean.
const clean = "dirty 0x0 queued false queue 0 dirtyCount 0"

// bookkeeping describes k's dirty columns and queued flag, and the store's queue length and dirtyCount.
func bookkeeping(s *Store, k schema.Key) string {
	e := entryOf(s, k)
	e.mu.Lock()
	dirty, queued := e.dirty, e.queued
	e.mu.Unlock()
	return fmt.Sprintf("dirty %#x queued %v queue %d dirtyCount %d", dirty, queued, queueLen(s), s.dirtyCount.Load())
}

func dbMatches(t *testing.T, d *fakedb.DB, want *schema.Row) {
	t.Helper()
	if got := d.Row(want.Key); !reflect.DeepEqual(got, want) {
		t.Fatalf("DB row %+v, want %+v", got, want)
	}
}

// hang is a Hook for a DB that never answers an Update.
func hang(ctx context.Context, st *fakedb.Stmt) error {
	if st.Kind != fakedb.UpdateBatch {
		return nil
	}
	<-ctx.Done()
	return fakedb.Ambiguous(st, ctx.Err())
}

func TestUpdateSetAndIncr(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1)) // Gen 101, Ver 3, Vals {1, "a", 10}
	old, _ := s.Get(context.Background(), k)
	r := update(t, s, k, set(1, "b"), incr(2, 5))
	want := &schema.Row{Key: k, Gen: 101, Ver: 4, Vals: []any{int64(1), "b", int64(15)}}
	if !reflect.DeepEqual(r, want) {
		t.Fatalf("Update = %+v, want %+v", r, want)
	}
	if !reflect.DeepEqual(old, testRow(k, 1)) {
		t.Fatalf("the earlier snapshot changed (I2): %+v", old)
	}
	if g, _ := s.Get(context.Background(), k); g != r {
		t.Fatalf("Get after Update = %+v, want the same row", g)
	}
	const dirtyQueued = "dirty 0x6 queued true queue 1 dirtyCount 1"
	if got := bookkeeping(s, k); got != dirtyQueued {
		t.Fatalf("after Update: %s, want %s", got, dirtyQueued)
	}
	update(t, s, k, incr(2, 1)) // dirty grows; the queue does not
	if got := bookkeeping(s, k); got != dirtyQueued {
		t.Fatalf("after a second Update: %s, want %s", got, dirtyQueued)
	}
	if d.Updates() != 0 {
		t.Fatal("Update wrote to the DB; it must only change memory")
	}
}

func TestUpdateLoadsFirst(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	if r := update(t, s, k, incr(2, 1)); r.Ver != 4 || r.Vals[2] != int64(11) {
		t.Fatalf("Update = %+v; want Ver 4, score 11", r)
	}
	if n := d.Selects(); n != 1 {
		t.Fatalf("Selects = %d, want 1", n)
	}
}

func TestUpdateMissingRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{incr(2, 1)}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if got := bookkeeping(s, k); got != clean {
		t.Fatalf("%s, want %s", got, clean)
	}
}

func TestUpdateLoadErrorHasNoEffect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		boom := errors.New("boom")
		d.Hook = func(context.Context, *fakedb.Stmt) error { return boom }
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		ch := Change{Cols: []ColChange{incr(2, 1)}}
		if _, err := s.Update(context.Background(), k, ch); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
		if snap, got := entryOf(s, k).snap.Load(), bookkeeping(s, k); snap != nil || got != clean {
			t.Fatalf("after a failed load: snap %v, %s; want nil, %s", snap, got, clean)
		}
		d.Hook = nil
		time.Sleep(s.cfg.LoadErrTTL)
		if r, err := s.Update(context.Background(), k, ch); err != nil || r.Vals[2] != int64(11) {
			t.Fatalf("Update after the DB recovered = %+v, %v", r, err)
		}
	})
}

// Acceptance (design.md 12.2 M2): an increment that fails leaves the whole Change without effect.
func TestFailedIncrementHasNoEffect(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	for id, score := range map[int64]any{1: nil, 2: int64(math.MaxInt64)} {
		k := key(t, s, id)
		r := testRow(k, id)
		r.Vals[2] = score
		d.Put(r)
		before, err := s.Get(context.Background(), k)
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "changed"), incr(2, 1)}})
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("score %v: err = %v, want ErrInvalid", score, err)
		}
		if after, _ := s.Get(context.Background(), k); after != before {
			t.Fatalf("score %v: row changed to %+v", score, after)
		}
		if got := bookkeeping(s, k); got != clean {
			t.Fatalf("score %v: %s, want %s", score, got, clean)
		}
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
		"primary key":    {set(0, int64(9))},
		"out of range":   {set(3, "x")},
		"negative":       {set(-1, "x")},
		"same col twice": {set(1, "x"), set(1, "y")},
		"unknown op":     {{Col: 1, Op: 9, Val: "x"}},
	} {
		if _, err := s.Update(context.Background(), k, Change{Cols: cols}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	if after, _ := s.Get(context.Background(), k); after != before {
		t.Fatalf("row changed to %+v", after)
	}
	if got := bookkeeping(s, k); got != clean {
		t.Fatalf("%s, want %s", got, clean)
	}
}

// Change.Gen (7.3): an update meant for one life must not change another.
func TestUpdateWithGen(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1)) // gen 101
	if _, err := s.Update(context.Background(), k, Change{Gen: 999, Cols: []ColChange{set(1, "x")}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Update with another gen: %v, want ErrNotFound", err)
	}
	if got := bookkeeping(s, k); got != clean {
		t.Fatalf("after the rejected update: %s, want %s", got, clean)
	}
	if r, err := s.Update(context.Background(), k, Change{Gen: 101, Cols: []ColChange{set(1, "x")}}); err != nil || r.Vals[1] != "x" {
		t.Fatalf("Update with the row's gen = %+v, %v", r, err)
	}
}
