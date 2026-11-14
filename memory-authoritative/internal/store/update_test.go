package store

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

// Columns of the test schema: 0 id (primary key), 1 name, 2 score.

func set(col int, v any) ColChange    { return ColChange{Col: col, Op: OpSet, Val: v} }
func incr(col int, d int64) ColChange { return ColChange{Col: col, Op: OpIncr, Val: d} }

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

func state(e *entry) (dirty uint64, queued bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dirty, e.queued
}

func TestUpdateSetAndIncr(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1)) // Gen 101, Ver 3, Vals {1, "a", 10}
	old, _ := s.Get(context.Background(), k)
	r, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b"), incr(2, 5)}})
	want := &schema.Row{Key: k, Gen: 101, Ver: 4, Vals: []any{int64(1), "b", int64(15)}}
	if err != nil || !reflect.DeepEqual(r, want) {
		t.Fatalf("Update = %+v, %v; want %+v", r, err, want)
	}
	if !reflect.DeepEqual(old, testRow(k, 1)) {
		t.Fatalf("the earlier snapshot changed (I2): %+v", old)
	}
	if g, _ := s.Get(context.Background(), k); g != r {
		t.Fatalf("Get after Update = %+v, want the same row", g)
	}
	dirty, queued := state(entryOf(s, k))
	if dirty != schema.ColBit(1)|schema.ColBit(2) || !queued || queueLen(s) != 1 || s.dirtyCount.Load() != 1 {
		t.Fatalf("dirty %#x queued %v queue %d dirtyCount %d", dirty, queued, queueLen(s), s.dirtyCount.Load())
	}
	// a second update: dirty grows, the queue does not
	if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{incr(2, 1)}}); err != nil {
		t.Fatal(err)
	}
	if queueLen(s) != 1 || s.dirtyCount.Load() != 1 {
		t.Fatalf("after a second update: queue %d dirtyCount %d; want 1, 1", queueLen(s), s.dirtyCount.Load())
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
	r, err := s.Update(context.Background(), k, Change{Cols: []ColChange{incr(2, 1)}})
	if err != nil || r.Ver != 4 || r.Vals[2] != int64(11) {
		t.Fatalf("Update = %+v, %v; want Ver 4, score 11", r, err)
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
	if queueLen(s) != 0 || s.dirtyCount.Load() != 0 {
		t.Fatalf("queue %d dirtyCount %d; want 0, 0", queueLen(s), s.dirtyCount.Load())
	}
}

func TestUpdateLoadErrorHasNoEffect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		boom := errors.New("boom")
		d.SelectHook = func(context.Context, schema.Key) error { return boom }
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		ch := Change{Cols: []ColChange{incr(2, 1)}}
		if _, err := s.Update(context.Background(), k, ch); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
		if entryOf(s, k).snap.Load() != nil || queueLen(s) != 0 || s.dirtyCount.Load() != 0 {
			t.Fatalf("after a failed load: snap %v, queue %d, dirtyCount %d; want nil, 0, 0",
				entryOf(s, k).snap.Load(), queueLen(s), s.dirtyCount.Load())
		}
		d.SelectHook = nil
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
		if dirty, _ := state(entryOf(s, k)); dirty != 0 {
			t.Fatalf("score %v: dirty %#x, want 0", score, dirty)
		}
	}
	if queueLen(s) != 0 || s.dirtyCount.Load() != 0 {
		t.Fatalf("queue %d dirtyCount %d; want 0, 0", queueLen(s), s.dirtyCount.Load())
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
	if dirty, _ := state(entryOf(s, k)); dirty != 0 || queueLen(s) != 0 || s.dirtyCount.Load() != 0 {
		t.Fatalf("dirty %#x queue %d dirtyCount %d; want 0, 0, 0", dirty, queueLen(s), s.dirtyCount.Load())
	}
}

// N goroutines each increment the same column K times: the result is exactly N*K (design.md 12.2 M2).
func TestConcurrentIncrements(t *testing.T) {
	const N, K = 16, 200
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	r := testRow(k, 1)
	r.Vals[2] = int64(0)
	d.Put(r)
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() {
			for range K {
				if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{incr(2, 1)}}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	got, err := s.Get(context.Background(), k)
	if err != nil || got.Vals[2] != int64(N*K) || got.Ver != 3+N*K {
		t.Fatalf("Get = %+v, %v; want score %d, Ver %d", got, err, N*K, 3+N*K)
	}
}
