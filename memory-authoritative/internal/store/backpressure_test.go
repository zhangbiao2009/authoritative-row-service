package store

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

// failWhileDown makes every UpdateBatch of d fail while the returned flag is set (it starts set). Set the
// flag, not d.Hook, once the flusher runs.
func failWhileDown(d *fakedb.DB) *atomic.Bool {
	var down atomic.Bool
	down.Store(true)
	d.Hook = func(ctx context.Context, st *fakedb.Stmt) error {
		if down.Load() {
			return failBatches(ctx, st)
		}
		return nil
	}
	return &down
}

// runFlusher starts RunFlusher with the store's own FlushInterval; stop ends it.
func runFlusher(s *Store) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { s.RunFlusher(ctx) })
	return func() {
		cancel()
		wg.Wait()
	}
}

func maxDirtyAge(d time.Duration) func(*Config) { return func(c *Config) { c.MaxDirtyAge = d } }

func tryUpdate(s *Store, k schema.Key) error {
	_, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "y")}})
	return err
}

// Acceptance (design.md 12.2 M6): with every flush failing, updates return ErrBusy once the oldest dirty
// row is older than MaxDirtyAge, updates of dirty rows included.
func TestBusyWhenDirtyTooOld(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, maxDirtyAge(2*time.Second))
		ks := dirtyRows(t, s, d, 2) // a and b, dirty since now
		a, b := ks[0], ks[1]
		c := key(t, s, 3)
		d.Put(testRow(c, 3))
		down := failWhileDown(d)
		stop := runFlusher(s)
		defer stop()
		for range 4 { // b is updated between failed rounds (Review Focus 4); a and b both stay dirty since t0
			time.Sleep(500 * time.Millisecond)
			if err := tryUpdate(s, b); err != nil {
				t.Fatalf("update before MaxDirtyAge: %v", err)
			}
		}
		time.Sleep(flushBackoffMax + s.cfg.FlushInterval)
		for _, k := range []schema.Key{a, b, c} {
			if err := tryUpdate(s, k); !errors.Is(err, ErrBusy) {
				t.Fatalf("update of %q after MaxDirtyAge: %v, want ErrBusy", k, err)
			}
		}
		if n := s.m.busyAge.Load(); n != 3 {
			t.Fatalf("busyAge %d, want 3", n)
		}
		down.Store(false)
		time.Sleep(flushBackoffMax + 2*s.cfg.FlushInterval)
		if err := tryUpdate(s, a); err != nil {
			t.Fatalf("update after the flusher caught up: %v", err)
		}
	})
}

// Acceptance (design.md 12.2 M6) and Review Focus 2: a quarantined row alone does not trigger the age limit.
func TestPoisonAloneDoesNotTriggerBusy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, maxDirtyAge(2*time.Second))
		ks := dirtyRows(t, s, d, 2)
		p, q := ks[0], ks[1]
		update(t, s, p, set(2, int64(-1)))
		d.Reject = negativeScore
		stop := runFlusher(s)
		defer stop()
		time.Sleep(10 * time.Second)
		if got := s.Poisoned(); !slices.Equal(got, []schema.Key{p}) {
			t.Fatalf("Poisoned() = %q, want [%q]", got, p)
		}
		if err := tryUpdate(s, q); err != nil {
			t.Fatalf("update with only a poison row dirty for 10s: %v", err)
		}
		if o := s.oldestDirty(); !o.IsZero() {
			t.Fatalf("oldestDirty %v, want zero: the poison row is left out", o)
		}
	})
}

func TestBusyWhenTooManyDirty(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, func(c *Config) { c.MaxDirty = 2 })
	ks := dirtyRows(t, s, d, 2) // a and b
	c := key(t, s, 3)
	d.Put(testRow(c, 3))
	if err := tryUpdate(s, c); !errors.Is(err, ErrBusy) {
		t.Fatalf("update of a clean row at MaxDirty: %v, want ErrBusy", err)
	}
	if err := tryUpdate(s, ks[0]); err != nil {
		t.Fatalf("update of a dirty row at MaxDirty: %v, want it let through", err)
	}
	if n := s.m.busyCount.Load(); n != 1 {
		t.Fatalf("busyCount %d, want 1", n)
	}
	flushAll(s)
	if err := tryUpdate(s, c); err != nil {
		t.Fatalf("update after the flush: %v", err)
	}
}

// Review Focus 5: ErrBusy changes nothing, and only Update answers it.
func TestBusyChangesNothing(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, func(c *Config) { c.MaxDirty = 1 })
	a := dirtyRows(t, s, d, 1)[0]
	c := key(t, s, 3)
	d.Put(testRow(c, 3))
	if _, err := s.Get(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	check := func(name string, k schema.Key) {
		t.Helper()
		snap, book, n := entryOf(s, k).snap.Load(), bookkeeping(s, k), s.dirtyCount.Load()
		if err := tryUpdate(s, k); !errors.Is(err, ErrBusy) {
			t.Fatalf("%s: %v, want ErrBusy", name, err)
		}
		if entryOf(s, k).snap.Load() != snap || bookkeeping(s, k) != book || s.dirtyCount.Load() != n {
			t.Fatalf("%s: ErrBusy changed the row or the bookkeeping", name)
		}
	}
	check("by count", c)
	s.tooOld.Store(true)
	check("by age", a)
	if _, err := s.Get(context.Background(), c); err != nil {
		t.Fatalf("Get while busy: %v", err)
	}
	if _, err := s.Create(context.Background(), newRow(key(t, s, 4), 4, 0)); err != nil {
		t.Fatalf("Create while busy: %v", err)
	}
	if err := s.Delete(context.Background(), c, 0); err != nil {
		t.Fatalf("Delete while busy: %v", err)
	}
}

func TestMaxDirtyAgeZeroDisables(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, maxDirtyAge(0))
		a := dirtyRows(t, s, d, 1)[0]
		failWhileDown(d)
		stop := runFlusher(s)
		defer stop()
		time.Sleep(time.Minute)
		if err := tryUpdate(s, a); err != nil {
			t.Fatalf("update with MaxDirtyAge 0: %v", err)
		}
	})
}

// Final review, Important 2: with a DB that hangs, each write takes DBTimeout, and a worker's round over
// its shards takes far longer than MaxDirtyAge. The age limit must still fire about MaxDirtyAge after the
// oldest update, not a whole round later.
func TestBusyWhenDBHangs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, maxDirtyAge(30*time.Second), func(c *Config) { c.NumShards = 256 })
		dirtyRows(t, s, d, 2000) // rows in every shard: a worker's round is 32 shards x DBTimeout
		d.Hook = hang
		start := time.Now()
		stop := runFlusher(s)
		defer stop()
		limit := s.cfg.MaxDirtyAge + s.cfg.DBTimeout + 2*s.cfg.FlushInterval
		for !s.tooOld.Load() && time.Since(start) < 10*time.Minute {
			time.Sleep(100 * time.Millisecond)
		}
		if took := time.Since(start); took > limit {
			t.Fatalf("the age limit fired after %v, want at most %v", took, limit)
		}
	})
}
