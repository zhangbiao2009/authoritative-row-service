package store

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

func TestFlushWritesDirtyColumns(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	r := update(t, s, k, set(1, "b"), incr(2, 5))
	flushAll(s)
	dbMatches(t, d, r)
	if n := d.Updates(); n != 1 {
		t.Fatalf("Updates = %d, want 1", n)
	}
	if got := bookkeeping(s, k); got != clean {
		t.Fatalf("after flush: %s, want %s", got, clean)
	}
	checkInvariants(t, s, d)
}

func TestFlushCoalesces(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	var r *schema.Row
	for range 10 {
		r = update(t, s, k, incr(2, 1))
	}
	flushAll(s)
	dbMatches(t, d, r)
	if n := d.Updates(); n != 1 || r.Vals[2] != int64(20) {
		t.Fatalf("Updates = %d, score %v; want 1, 20", n, r.Vals[2])
	}
}

// Mutation NoMaskMerge (tla-mapping.md 6): a failed flush must keep its columns dirty.
func TestFlushFailureKeepsColumnsDirty(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	update(t, s, k, incr(2, 5))
	boom := errors.New("boom")
	d.Hook = func(context.Context, *fakedb.Stmt) error { return boom }
	flushAll(s)
	if got, want := bookkeeping(s, k), "dirty 0x4 queued true queue 1 dirtyCount 1"; got != want {
		t.Fatalf("after a failed flush: %s, want %s", got, want)
	}
	dbMatches(t, d, testRow(k, 1))
	d.Hook = nil
	r := update(t, s, k, set(1, "b"))
	flushAll(s)
	dbMatches(t, d, r) // name "b" and score 15
	checkInvariants(t, s, d)
}

func TestFlushRequeuesWhenOpBusy(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	r := update(t, s, k, incr(2, 1))
	e := entryOf(s, k)
	if err := e.lockOp(context.Background()); err != nil {
		t.Fatal(err)
	}
	flushAll(s)
	if got, want := bookkeeping(s, k), "dirty 0x4 queued true queue 1 dirtyCount 1"; d.Updates() != 0 || got != want {
		t.Fatalf("flush while op is held: Updates %d, %s; want 0, %s", d.Updates(), got, want)
	}
	e.unlockOp()
	flushAll(s)
	dbMatches(t, d, r)
	checkInvariants(t, s, d)
}

// Mutation NoVerCheck (tla-mapping.md 6): a flush that timed out and executes late must not overwrite a
// newer one.
func TestLateFlushDoesNotOverwriteNewer(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	update(t, s, k, incr(2, 1)) // Ver 4
	var late func()
	d.Hook = func(_ context.Context, st *fakedb.Stmt) error {
		late = st.Exec
		return fakedb.Ambiguous(st, context.DeadlineExceeded)
	}
	flushAll(s)
	d.Hook = nil
	r := update(t, s, k, incr(2, 1)) // Ver 5, score 12
	flushAll(s)
	dbMatches(t, d, r)
	late() // the Ver 4 statement lands now
	dbMatches(t, d, r)
	checkInvariants(t, s, d)
}

// Updates of a key do not wait for its flush, the flush gives up after DBTimeout, and the row stays dirty.
func TestUpdateNotBlockedByHangingFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		update(t, s, k, incr(2, 1))
		d.Hook = hang
		var wg sync.WaitGroup
		wg.Go(func() { flushAll(s) })
		synctest.Wait() // the flush holds op and waits for the DB
		start := time.Now()
		r := update(t, s, k, incr(2, 1))
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("Update waited %v for the flush", waited)
		}
		wg.Wait()
		if waited := time.Since(start); waited != s.cfg.DBTimeout {
			t.Fatalf("flush gave up after %v, want DBTimeout %v", waited, s.cfg.DBTimeout)
		}
		if got, want := bookkeeping(s, k), "dirty 0x4 queued true queue 1 dirtyCount 1"; got != want {
			t.Fatalf("after a timed-out flush: %s, want %s", got, want)
		}
		d.Hook = nil
		flushAll(s)
		dbMatches(t, d, r)
		checkInvariants(t, s, d)
	})
}

// I5 (OneWriterPerKey): while one write of a key is in progress, the next flush round must not start
// another; it finds op held and puts the entry back.
func TestSecondFlushWaitsForFirstWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		update(t, s, k, incr(2, 1))
		d.Hook = hang
		var wg sync.WaitGroup
		wg.Go(func() { flushAll(s) })
		synctest.Wait()                  // the first write is in progress
		r := update(t, s, k, incr(2, 1)) // queues the entry again
		wg.Go(func() { flushAll(s) })
		synctest.Wait()
		if n := d.Updates(); n != 1 {
			t.Fatalf("Updates = %d while the first write is in progress, want 1", n)
		}
		wg.Wait()
		if got, want := bookkeeping(s, k), "dirty 0x4 queued true queue 1 dirtyCount 1"; got != want {
			t.Fatalf("after both rounds: %s, want %s", got, want)
		}
		d.Hook = nil
		flushAll(s)
		dbMatches(t, d, r)
		checkInvariants(t, s, d)
	})
}

// W_UpdateDuringFlush: an Update between the flush's snapshot and its write is written by the next flush.
func TestUpdateDuringFlushIsWrittenNext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		update(t, s, k, set(1, "x"))
		release := make(chan struct{})
		script(d, map[fakedb.Kind][]step{fakedb.UpdateBatch: {blockOn(release)}})
		var wg sync.WaitGroup
		wg.Go(func() { flushAll(s) })
		synctest.Wait() // the flush has taken its snapshot and waits for the DB
		r := update(t, s, k, set(1, "y"))
		if n := queueLen(s); n != 1 {
			t.Fatalf("queue %d during the flush, want 1", n)
		}
		close(release)
		wg.Wait()
		d.Hook = nil
		flushAll(s)
		dbMatches(t, d, r)
		checkInvariants(t, s, d)
	})
}

func TestRunFlusher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		stop := startFlusher(s)
		r := update(t, s, k, set(1, "b"))
		time.Sleep(s.cfg.FlushInterval)
		synctest.Wait()
		dbMatches(t, d, r)
		stop()
		checkInvariants(t, s, d)
	})
}

func oneShard(c *Config) { c.NumShards = 1 }

// dirtyRows creates n rows in the DB and updates each, in key order 1..n; it returns their keys.
func dirtyRows(t *testing.T, s *Store, d *fakedb.DB, n int) []schema.Key {
	t.Helper()
	var ks []schema.Key
	for i := range n {
		k := key(t, s, int64(i+1))
		d.Put(testRow(k, int64(i+1)))
		update(t, s, k, set(1, "x"))
		ks = append(ks, k)
	}
	return ks
}

// failBatches is a Hook under which every UpdateBatch is definitely not executed.
func failBatches(_ context.Context, st *fakedb.Stmt) error {
	if st.Kind == fakedb.UpdateBatch {
		return fail(db.ErrNotExecuted)(st)
	}
	return nil
}

func TestFlushBatches(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, oneShard, func(c *Config) { c.FlushBatchMax = 2 })
	dirtyRows(t, s, d, 5)
	rec := script(d, nil)
	flushAll(s)
	var sizes []int
	for _, st := range rec.of(fakedb.UpdateBatch) {
		sizes = append(sizes, len(st.Keys))
		if !slices.IsSorted(st.Keys) {
			t.Fatalf("batch keys %q not in key order", st.Keys)
		}
	}
	if !slices.Equal(sizes, []int{2, 2, 1}) {
		t.Fatalf("batch sizes %v, want [2 2 1]", sizes)
	}
	checkInvariants(t, s, d)
}

func TestFlushBatchFailureMergesBack(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, oneShard)
	ks := dirtyRows(t, s, d, 3)
	d.Hook = failBatches
	flushAll(s)
	for _, k := range ks {
		if got, want := bookkeeping(s, k), "dirty 0x2 queued true queue 3 dirtyCount 3"; got != want {
			t.Fatalf("after a failed batch: %s, want %s", got, want)
		}
	}
	d.Hook = nil
	drain(t, s)
	checkInvariants(t, s, d)
}

// After an ordinary failure the shard stops for the round (decision 3): one statement, not one per batch.
func TestFlushStopsShardAfterFailedBatch(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, oneShard, func(c *Config) { c.FlushBatchMax = 2 })
	ks := dirtyRows(t, s, d, 6)
	rec := script(d, map[fakedb.Kind][]step{fakedb.UpdateBatch: {fail(db.ErrNotExecuted)}})
	flushAll(s)
	if n := len(rec.of(fakedb.UpdateBatch)); n != 1 {
		t.Fatalf("%d UpdateBatch statements, want 1", n)
	}
	for _, k := range ks {
		if got, want := bookkeeping(s, k), "dirty 0x2 queued true queue 6 dirtyCount 6"; got != want {
			t.Fatalf("after the round: %s, want %s", got, want)
		}
	}
	d.Hook = nil
	drain(t, s)
	checkInvariants(t, s, d)
}

func TestFlushBacksOffWhileDBFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d) // FlushInterval 200ms
		k := dirtyRows(t, s, d, 1)[0]
		down := failWhileDown(d)
		stop := runFlusher(s)
		time.Sleep(10 * time.Second)
		if n := d.Updates(); n < 3 || n > 12 {
			t.Fatalf("%d UpdateBatch calls in 10s, want 3 to 12 (about 50 without backoff)", n)
		}
		down.Store(false)
		time.Sleep(flushBackoffMax + s.cfg.FlushInterval)
		if got := bookkeeping(s, k); got != clean {
			t.Fatalf("after the DB recovered: %s, want %s", got, clean)
		}
		stop()
		checkInvariants(t, s, d)
	})
}

func TestOldestDirtyKeepsEarliest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		if !s.oldestDirty().IsZero() {
			t.Fatalf("oldestDirty %v with nothing dirty, want zero", s.oldestDirty())
		}
		t0 := time.Now()
		k := dirtyRows(t, s, d, 1)[0]
		d.Hook = failBatches
		time.Sleep(time.Second)
		flushAll(s) // fails at t0+1s and merges back
		update(t, s, k, set(1, "y"))
		time.Sleep(time.Second)
		flushAll(s)
		if got := s.oldestDirty(); !got.Equal(t0) {
			t.Fatalf("oldestDirty %v, want t0 %v: the merge-back must keep the earliest dirtySince", got, t0)
		}
		d.Hook = nil
		flushAll(s)
		if got := s.oldestDirty(); !got.IsZero() {
			t.Fatalf("oldestDirty %v after the flush, want zero", got)
		}
	})
}
