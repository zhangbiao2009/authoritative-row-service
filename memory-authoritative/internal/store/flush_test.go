package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

func update(t *testing.T, s *Store, k schema.Key, cols ...ColChange) *schema.Row {
	t.Helper()
	r, err := s.Update(context.Background(), k, Change{Cols: cols})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func dbMatches(t *testing.T, d *fakedb.DB, want *schema.Row) {
	t.Helper()
	if got := d.Row(want.Key); !reflect.DeepEqual(got, want) {
		t.Fatalf("DB row %+v, want %+v", got, want)
	}
}

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
	if dirty, queued := state(entryOf(s, k)); dirty != 0 || queued || queueLen(s) != 0 || s.dirtyCount.Load() != 0 {
		t.Fatalf("after flush: dirty %#x queued %v queue %d dirtyCount %d", dirty, queued, queueLen(s), s.dirtyCount.Load())
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
	d.UpdateHook = func(context.Context, schema.Key, func()) error { return boom }
	flushAll(s)
	dirty, queued := state(entryOf(s, k))
	if dirty != schema.ColBit(2) || !queued || queueLen(s) != 1 || s.dirtyCount.Load() != 1 {
		t.Fatalf("after a failed flush: dirty %#x queued %v queue %d dirtyCount %d", dirty, queued, queueLen(s), s.dirtyCount.Load())
	}
	dbMatches(t, d, testRow(k, 1))
	d.UpdateHook = nil
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
	if dirty, queued := state(e); d.Updates() != 0 || dirty == 0 || !queued || queueLen(s) != 1 {
		t.Fatalf("flush while op is held: Updates %d dirty %#x queued %v queue %d", d.Updates(), dirty, queued, queueLen(s))
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
	d.UpdateHook = func(_ context.Context, _ schema.Key, exec func()) error {
		late = exec
		return context.DeadlineExceeded
	}
	flushAll(s)
	d.UpdateHook = nil
	r := update(t, s, k, incr(2, 1)) // Ver 5, score 12
	flushAll(s)
	dbMatches(t, d, r)
	late() // the Ver 4 statement lands now
	dbMatches(t, d, r)
	checkInvariants(t, s, d)
}

// A DB that never answers: Updates of the same key do not wait for the flush, the flush gives up after
// DBTimeout, and the row stays dirty.
func TestUpdateNotBlockedByHangingFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		update(t, s, k, incr(2, 1))
		d.UpdateHook = func(ctx context.Context, _ schema.Key, _ func()) error {
			<-ctx.Done()
			return ctx.Err()
		}
		done := make(chan struct{})
		go func() {
			flushAll(s)
			close(done)
		}()
		synctest.Wait() // the flush holds op and waits for the DB
		start := time.Now()
		r := update(t, s, k, incr(2, 1))
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("Update waited %v for the flush", waited)
		}
		<-done
		if waited := time.Since(start); waited != s.cfg.DBTimeout {
			t.Fatalf("flush gave up after %v, want DBTimeout %v", waited, s.cfg.DBTimeout)
		}
		if dirty, queued := state(entryOf(s, k)); dirty == 0 || !queued {
			t.Fatalf("after a timed-out flush: dirty %#x queued %v", dirty, queued)
		}
		d.UpdateHook = nil
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
		d.UpdateHook = func(ctx context.Context, _ schema.Key, _ func()) error {
			<-ctx.Done() // a DB that never answers
			return ctx.Err()
		}
		first, second := make(chan struct{}), make(chan struct{})
		go func() {
			flushAll(s)
			close(first)
		}()
		synctest.Wait()                  // the first write is in progress
		r := update(t, s, k, incr(2, 1)) // queues the entry again
		go func() {
			flushAll(s)
			close(second)
		}()
		synctest.Wait()
		if n := d.Updates(); n != 1 {
			t.Fatalf("Updates = %d while the first write is in progress, want 1", n)
		}
		<-first
		<-second
		if n := queueLen(s); n != 1 {
			t.Fatalf("queue %d after both rounds, want 1", n)
		}
		d.UpdateHook = nil
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
		d.UpdateHook = func(_ context.Context, _ schema.Key, exec func()) error {
			<-release
			exec()
			return nil
		}
		done := make(chan struct{})
		go func() {
			flushAll(s)
			close(done)
		}()
		synctest.Wait() // the flush has taken its snapshot and waits for the DB
		r := update(t, s, k, set(1, "y"))
		if n := queueLen(s); n != 1 {
			t.Fatalf("queue %d during the flush, want 1", n)
		}
		close(release)
		<-done
		d.UpdateHook = nil
		flushAll(s)
		dbMatches(t, d, r)
		checkInvariants(t, s, d)
	})
}

// A zero FlushInterval would panic in RunFlusher; a zero DBTimeout would fail every write, so no row would
// ever be flushed and nothing would say why.
func TestNewRejectsNonPositiveDurations(t *testing.T) {
	sch, err := schema.New("t", []schema.Column{{Name: "id", Type: schema.Int8}}, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Config){
		"FlushInterval 0": func(c *Config) { c.FlushInterval = 0 },
		"DBTimeout 0":     func(c *Config) { c.DBTimeout = 0 },
		"DBTimeout < 0":   func(c *Config) { c.DBTimeout = -time.Second },
	} {
		cfg := DefaultConfig()
		change(&cfg)
		if _, err := New(sch, fakedb.New(), cfg); err == nil {
			t.Errorf("New accepted %s", name)
		}
	}
}

func TestRunFlusher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			s.RunFlusher(ctx)
			close(done)
		}()
		r := update(t, s, k, set(1, "b"))
		time.Sleep(s.cfg.FlushInterval)
		synctest.Wait()
		dbMatches(t, d, r)
		cancel()
		<-done
		checkInvariants(t, s, d)
	})
}
