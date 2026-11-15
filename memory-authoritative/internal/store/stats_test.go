package store

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/db/fakedb"
)

// TestStats runs a script in which each step moves exactly the counters it names.
func TestStats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, oneShard)
		ctx := context.Background()
		var want Stats
		check := func(step string) {
			t.Helper()
			if got := s.Stats(); got != want {
				t.Fatalf("after %s:\n got  %+v\n want %+v", step, got, want)
			}
		}
		k1, k2, k3, k4, k5 := key(t, s, 1), key(t, s, 2), key(t, s, 3), key(t, s, 4), key(t, s, 5)
		d.Put(testRow(k1, 1))

		s.Get(ctx, k1)
		want.Misses, want.Entries = 1, 1
		check("a Get that loads")
		s.Get(ctx, k1)
		want.Hits = 1
		check("a Get from memory")
		s.Get(ctx, k2)
		want.Misses, want.Entries, want.Absent = 2, 2, 1
		check("a Get of a missing row")

		update(t, s, k1, set(1, "b"))
		want.Dirty = 1
		check("an Update")
		script(d, map[fakedb.Kind][]step{fakedb.UpdateBatch: {func(*fakedb.Stmt) error { time.Sleep(time.Millisecond); return nil }}})
		flushAll(s)
		want.Dirty, want.FlushBatches, want.FlushRows, want.FlushTime = 0, 1, 1, time.Millisecond
		check("a flush")

		update(t, s, k1, set(1, "c"))
		d.Hook = failBatches
		flushAll(s)
		want.Dirty, want.FlushBatches, want.FlushRows, want.FlushFailures = 1, 2, 2, 1
		check("a failed flush")
		time.Sleep(time.Second)
		want.OldestDirtyAge = time.Second
		check("a second with a row dirty")
		d.Hook = nil
		flushAll(s)
		want.Dirty, want.FlushBatches, want.FlushRows, want.OldestDirtyAge = 0, 3, 3, 0
		check("the flush after it")

		script(d, map[fakedb.Kind][]step{fakedb.Select: {fail(db.ErrNotExecuted)}})
		if _, err := s.Get(ctx, k3); err == nil {
			t.Fatal("Get with a failing Select succeeded")
		}
		d.Hook = nil
		want.Misses, want.Entries, want.LoadErrors = 3, 3, 1
		check("a load error")

		script(d, map[fakedb.Kind][]step{fakedb.Insert: {lost}})
		if _, err := s.Create(ctx, newRow(k4, 4, 0)); err != nil {
			t.Fatalf("Create: %v", err)
		}
		d.Hook = nil
		want.Entries, want.Ambiguous, want.GenClaimed = 4, 1, 1
		check("a create whose first attempt was ambiguous")

		if !evict(t, s, k2) {
			t.Fatal("the negative-cache entry was not evicted")
		}
		want.Entries, want.Absent, want.Evictions = 3, 0, 1
		check("an eviction")

		e1 := entryOf(s, k1)
		if err := e1.lockOp(ctx); err != nil {
			t.Fatal(err)
		}
		deleted := make(chan error, 1)
		go func() { deleted <- s.Delete(ctx, k1, 0) }()
		synctest.Wait()
		time.Sleep(time.Second)
		e1.unlockOp()
		if err := <-deleted; err != nil {
			t.Fatalf("Delete: %v", err)
		}
		want.OpWaits, want.OpWaitTime, want.Absent = 1, time.Second, 1
		check("a Delete that waited a second for op")

		s.fence([]db.SessionID{1})
		script(d, map[fakedb.Kind][]step{fakedb.Fence: {fail(db.ErrNotExecuted)}})
		s.fence([]db.SessionID{2})
		d.Hook = nil
		want.Fences, want.FencesUnconfirmed = 2, 1
		check("a fence and an unconfirmed one")

		giveUp(t, s, d, newRow(k5, 5, 0))()
		want.Entries, want.OutcomeUnknown = 4, 1
		check("a create answered ErrOutcomeUnknown")
	})
}
