package store

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

// evict runs the dead protocol on k's entry, with no policy condition.
func evict(t *testing.T, s *Store, k schema.Key) bool {
	t.Helper()
	e := entryOf(s, k)
	if e == nil {
		t.Fatalf("key %q has no entry", k)
	}
	return s.tryEvict(s.shardFor(k), e, always)
}

// at runs f once, at the failpoint named name.
func at(s *Store, name string, f func(e *entry)) {
	var once sync.Once
	s.failpoint = func(fp string, e *entry) {
		if fp == name {
			once.Do(func() { f(e) })
		}
	}
}

func TestEvictRemovesCleanEntry(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	r := loaded(t, s, d, k)
	if !evict(t, s, k) || entryOf(s, k) != nil || s.entryCount.Load() != 0 {
		t.Fatalf("after evict: entry %v, entryCount %d; want removed, 0", entryOf(s, k), s.entryCount.Load())
	}
	if g, err := s.Get(context.Background(), k); err != nil || !reflect.DeepEqual(g, r) || d.Selects() != 2 {
		t.Fatalf("Get after eviction = %+v, %v, Selects %d; want the same row, reloaded", g, err, d.Selects())
	}
	checkInvariants(t, s, d)
}

// Mutation EvictDirty (I10).
func TestEvictKeepsDirtyEntry(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	loaded(t, s, d, k)
	update(t, s, k, set(1, "x"))
	if evict(t, s, k) || entryOf(s, k) == nil {
		t.Fatal("a dirty entry was evicted")
	}
	flushAll(s)
	if !evict(t, s, k) {
		t.Fatal("a clean entry was not evicted")
	}
	checkInvariants(t, s, d)
}

func TestEvictSkipsBusyOp(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	loaded(t, s, d, k)
	e := entryOf(s, k)
	if err := e.lockOp(context.Background()); err != nil {
		t.Fatal(err)
	}
	if evict(t, s, k) {
		t.Fatal("evicted while op was held")
	}
	e.unlockOp()
	if !evict(t, s, k) {
		t.Fatal("not evicted once op was free")
	}
}

// Review Focus 4; gap evict-dead (tla-mapping.md 3.1); mutations EvictNoDead, UpdateIgnoresDead. An Update that
// meets the entry between dead and its removal from the map spins, then lands on a fresh entry. Real time:
// a spinning goroutine never blocks, so synctest could not wait for it.
func TestUpdateDuringEvictionIsKept(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	loaded(t, s, d, k)
	var wg sync.WaitGroup
	var got *schema.Row
	var err error
	at(s, "evict-dead", func(*entry) {
		wg.Go(func() { got, err = s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "x")}}) })
		time.Sleep(50 * time.Millisecond) // the Update reaches the dead entry and spins
	})
	if !evict(t, s, k) {
		t.Fatal("not evicted")
	}
	wg.Wait()
	if err != nil || got.Vals[1] != "x" {
		t.Fatalf("Update = %+v, %v", got, err)
	}
	if g, _ := s.Get(context.Background(), k); g.Vals[1] != "x" {
		t.Fatalf("Get after the update = %+v, want name x: the update was lost", g)
	}
	flushAll(s)
	memMatchesDB(t, s, d, k)
	checkInvariants(t, s, d)
}

// Dead protocol rule 3: a lock-free reader of the entry being removed reads its last value.
func TestGetDuringEvictionSeesRemovalState(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	r := loaded(t, s, d, k)
	at(s, "evict-dead", func(*entry) {
		if g, err := s.Get(context.Background(), k); err != nil || g != r {
			t.Errorf("Get during eviction = %+v, %v; want the row as it was", g, err)
		}
	})
	evict(t, s, k)
}

// lockOpLive. A Create or Delete that queued for the op of an entry being evicted must not run on that dead
// entry: memory would change only in the dead entry, while a new entry for the key holds what the DB had
// before the statement.
func TestCreateAndDeleteSkipDeadEntry(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			d := fakedb.New()
			s := newTestStore(t, d)
			k := key(t, s, 1)
			loaded(t, s, d, k)
			release := make(chan struct{})
			script(d, map[fakedb.Kind][]step{fakedb.Delete: {blockOn(release)}})
			done := make(chan error, 1)
			at(s, "evict-dead", func(*entry) {
				go func() { done <- s.Delete(context.Background(), k, 0) }()
				synctest.Wait() // the Delete queues for the op of the entry being evicted
			})
			evict(t, s, k)
			synctest.Wait() // the Delete statement is held
			if _, err := s.Get(context.Background(), k); err != nil {
				t.Fatalf("Get while the delete runs: %v; want the row", err)
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			notFound(t, s, k)
			checkInvariants(t, s, d)
		})
	})
	t.Run("create", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			d := fakedb.New()
			s := newTestStore(t, d)
			k := key(t, s, 1)
			notFound(t, s, k) // a negative-cache entry
			release := make(chan struct{})
			script(d, map[fakedb.Kind][]step{fakedb.Insert: {blockOn(release)}})
			done := make(chan error, 1)
			at(s, "evict-dead", func(*entry) {
				go func() { _, err := s.Create(context.Background(), newRow(k, 1, 100)); done <- err }()
				synctest.Wait()
			})
			evict(t, s, k)
			synctest.Wait() // the INSERT is held
			notFound(t, s, k)
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if r, err := s.Get(context.Background(), k); err != nil || r.Gen != 100 {
				t.Fatalf("Get after the create = %+v, %v; want the created row", r, err)
			}
			checkInvariants(t, s, d)
		})
	})
}

// load checks dead after taking op: a load queued behind the removal looks the key up again.
func TestLoadOfDeadEntryStartsOver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		s.getOrCreate(k) // an entry with snap nil
		got := make(chan *schema.Row, 1)
		at(s, "evict-dead", func(*entry) {
			go func() { r, _ := s.Get(context.Background(), k); got <- r }()
			synctest.Wait() // the Get finds snap nil and queues on op, which tryEvict holds
		})
		if !evict(t, s, k) {
			t.Fatal("not evicted")
		}
		if r := <-got; r == nil || r.Gen != 101 {
			t.Fatalf("Get = %+v, want the row", r)
		}
		if n := d.Selects(); n != 1 {
			t.Fatalf("Selects = %d, want 1: the load of the dead entry must not read", n)
		}
	})
}

// The flusher drops a dead entry still in its queue.
func TestFlushDropsDeadEntry(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	loaded(t, s, d, k)
	e := entryOf(s, k)
	e.mu.Lock()
	e.queued = true
	e.mu.Unlock()
	s.enqueue(e)
	if !evict(t, s, k) {
		t.Fatal("not evicted")
	}
	flushAll(s)
	if d.Updates() != 0 || queueLen(s) != 0 || e.queued {
		t.Fatalf("Updates %d, queue %d, queued %v; want 0, 0, false", d.Updates(), queueLen(s), e.queued)
	}
}

// ttls sets the eviction config of a test Store.
func ttls(idle, negative time.Duration) func(*Config) {
	return func(c *Config) { c.IdleTTL, c.NegativeIdleTTL = idle, negative }
}

// runEvictor starts RunEvictor; the returned func stops it and waits for it.
func runEvictor(s *Store) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { s.RunEvictor(ctx) })
	return func() {
		cancel()
		wg.Wait()
	}
}

func TestIdleEntryEvicted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, ttls(10*time.Second, time.Hour))
		k := key(t, s, 1)
		loaded(t, s, d, k)
		defer runEvictor(s)()
		time.Sleep(5 * time.Second)
		if entryOf(s, k) == nil {
			t.Fatal("evicted after 5s of a 10s IdleTTL")
		}
		s.Get(context.Background(), k) // an access
		time.Sleep(9 * time.Second)
		if entryOf(s, k) == nil {
			t.Fatal("evicted 9s after its last access")
		}
		time.Sleep(5 * time.Second) // seconds granularity, up to 1s of clock lag, and one EvictInterval
		if entryOf(s, k) != nil {
			t.Fatal("not evicted 14s after its last access")
		}
		loaded(t, s, d, k) // a miss reloads it
	})
}

func TestNegativeEntryUsesNegativeTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, ttls(time.Hour, 5*time.Second))
		missing, deleted, row := key(t, s, 1), key(t, s, 2), key(t, s, 3)
		notFound(t, s, missing)
		create(t, s, newRow(deleted, 2, 100))
		del(t, s, deleted, 0)
		loaded(t, s, d, row)
		defer runEvictor(s)()
		time.Sleep(3 * time.Second)
		if entryOf(s, missing) == nil || entryOf(s, deleted) == nil {
			t.Fatal("a negative entry was evicted before NegativeIdleTTL")
		}
		time.Sleep(5 * time.Second)
		if entryOf(s, missing) != nil || entryOf(s, deleted) != nil || entryOf(s, row) == nil {
			t.Fatalf("after 8s: missing %v, tombstone %v, row %v; want gone, gone, kept",
				entryOf(s, missing) != nil, entryOf(s, deleted) != nil, entryOf(s, row) != nil)
		}
		notFound(t, s, deleted) // still deleted, from the DB
	})
}

// Review Focus 1.
func TestFailedLoadEntryIsEvicted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, ttls(time.Hour, 5*time.Second))
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		script(d, map[fakedb.Kind][]step{fakedb.Select: {fail(errors.New("boom"))}})
		if _, err := s.Get(context.Background(), k); err == nil {
			t.Fatal("Get succeeded; want the load error")
		}
		if e := entryOf(s, k); e == nil || e.snap.Load() != nil {
			t.Fatal("want an entry with no row after a failed load")
		}
		defer runEvictor(s)()
		time.Sleep(8 * time.Second)
		if entryOf(s, k) != nil {
			t.Fatal("the entry of a failed load was not evicted after NegativeIdleTTL")
		}
		loaded(t, s, d, k)
	})
}

// Review Focus 2; mutation EvictDirty (I10).
func TestDirtyEntryNotEvicted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, ttls(time.Second, time.Second), func(c *Config) { c.MaxEntries = 1 })
		var ks []schema.Key
		for i := range 3 {
			k := key(t, s, int64(i+1))
			loaded(t, s, d, k)
			update(t, s, k, set(1, "x"))
			ks = append(ks, k)
		}
		stop := runEvictor(s)
		time.Sleep(10 * time.Second)
		for _, k := range ks {
			if entryOf(s, k) == nil {
				t.Fatal("a dirty entry was evicted")
			}
		}
		drain(t, s) // not one flushAll: a round may find op held by the evictor and requeue the entry
		time.Sleep(3 * time.Second)
		stop()
		if n := s.entryCount.Load(); n != 0 {
			t.Fatalf("entryCount %d after the rows were flushed, want 0", n)
		}
		checkInvariants(t, s, d)
	})
}

// The eligible re-check (gap evict-sample): an entry accessed after the sweep chose it is kept.
func TestSampledEntryAccessedAgainIsKept(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, ttls(time.Second, time.Second))
	k := key(t, s, 1)
	loaded(t, s, d, k)
	s.nowSec.Add(5) // idle long enough to be chosen
	fired := false
	at(s, "evict-sample", func(*entry) {
		s.Get(context.Background(), k) // the access comes between the sample and tryEvict
		fired = true
	})
	s.idleSweepShard(s.shardFor(k))
	if !fired {
		t.Fatal("the sweep did not reach evict-sample")
	}
	if entryOf(s, k) == nil {
		t.Fatal("evicted although it was accessed after the sweep chose it")
	}
}

func TestCapacityEvictsOldestFirst(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, ttls(time.Hour, time.Hour), func(c *Config) { c.NumShards, c.MaxEntries = 1, 10 }) // one shard: exact LRU
	var ks []schema.Key
	for i := range 20 {
		s.nowSec.Store(int64(1000 + i)) // each key accessed at its own second
		k := key(t, s, int64(i+1))
		create(t, s, newRow(k, int64(i+1), 0))
		ks = append(ks, k)
	}
	s.nowSec.Store(2000)
	s.evictOnce()
	if n := s.entryCount.Load(); n != 9 {
		t.Fatalf("entryCount %d, want 9 (the low-water mark)", n)
	}
	for i, k := range ks {
		if kept := entryOf(s, k) != nil; kept != (i >= 11) {
			t.Fatalf("key %d (accessed at second %d): kept %v; want only the 9 most recent kept", i, 1000+i, kept)
		}
	}
	checkInvariants(t, s, d)
}

// Review Focus 3.
func TestCapacityEvictionAllDirtyReturns(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, func(c *Config) { c.MaxEntries = 2 })
	for i := range 6 {
		k := key(t, s, int64(i+1))
		loaded(t, s, d, k)
		update(t, s, k, set(1, "x"))
	}
	s.evictOnce() // returns
	if n := s.entryCount.Load(); n != 6 {
		t.Fatalf("entryCount %d, want 6: every entry is dirty", n)
	}
}

// Review Focus 5: hole 2 with the tombstone evicted before the retry.
func TestRetryAfterTombstoneEvicted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		giveUp(t, s, d, newRow(k, 1, 100))()
		del(t, s, k, 0)
		if !evict(t, s, k) {
			t.Fatal("the tombstone was not evicted")
		}
		if r, err := s.Create(context.Background(), newRow(k, 1, 100)); r != nil || err != nil {
			t.Fatalf("retry = %+v, %v; want (nil, nil): the create took effect earlier", r, err)
		}
		if d.Row(k) != nil {
			t.Fatalf("the deleted row is back: %+v", d.Row(k))
		}
		notFound(t, s, k)
	})
}

func TestRunEvictorStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestStore(t, fakedb.New())
		runEvictor(s)() // returns once RunEvictor has
	})
}
