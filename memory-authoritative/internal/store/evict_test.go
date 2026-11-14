package store

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

func always(*entry) bool { return true }

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
	if r := d.Row(k); r.Vals[1] != "x" {
		t.Fatalf("DB row %+v, want name x", r)
	}
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
		e := s.getOrCreate(k) // snap nil
		if err := e.lockOp(context.Background()); err != nil {
			t.Fatal(err)
		}
		got := make(chan *schema.Row, 1)
		go func() { r, _ := s.Get(context.Background(), k); got <- r }()
		synctest.Wait() // the Get queues on op
		// remove e as tryEvict does, while holding its op
		e.mu.Lock()
		e.dead = true
		e.mu.Unlock()
		sh := s.shardFor(k)
		sh.mu.Lock()
		delete(sh.m, k)
		s.entryCount.Add(-1)
		sh.mu.Unlock()
		e.unlockOp()
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
