package store

import (
	"context"
	"maps"
	"reflect"
	"slices"
	"testing"

	"rowsvc/internal/db/fakedb"
)

// checkInvariants checks the properties of tla-mapping.md section 5 for every entry (M2, M5). Call it at a
// quiescent point: no flush round, Update or load running.
func checkInvariants(t *testing.T, s *Store, d *fakedb.DB) {
	t.Helper()
	for _, sh := range s.shards {
		sh.qmu.Lock()
		queue := slices.Clone(sh.queue)
		sh.qmu.Unlock()
		inQueue := make(map[*entry]bool, len(queue))
		for _, e := range queue {
			inQueue[e] = true
		}
		sh.mu.RLock()
		inMap := maps.Clone(sh.m)
		sh.mu.RUnlock()
		entries := slices.Collect(maps.Values(inMap))
		for _, e := range queue { // a dead entry may still be queued: the flusher drops it
			e.mu.Lock()
			dead, dirty := e.dead, e.dirty
			e.mu.Unlock()
			if !dead && inMap[e.id] != e {
				t.Errorf("key %q: a queued live entry is not the map's entry (LiveEntryInMap, I1)", e.id)
			}
			if dead && dirty != 0 {
				t.Errorf("key %q: a dead entry has dirty %#x (NoDirtyEviction, I10)", e.id, dirty)
			}
		}
		for _, e := range entries {
			if !e.tryLockOp() {
				t.Errorf("key %q: op is still held (NoLockLeak)", e.id)
				continue
			}
			e.mu.Lock()
			r, dirty, queued, dead := e.snap.Load(), e.dirty, e.queued, e.dead
			e.mu.Unlock()
			e.unlockOp()
			if dead {
				t.Errorf("key %q: a dead entry is still in the map (LiveEntryInMap, I1)", e.id)
			}
			if dirty != 0 && !queued {
				t.Errorf("key %q: dirty %#x but not queued (DirtyImpliesQueued)", e.id, dirty)
			}
			if queued && !inQueue[e] {
				t.Errorf("key %q: queued but not in its shard's queue (QueuedIsTracked)", e.id)
			}
			dbr := d.Row(e.id)
			switch {
			case r == nil:
			case r == absent:
				if dbr != nil {
					t.Errorf("key %q: absent in memory, %+v in the DB (QuiescentConsistent)", e.id, dbr)
				}
			case dirty == 0 && !reflect.DeepEqual(dbr, r):
				t.Errorf("key %q: clean, but memory %+v, DB %+v (QuiescentConsistent, I4)", e.id, r, dbr)
			case dbr != nil && dbr.Gen == r.Gen && dbr.Ver > r.Ver:
				t.Errorf("key %q: DB ver %d ahead of memory ver %d (DbNotAhead)", e.id, dbr.Ver, r.Ver)
			}
		}
	}
}

// drain runs flush rounds until no entry is dirty or queued (L3 EventuallyFlushed).
func drain(t *testing.T, s *Store) {
	t.Helper()
	for range 100 {
		if queueLen(s) == 0 && s.dirtyCount.Load() == 0 {
			return
		}
		flushAll(s)
	}
	t.Fatalf("not flushed after 100 rounds: queue %d, dirtyCount %d", queueLen(s), s.dirtyCount.Load())
}

// flushAll runs one flush round over every shard.
func flushAll(s *Store) {
	for _, sh := range s.shards {
		s.flushShard(context.Background(), sh)
	}
}
