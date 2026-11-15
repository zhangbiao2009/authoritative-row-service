package store

import (
	"cmp"
	"context"
	"slices"
	"time"
)

const sampleK = 64 // entries sampled per round (design.md 7.7)

// RunEvictor keeps nowSec current and, every EvictInterval, removes idle entries and, above MaxEntries,
// the least recently accessed ones, until ctx is cancelled. The caller starts it.
func (s *Store) RunEvictor(ctx context.Context) {
	clock := time.NewTicker(time.Second)
	defer clock.Stop()
	sweep := time.NewTicker(s.cfg.EvictInterval)
	defer sweep.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-clock.C:
			s.nowSec.Store(time.Now().Unix())
		case <-sweep.C:
			s.nowSec.Store(time.Now().Unix()) // the sweep judges idleness by the current second
			s.evictOnce()
		}
	}
}

// evictOnce runs one sweep of every shard, then capacity eviction if the table is above MaxEntries.
func (s *Store) evictOnce() {
	for _, sh := range s.shards {
		s.idleSweepShard(sh)
	}
	n, high := s.entryCount.Load(), int64(s.cfg.MaxEntries)
	if n <= high {
		return
	}
	shards := int64(len(s.shards))
	quota := (n - high*9/10 + shards - 1) / shards // down to the low-water mark, an equal share per shard
	for _, sh := range s.shards {
		s.capacityEvictShard(sh, int(quota))
	}
}

// idleSweepShard removes entries idle beyond their TTL, Redis style: a random sample, and another round
// right away while more than a quarter of the sample was expired.
func (s *Store) idleSweepShard(sh *shard) {
	now := s.nowSec.Load()
	expired := func(e *entry) bool { return s.idleExpired(e, now) }
	for range s.cfg.MaxSweepRounds {
		sampled := 0
		cands := make([]*entry, 0, sampleK)
		sh.mu.RLock()
		for _, e := range sh.m { // map iteration starts at a random point: the first K are a random sample
			if expired(e) {
				cands = append(cands, e)
			}
			if sampled++; sampled == sampleK {
				break
			}
		}
		sh.mu.RUnlock() // before any entry lock: shard.mu is a leaf lock
		s.fail("evict-sample", nil)
		for _, e := range cands {
			s.tryEvict(sh, e, expired)
		}
		if len(cands)*4 <= sampled || sampled < sampleK { // a smaller sample was the whole shard: nothing new to find
			return
		}
	}
}

// idleExpired reports whether e has been idle beyond its TTL: IdleTTL for a row, NegativeIdleTTL for a
// tombstone, a negative-cache entry or a failed load.
func (s *Store) idleExpired(e *entry, now int64) bool {
	ttl := s.cfg.IdleTTL
	if r := e.snap.Load(); r == nil || r == absent {
		ttl = s.cfg.NegativeIdleTTL
	}
	return now-e.lastAccess.Load() > int64(ttl/time.Second)
}

// capacityEvictShard removes up to quota of the shard's least recently accessed entries, from samples. Each
// sample gives up at most its oldest quarter, so however large the quota, no entry with sampleK*3/4 or more
// newer entries in the shard is chosen; a sample smaller than sampleK is the whole shard, and gives up its
// oldest. A sample that yields nothing (the oldest all dirty or busy) ends it: backpressure is what limits
// growth then. Every round but the last evicts at least one entry, so there are at most quota rounds.
func (s *Store) capacityEvictShard(sh *shard, quota int) {
	type cand struct {
		e  *entry
		at int64 // lastAccess when sampled, so that the order holds while touches go on
	}
	for {
		cands := make([]cand, 0, sampleK)
		sh.mu.RLock()
		for _, e := range sh.m {
			if cands = append(cands, cand{e, e.lastAccess.Load()}); len(cands) == sampleK {
				break
			}
		}
		sh.mu.RUnlock()
		slices.SortFunc(cands, func(a, b cand) int { return cmp.Compare(a.at, b.at) })
		oldest := cands
		if len(cands) == sampleK {
			oldest = cands[:sampleK/4]
		}
		evicted := 0
		for _, c := range oldest {
			if evicted == quota {
				break
			}
			if s.tryEvict(sh, c.e, always) {
				evicted++
			}
		}
		quota -= evicted
		if quota <= 0 || evicted == 0 || len(cands) < sampleK { // done; nothing evictable; or that was the whole shard
			return
		}
	}
}

// always is the eligible condition of capacity eviction: any clean, idle-or-not entry.
func always(*entry) bool { return true }

// tryEvict removes e from memory by the dead protocol (design.md 7.7). It gives up if e's op is busy (a DB
// operation in progress), and re-checks eligible under mu: e may have been accessed since it was chosen.
// The only safety condition is dirty == 0 with op held, so that the DB holds exactly what memory holds (I4,
// I10); a later access is then a miss that loads the same state back. It reports whether e was removed.
func (s *Store) tryEvict(sh *shard, e *entry, eligible func(*entry) bool) bool {
	if !e.tryLockOp() {
		return false
	}
	defer e.unlockOp()
	// TLA: V_evict
	e.mu.Lock()
	ok := !e.dead && e.dirty == 0 && eligible(e) // eligible only reads atomics: pure in-memory work under mu
	if ok {
		e.dead = true // op and mu held (I9)
	}
	e.mu.Unlock() // before shard.mu: mu stays a leaf lock
	if !ok {
		return false
	}
	s.fail("evict-dead", e)
	s.removeLocked(sh, e)
	return true
}

// removeLocked deletes e from the shard's map. The caller holds op and has set dead (7.7).
func (s *Store) removeLocked(sh *shard, e *entry) {
	// TLA: V_remove
	sh.mu.Lock()
	if sh.m[e.id] == e {
		delete(sh.m, e.id)
		s.entryCount.Add(-1)
	}
	sh.mu.Unlock()
}
