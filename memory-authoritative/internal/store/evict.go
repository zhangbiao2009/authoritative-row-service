package store

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
	// TLA: V_remove. Still holding op.
	sh.mu.Lock()
	if sh.m[e.id] == e {
		delete(sh.m, e.id)
		s.entryCount.Add(-1)
	}
	sh.mu.Unlock()
	return true
}
