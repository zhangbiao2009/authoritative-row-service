package store

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"rowsvc/internal/schema"
)

// Poisoned returns the keys of the quarantined rows (design.md 7.9), in key order: rows whose update was
// acknowledged but the DB definitely rejects. A human decides: correct them with an Update, which is
// retried at once, or DiscardPoisoned.
func (s *Store) Poisoned() []schema.Key {
	var keys []schema.Key
	for _, sh := range s.shards {
		sh.mu.RLock()
		es := slices.Collect(maps.Values(sh.m))
		sh.mu.RUnlock() // before any entry lock: shard.mu is a leaf lock
		for _, e := range es {
			e.mu.Lock()
			if e.poisoned {
				keys = append(keys, e.id)
			}
			e.mu.Unlock()
		}
	}
	slices.Sort(keys)
	return keys
}

// DiscardPoisoned drops the in-memory changes of a quarantined row, acknowledged updates included: the
// next access loads the DB's row (7.9 point 4). ErrNotFound if key is not quarantined. The entry leaves
// memory by the dead protocol (7.7), so memory never holds the discarded values with dirty == 0.
func (s *Store) DiscardPoisoned(ctx context.Context, key schema.Key) error {
	e, err := s.lockOpLive(ctx, key)
	if err != nil {
		return err
	}
	defer e.unlockOp()
	e.mu.Lock()
	ok := e.poisoned
	if ok {
		s.clearDirtyLocked(e)
		s.unpoisonLocked(e)
		e.dead = true // op and mu held (I9)
	}
	e.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: key %q is not quarantined", ErrNotFound, key)
	}
	s.removeLocked(s.shardFor(key), e)
	return nil
}
