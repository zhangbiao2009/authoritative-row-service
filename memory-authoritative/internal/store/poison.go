package store

import (
	"maps"
	"slices"

	"rowsvc/internal/schema"
)

// Poisoned returns the keys of the quarantined rows (design.md 7.9), in key order: rows whose update was
// acknowledged but the DB definitely rejects. A human corrects them with an Update, which is retried at
// once.
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
