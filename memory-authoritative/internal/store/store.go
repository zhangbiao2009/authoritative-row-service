// Package store holds one table's rows in memory and is their authority (design.md 3, 4).
//
// Locks (design.md 6). Only op may be held while taking another lock (I8):
//   - entry.op: per key, held across DB IO; serializes the DB operations of one key (load, flush). The
//     root lock.
//   - entry.mu: leaf lock; guards writes of snap (I3), dirty and queued.
//   - shard.mu: leaf lock; guards the shard's map.
//   - shard.qmu: leaf lock; guards the shard's flush queue.
//
// While holding a leaf lock: take no other lock, do no channel operation, no IO, no logging.
// snap is read without any lock: a published Row is never modified (I2).
package store

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/schema"
)

var (
	ErrNotFound = errors.New("not found")
	ErrInvalid  = schema.ErrInvalid // the same value, so errors.Is works whichever package wrapped it
)

type Config struct {
	NumShards  int           // power of 2
	LoadErrTTL time.Duration // after a load failure, how long the same error is returned directly
}

// DefaultConfig returns the reference values of design.md 11.4.
func DefaultConfig() Config {
	return Config{NumShards: 256, LoadErrTTL: 100 * time.Millisecond}
}

// Store is one table.
type Store struct {
	schema *schema.Schema // immutable (I11)
	shards []*shard
	db     db.DB
	cfg    Config
	seed   maphash.Seed

	dirtyCount atomic.Int64 // entries with dirty != 0; changed only with that entry's mu held
}

func New(sch *schema.Schema, d db.DB, cfg Config) (*Store, error) {
	if cfg.NumShards <= 0 || cfg.NumShards&(cfg.NumShards-1) != 0 {
		return nil, fmt.Errorf("NumShards %d is not a positive power of 2", cfg.NumShards)
	}
	s := &Store{schema: sch, db: d, cfg: cfg, seed: maphash.MakeSeed(), shards: make([]*shard, cfg.NumShards)}
	for i := range s.shards {
		s.shards[i] = &shard{m: make(map[schema.Key]*entry)}
	}
	return s, nil
}

type shard struct {
	mu sync.RWMutex          // leaf lock: guards m only
	m  map[schema.Key]*entry // at most one entry per key (I1)

	qmu sync.Mutex // leaf lock: guards queue only
	// Entries waiting to be flushed. A slice, so appending never blocks; an entry is in it at most once,
	// because only the caller that sets entry.queued appends it.
	queue []*entry
}

// absent is the snap of a row that definitely does not exist: a tombstone or a negative cache entry.
var absent = &schema.Row{}

// entry is the fixed slot of one key.
type entry struct {
	id schema.Key

	// Read without locks; written only with mu held (I3). nil = not loaded; absent = does not exist.
	snap atomic.Pointer[schema.Row]

	// op serializes this key's DB operations and is held across DB IO. A channel of capacity 1, so
	// that waiting for it can be cancelled by ctx.
	op chan struct{}

	// mu is a leaf lock (I8): hold it only for in-memory work.
	mu     sync.Mutex
	dirty  uint64 // columns changed in memory and not yet written to the DB (schema.ColBit); mu
	queued bool   // in its shard's flush queue, or about to be appended to it; mu

	// Accessed only with op held.
	lastLoadErr   error
	lastLoadErrAt time.Time
}

func (s *Store) shardFor(key schema.Key) *shard {
	return s.shards[maphash.String(s.seed, string(key))&uint64(len(s.shards)-1)]
}

// getOrCreate returns the key's entry, adding one if the map has none (I1).
func (s *Store) getOrCreate(key schema.Key) *entry {
	// TLA: Goc
	sh := s.shardFor(key)
	sh.mu.RLock()
	e := sh.m[key]
	sh.mu.RUnlock()
	if e != nil {
		return e
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e = sh.m[key]; e == nil { // another goroutine may have added it between the two locks
		e = &entry{id: key, op: make(chan struct{}, 1)}
		sh.m[key] = e
	}
	return e
}

func (e *entry) lockOp(ctx context.Context) error {
	select {
	case e.op <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *entry) tryLockOp() bool {
	select {
	case e.op <- struct{}{}:
		return true
	default:
		return false
	}
}

func (e *entry) unlockOp() { <-e.op }

// markDirtyLocked adds mask (non-zero) to e's dirty columns. The caller holds e.mu.
func (s *Store) markDirtyLocked(e *entry, mask uint64) {
	if e.dirty == 0 {
		s.dirtyCount.Add(1)
	}
	e.dirty |= mask
}

// clearDirtyLocked marks e clean. The caller holds e.mu.
func (s *Store) clearDirtyLocked(e *entry) {
	if e.dirty != 0 {
		s.dirtyCount.Add(-1)
	}
	e.dirty = 0
}

// enqueue appends e to its shard's flush queue. The caller holds no lock: it is called after e.mu is
// released, because qmu is a leaf lock too (I8).
func (s *Store) enqueue(e *entry) {
	sh := s.shardFor(e.id)
	sh.qmu.Lock()
	sh.queue = append(sh.queue, e)
	sh.qmu.Unlock()
}
