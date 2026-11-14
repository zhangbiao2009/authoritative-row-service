// Package store caches one table's rows in memory. PostgreSQL is authoritative; every write reaches it
// before it is acknowledged (design.md 1, 3).
//
// Locks (design.md 6). Only op may be held while taking another lock, and the only other lock is
// shard.mu (I8):
//   - entry.op: per key, held across DB IO; serializes everything that changes the entry. The root lock.
//   - shard.mu: leaf lock; guards the shard's map only. While holding it: take no other lock, do no
//     channel operation, no IO, no logging.
//
// There is no per-entry mu; entry.snap says how it is read and written (I2, I3).
package store

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"rowcache/internal/db"
	"rowcache/internal/schema"
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
	return Config{
		NumShards:  256,
		LoadErrTTL: 100 * time.Millisecond,
	}
}

// Store is one table.
type Store struct {
	schema *schema.Schema // immutable (I10)
	shards []*shard
	db     db.DB
	cfg    Config
	seed   maphash.Seed
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
}

// absent is the snap of a row that definitely does not exist: a tombstone or a negative cache entry.
var absent = &schema.Row{}

// entry is the fixed slot of one key.
type entry struct {
	id schema.Key

	// Read without locks, since a published Row is never modified (I2); written only with op held (I3). nil = not loaded; absent = does not exist;
	// otherwise the row, equal to the DB row while op is free (I4).
	snap atomic.Pointer[schema.Row]

	// op serializes this key's loads and writes and is held across DB IO (the root lock, I8). A channel
	// of capacity 1, so that waiting for it can be cancelled by ctx.
	op chan struct{}

	// Accessed only with op held.
	lastLoadErr   error
	lastLoadErrAt time.Time
}

func (s *Store) shardFor(key schema.Key) *shard {
	return s.shards[maphash.String(s.seed, string(key))&uint64(len(s.shards)-1)]
}

// getOrCreate returns the key's entry, adding one if the map has none (I1).
func (s *Store) getOrCreate(key schema.Key) *entry {
	// TLA: Goc (the double lookup is one atomic step in the model)
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

func (e *entry) unlockOp() { <-e.op }
