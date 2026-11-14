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
	ErrNotFound       = errors.New("not found")
	ErrAlreadyExists  = errors.New("already exists")
	ErrOutcomeUnknown = errors.New("outcome unknown") // the caller's ctx expired; the create or delete goes on (7.4)
	ErrInvalid        = schema.ErrInvalid             // the same value, so errors.Is works whichever package wrapped it
	ErrInternal       = errors.New("internal")        // an invariant is broken
)

type Config struct {
	NumShards      int           // power of 2
	LoadErrTTL     time.Duration // after a load failure, how long the same error is returned directly
	FlushInterval  time.Duration // how often RunFlusher writes dirty rows back
	DBTimeout      time.Duration // timeout for each DB call on the Go side (design.md 11.4)
	OpFenceTimeout time.Duration // how long one FenceSessions call may wait for the sessions to exit (7.4)
}

// DefaultConfig returns the reference values of design.md 11.4.
func DefaultConfig() Config {
	return Config{
		NumShards:      256,
		LoadErrTTL:     100 * time.Millisecond,
		FlushInterval:  200 * time.Millisecond,
		DBTimeout:      3 * time.Second,
		OpFenceTimeout: 10 * time.Second,
	}
}

// Store is one table.
type Store struct {
	schema *schema.Schema // immutable (I11)
	shards []*shard
	db     db.DB
	cfg    Config
	seed   maphash.Seed

	dirtyCount atomic.Int64   // entries with dirty != 0; changed only with that entry's mu held
	inflight   sync.WaitGroup // runSync goroutines: creates and deletes still running
}

func New(sch *schema.Schema, d db.DB, cfg Config) (*Store, error) {
	if cfg.NumShards <= 0 || cfg.NumShards&(cfg.NumShards-1) != 0 {
		return nil, fmt.Errorf("NumShards %d is not a positive power of 2", cfg.NumShards)
	}
	if cfg.FlushInterval <= 0 || cfg.DBTimeout <= 0 || cfg.OpFenceTimeout <= 0 {
		return nil, fmt.Errorf("FlushInterval %v, DBTimeout %v and OpFenceTimeout %v must be positive",
			cfg.FlushInterval, cfg.DBTimeout, cfg.OpFenceTimeout)
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

// markDirtyLocked adds mask (non-zero) to e's dirty columns.
func (s *Store) markDirtyLocked(e *entry, mask uint64) {
	if e.dirty == 0 {
		s.dirtyCount.Add(1)
	}
	e.dirty |= mask
}

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

// dbCtx bounds one DB call made by background work, which has no caller's ctx (decision 5 of the M3 plan;
// the service-lifetime ctx comes in M6).
func (s *Store) dbCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.cfg.DBTimeout)
}

const (
	backoffMin = 10 * time.Millisecond
	backoffMax = time.Second
)

// backoff waits before the next attempt of an until-sure loop: backoffMin doubled per attempt, at most
// backoffMax.
func backoff(attempt int) {
	d := backoffMin
	for range attempt {
		if d >= backoffMax {
			break
		}
		d *= 2
	}
	time.Sleep(min(d, backoffMax))
}
