// Package store holds one table's rows in memory and is their authority (design.md 3, 4).
//
// Locks (design.md 6). Only op may be held while taking another lock (I8):
//   - entry.op: per key, held across DB IO; serializes the DB operations of one key (load, create, delete,
//     flush, eviction). The root lock.
//   - entry.mu: leaf lock; guards writes of snap (I3), dirty and queued.
//   - shard.mu: leaf lock; guards the shard's map. Taken with op held (eviction), never with mu held.
//   - shard.qmu: leaf lock; guards the shard's flush queue.
//
// The dead protocol (design.md 7.7): the remover holds op and mu, finds dirty == 0, sets dead, then deletes
// the entry from the map. Whoever takes op or mu checks dead first and, if it is set, looks the key up
// again. Lock-free readers (Get) do not check it: they read the value the entry had when it was removed.
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
	ErrBusy           = errors.New("busy")            // backpressure (11.1): the update was not applied
	ErrInvalid        = schema.ErrInvalid             // the same value, so errors.Is works whichever package wrapped it
	ErrInternal       = errors.New("internal")        // an invariant is broken
)

type Config struct {
	NumShards      int           // power of 2
	LoadErrTTL     time.Duration // after a load failure, how long the same error is returned directly
	FlushInterval  time.Duration // how often RunFlusher writes dirty rows back
	DBTimeout      time.Duration // timeout for each DB call on the Go side (design.md 11.4)
	OpFenceTimeout time.Duration // how long one FenceSessions call may wait for the sessions to exit (7.4)
	FlushBatchMax  int           // rows per UpdateBatch, at most (design.md 7.6)

	// Backpressure (design.md 11.1).
	MaxDirty    int           // updates of clean rows get ErrBusy while this many rows are dirty
	MaxDirtyAge time.Duration // every update gets ErrBusy while the oldest dirty row is older; 0: no limit

	// Eviction (design.md 7.7). TTLs count in whole seconds; 0 means "not accessed in the current second".
	IdleTTL         time.Duration // a loaded row idle this long is removed from memory
	NegativeIdleTTL time.Duration // the same for tombstones, negative-cache entries and failed loads
	EvictInterval   time.Duration // how often RunEvictor sweeps
	MaxSweepRounds  int           // sample rounds per shard per sweep, at most
	MaxEntries      int           // high-water mark of entries; capacity eviction brings them to 90% of it
}

// DefaultConfig returns the reference values of design.md 11.4.
func DefaultConfig() Config {
	return Config{
		NumShards:      256,
		LoadErrTTL:     100 * time.Millisecond,
		FlushInterval:  200 * time.Millisecond,
		DBTimeout:      3 * time.Second,
		OpFenceTimeout: 10 * time.Second,
		FlushBatchMax:  500,
		MaxDirty:       100_000, // 11.4: TBD; a tenth of MaxEntries
		MaxDirtyAge:    30 * time.Second,

		IdleTTL:         10 * time.Minute, // 11.4: TBD
		NegativeIdleTTL: time.Minute,      // 11.4: 1-5 minutes
		EvictInterval:   time.Second,
		MaxSweepRounds:  16,
		MaxEntries:      1_000_000, // 11.4: TBD, set by the memory budget
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
	entryCount atomic.Int64   // entries in the maps
	m          metrics        // design.md 11.2
	// The oldest dirty row that is not quarantined is older than MaxDirtyAge (11.1). Refreshed by the
	// flusher after each round.
	tooOld atomic.Bool
	nowSec atomic.Int64 // coarse clock in Unix seconds, kept current by RunEvictor; cheaper than time.Now per access

	// failpoint, if set, runs at the named points of a gap that is not a DB call (tla-mapping.md 3.1). Tests
	// only; nil in production.
	failpoint func(at string, e *entry)
}

// fail runs the failpoint at, if there is one.
func (s *Store) fail(at string, e *entry) {
	if s.failpoint != nil {
		s.failpoint(at, e)
	}
}

func New(sch *schema.Schema, d db.DB, cfg Config) (*Store, error) {
	if cfg.NumShards <= 0 || cfg.NumShards&(cfg.NumShards-1) != 0 {
		return nil, fmt.Errorf("NumShards %d is not a positive power of 2", cfg.NumShards)
	}
	if cfg.FlushInterval <= 0 || cfg.DBTimeout <= 0 || cfg.OpFenceTimeout <= 0 {
		return nil, fmt.Errorf("FlushInterval %v, DBTimeout %v and OpFenceTimeout %v must be positive",
			cfg.FlushInterval, cfg.DBTimeout, cfg.OpFenceTimeout)
	}
	if cfg.FlushBatchMax <= 0 || cfg.MaxDirty <= 0 || cfg.MaxDirtyAge < 0 {
		return nil, fmt.Errorf("FlushBatchMax %d and MaxDirty %d must be positive, MaxDirtyAge %v not negative",
			cfg.FlushBatchMax, cfg.MaxDirty, cfg.MaxDirtyAge)
	}
	if cfg.IdleTTL < 0 || cfg.NegativeIdleTTL < 0 || cfg.EvictInterval <= 0 || cfg.MaxSweepRounds <= 0 || cfg.MaxEntries <= 0 {
		return nil, fmt.Errorf("eviction config: TTLs must not be negative, EvictInterval %v, MaxSweepRounds %d and MaxEntries %d must be positive",
			cfg.EvictInterval, cfg.MaxSweepRounds, cfg.MaxEntries)
	}
	s := &Store{schema: sch, db: d, cfg: cfg, seed: maphash.MakeSeed(), shards: make([]*shard, cfg.NumShards)}
	s.nowSec.Store(time.Now().Unix())
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

	// UnixNano of the earliest dirtySince among the shard's dirty rows, as its last flush found it; 0 if
	// none. Written by that flush only.
	oldestDirty atomic.Int64
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
	// When dirty last went from 0 to non-zero; mu. A failed flush keeps the earlier time (11.1).
	dirtySince time.Time
	// The DB definitely rejected its flush: quarantined (7.9). It stays dirty, is written alone, and is
	// left out of the age of 11.1; mu.
	poisoned bool
	// dead: removed from the map, or about to be (I1). Written with op and mu held, read with either (I9).
	dead bool

	lastAccess atomic.Int64 // nowSec of the last access; read without locks by the evictor

	// Accessed only with op held.
	lastLoadErr   error
	lastLoadErrAt time.Time
	poisonVer     int64     // ver of the row the DB rejected; a newer one is retried at once
	poisonRetryAt time.Time // when the same row is tried again
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
		e.lastAccess.Store(s.nowSec.Load()) // a new entry is not idle from birth
		sh.m[key] = e
		s.entryCount.Add(1)
	}
	return e
}

// touch records an access. It writes only when the second has changed, so that hits do not all write the
// same cache line.
func (s *Store) touch(e *entry) {
	if now := s.nowSec.Load(); e.lastAccess.Load() != now {
		e.lastAccess.Store(now)
	}
}

// lockOpLive returns key's entry with op held and dead false (design.md 7.1).
func (s *Store) lockOpLive(ctx context.Context, key schema.Key) (*entry, error) {
	for {
		e := s.getOrCreate(key)
		if err := e.lockOp(ctx); err != nil {
			return nil, err
		}
		if !e.dead { // op is enough to read dead (I9)
			return e, nil
		}
		e.unlockOp() // removed: the map holds another entry for the key, or will
	}
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
		e.dirtySince = time.Now() // in-memory work: allowed under mu
	}
	e.dirty |= mask
}

func (s *Store) clearDirtyLocked(e *entry) {
	if e.dirty != 0 {
		s.dirtyCount.Add(-1)
	}
	e.dirty = 0
}

// unpoisonLocked takes e out of quarantine: the DB holds its row, or the row is gone.
func (s *Store) unpoisonLocked(e *entry) {
	if e.poisoned {
		e.poisoned = false
		s.m.poisoned.Add(-1)
	}
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
