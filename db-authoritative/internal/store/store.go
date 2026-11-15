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

	ErrOutcomeUnknown = errors.New("outcome unknown") // the caller's ctx expired before the outcome was settled
	ErrInternal       = errors.New("internal")        // an invariant is broken
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

	inflight sync.WaitGroup  // runSync goroutines
	bgCtx    context.Context // for DB calls that must finish whatever the caller does (design.md 7.1)
}

func New(sch *schema.Schema, d db.DB, cfg Config) (*Store, error) {
	if cfg.NumShards <= 0 || cfg.NumShards&(cfg.NumShards-1) != 0 {
		return nil, fmt.Errorf("NumShards %d is not a positive power of 2", cfg.NumShards)
	}
	s := &Store{schema: sch, db: d, cfg: cfg, seed: maphash.MakeSeed(), bgCtx: context.Background(), shards: make([]*shard, cfg.NumShards)}
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
	if err := ctx.Err(); err != nil {
		return err // already expired: the request has not started, and select would pick at random
	}
	select {
	case e.op <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *entry) unlockOp() { <-e.op }

// lockOpLive acquires op on the key's entry. (Until M5 no entry is ever dead, so this is getOrCreate + lockOp.)
// TLA: X_lock; a ctx error while queued is X_qtimeout.
func (s *Store) lockOpLive(ctx context.Context, key schema.Key) (*entry, error) {
	e := s.getOrCreate(key)
	if err := e.lockOp(ctx); err != nil {
		return nil, err // timed out while queued: this request did nothing
	}
	return e, nil
}

// runSync runs fn in a background goroutine that already holds op, and releases op only when fn returns,
// that is, when the outcome is settled. If the client's ctx expires first, the client gets ErrOutcomeUnknown
// and fn keeps running. Used by Create, Update and Delete.
func runSync[T any](ctx context.Context, s *Store, e *entry, fn func(*entry) (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	s.inflight.Add(1)
	go func() {
		defer s.inflight.Done()
		// No defer for op or the answer: a panic in fn crashes the process with op held, instead of
		// releasing op and sending a zero result (which Create would read as success).
		v, err := fn(e)
		// Release op first, then answer: a caller that has its answer always finds op free (TLA: FinishOp).
		e.unlockOp()
		done <- result{v, err}
	}()
	select {
	case r := <-done:
		return r.v, r.err
	case <-ctx.Done(): // TLA: X_wait, the client giving up
		var zero T
		return zero, ErrOutcomeUnknown
	}
}

// backoff waits before retry number attempt: 10 ms doubled per attempt, at most 1 s. M6 makes it return an
// error once bgCtx is cancelled (shutdown).
func (s *Store) backoff(attempt int) {
	time.Sleep(min(10*time.Millisecond<<min(attempt, 7), time.Second))
}

// fenceAndReadLocked settles a statement whose outcome is ambiguous (design.md 7.3, without the owner
// check: M6). Requires op (I5). It terminates the statement's session and confirms it is gone, then reads
// the row and stores it in the snapshot, so that I4 holds again when op is released. Both steps are retried
// until they succeed: until the fence is confirmed the statement may still execute, so op must not be
// released. If the statement did not take effect, the row read equals the snapshot already held.
func (s *Store) fenceAndReadLocked(e *entry, amb error) (*schema.Row, error) {
	// TLA: B_2, the ambiguous branch (the connection fence)
	for attempt := 0; s.db.FenceSession(s.bgCtx, db.SessionOf(amb)) != nil; attempt++ {
		s.backoff(attempt)
	}
	// TLA: B_3 (the read). M6 adds the owner check after it.
	for attempt := 0; ; attempt++ {
		r, err := s.db.Select(s.bgCtx, e.id)
		if errors.Is(err, db.ErrNoRows) {
			r, err = absent, nil
		}
		if err == nil {
			e.snap.Store(r)
			return r, nil
		}
		s.backoff(attempt)
	}
}
