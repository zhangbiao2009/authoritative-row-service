package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

// startFlusher runs RunFlusher with a 1ms FlushInterval; the returned func stops it and waits for it.
func startFlusher(s *Store) (stop func()) {
	s.cfg.FlushInterval = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { s.RunFlusher(ctx) })
	return func() {
		cancel()
		wg.Wait()
	}
}

// Acceptance (design.md 12.2 M2): N goroutines each increment the same column K times while the flusher
// runs; the final value is exactly N*K, in memory and in the DB.
func TestConcurrentIncrementsMatchDB(t *testing.T) {
	const N, K = 16, 200
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	r := testRow(k, 1)
	r.Vals[2] = int64(0)
	d.Put(r)
	stop := startFlusher(s)
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() {
			for range K {
				if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{incr(2, 1)}}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	stop()
	drain(t, s)
	got, err := s.Get(context.Background(), k)
	if err != nil || got.Vals[2] != int64(N*K) || got.Ver != 3+N*K {
		t.Fatalf("Get = %+v, %v; want score %d, Ver %d", got, err, N*K, 3+N*K)
	}
	dbMatches(t, d, got)
	checkInvariants(t, s, d)
}

// Acceptance (design.md 12.2 M2): a stress test with failed and late flushes; afterwards memory and the DB
// match row for row (DbMatchesAbsWhenIdle), and every acknowledged increment is counted.
func TestStressUpdates(t *testing.T) {
	const keys, workers, ops = 4, 64, 500
	d := fakedb.New()
	s := newTestStore(t, d)
	ks := make([]schema.Key, keys)
	for i := range keys {
		ks[i] = key(t, s, int64(i))
		d.Put(testRow(ks[i], int64(i))) // score 10
	}
	var sums [keys]atomic.Int64 // acknowledged increments per key
	var writes, failed, late atomic.Int64
	var lateExecs sync.WaitGroup
	d.Hook = func(_ context.Context, st *fakedb.Stmt) error {
		if st.Kind != fakedb.Update {
			return nil
		}
		n := rand.IntN(10)
		switch writes.Add(1) { // the first two writes fail and are late, so both faults happen in every run
		case 1:
			n = 7
		case 2:
			n = 9
		}
		switch {
		case n < 7:
			return nil
		case n < 9:
			failed.Add(1)
			return errors.New("injected: definitely not executed")
		default:
			late.Add(1)
			lateExecs.Go(func() {
				time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
				st.Exec()
			})
			return fakedb.Ambiguous(st, context.DeadlineExceeded)
		}
	}
	stop := startFlusher(s)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range ops {
				i := rand.IntN(keys)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // L1 EventuallyResponds
				var err error
				switch n := rand.IntN(10); {
				case n < 4:
					_, err = s.Get(ctx, ks[i])
				case n < 7:
					_, err = s.Update(ctx, ks[i], Change{Cols: []ColChange{set(1, fmt.Sprint(rand.IntN(1000)))}})
				default:
					delta := int64(rand.IntN(11) - 5)
					if _, err = s.Update(ctx, ks[i], Change{Cols: []ColChange{incr(2, delta)}}); err == nil {
						sums[i].Add(delta)
					}
				}
				cancel()
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	stop()
	lateExecs.Wait()
	d.Hook = nil
	drain(t, s)
	checkInvariants(t, s, d)
	for i, k := range ks {
		r, err := s.Get(context.Background(), k)
		if err != nil || r.Vals[2] != 10+sums[i].Load() {
			t.Fatalf("key %d: Get = %+v, %v; want score %d", i, r, err, 10+sums[i].Load())
		}
		dbMatches(t, d, r)
	}
	if failed.Load() == 0 || late.Load() == 0 {
		t.Fatalf("no fault was exercised: %d failed, %d late flushes", failed.Load(), late.Load())
	}
	t.Logf("%d flushes, %d failed, %d late", d.Updates(), failed.Load(), late.Load())
}

// opCtx is a client's ctx: 5ms for shortPercent of the calls, to get unknown outcomes and timeouts while
// queued, else 5s (L1 EventuallyResponds).
func opCtx(shortPercent int) (context.Context, context.CancelFunc) {
	if rand.IntN(100) < shortPercent {
		return context.WithTimeout(context.Background(), 5*time.Millisecond)
	}
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// nonVacuous fails t for every counter that is 0: the test did not reach that case (tla-mapping.md section 8).
func nonVacuous(t *testing.T, counters map[string]int64) {
	t.Helper()
	for name, n := range counters {
		if n == 0 {
			t.Errorf("non-vacuity: no %s", name)
		}
	}
	t.Logf("%v", counters)
}

// Acceptance (design.md 12.2 M3): a stress test with fault injection. Afterwards memory and the DB match row
// for row, and the history checks of tla-mapping.md section 5 hold. synctest: the backoffs and timeouts run
// on virtual time; the goroutines still run concurrently.
func TestStressCreateDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const keys, workers, ops = 4, 64, 300
		d := fakedb.New()
		s := newTestStore(t, d, evictHard)
		ks := make([]schema.Key, keys)
		for i := range keys {
			ks[i] = key(t, s, int64(i))
		}
		tag := func(gen int64) string { return fmt.Sprint("g", gen) }
		d.RowCheck = func(r *schema.Row) error { // DbValsBelongToGen
			if r.Vals[1] != tag(r.Gen) {
				return fmt.Errorf("name %v in a row of gen %d", r.Vals[1], r.Gen)
			}
			return nil
		}

		f := startFaults(d, ks)
		watchEvictions(s, f)
		stopEvictor := runEvictor(s)

		// History, recorded by the driver.
		var seq atomic.Int64 // counts successful deletes
		var deleted sync.Map // gen -> seq of its successful delete
		type calls struct {  // the answers to the Create, and to the Delete, calls of one gen
			key                               schema.Key
			unknown, success, failed, refused bool // refused: ErrAlreadyExists or a cross-row rejection
			deleteUnknown                     bool
		}
		var hmu sync.Mutex
		history := map[int64]*calls{}
		var gens atomic.Int64
		gens.Store(1000)
		var recreates, outcomeUnknown, queueTimeouts atomic.Int64
		var deletedOnce [keys]atomic.Bool

		allowed := func(op string, err error) bool {
			switch {
			case classify(err) == unexpected:
				t.Errorf("%s: unexpected error %v", op, err)
				return false
			case errors.Is(err, ErrOutcomeUnknown):
				outcomeUnknown.Add(1)
			case errors.Is(err, context.DeadlineExceeded):
				queueTimeouts.Add(1)
			}
			return true
		}
		noResurrection := func(op string, r *schema.Row, start int64) {
			if v, ok := deleted.Load(r.Gen); ok && v.(int64) <= start {
				t.Errorf("%s returned gen %d, deleted before it started (NoResurrection)", op, r.Gen)
			}
		}

		stop := startFlusher(s)
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				seen := [keys]int64{} // the gen this worker last saw per key; 0 if none
				for range ops {
					think()
					i := rand.IntN(keys)
					k := ks[i]
					get := func() {
						start := seq.Load()
						ctx, cancel := opCtx(0)
						r, err := s.Get(ctx, k)
						cancel()
						if allowed("Get", err) && r != nil {
							noResurrection("Get", r, start)
							seen[i] = r.Gen
						}
					}
					switch op := rand.IntN(4); {
					case op == 0, op == 3 && seen[i] == 0: // a Delete always names its gen
						get()
					case op == 1:
						ch := Change{Gen: seen[i], Cols: []ColChange{incr(2, int64(rand.IntN(11)-5))}}
						if seen[i] != 0 && rand.IntN(2) == 0 {
							ch.Cols = []ColChange{set(1, tag(seen[i]))}
						}
						start := seq.Load()
						ctx, cancel := opCtx(0)
						r, err := s.Update(ctx, k, ch)
						cancel()
						if allowed("Update", err) && r != nil {
							noResurrection("Update", r, start)
							seen[i] = r.Gen
						}
					case op == 2:
						gen := gens.Add(1)
						in := newRow(k, int64(i), gen)
						in.Vals[1] = tag(gen)
						h := &calls{key: k}
						hmu.Lock()
						history[gen] = h
						hmu.Unlock()
						for attempt := 0; ; attempt++ { // retried like the SDK: same gen, after an unknown outcome
							short := 0
							if attempt == 0 {
								short = 50 // unknown outcomes, and so SDK-like retries
							}
							ctx, cancel := opCtx(short)
							r, err := s.Create(ctx, in)
							cancel()
							if !allowed("Create", err) {
								break
							}
							hmu.Lock()
							switch {
							case err == nil:
								h.success = true
							case errors.Is(err, ErrOutcomeUnknown):
								h.unknown = true
							default:
								h.failed = true
								h.refused = h.refused || errors.Is(err, ErrAlreadyExists) || db.IsCrossRow(err)
							}
							hmu.Unlock()
							if err == nil && r != nil {
								seen[i] = gen
								if deletedOnce[i].Load() {
									recreates.Add(1)
								}
							}
							if !errors.Is(err, ErrOutcomeUnknown) || attempt == 3 {
								break
							}
							// Back off before the retry, as the SDK does (7.10): other clients may delete and
							// create the key again meanwhile (hole 6).
							time.Sleep(time.Duration(rand.IntN(400)) * time.Millisecond)
						}
					default:
						gen := seen[i]
						ctx, cancel := opCtx(10)
						err := s.Delete(ctx, k, gen)
						cancel()
						if !allowed("Delete", err) {
							break
						}
						if errors.Is(err, ErrOutcomeUnknown) {
							hmu.Lock()
							history[gen].deleteUnknown = true
							hmu.Unlock()
						}
						if err == nil {
							deletedOnce[i].Store(true)
							deleted.Store(gen, seq.Add(1))
							seen[i] = 0
						}
					}
				}
			})
		}
		wg.Wait()

		// Faults stop: everything must settle.
		f.stop()
		s.inflight.Wait() // L2 BackgroundSettles
		stop()
		stopEvictor()
		drain(t, s)
		checkInvariants(t, s, d)
		// An evicted key loads again below; a load error injected just before the faults stopped is still
		// cached for LoadErrTTL.
		time.Sleep(s.cfg.LoadErrTTL)
		for _, k := range ks { // DbMatchesAbsWhenIdle
			r, err := s.Get(context.Background(), k)
			switch {
			case errors.Is(err, ErrNotFound):
				if d.Row(k) != nil {
					t.Errorf("key %q: absent in memory, %+v in the DB", k, d.Row(k))
				}
			case err != nil:
				t.Errorf("key %q: Get: %v", k, err)
			default:
				dbMatches(t, d, r)
				if d.Deleted(k, r.Gen) {
					t.Errorf("key %q: gen %d was deleted and is back (NoResurrection)", k, r.Gen)
				}
			}
		}
		for gen, h := range history {
			claimed := d.Claimed(h.key, gen)
			// CreateFailureIsFinal. "Already exists" and a cross-row rejection are final even after an earlier
			// unknown answer: op serializes the calls of a gen, so the earlier one had settled (hole 6).
			if claimed && (h.refused || h.failed && !h.unknown && !h.success) {
				t.Errorf("gen %d: its creates were answered failed (refused %v), yet it is in the dedup table (CreateFailureIsFinal)", gen, h.refused)
			}
			// The delete side: a row is deleted only by a Delete call that was answered success or unknown, and
			// a success means the row is gone (hole 4).
			_, deleteSuccess := deleted.Load(gen)
			if dl := d.Deleted(h.key, gen); dl && !deleteSuccess && !h.deleteUnknown || !dl && deleteSuccess {
				t.Errorf("gen %d: deleted in the DB %v, but its Delete calls were answered success %v, unknown %v", gen, dl, deleteSuccess, h.deleteUnknown)
			}
		}

		st := d.Stats()
		counters := map[string]int64{
			"late statements": int64(st.Late), "effective late statements": int64(st.LateEffective),
			"gen claimed": int64(st.GenClaimed), "cross-row rejections": int64(st.CrossRow), "fenced sessions": int64(st.Fenced),
			"deletes": seq.Load(), "re-creates": recreates.Load(), "unknown outcomes": outcomeUnknown.Load(),
			"queue timeouts": queueTimeouts.Load(), "not executed after ambiguous": f.ambThenNotExecuted.Load(),
			"not executed after a fence": f.notExecutedAfterFence.Load(), "late inserts of a deleted gen": f.lateInsertOfDeleted.Load(),
			"retries finding the gen claimed": f.retryFindsOtherLife.Load(),
			"evictions":                       f.evictions.Load(), "reloads after eviction": f.reloads.Load(),
		}
		nonVacuous(t, counters)
	})
}
