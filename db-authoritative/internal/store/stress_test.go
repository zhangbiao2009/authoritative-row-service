package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"rowcache/internal/db"
	"rowcache/internal/db/fakedb"
	"rowcache/internal/schema"
)

// Concurrent Gets and Updates on a few keys, no faults. Every answer must be one the model allows, every
// operation must return (EventuallyResponds), and at the end memory and DB agree and each column 2 is
// exactly the increments that ran.
func TestStressUpdates(t *testing.T) {
	const goroutines, ops = 64, 200
	const staleGen = 999
	d := fakedb.New()
	s := newTestStore(t, d)
	keys := make([]schema.Key, 4)
	for i := range keys {
		keys[i] = key(t, s, int64(i+1))
		d.Put(testRow(keys[i], int64(i+1)))
	}
	// Slow about one Update in eight, so the 1 ms ctx sometimes expires while the write is in flight.
	d.Hook = func(_ context.Context, st *fakedb.Stmt) error {
		if st.Kind == fakedb.Update && rand.IntN(8) == 0 {
			time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		}
		return nil
	}
	applied := make([]atomic.Int64, len(keys)) // INCRs answered OK or ErrOutcomeUnknown: each ran (no faults)
	var gaveUp atomic.Int64                    // all Updates answered ErrOutcomeUnknown
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(g), 1))
			for n := 0; n < ops; n++ {
				ki := rng.IntN(len(keys))
				short := rng.IntN(10) == 0
				timeout := 5 * time.Second
				if short {
					timeout = time.Millisecond
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				start := time.Now()
				if rng.IntN(2) == 0 {
					r, err := s.Get(ctx, keys[ki])
					if err == nil && r == nil {
						t.Error("Get returned no row and no error")
					}
					if err != nil && !(short && errors.Is(err, context.DeadlineExceeded)) {
						t.Errorf("Get: %v", err)
					}
				} else {
					isIncr := rng.IntN(2) == 0
					var ch Change
					if isIncr {
						ch = Change{Cols: []ColChange{incr(2, 1)}}
					} else {
						ch = Change{Cols: []ColChange{set(1, "v")}}
						if rng.IntN(10) == 0 {
							ch.Gen = staleGen // stale: only on SET, since an INCR answered ErrOutcomeUnknown is counted as applied
						}
					}
					_, err := s.Update(ctx, keys[ki], ch)
					switch {
					case err == nil:
						if isIncr {
							applied[ki].Add(1)
						}
					case errors.Is(err, ErrOutcomeUnknown):
						gaveUp.Add(1)
						if isIncr {
							applied[ki].Add(1)
						}
					case errors.Is(err, ErrNotFound) && ch.Gen == staleGen:
					case errors.Is(err, context.DeadlineExceeded) && short:
					default:
						t.Errorf("Update: %v", err)
					}
				}
				cancel()
				if time.Since(start) > 5*time.Second {
					t.Errorf("an operation took %v (EventuallyResponds)", time.Since(start))
				}
			}
		})
	}
	wg.Wait()
	s.inflight.Wait()
	if gaveUp.Load() == 0 {
		t.Error("no Update was answered ErrOutcomeUnknown: the stress test never reached W_ClientGaveUp")
	}
	checkInvariants(t, s, d)
	for i, k := range keys {
		want := 10 + applied[i].Load()
		if got := d.Row(k).Vals[2]; got != want {
			t.Errorf("key %d: DB score = %v, want %d", i, got, want)
		}
	}
}

// failed reports whether err is a write answer of design.md 7.3 that means "did not happen, and never will":
// not executed, rejected, or timed out while queued for op. An ambiguity reaching the caller is not one.
func failed(err error) bool {
	return !db.IsAmbiguous(err) &&
		(errors.Is(err, db.ErrNotExecuted) || errors.Is(err, db.ErrRejected) || errors.Is(err, context.DeadlineExceeded))
}

// A create the stress test sent: the key's index and the gen; first is false for an SDK retry.
type createReq struct {
	ki    int
	gen   int64
	first bool
}

// Get, Create, Update and Delete on a few keys under every fault (design.md 12.1 item 3). Every answer must
// be one of 7.3; at the end, once the faults stop and the background writes settle (BackgroundSettles), the
// invariants hold, every answer agrees with the DB, and the counters show the hard cases were reached.
func TestStressAll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const goroutines, ops = 64, 100
		d := fakedb.New()
		d.RowCheck = func(r *schema.Row) error { // DbValsBelongToGen: column 1 starts with the row's gen
			if v, _ := r.Vals[1].(string); !strings.HasPrefix(v, strconv.FormatInt(r.Gen, 10)+"/") {
				return fmt.Errorf("column 1 is %q", v)
			}
			return nil
		}
		s := newTestStore(t, d)
		keys := make([]schema.Key, 4) // initially absent
		for i := range keys {
			keys[i] = key(t, s, int64(i+1))
		}
		f := startFaults(d, keys)

		var nextGen atomic.Int64 // every create's gen is new, except an SDK retry's
		deleted := make([]atomic.Bool, len(keys))
		var deletes, recreates, gaveUp, rejected, claimedNoRow, updates atomic.Int64
		countRejected := func(err error) {
			if errors.Is(err, db.ErrRejected) {
				rejected.Add(1)
			}
		}
		var mu sync.Mutex
		var okVals, failedVals []string // SET values answered OK / with a failure
		var failedCreates []createReq   // first attempts answered with a failure
		var wg sync.WaitGroup
		for g := range goroutines {
			wg.Go(func() {
				rng := rand.New(rand.NewPCG(uint64(g), 2))
				lastGen := make([]int64, len(keys)) // per key, the gen this goroutine last saw
				var retry *createReq                // a create whose first attempt answered ErrOutcomeUnknown
				for n := range ops {
					ki := rng.IntN(len(keys))
					timeout := 5 * time.Second
					if rng.IntN(10) == 0 {
						timeout = time.Millisecond
					}
					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					start := time.Now()
					op := rng.IntN(100)
					if op >= 50 && op < 80 && lastGen[ki] == 0 {
						op = 0 // an Update tags its value with a gen this goroutine saw: Get one first
					}
					switch {
					case op < 30:
						r, err := s.Get(ctx, keys[ki])
						switch {
						case err == nil && r != nil:
							lastGen[ki] = r.Gen
						case db.IsAmbiguous(err):
							t.Errorf("Get: %v", err)
						case errors.Is(err, ErrNotFound), errors.Is(err, db.ErrNotExecuted), errors.Is(err, context.DeadlineExceeded):
						default:
							t.Errorf("Get = %+v, %v", r, err)
						}
					case op < 50:
						c := createReq{ki, nextGen.Add(1), true}
						if retry != nil {
							c, retry = *retry, nil
							c.first = false
						}
						in := &schema.Row{Key: keys[c.ki], Gen: c.gen, Vals: []any{int64(c.ki + 1), fmt.Sprintf("%d/create", c.gen), int64(10)}}
						r, err := s.Create(ctx, in)
						switch {
						case err == nil && r != nil:
							if r.Gen != c.gen {
								t.Errorf("Create gen %d = %+v", c.gen, r)
							}
							lastGen[c.ki] = r.Gen
							if deleted[c.ki].Load() {
								recreates.Add(1)
							}
						case err == nil: // created earlier, deleted since: only a retry can be told that
							claimedNoRow.Add(1)
							if c.first {
								t.Errorf("first create of gen %d answered (nil, nil)", c.gen)
							}
						case errors.Is(err, ErrOutcomeUnknown):
							gaveUp.Add(1)
							if c.first {
								retry = &c
							}
						case errors.Is(err, ErrAlreadyExists), failed(err):
							countRejected(err)
							if c.first {
								mu.Lock()
								failedCreates = append(failedCreates, c)
								mu.Unlock()
							}
						default:
							t.Errorf("Create gen %d = %+v, %v", c.gen, r, err)
						}
					case op < 80:
						val := fmt.Sprintf("%d/%d/%d", lastGen[ki], g, n) // unique, tagged with the gen
						r, err := s.Update(ctx, keys[ki], Change{Cols: []ColChange{set(1, val)}, Gen: lastGen[ki]})
						switch {
						case err == nil:
							if r.Gen != lastGen[ki] || r.Vals[1] != val {
								t.Errorf("Update to %q = %+v", val, r)
							}
							updates.Add(1)
							mu.Lock()
							okVals = append(okVals, val)
							mu.Unlock()
						case errors.Is(err, ErrOutcomeUnknown):
							gaveUp.Add(1)
						case errors.Is(err, ErrNotFound), failed(err):
							countRejected(err)
							mu.Lock()
							failedVals = append(failedVals, val)
							mu.Unlock()
						default:
							t.Errorf("Update to %q: %v", val, err)
						}
					default:
						var gen int64
						if rng.IntN(2) == 0 {
							gen = lastGen[ki]
						}
						err := s.Delete(ctx, keys[ki], gen)
						switch {
						case err == nil:
							deletes.Add(1)
							deleted[ki].Store(true)
						case errors.Is(err, ErrOutcomeUnknown):
							gaveUp.Add(1)
						case errors.Is(err, ErrNotFound), failed(err):
							countRejected(err)
						default:
							t.Errorf("Delete gen %d: %v", gen, err)
						}
					}
					cancel()
					if time.Since(start) > timeout {
						t.Errorf("an operation took %v, its deadline was %v (EventuallyResponds)", time.Since(start), timeout)
					}
				}
			})
		}
		wg.Wait()
		f.stop()
		s.inflight.Wait() // BackgroundSettles
		checkInvariants(t, s, d)

		applied := map[string]bool{}
		for _, r := range d.AppliedUpdates() {
			applied[r.Vals[1].(string)] = true
		}
		for _, v := range okVals {
			if !applied[v] {
				t.Errorf("UpdateAnswerSound: the update to %q was answered OK and never applied", v)
			}
		}
		for _, v := range failedVals {
			if applied[v] {
				t.Errorf("UpdateAnswerSound: the update to %q was answered with a failure and applied", v)
			}
		}
		for _, c := range failedCreates { // first attempts only (M3 plan, decision 8)
			if d.Claimed(keys[c.ki], c.gen) {
				t.Errorf("CreateFailureIsFinal: the create of key %d gen %d was answered with a failure and took effect", c.ki, c.gen)
			}
		}

		counters := []struct {
			name string
			n    int64
		}{
			{"W_Delete: deletes answered OK", deletes.Load()},
			{"W_Recreate: creates answered OK after a delete of the key", recreates.Load()},
			{"W_AmbStmtInFlight: answers lost", f.lost.Load()},
			{"W_AmbStmtInFlight: late Execs that landed", f.landed.Load()},
			{"W_FenceThenNotApplied: kept statements dropped by a fence", f.dropped.Load()},
			{"W_CreateAmb: Inserts answered ambiguous", f.amb[fakedb.Insert].Load()},
			{"W_FenceThenApplied: Updates answered ambiguous", f.amb[fakedb.Update].Load()},
			{"W_DeleteAmb: Deletes answered ambiguous", f.amb[fakedb.Delete].Load()},
			{"W_ClientGaveUp: ErrOutcomeUnknown answers", gaveUp.Load()},
			{"W_Rejected: ErrRejected answers", rejected.Load()},
		}
		for _, c := range counters {
			t.Logf("%s: %d", c.name, c.n)
			if c.n == 0 {
				t.Errorf("non-vacuity: %s is 0", c.name)
			}
		}
		t.Logf("not asserted: W_DedupClaimed: creates answered (nil, nil): %d; W_RetryFindsOtherLife: %d; "+
			"updates answered OK: %d; never executed: %d; writes not executed: %d; fences not confirmed: %d; reads failed: %d",
			claimedNoRow.Load(), f.retryFindsOtherLife.Load(), updates.Load(), f.never.Load(), f.notExecuted.Load(),
			f.fencesFailed.Load(), f.readsFailed.Load())
	})
}
