package store

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
