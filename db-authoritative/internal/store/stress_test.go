package store

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
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
	d := fakedb.New()
	s := newTestStore(t, d)
	keys := make([]schema.Key, 4)
	for i := range keys {
		keys[i] = key(t, s, int64(i+1))
		d.Put(testRow(keys[i], int64(i+1)))
	}
	var mu sync.Mutex
	okIncr := make([]int, len(keys))      // answered OK
	unknownIncr := make([]int, len(keys)) // answered ErrOutcomeUnknown: it still ran (no faults)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 1))
			for n := 0; n < ops; n++ {
				ki := rng.IntN(len(keys))
				short := rng.IntN(10) == 0
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if short {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
				}
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
							ch.Gen = 999 // stale: only on SET, since an INCR answered ErrOutcomeUnknown is counted as applied
						}
					}
					_, err := s.Update(ctx, keys[ki], ch)
					switch {
					case err == nil:
						if isIncr {
							mu.Lock()
							okIncr[ki]++
							mu.Unlock()
						}
					case errors.Is(err, ErrOutcomeUnknown):
						if isIncr {
							mu.Lock()
							unknownIncr[ki]++
							mu.Unlock()
						}
					case errors.Is(err, ErrNotFound) && ch.Gen == 999:
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
		}()
	}
	wg.Wait()
	s.inflight.Wait()
	checkInvariants(t, s, d)
	for i, k := range keys {
		want := int64(10 + okIncr[i] + unknownIncr[i])
		if got := d.Row(k).Vals[2]; got != want {
			t.Errorf("key %d: DB score = %v, want %d (%d OK + %d unknown)", i, got, want, okIncr[i], unknownIncr[i])
		}
	}
}
