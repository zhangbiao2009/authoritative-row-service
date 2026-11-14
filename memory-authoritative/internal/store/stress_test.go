package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	var failed, late atomic.Int64
	var lateExecs sync.WaitGroup
	d.UpdateHook = func(_ context.Context, _ schema.Key, exec func()) error {
		switch n := rand.IntN(10); {
		case n < 7:
			exec()
			return nil
		case n < 9:
			failed.Add(1)
			return errors.New("injected: definitely not executed")
		default:
			late.Add(1)
			lateExecs.Go(func() {
				time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
				exec()
			})
			return context.DeadlineExceeded
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
	d.UpdateHook = nil
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
