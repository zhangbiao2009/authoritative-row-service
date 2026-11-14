package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"rowcache/internal/db"
	"rowcache/internal/db/fakedb"
	"rowcache/internal/schema"
)

// Concurrent misses queue on op; only the first loads (TLA: L_lock, L_load; the re-check of snap).
func TestConcurrentMissesSelectOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		release := make(chan struct{})
		d.Hook = func(context.Context, *fakedb.Stmt) error { <-release; return nil }
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		var wg sync.WaitGroup
		for range 32 {
			wg.Go(func() {
				if _, err := s.Get(context.Background(), k); err != nil {
					t.Error(err)
				}
			})
		}
		synctest.Wait() // one Get is inside Select, the rest wait for op
		close(release)
		wg.Wait()
		if n := d.Selects(); n != 1 {
			t.Fatalf("Selects = %d, want 1", n)
		}
	})
}

func TestLoadErrorIsCachedForLoadErrTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		boom := errors.New("boom")
		d.Hook = func(context.Context, *fakedb.Stmt) error { return boom }
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		if _, err := s.Get(context.Background(), k); !errors.Is(err, db.ErrNotExecuted) {
			t.Fatalf("first Get: err = %v, want ErrNotExecuted", err)
		}
		d.Hook = nil // the DB is fine again; a second Get must still get the cached error
		if _, err := s.Get(context.Background(), k); !errors.Is(err, db.ErrNotExecuted) {
			t.Fatalf("Get within LoadErrTTL: err = %v, want the cached ErrNotExecuted", err)
		}
		if n := d.Selects(); n != 1 {
			t.Fatalf("Selects within LoadErrTTL = %d, want 1", n)
		}
		time.Sleep(s.cfg.LoadErrTTL)
		if _, err := s.Get(context.Background(), k); err != nil {
			t.Fatalf("Get after LoadErrTTL: %v", err)
		}
		if n := d.Selects(); n != 2 {
			t.Fatalf("Selects = %d, want 2", n)
		}
	})
}

// The caller's own cancellation says nothing about the DB, so it is not cached.
func TestCancelledLoadIsNotCached(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	ctx, cancel := context.WithCancel(context.Background())
	// The caller gives up while Select runs; Select then returns the context error.
	d.Hook = func(context.Context, *fakedb.Stmt) error { cancel(); return nil }
	if _, err := s.Get(ctx, k); !errors.Is(err, context.Canceled) || errors.Is(err, db.ErrNotExecuted) {
		t.Fatalf("err = %v, want context.Canceled itself (DEADLINE_EXCEEDED, not UNAVAILABLE)", err)
	}
	if _, err := s.Get(context.Background(), k); err != nil {
		t.Fatalf("Get after a cancelled load: %v", err)
	}
	if n := d.Selects(); n != 2 {
		t.Fatalf("Selects = %d, want 2", n)
	}
}

// design.md 7.2: any failure of a read means "this request did nothing" (UNAVAILABLE).
func TestLoadErrorIsNotExecuted(t *testing.T) {
	d := fakedb.New()
	boom := errors.New("boom")
	d.Hook = func(context.Context, *fakedb.Stmt) error { return boom }
	s := newTestStore(t, d)
	k := key(t, s, 1)
	_, err := s.Get(context.Background(), k)
	if !errors.Is(err, db.ErrNotExecuted) || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want ErrNotExecuted mentioning boom", err)
	}
}

// A Get that times out while queued for op leaves the queue (TLA: L_lock).
func TestGetTimesOutWhileQueuedForOp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		release := make(chan struct{})
		d.Hook = func(context.Context, *fakedb.Stmt) error { <-release; return nil }
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		firstDone := make(chan error, 1)
		go func() {
			_, err := s.Get(context.Background(), k)
			firstDone <- err
		}()
		synctest.Wait() // the first Get holds op and is inside Select
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := s.Get(ctx, k); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued Get: err = %v, want DeadlineExceeded", err)
		}
		close(release)
		if err := <-firstDone; err != nil {
			t.Fatalf("first Get: %v", err)
		}
	})
}

// Many keys, many goroutines; run under -race.
func TestGetConcurrentKeys(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	keys := make([]schema.Key, 8)
	for id := range int64(8) {
		keys[id] = key(t, s, id)
		if id%2 == 0 {
			d.Put(testRow(keys[id], id))
		}
	}
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 200 {
				id := int64((g + i) % 8)
				r, err := s.Get(context.Background(), keys[id])
				switch {
				case id%2 == 0 && (err != nil || r.Gen != id+100):
					t.Errorf("Get(%d) = %+v, %v", id, r, err)
				case id%2 == 1 && !errors.Is(err, ErrNotFound):
					t.Errorf("Get(%d): err = %v, want ErrNotFound", id, err)
				}
			}
		})
	}
	wg.Wait()
	if n := d.Selects(); n != 8 {
		t.Fatalf("Selects = %d, want 8 (one per key)", n)
	}
}
