package store

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

// faults injects random DB faults into a fakedb while on (design.md 12.1 item 2). Run it inside synctest:
// its delays are long in wall-clock terms.
//   - Insert, Update, Delete: 55% normal; 10% executed, answer lost; 10% ambiguous, executed late (up to
//     100ms after); 10% ambiguous, never executed; 15% not executed. Each after a random 0-500µs.
//   - Select and GenClaimed: 5% not executed. Fence: normal.
//   - A flipper turns the cross-row switch of a random key on or off every 1ms.
//
// The counters are witnesses (tla-mapping.md section 8): they show a test reached the hard cases.
type faults struct {
	d    *fakedb.DB
	keys []schema.Key
	on   atomic.Bool
	late sync.WaitGroup // late Execs not yet run

	ambThenNotExecuted    atomic.Int64 // "not executed" for an Insert/Delete of a life that had an ambiguous answer
	notExecutedAfterFence atomic.Int64 // the same, after one of its sessions was fenced
	lateInsertOfDeleted   atomic.Int64 // a late INSERT arriving after its life was deleted
	retryFindsOtherLife   atomic.Int64 // a GenClaimed lookup that found the gen claimed

	mu        sync.Mutex
	ambiguous map[stmtOf]bool         // answered "ambiguous" at least once
	sessionOf map[db.SessionID]stmtOf // the sessions of those answers
	fenced    map[stmtOf]bool         // one of those sessions was fenced

	flipping, flipped chan struct{}
}

// stmtOf names the Inserts, or the Deletes, of one life.
type stmtOf struct {
	kind fakedb.Kind
	key  schema.Key
	gen  int64
}

// startFaults installs the faults as d's hook, turns them on, and starts the cross-row flipper on keys.
func startFaults(d *fakedb.DB, keys []schema.Key) *faults {
	f := &faults{
		d: d, keys: keys,
		ambiguous: map[stmtOf]bool{}, sessionOf: map[db.SessionID]stmtOf{}, fenced: map[stmtOf]bool{},
		flipping: make(chan struct{}), flipped: make(chan struct{}),
	}
	f.on.Store(true)
	d.Hook = f.hook
	go func() {
		defer close(f.flipped)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-f.flipping:
				return
			case <-tick.C:
				d.SetCrossRow(keys[rand.IntN(len(keys))], rand.IntN(2) == 0)
			}
		}
	}()
	return f
}

// stop turns the faults off, stops the flipper, clears every cross-row switch and waits for the late
// statements to run. The hook stays installed, answering normally.
func (f *faults) stop() {
	f.on.Store(false)
	close(f.flipping)
	<-f.flipped
	for _, k := range f.keys {
		f.d.SetCrossRow(k, false)
	}
	f.late.Wait()
}

func (f *faults) hook(_ context.Context, st *fakedb.Stmt) error {
	if st.Kind == fakedb.GenClaimed {
		if f.on.Load() && rand.IntN(100) < 5 {
			return fail(db.ErrNotExecuted)(st)
		}
		st.Exec()
		if f.d.Claimed(st.Key, st.Gen) { // W_RetryFindsOtherLife: a retry of a create met another life
			f.retryFindsOtherLife.Add(1)
		}
		return nil
	}
	if !f.on.Load() {
		return nil
	}
	time.Sleep(time.Duration(rand.IntN(500)) * time.Microsecond)
	switch st.Kind {
	case fakedb.Fence:
		f.mu.Lock()
		for _, id := range st.Sessions {
			f.fenced[f.sessionOf[id]] = true
		}
		f.mu.Unlock()
		return nil
	case fakedb.Select:
		if rand.IntN(100) < 5 {
			return fail(db.ErrNotExecuted)(st)
		}
		return nil
	}
	l, write := stmtOf{st.Kind, st.Key, st.Gen}, st.Kind == fakedb.Insert || st.Kind == fakedb.Delete
	markAmbiguous := func() {
		if write {
			f.mu.Lock()
			f.ambiguous[l] = true
			f.sessionOf[st.Session] = l
			f.mu.Unlock()
		}
	}
	switch n := rand.IntN(100); {
	case n < 55:
		return nil
	case n < 65:
		markAmbiguous()
		return lost(st)
	case n < 75:
		markAmbiguous()
		exec := st.Exec
		f.late.Go(func() {
			// Virtual time: a window much wider than the backoff, so a late statement can land after later
			// attempts and later operations, not only before the next attempt.
			time.Sleep(time.Duration(rand.IntN(100_000)) * time.Microsecond)
			if l.kind == fakedb.Insert && f.d.Deleted(l.key, l.gen) { // W_LateInsertOfDeletedGen
				f.lateInsertOfDeleted.Add(1)
			}
			exec()
		})
		return fakedb.Ambiguous(st, context.DeadlineExceeded)
	case n < 85:
		markAmbiguous()
		return fakedb.Ambiguous(st, context.DeadlineExceeded) // never executes
	default:
		f.mu.Lock()
		if write && f.ambiguous[l] {
			f.ambThenNotExecuted.Add(1)
		}
		if write && f.fenced[l] {
			f.notExecutedAfterFence.Add(1)
		}
		f.mu.Unlock()
		return fail(db.ErrNotExecuted)(st)
	}
}
