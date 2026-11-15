package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"rowcache/internal/db"
	"rowcache/internal/db/fakedb"
	"rowcache/internal/schema"
)

// faults injects random faults into a fakedb while on (design.md 12.1 item 2). Run it inside synctest: its
// delays are long in wall-clock terms.
//   - Insert, Update, Delete: 55% normal; 10% executed, answer lost; 10% ambiguous, executed 0-50ms later
//     (unless fenced first); 10% ambiguous, never executed; 15% not executed. Each after a random 0-500µs.
//   - Select and GenClaimed: 5% not executed. Fence: 20% not confirmed (the kept statement may still land).
//   - A flipper turns the rejection switch of a random key on or off every 1ms.
//
// The counters are witnesses (tla-mapping.md section 8): they show a test reached the hard cases.
type faults struct {
	d    *fakedb.DB
	keys []schema.Key
	on   atomic.Bool
	late sync.WaitGroup // late Execs not yet run

	amb                 [fakedb.Fence + 1]atomic.Int64 // writes answered ambiguous, by kind (W_CreateAmb, W_FenceThenApplied, W_DeleteAmb)
	lost                atomic.Int64                   // executed, answer lost (W_AmbStmtInFlight)
	landed              atomic.Int64                   // late Execs that ran before a fence of their session (W_AmbStmtInFlight)
	dropped             atomic.Int64                   // kept statements a confirmed fence dropped (W_FenceThenNotApplied)
	never               atomic.Int64                   // ambiguous, kept, and never executed
	notExecuted         atomic.Int64                   // writes answered db.ErrNotExecuted (W_NotExecuted)
	fencesFailed        atomic.Int64                   // fences answered "not confirmed"
	readsFailed         atomic.Int64                   // Selects and GenClaimed lookups answered "not executed"
	retryFindsOtherLife atomic.Int64                   // GenClaimed found the gen claimed while another life is there (W_RetryFindsOtherLife)

	mu   sync.Mutex
	kept map[db.SessionID]bool // answered ambiguous, not executed: neither landed nor dropped yet

	flipping, flipped chan struct{}
}

// startFaults installs the faults as d's hook, turns them on, and starts the rejection flipper on keys.
func startFaults(d *fakedb.DB, keys []schema.Key) *faults {
	f := &faults{
		d: d, keys: keys,
		kept:     map[db.SessionID]bool{},
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
				d.SetRejecting(keys[rand.IntN(len(keys))], rand.IntN(2) == 0)
			}
		}
	}()
	return f
}

// stop turns the faults off, stops the flipper, clears every rejection switch and waits for the late
// Execs to land or find their statement dropped by a fence. The hook stays installed, answering normally.
func (f *faults) stop() {
	f.mu.Lock() // a late Exec is added under mu while on, so none is added after late.Wait below
	f.on.Store(false)
	f.mu.Unlock()
	close(f.flipping)
	<-f.flipped
	for _, k := range f.keys {
		f.d.SetRejecting(k, false)
	}
	f.late.Wait()
}

// takeLocked reports whether the statement of session id is still kept, and stops keeping it. Requires f.mu.
func (f *faults) takeLocked(id db.SessionID) bool {
	ok := f.kept[id]
	delete(f.kept, id)
	return ok
}

// settle is the answer of a statement the faults leave alone. A Fence is then confirmed: the statement of the
// session, if kept, is dropped. (The fence executes right after this returns, and a late Exec takes mu before
// it runs, so a kept statement either landed before or never.)
func (f *faults) settle(st *fakedb.Stmt) error {
	if st.Kind == fakedb.Fence {
		f.mu.Lock()
		if f.takeLocked(st.Session) {
			f.dropped.Add(1)
		}
		f.mu.Unlock()
	}
	return nil
}

func (f *faults) hook(ctx context.Context, st *fakedb.Stmt) error {
	if st.Kind == fakedb.GenClaimed {
		if f.on.Load() && rand.IntN(100) < 5 {
			f.readsFailed.Add(1)
			return fmt.Errorf("%w: injected", db.ErrNotExecuted)
		}
		st.Exec()
		if f.d.Claimed(st.Key, st.Gen) {
			if r := f.d.Row(st.Key); r != nil && r.Gen != st.Gen {
				f.retryFindsOtherLife.Add(1)
			}
		}
		return nil
	}
	if !f.on.Load() {
		return f.settle(st)
	}
	// The delay ends early if ctx does, as a real driver's would: an operation never overruns its deadline
	// (EventuallyResponds). Writes and fences run on bgCtx, which is never done.
	select {
	case <-time.After(time.Duration(rand.IntN(500)) * time.Microsecond):
	case <-ctx.Done():
	}
	if !f.on.Load() { // stopped while this hook slept: no new fault after stop
		return f.settle(st)
	}
	switch st.Kind {
	case fakedb.Fence:
		if rand.IntN(100) < 20 {
			f.fencesFailed.Add(1)
			return fmt.Errorf("%w: injected", db.ErrNotExecuted) // not confirmed: the session goes on, its kept statement may land
		}
		return f.settle(st)
	case fakedb.Select:
		if rand.IntN(100) < 5 {
			f.readsFailed.Add(1)
			return fmt.Errorf("%w: injected", db.ErrNotExecuted)
		}
		return nil
	}
	switch n := rand.IntN(100); {
	case n < 55:
		return nil
	case n < 65:
		f.amb[st.Kind].Add(1)
		f.lost.Add(1)
		return lost(st)
	case n < 85: // ambiguous, kept: neither landed nor dropped yet
		late := n < 75
		f.mu.Lock()
		if late {
			if !f.on.Load() { // stop ran since the check above: register nothing after its late.Wait
				f.mu.Unlock()
				return nil
			}
			f.late.Add(1)
		}
		f.kept[st.Session] = true
		f.mu.Unlock()
		f.amb[st.Kind].Add(1)
		if late {
			id, exec := st.Session, st.Exec
			go func() {
				defer f.late.Done()
				// Virtual time: a window wider than the backoff, so a late statement can land after a fence
				// that was not confirmed, or be dropped by one that was.
				time.Sleep(time.Duration(rand.IntN(50_000)) * time.Microsecond)
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.takeLocked(id) {
					exec()
					f.landed.Add(1)
				}
			}()
		} else {
			f.never.Add(1)
		}
		return fakedb.Ambiguous(st, context.DeadlineExceeded) // else never executes
	default:
		f.notExecuted.Add(1)
		return fmt.Errorf("%w: injected", db.ErrNotExecuted)
	}
}
