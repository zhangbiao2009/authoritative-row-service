package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"rowcache/internal/db/fakedb"
)

// A step decides one statement: it returns what the caller gets, and calls st.Exec if and when the
// statement executes. A nil step lets the statement run normally.
type step func(st *fakedb.Stmt) error

// lost executes the statement, but its caller is told the outcome is ambiguous.
func lost(st *fakedb.Stmt) error {
	st.Exec()
	return fakedb.Ambiguous(st, context.DeadlineExceeded)
}

// amb tells the caller the outcome is ambiguous and keeps the statement, so the test can execute it late.
func amb(late *func()) step {
	return func(st *fakedb.Stmt) error {
		*late = st.Exec
		return fakedb.Ambiguous(st, context.DeadlineExceeded)
	}
}

// fail tells the caller err; the statement does not execute.
func fail(err error) step {
	return func(*fakedb.Stmt) error { return fmt.Errorf("%w: injected", err) }
}

// blockOn holds the statement until release is closed, then lets it run.
func blockOn(release chan struct{}) step {
	return func(*fakedb.Stmt) error { <-release; return nil }
}

// recorder is a Hook that runs a script and records every statement.
type recorder struct {
	mu    sync.Mutex
	steps map[fakedb.Kind][]step // the next statement of a kind runs the first step left for it
	seen  []fakedb.Stmt          // without Exec
}

func script(d *fakedb.DB, steps map[fakedb.Kind][]step) *recorder {
	r := &recorder{steps: steps}
	d.Hook = func(_ context.Context, st *fakedb.Stmt) error {
		r.mu.Lock()
		seen := *st
		seen.Exec = nil
		r.seen = append(r.seen, seen)
		var f step
		if s := r.steps[st.Kind]; len(s) > 0 {
			f, r.steps[st.Kind] = s[0], s[1:]
		}
		r.mu.Unlock()
		if f == nil {
			return nil
		}
		return f(st)
	}
	return r
}

// of returns the statements of kind k seen so far.
func (r *recorder) of(k fakedb.Kind) []fakedb.Stmt {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []fakedb.Stmt
	for _, st := range r.seen {
		if st.Kind == k {
			out = append(out, st)
		}
	}
	return out
}

// wantFences checks that n Fence statements were sent, each terminating the session of the first
// statement of kind k: the attempt the test kept ambiguous.
func (r *recorder) wantFences(t *testing.T, n int, k fakedb.Kind) {
	t.Helper()
	session := r.of(k)[0].Session
	f := r.of(fakedb.Fence)
	if len(f) != n {
		t.Fatalf("Fence statements %+v, want %d", f, n)
	}
	for _, st := range f {
		if st.Session != session {
			t.Fatalf("Fence statements %+v, want each with session %d", f, session)
		}
	}
}
