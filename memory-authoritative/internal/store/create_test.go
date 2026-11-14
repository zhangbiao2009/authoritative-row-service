package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

func newRow(k schema.Key, id, gen int64) *schema.Row {
	return &schema.Row{Key: k, Gen: gen, Vals: []any{id, "a", int64(10)}}
}

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

// landBefore lets the kept statement land just before this one: the other row has changed, then the late
// statement executes, then this one goes on (TLC's counterexample for the first fix of hole 5).
func landBefore(d *fakedb.DB, k schema.Key, late *func()) step {
	return func(*fakedb.Stmt) error {
		d.SetCrossRow(k, false)
		(*late)()
		return nil
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

// wantFences checks that n Fence statements were sent, each terminating exactly the session of the first
// statement of kind k: the attempt the test kept ambiguous.
func (r *recorder) wantFences(t *testing.T, n int, k fakedb.Kind) {
	t.Helper()
	session := r.of(k)[0].Session
	f := r.of(fakedb.Fence)
	if len(f) != n {
		t.Fatalf("Fence statements %+v, want %d", f, n)
	}
	for _, st := range f {
		if !reflect.DeepEqual(st.Sessions, []db.SessionID{session}) {
			t.Fatalf("Fence statements %+v, want each with session %d", f, session)
		}
	}
}

// giveUp creates in with a client that gives up while the INSERT is held; the create goes on in the
// background. settle lets the INSERT run and waits for the create to finish. Call it inside synctest.
func giveUp(t *testing.T, s *Store, d *fakedb.DB, in *schema.Row) (settle func()) {
	t.Helper()
	release := make(chan struct{})
	script(d, map[fakedb.Kind][]step{fakedb.Insert: {blockOn(release)}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if r, err := s.Create(ctx, in); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("Create = %+v, %v; want ErrOutcomeUnknown", r, err)
	}
	return func() {
		close(release)
		s.inflight.Wait() // L2 BackgroundSettles
		d.Hook = nil
	}
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

func create(t *testing.T, s *Store, in *schema.Row) *schema.Row {
	t.Helper()
	r, err := s.Create(context.Background(), in)
	if err != nil || r == nil {
		t.Fatalf("Create = %+v, %v", r, err)
	}
	return r
}

func notFound(t *testing.T, s *Store, k schema.Key) {
	t.Helper()
	if r, err := s.Get(context.Background(), k); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %+v, %v; want ErrNotFound", r, err)
	}
}

func TestCreateInsertsRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	r := create(t, s, newRow(k, 1, 0))
	if r.Gen == 0 || r.Ver != 0 || !reflect.DeepEqual(r.Vals, newRow(k, 1, 0).Vals) {
		t.Fatalf("Create = %+v; want a non-zero gen, ver 0, the given values", r)
	}
	if g, err := s.Get(context.Background(), k); err != nil || g != r {
		t.Fatalf("Get = %p, %v; want the created row %p", g, err, r)
	}
	dbMatches(t, d, r)
	if !d.Claimed(k, r.Gen) {
		t.Fatal("no dedup record")
	}
	checkInvariants(t, s, d)
}

func TestCreateExistingIsAlreadyExists(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	rec := script(d, nil)
	create(t, s, newRow(k, 1, 0))
	if r, err := s.Create(context.Background(), newRow(k, 1, 0)); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second Create = %+v, %v; want ErrAlreadyExists", r, err)
	}
	if n := len(rec.of(fakedb.Insert)); n != 1 {
		t.Fatalf("Insert statements = %d, want 1", n)
	}
}

func TestCreateSameGenIsIdempotent(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	rec := script(d, nil)
	r1 := create(t, s, newRow(k, 1, 100))
	if r2 := create(t, s, newRow(k, 1, 100)); r2 != r1 {
		t.Fatalf("retry with the same gen = %+v, want the row %+v", r2, r1)
	}
	if n := len(rec.of(fakedb.Insert)); n != 1 {
		t.Fatalf("Insert statements = %d, want 1", n)
	}
}

// Review Focus 1.
func TestCreateRejectsBadRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	rec := script(d, nil)
	k1, k2 := key(t, s, 1), key(t, s, 2)
	for name, in := range map[string]*schema.Row{
		"two values":             {Key: k1, Vals: []any{int64(1), "a"}},
		"key of another id":      {Key: k2, Vals: []any{int64(1), "a", int64(10)}},
		"primary key wrong type": {Key: k1, Vals: []any{"1", "a", int64(10)}},
	} {
		if r, err := s.Create(context.Background(), in); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Create = %+v, %v; want ErrInvalid", name, r, err)
		}
	}
	if n := rec.count(); n != 0 {
		t.Fatalf("%d statements sent, want none", n)
	}
}

// Mutation CreateNoLoad: create decides from memory, loaded first.
func TestCreateLoadsFirst(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1)) // gen 101, not loaded
	rec := script(d, nil)
	if r, err := s.Create(context.Background(), newRow(k, 1, 7)); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Create = %+v, %v; want ErrAlreadyExists", r, err)
	}
	if d.Claimed(k, 7) || len(rec.of(fakedb.Insert)) != 0 {
		t.Fatalf("claimed %v, Insert statements %d; want the gen not burned, no Insert", d.Claimed(k, 7), len(rec.of(fakedb.Insert)))
	}
}

// Review Focus 2.
func TestCreateLoadErrorHasNoEffect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		rec := script(d, map[fakedb.Kind][]step{fakedb.Select: {fail(db.ErrNotExecuted)}})
		if r, err := s.Create(context.Background(), newRow(k, 1, 100)); !db.NotExecuted(err) {
			t.Fatalf("Create = %+v, %v; want the load error", r, err)
		}
		if n := len(rec.of(fakedb.Insert)); n != 0 {
			t.Fatalf("Insert statements = %d, want 0", n)
		}
		checkInvariants(t, s, d)
		d.Hook = nil
		time.Sleep(s.cfg.LoadErrTTL)
		create(t, s, newRow(k, 1, 100))
	})
}

func TestCreateQueueTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		rec := script(d, nil)
		e := s.getOrCreate(k)
		if err := e.lockOp(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()
		if r, err := s.Create(ctx, newRow(k, 1, 100)); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != time.Second {
			t.Fatalf("Create = %+v, %v after %v; want DeadlineExceeded after 1s", r, err, time.Since(start))
		}
		if n := rec.count(); n != 0 {
			t.Fatalf("%d statements sent, want none", n)
		}
		e.unlockOp()
	})
}

func TestCreateOutcomeUnknown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		settle := giveUp(t, s, d, newRow(k, 1, 100))
		notFound(t, s, k) // linearized before the create, which has not taken effect
		settle()
		if r, err := s.Get(context.Background(), k); err != nil || r.Gen != 100 {
			t.Fatalf("Get after the create settled = %+v, %v", r, err)
		}
		checkInvariants(t, s, d)
	})
}

func TestCreateAmbiguousRetriesSameGen(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	rec := script(d, map[fakedb.Kind][]step{fakedb.Insert: {lost}})
	r := create(t, s, newRow(k, 1, 100))
	ins := rec.of(fakedb.Insert)
	if r.Gen != 100 || len(ins) != 2 || ins[0].Gen != 100 || ins[1].Gen != 100 {
		t.Fatalf("Create = %+v, Inserts %+v; want gen 100, two attempts with gen 100", r, ins)
	}
	dbMatches(t, d, r)
	checkInvariants(t, s, d)
}

func TestCreateNotExecutedWithoutAmbiguityIsFinal(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	rec := script(d, map[fakedb.Kind][]step{fakedb.Insert: {fail(db.ErrNotExecuted)}})
	if r, err := s.Create(context.Background(), newRow(k, 1, 100)); !db.NotExecuted(err) {
		t.Fatalf("Create = %+v, %v; want not executed", r, err)
	}
	if n := len(rec.of(fakedb.Insert)); n != 1 {
		t.Fatalf("Insert statements = %d, want 1", n)
	}
	notFound(t, s, k)
}

// An ambiguous attempt, then a rejection by the row's own data: the earlier attempt had the same
// parameters, so it was rejected too (8.4).
func TestCreateRejectedByOwnDataIsFinal(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	var late func()
	script(d, map[fakedb.Kind][]step{fakedb.Insert: {amb(&late), fail(db.ErrRejected)}})
	if r, err := s.Create(context.Background(), newRow(k, 1, 100)); !errors.Is(err, db.ErrRejected) {
		t.Fatalf("Create = %+v, %v; want ErrRejected", r, err)
	}
	notFound(t, s, k)
}

// Mutation LoadNoRecheck: a Get queued behind a create must not load over the created row.
func TestGetQueuedBehindCreateKeepsNewerRow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		release := make(chan struct{})
		script(d, map[fakedb.Kind][]step{fakedb.Select: {blockOn(release)}})
		created := make(chan *schema.Row, 1)
		go func() {
			r, err := s.Create(context.Background(), newRow(k, 1, 100))
			if err != nil {
				t.Error(err)
			}
			created <- r
		}()
		synctest.Wait() // the create holds op and waits for its load
		got := make(chan *schema.Row, 1)
		go func() {
			r, err := s.Get(context.Background(), k)
			if err != nil {
				t.Error(err)
			}
			got <- r
		}()
		synctest.Wait() // the Get saw nil and queues on op
		close(release)
		if c, g := <-created, <-got; c == nil || g != c {
			t.Fatalf("Get = %p, want the created row %p", g, c)
		}
		if n := d.Selects(); n != 1 {
			t.Fatalf("Selects = %d, want 1: the queued load must re-check snap", n)
		}
	})
}

// Hole 4, create (13.2; trace: hole4): the first attempt executed but its answer was lost; the second
// was not executed. That says nothing about the first: keep going.
func TestCreateAmbiguousThenNotExecutedIsNotFinal(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	rec := script(d, map[fakedb.Kind][]step{fakedb.Insert: {lost, fail(db.ErrNotExecuted)}})
	r := create(t, s, newRow(k, 1, 100))
	if r.Gen != 100 || len(rec.of(fakedb.Insert)) != 3 {
		t.Fatalf("Create = %+v after %d Inserts; want gen 100 after 3", r, len(rec.of(fakedb.Insert)))
	}
	dbMatches(t, d, r)
	checkInvariants(t, s, d)
}

// Hole 5, create (13.2; trace: hole5): an ambiguous attempt, then a rejection because of another row. Once
// that row changes, the first attempt could land; it must be fenced before "failed" is answered.
func TestCrossRowRejectionAfterAmbiguousIsFenced(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.SetCrossRow(k, true)
	var late func()
	rec := script(d, map[fakedb.Kind][]step{fakedb.Insert: {amb(&late)}})
	if r, err := s.Create(context.Background(), newRow(k, 1, 100)); !db.IsCrossRow(err) {
		t.Fatalf("Create = %+v, %v; want a cross-row rejection", r, err)
	}
	rec.wantFences(t, 1, fakedb.Insert)
	d.SetCrossRow(k, false)
	late() // the first attempt arrives after the other row changed
	if d.Row(k) != nil || d.Claimed(k, 100) {
		t.Fatalf("the fenced attempt landed: row %+v, claimed %v", d.Row(k), d.Claimed(k, 100))
	}
	notFound(t, s, k)
	checkInvariants(t, s, d)
}

// The first fix of hole 5 also cleared unsure at the fence (design.md 7.4). TLC's counterexample: A
// ambiguous, B rejected because of another row, A lands, fence, C not executed.
func TestFenceKeepsUnsure(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.SetCrossRow(k, true)
	var late func()
	script(d, map[fakedb.Kind][]step{
		fakedb.Insert: {amb(&late), nil, fail(db.ErrNotExecuted)},
		fakedb.Fence:  {landBefore(d, k, &late)}, // A lands just before its session is terminated
	})
	r, err := s.Create(context.Background(), newRow(k, 1, 100))
	if err != nil || r == nil || r.Gen != 100 {
		t.Fatalf("Create = %+v, %v; want the row of gen 100, which is in the DB", r, err)
	}
	dbMatches(t, d, r)
	checkInvariants(t, s, d)
}

// Review Focus 3: a fence that cannot be confirmed does not end the loop.
func TestFenceNotConfirmedKeepsTrying(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.SetCrossRow(k, true)
		var late func()
		rec := script(d, map[fakedb.Kind][]step{
			fakedb.Insert: {amb(&late)},
			fakedb.Fence:  {fail(errors.New("not confirmed"))},
		})
		if r, err := s.Create(context.Background(), newRow(k, 1, 100)); !db.IsCrossRow(err) {
			t.Fatalf("Create = %+v, %v; want a cross-row rejection", r, err)
		}
		rec.wantFences(t, 2, fakedb.Insert)
		d.SetCrossRow(k, false)
		late()
		if d.Row(k) != nil {
			t.Fatalf("the fenced attempt landed: %+v", d.Row(k))
		}
	})
}

// A caller that gave up (ErrOutcomeUnknown) may reuse its row; the create goes on with the row as it was
// when Create was called.
func TestCreateDoesNotKeepCallersRow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		in := newRow(k, 1, 100)
		settle := giveUp(t, s, d, in)
		in.Vals[1] = "reused" // the caller reuses its row
		settle()
		if r := d.Row(k); r == nil || r.Vals[1] != "a" {
			t.Fatalf("DB row %+v; want the values given to Create (name \"a\")", r)
		}
		memMatchesDB(t, s, d, k)
	})
}
