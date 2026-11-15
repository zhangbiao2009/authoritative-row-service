package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"rowcache/internal/db"
	"rowcache/internal/db/fakedb"
	"rowcache/internal/schema"
)

func newRow(k schema.Key, id, gen int64) *schema.Row {
	return &schema.Row{Key: k, Gen: gen, Vals: []any{id, "a", int64(10)}}
}

// giveUp creates in with a client that gives up while the INSERT is blocked; the create goes on in the
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
		s.inflight.Wait()
		d.Hook = nil
	}
}

func TestCreateInsertsRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	r, err := s.Create(context.Background(), newRow(k, 1, 7))
	if err != nil || r == nil || r.Ver != 0 || r.Gen != 7 || !reflect.DeepEqual(r, d.Row(k)) {
		t.Fatalf("Create = %+v, %v; DB %+v", r, err, d.Row(k))
	}
	if g, err := s.Get(context.Background(), k); err != nil || g != r {
		t.Fatalf("Get = %p, %v; want the created row %p", g, err, r)
	}
	if !d.Claimed(k, 7) {
		t.Fatal("no dedup record")
	}
	if !opFree(entryOf(s, k)) {
		t.Fatal("op held after Create returned")
	}
	checkInvariants(t, s, d)
}

func TestCreateNewGenIsNonZero(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	r, err := s.Create(context.Background(), newRow(k, 1, 0))
	if err != nil || r == nil || r.Gen == 0 || !reflect.DeepEqual(r, d.Row(k)) {
		t.Fatalf("Create = %+v, %v; DB %+v; want a non-zero gen equal to the DB row's", r, err, d.Row(k))
	}
}

func TestCreateSameGenReturnsRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	cached, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatal(err)
	}
	rec := script(d, nil)
	if r, err := s.Create(context.Background(), newRow(k, 1, 101)); err != nil || r != cached {
		t.Fatalf("Create = %p, %v; want the cached row %p", r, err, cached)
	}
	if n := rec.count(fakedb.Insert); n != 0 {
		t.Fatalf("Insert statements = %d, want 0", n)
	}
}

func TestCreateLoadsFirst(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	rec := script(d, nil)
	if r, err := s.Create(context.Background(), newRow(k, 1, 7)); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Create = %+v, %v; want ErrAlreadyExists", r, err)
	}
	if n, m := rec.count(fakedb.Select), rec.count(fakedb.Insert); n != 1 || m != 0 {
		t.Fatalf("Select %d, Insert %d; want 1, 0", n, m)
	}
	if n := rec.count(fakedb.GenClaimed); n != 1 { // another life: the dedup table is asked before ALREADY_EXISTS
		t.Fatalf("GenClaimed lookups = %d, want 1", n)
	}
}

func TestCreateLoadErrorSendsNothing(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	rec := script(d, map[fakedb.Kind][]step{fakedb.Select: {fail(db.ErrNotExecuted)}})
	if r, err := s.Create(context.Background(), newRow(k, 1, 7)); !errors.Is(err, db.ErrNotExecuted) {
		t.Fatalf("Create = %+v, %v; want db.ErrNotExecuted", r, err)
	}
	if n := rec.count(fakedb.Insert); n != 0 {
		t.Fatalf("Insert statements = %d, want 0", n)
	}
}

func TestCreateRejectsBadRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	rec := script(d, nil)
	for name, in := range map[string]*schema.Row{
		"too few values": {Key: k, Gen: 7, Vals: []any{int64(1), "a"}},
		"key mismatch":   newRow(k, 2, 7),
	} {
		if r, err := s.Create(context.Background(), in); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: Create = %+v, %v; want ErrInvalid", name, r, err)
		}
	}
	if len(rec.seen) != 0 {
		t.Fatalf("statements sent: %+v", rec.seen)
	}
}

// Hole 2: the create took effect, the row was deleted, and the retry finds the dedup record.
func TestCreateGenClaimedIsSuccessWithNoRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	ctx := context.Background()
	if _, err := d.InsertDedup(ctx, newRow(k, 1, 7)); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete(ctx, k, 7); err != nil {
		t.Fatal(err)
	}
	if r, err := s.Create(ctx, newRow(k, 1, 7)); r != nil || err != nil {
		t.Fatalf("Create = %+v, %v; want (nil, nil)", r, err)
	}
	notFound(t, s, k)
	checkInvariants(t, s, d)
}

func notFound(t *testing.T, s *Store, k schema.Key) {
	t.Helper()
	if r, err := s.Get(context.Background(), k); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %+v, %v; want ErrNotFound", r, err)
	}
}

func TestCreateDuplicateIsInternal(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	notFound(t, s, k) // caches absent
	d.Put(testRow(k, 1))
	if r, err := s.Create(context.Background(), newRow(k, 1, 7)); !errors.Is(err, ErrInternal) {
		t.Fatalf("Create = %+v, %v; want ErrInternal", r, err)
	}
	if entryOf(s, k).snap.Load() != nil {
		t.Fatal("snapshot kept after an invariant broke")
	}
	if r, err := s.Get(context.Background(), k); err != nil || !reflect.DeepEqual(r, d.Row(k)) || r.Gen != 101 {
		t.Fatalf("Get = %+v, %v; want the Put row", r, err)
	}
}

func TestCreateRejected(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.SetRejecting(k, true)
	if r, err := s.Create(context.Background(), newRow(k, 1, 7)); !errors.Is(err, db.ErrRejected) {
		t.Fatalf("Create = %+v, %v; want db.ErrRejected", r, err)
	}
	if d.Claimed(k, 7) {
		t.Fatal("dedup record written by a rejected create")
	}
	d.SetRejecting(k, false)
	if r, err := s.Create(context.Background(), newRow(k, 1, 7)); err != nil || r == nil || r.Gen != 7 {
		t.Fatalf("second Create = %+v, %v; want OK", r, err)
	}
	checkInvariants(t, s, d)
}

// W_CreateAmb: the INSERT took effect and its answer was lost.
func TestCreateAmbiguousApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		rec := script(d, map[fakedb.Kind][]step{fakedb.Insert: {lost}})
		r, err := s.Create(context.Background(), newRow(k, 1, 7))
		if err != nil || r == nil || !reflect.DeepEqual(r, d.Row(k)) {
			t.Fatalf("Create = %+v, %v; DB %+v", r, err, d.Row(k))
		}
		rec.wantFences(t, 1, fakedb.Insert)
		checkInvariants(t, s, d)
	})
}

func TestCreateAmbiguousNotApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		var late func()
		script(d, map[fakedb.Kind][]step{fakedb.Insert: {amb(&late)}})
		if r, err := s.Create(context.Background(), newRow(k, 1, 7)); !errors.Is(err, db.ErrNotExecuted) {
			t.Fatalf("Create = %+v, %v; want db.ErrNotExecuted", r, err)
		}
		late() // the fence discarded it
		if d.Row(k) != nil || d.Claimed(k, 7) {
			t.Fatalf("the late INSERT changed the DB: %+v", d.Row(k))
		}
		checkInvariants(t, s, d)
	})
}

// The dedup lookup after the fence fails: the answer is unknown, op is free, and a retry with the same gen
// decides it.
// design.md 7.5, decision 8 of 13.1: after the fence, a failing dedup lookup is retried until it succeeds
// while op is held, so the answer is definite (here: not executed), never OUTCOME_UNKNOWN.
func TestCreateAmbiguousDedupReadFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		var late func()
		rec := script(d, map[fakedb.Kind][]step{
			fakedb.Insert:     {amb(&late)},
			fakedb.GenClaimed: {fail(db.ErrNotExecuted), fail(db.ErrNotExecuted)},
		})
		if r, err := s.Create(context.Background(), newRow(k, 1, 7)); !errors.Is(err, db.ErrNotExecuted) {
			t.Fatalf("Create = %+v, %v; want ErrNotExecuted", r, err)
		}
		if n := rec.count(fakedb.GenClaimed); n != 3 {
			t.Fatalf("GenClaimed lookups = %d, want 3 (two failed, then one answered)", n)
		}
		if !opFree(entryOf(s, k)) {
			t.Fatal("op held")
		}
		if d.Claimed(k, 7) || d.Pending() != 0 {
			t.Fatalf("Claimed = %v, Pending = %d; want false, 0", d.Claimed(k, 7), d.Pending())
		}
		checkInvariants(t, s, d)
	})
}

// The retried lookup finds the gen claimed by an earlier life (an SDK retry, hole 2): success with no row.
func TestCreateAmbiguousDedupReadRetriedFindsClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		if _, err := d.InsertDedup(context.Background(), newRow(k, 1, 7)); err != nil {
			t.Fatal(err)
		}
		if err := d.Delete(context.Background(), k, 7); err != nil {
			t.Fatal(err)
		}
		var late func()
		script(d, map[fakedb.Kind][]step{
			fakedb.Insert:     {amb(&late)},
			fakedb.GenClaimed: {fail(db.ErrNotExecuted)},
		})
		if r, err := s.Create(context.Background(), newRow(k, 1, 7)); r != nil || err != nil {
			t.Fatalf("Create = %+v, %v; want (nil, nil)", r, err)
		}
		notFound(t, s, k)
		checkInvariants(t, s, d)
	})
}

// A gen generated by this create cannot be claimed except by its own INSERT, which writes the row too: after
// the fence an absent row is a definite "not executed", with no dedup lookup (design.md 7.5).
func TestCreateFreshGenSkipsDedupLookup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		var late func()
		rec := script(d, map[fakedb.Kind][]step{
			fakedb.Insert:     {amb(&late)},
			fakedb.GenClaimed: {fail(db.ErrNotExecuted)},
		})
		in := newRow(k, 1, 0)
		if r, err := s.Create(context.Background(), in); !errors.Is(err, db.ErrNotExecuted) {
			t.Fatalf("Create = %+v, %v; want ErrNotExecuted", r, err)
		}
		if n := rec.count(fakedb.GenClaimed); n != 0 {
			t.Fatalf("GenClaimed lookups = %d, want 0", n)
		}
		checkInvariants(t, s, d)
	})
}

func TestCreateClientGivesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		settle := giveUp(t, s, d, newRow(k, 1, 7))
		if opFree(entryOf(s, k)) {
			t.Fatal("op free while the INSERT is in flight (I5)")
		}
		settle()
		if r, err := s.Get(context.Background(), k); err != nil || r == nil || !reflect.DeepEqual(r, d.Row(k)) {
			t.Fatalf("Get = %+v, %v; want the created row", r, err)
		}
		checkInvariants(t, s, d)
	})
}

func TestUpdateWaitsForCreate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		release := make(chan struct{})
		script(d, map[fakedb.Kind][]step{fakedb.Insert: {blockOn(release)}})
		go s.Create(context.Background(), newRow(k, 1, 7))
		synctest.Wait() // the create is blocked in the INSERT, holding op
		var got *schema.Row
		var err error
		done := make(chan struct{})
		go func() {
			defer close(done)
			got, err = s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}})
		}()
		synctest.Wait()
		close(release)
		<-done
		s.inflight.Wait()
		if err != nil || got == nil || got.Ver != 1 || got.Vals[1] != "b" || got.Gen != 7 {
			t.Fatalf("Update = %+v, %v; want ver 1 on the new row", got, err)
		}
		checkInvariants(t, s, d)
	})
}

// The background create must not read the caller's row after Create has returned.
func TestCreateDoesNotKeepCallerRow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k, other := key(t, s, 1), key(t, s, 2)
		release := make(chan struct{})
		script(d, map[fakedb.Kind][]step{fakedb.Select: {blockOn(release)}})
		in := newRow(k, 1, 7)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if r, err := s.Create(ctx, in); !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("Create = %+v, %v; want ErrOutcomeUnknown", r, err)
		}
		in.Key = other // the caller reuses its row
		in.Vals[1] = "changed"
		close(release)
		s.inflight.Wait()
		if r := d.Row(k); r == nil || r.Vals[1] != "a" {
			t.Fatalf("DB row of the original key = %+v, want the original values", r)
		}
		if r := d.Row(other); r != nil {
			t.Fatalf("DB row of the other key = %+v, want none", r)
		}
		checkInvariants(t, s, d)
	})
}
