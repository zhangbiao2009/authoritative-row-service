package store

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"rowcache/internal/db"
	"rowcache/internal/db/fakedb"
)

func deletes(rec *recorder) int { return len(rec.of(fakedb.Delete)) }

func TestDeleteRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	if _, err := d.InsertDedup(context.Background(), newRow(k, 1, 101)); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(context.Background(), k, 0); err != nil {
		t.Fatalf("Delete = %v", err)
	}
	notFound(t, s, k)
	if d.Row(k) != nil {
		t.Fatalf("DB row %+v after Delete", d.Row(k))
	}
	if !d.Claimed(k, 101) { // I12
		t.Fatal("dedup record deleted with the row")
	}
	if !opFree(entryOf(s, k)) {
		t.Fatal("op held after Delete returned")
	}
}

func TestDeleteGenMismatch(t *testing.T) {
	d := fakedb.New()
	rec := script(d, nil)
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	if err := s.Delete(context.Background(), k, 999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete = %v, want ErrNotFound", err)
	}
	if n := deletes(rec); n != 0 {
		t.Fatalf("%d Delete statements sent, want 0", n)
	}
	if d.Row(k) == nil {
		t.Fatal("row deleted")
	}
}

func TestDeleteMissingIsNotFound(t *testing.T) {
	d := fakedb.New()
	rec := script(d, nil)
	s := newTestStore(t, d)
	k := key(t, s, 1)
	for i := 0; i < 2; i++ {
		if err := s.Delete(context.Background(), k, 0); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Delete #%d = %v, want ErrNotFound", i, err)
		}
		if n := deletes(rec); n != 0 {
			t.Fatalf("%d Delete statements sent, want 0", n)
		}
		if !opFree(entryOf(s, k)) {
			t.Fatal("op held")
		}
	}
}

func TestDeleteZeroRowsIsInternal(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	if _, err := s.Get(context.Background(), k); err != nil {
		t.Fatal(err)
	}
	r := testRow(k, 1)
	r.Gen = 999
	d.Put(r) // behind the cache
	if err := s.Delete(context.Background(), k, 0); !errors.Is(err, ErrInternal) {
		t.Fatalf("Delete = %v, want ErrInternal", err)
	}
	if entryOf(s, k).snap.Load() != nil {
		t.Fatal("snapshot kept after an invariant broke")
	}
}

func TestDeleteRejected(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	d.SetRejecting(k, true)
	if err := s.Delete(context.Background(), k, 0); !errors.Is(err, db.ErrRejected) {
		t.Fatalf("Delete = %v, want ErrRejected", err)
	}
	if d.Row(k) == nil {
		t.Fatal("row deleted")
	}
	if r, err := s.Get(context.Background(), k); err != nil || r.Gen != 101 {
		t.Fatalf("Get = %+v, %v; want the row", r, err)
	}
}

// TLA: B_2 ambiguous, the statement executed (W_DeleteAmb).
func TestDeleteAmbiguousApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		rec := script(d, map[fakedb.Kind][]step{fakedb.Delete: {lost}})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		if err := s.Delete(context.Background(), k, 0); err != nil {
			t.Fatalf("Delete = %v", err)
		}
		rec.wantFences(t, 1, fakedb.Delete)
		if entryOf(s, k).snap.Load() != absent {
			t.Fatal("absent not cached")
		}
		checkInvariants(t, s, d)
	})
}

// TLA: B_2 ambiguous, the fence dropped the statement: not executed, and the kept statement can never land.
func TestDeleteAmbiguousNotApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		var late func()
		rec := script(d, map[fakedb.Kind][]step{fakedb.Delete: {amb(&late)}})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		if err := s.Delete(context.Background(), k, 0); !errors.Is(err, db.ErrNotExecuted) {
			t.Fatalf("err = %v, want ErrNotExecuted", err)
		}
		rec.wantFences(t, 1, fakedb.Delete)
		late() // too late: the session is fenced
		if d.Row(k) == nil {
			t.Fatal("row deleted after the answer")
		}
		checkInvariants(t, s, d)
	})
}

func TestUpdateAfterDeleteIsNotFound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		release := make(chan struct{})
		rec := script(d, map[fakedb.Kind][]step{fakedb.Delete: {blockOn(release)}})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		var uerr error
		dres := make(chan error, 1)
		go func() { dres <- s.Delete(context.Background(), k, 0) }()
		synctest.Wait() // the Delete is blocked in its statement, holding op
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, uerr = s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "x")}})
		}()
		synctest.Wait()
		close(release)
		<-done
		derr := <-dres
		if derr != nil || !errors.Is(uerr, ErrNotFound) {
			t.Fatalf("Delete = %v, Update = %v; want nil, ErrNotFound", derr, uerr)
		}
		if n := len(rec.of(fakedb.Update)); n != 0 {
			t.Fatalf("%d Update statements sent, want 0", n)
		}
	})
}

// Hole 2: an SDK retry of a create must not resurrect a deleted row (NoResurrection).
func TestCreateRetryAfterDeleteDoesNotResurrect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		giveUp(t, s, d, newRow(k, 1, 7))()
		if err := s.Delete(context.Background(), k, 0); err != nil {
			t.Fatalf("Delete = %v", err)
		}
		if r, err := s.Create(context.Background(), newRow(k, 1, 7)); r != nil || err != nil {
			t.Fatalf("retry = %+v, %v; want nil, nil", r, err)
		}
		notFound(t, s, k)
		if d.Row(k) != nil {
			t.Fatalf("DB row %+v: resurrected", d.Row(k))
		}
		if v := d.Violation(); v != nil {
			t.Fatal(v)
		}
	})
}

// Hole 6: a retry of a create that took effect must not answer "already exists" because another life is there.
func TestCreateRetryFindsOtherLife(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		giveUp(t, s, d, newRow(k, 1, 7))()
		if err := s.Delete(context.Background(), k, 0); err != nil {
			t.Fatalf("Delete = %v", err)
		}
		if r, err := s.Create(context.Background(), newRow(k, 1, 8)); err != nil || r == nil || r.Gen != 8 {
			t.Fatalf("Create gen 8 = %+v, %v", r, err)
		}
		if r, err := s.Create(context.Background(), newRow(k, 1, 7)); r != nil || err != nil {
			t.Fatalf("retry = %+v, %v; want nil, nil", r, err)
		}
		if r := d.Row(k); r == nil || r.Gen != 8 {
			t.Fatalf("DB row %+v, want gen 8", r)
		}
		if v := d.Violation(); v != nil {
			t.Fatal(v)
		}
	})
}
