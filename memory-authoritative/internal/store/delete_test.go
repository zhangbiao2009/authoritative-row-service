package store

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

func del(t *testing.T, s *Store, k schema.Key, gen int64) {
	t.Helper()
	if err := s.Delete(context.Background(), k, gen); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

// loaded puts testRow(k, 1) (gen 101) in the DB and loads it.
func loaded(t *testing.T, s *Store, d *fakedb.DB, k schema.Key) *schema.Row {
	t.Helper()
	d.Put(testRow(k, 1))
	r, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// memMatchesDB checks that Get and the DB hold the same row.
func memMatchesDB(t *testing.T, s *Store, d *fakedb.DB, k schema.Key) {
	t.Helper()
	r, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	dbMatches(t, d, r)
}

func TestDeleteRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	create(t, s, newRow(k, 1, 100))
	del(t, s, k, 0)
	notFound(t, s, k)
	if d.Row(k) != nil || !d.Claimed(k, 100) {
		t.Fatalf("DB row %+v, claimed %v; want no row, the dedup record kept", d.Row(k), d.Claimed(k, 100))
	}
	checkInvariants(t, s, d)
}

func TestDeleteGenMismatch(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	r := create(t, s, newRow(k, 1, 100))
	if err := s.Delete(context.Background(), k, 999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete of another gen: %v, want ErrNotFound", err)
	}
	dbMatches(t, d, r)
	del(t, s, k, 100)
	notFound(t, s, k)
}

// Mutation LeakOpOnNotFound.
func TestDeleteMissingIsNotFound(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	rec := script(d, nil)
	if err := s.Delete(context.Background(), k, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete: %v, want ErrNotFound", err)
	}
	if n := len(rec.of(fakedb.Delete)); n != 0 {
		t.Fatalf("Delete statements = %d, want 0", n)
	}
	checkInvariants(t, s, d) // op released
}

// Mutation DeleteKeepsDirty; gap delete-db-done (tla-mapping.md 3.1): an Update after the DB DELETE and
// before the tombstone is published is discarded with the row.
func TestUpdateDuringDeleteIsDiscarded(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	loaded(t, s, d, k)
	script(d, map[fakedb.Kind][]step{fakedb.Delete: {func(st *fakedb.Stmt) error {
		st.Exec()
		if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "x")}}); err != nil {
			t.Errorf("Update during the delete: %v", err)
		}
		return nil
	}}})
	del(t, s, k, 0)
	notFound(t, s, k)
	if s.dirtyCount.Load() != 0 {
		t.Fatalf("dirtyCount %d after the delete, want 0", s.dirtyCount.Load())
	}
	flushAll(s)
	if got := bookkeeping(s, k); got != clean || d.Updates() != 0 {
		t.Fatalf("after a flush: %s, Updates %d; want %s, 0", got, d.Updates(), clean)
	}
	checkInvariants(t, s, d)
}

// Mutation FlushNoOp; Review Focus 4: a Delete waits for a flush write of the same key (I5).
func TestDeleteWaitsForFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		loaded(t, s, d, k)
		update(t, s, k, set(1, "x"))
		release := make(chan struct{})
		rec := script(d, map[fakedb.Kind][]step{fakedb.Update: {blockOn(release)}})
		var wg sync.WaitGroup
		wg.Go(func() { flushAll(s) })
		synctest.Wait() // the flush write is in progress
		deleted := make(chan error, 1)
		go func() { deleted <- s.Delete(context.Background(), k, 0) }()
		synctest.Wait()
		if n := len(rec.of(fakedb.Delete)); n != 0 {
			t.Fatalf("a Delete statement was sent while the flush write was in progress")
		}
		close(release)
		wg.Wait()
		if err := <-deleted; err != nil || d.Row(k) != nil {
			t.Fatalf("Delete: %v, DB row %+v; want nil, no row", err, d.Row(k))
		}
	})
}

// Review Focus 5.
func TestDeleteOutcomeUnknown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		r := loaded(t, s, d, k)
		release := make(chan struct{})
		script(d, map[fakedb.Kind][]step{fakedb.Delete: {blockOn(release)}})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Delete(ctx, k, 0); !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("Delete: %v, want ErrOutcomeUnknown", err)
		}
		if g, err := s.Get(context.Background(), k); err != nil || g != r {
			t.Fatalf("Get while the delete runs = %+v, %v; want the row (linearized before the delete)", g, err)
		}
		close(release)
		s.inflight.Wait()
		notFound(t, s, k)
		if d.Row(k) != nil {
			t.Fatalf("DB row %+v after the delete settled", d.Row(k))
		}
		checkInvariants(t, s, d)
	})
}

func TestDeleteAmbiguousRetries(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	loaded(t, s, d, k)
	rec := script(d, map[fakedb.Kind][]step{fakedb.Delete: {lost}})
	del(t, s, k, 0)
	if n := len(rec.of(fakedb.Delete)); n != 2 {
		t.Fatalf("Delete statements = %d, want 2", n)
	}
	notFound(t, s, k)
	checkInvariants(t, s, d)
}

// Design 8.2 counterexample 3; mutation NoGenCheck: a late DELETE must not delete the next life.
func TestLateDeleteDoesNotDeleteNewLife(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	create(t, s, newRow(k, 1, 100))
	var late func()
	var session db.SessionID
	script(d, map[fakedb.Kind][]step{fakedb.Delete: {amb(&late, &session)}})
	del(t, s, k, 0) // the first DELETE is stuck; the retry deletes the row
	create(t, s, newRow(k, 1, 200))
	late() // the first DELETE (gen 100) lands now
	if r := d.Row(k); r == nil || r.Gen != 200 {
		t.Fatalf("DB row %+v, want gen 200", r)
	}
	memMatchesDB(t, s, d, k)
}

// Hole 4, delete (13.2): the first DELETE executed, its answer was lost; the second was not executed.
func TestDeleteAmbiguousThenNotExecutedIsNotFinal(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	loaded(t, s, d, k)
	rec := script(d, map[fakedb.Kind][]step{fakedb.Delete: {lost, fail(db.ErrNotExecuted)}})
	del(t, s, k, 0)
	if n := len(rec.of(fakedb.Delete)); n != 3 {
		t.Fatalf("Delete statements = %d, want 3", n)
	}
	notFound(t, s, k)
	checkInvariants(t, s, d)
}

// Hole 5, delete (13.2): a child row blocks the retry; once it is gone, the first DELETE would land.
func TestDeleteCrossRowAfterAmbiguousIsFenced(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	loaded(t, s, d, k)
	d.SetCrossRow(k, true)
	var late func()
	var session db.SessionID
	rec := script(d, map[fakedb.Kind][]step{fakedb.Delete: {amb(&late, &session)}})
	if err := s.Delete(context.Background(), k, 0); !db.IsCrossRow(err) {
		t.Fatalf("Delete: %v, want a cross-row rejection", err)
	}
	if f := rec.of(fakedb.Fence); len(f) != 1 || !reflect.DeepEqual(f[0].Sessions, []db.SessionID{session}) {
		t.Fatalf("Fence statements %+v, want one with session %d", f, session)
	}
	d.SetCrossRow(k, false)
	late()
	memMatchesDB(t, s, d, k) // the row is still in both
	checkInvariants(t, s, d)
}

// Hole 1 (13.2; trace: hole1); mutations ReleaseOnAmbiguous, SelectConfirms, DedupDeletedWithRow: an
// INSERT that times out and lands after a delete must not resurrect the row.
func TestLateInsertDoesNotResurrect(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	var late func()
	var session db.SessionID
	script(d, map[fakedb.Kind][]step{fakedb.Insert: {amb(&late, &session)}})
	if r := create(t, s, newRow(k, 1, 100)); r.Gen != 100 {
		t.Fatalf("Create = %+v, want gen 100", r)
	}
	del(t, s, k, 100)
	late() // INSERT A finally executes
	if d.Row(k) != nil {
		t.Fatalf("the deleted row is back: %+v", d.Row(k))
	}
	notFound(t, s, k)
	checkInvariants(t, s, d)
}

// createGivesUp creates gen 100 with a client that gives up while the INSERT is slow; the INSERT then
// succeeds in the background.
func createGivesUp(t *testing.T, s *Store, d *fakedb.DB, k schema.Key) {
	t.Helper()
	release := make(chan struct{})
	script(d, map[fakedb.Kind][]step{fakedb.Insert: {blockOn(release)}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if r, err := s.Create(ctx, newRow(k, 1, 100)); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("Create = %+v, %v; want ErrOutcomeUnknown", r, err)
	}
	close(release)
	s.inflight.Wait()
	d.Hook = nil
}

// Hole 2 (13.2; trace: hole2): the client's retry, after someone deleted the row, must not insert it again.
func TestCreateRetryAfterDeleteDoesNotResurrect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		createGivesUp(t, s, d, k)
		del(t, s, k, 0)
		if r, err := s.Create(context.Background(), newRow(k, 1, 100)); r != nil || err != nil {
			t.Fatalf("retry = %+v, %v; want (nil, nil): the create took effect earlier", r, err)
		}
		if d.Row(k) != nil {
			t.Fatalf("the deleted row is back: %+v", d.Row(k))
		}
		notFound(t, s, k)
	})
}

// Hole 6 (13.2; trace: hole6): a retry that finds another life must not be told "already exists".
func TestCreateRetryFindsOtherLife(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		k := key(t, s, 1)
		createGivesUp(t, s, d, k)
		del(t, s, k, 0)
		r200 := create(t, s, newRow(k, 1, 200))
		if r, err := s.Create(context.Background(), newRow(k, 1, 100)); r != nil || err != nil {
			t.Fatalf("retry of gen 100 = %+v, %v; want (nil, nil), not ErrAlreadyExists", r, err)
		}
		if g, _ := s.Get(context.Background(), k); g != r200 {
			t.Fatalf("Get = %+v, want the gen 200 row", g)
		}
		dbMatches(t, d, r200)
		if r, err := s.Create(context.Background(), newRow(k, 1, 300)); !errors.Is(err, ErrAlreadyExists) || d.Claimed(k, 300) {
			t.Fatalf("Create of a new gen = %+v, %v, claimed %v; want ErrAlreadyExists, not claimed", r, err, d.Claimed(k, 300))
		}
	})
}

// lateFlush creates gen 100, updates it, and runs a flush whose write times out; it returns the write.
func lateFlush(t *testing.T, s *Store, d *fakedb.DB, k schema.Key) (late func()) {
	t.Helper()
	create(t, s, newRow(k, 1, 100))
	update(t, s, k, set(1, "old"))
	var session db.SessionID
	script(d, map[fakedb.Kind][]step{fakedb.Update: {amb(&late, &session)}})
	flushAll(s)
	d.Hook = nil
	return late
}

// Design 8.2 counterexample 2; mutation NoGenCheck; gap flush-late: a late flush of the old life must not
// write into the new one.
func TestLateFlushAcrossDeleteAndCreate(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	late := lateFlush(t, s, d, k)
	del(t, s, k, 0)
	in := newRow(k, 1, 200)
	in.Vals[1] = "new"
	r := create(t, s, in)
	late()
	dbMatches(t, d, r) // gen 200, ver 0, name "new"
	memMatchesDB(t, s, d, k)
}

// Mutation FlushUpsert: a late flush after a delete must not insert the row again.
func TestLateFlushAfterDeleteDoesNotResurrect(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	late := lateFlush(t, s, d, k)
	del(t, s, k, 0)
	late()
	if d.Row(k) != nil {
		t.Fatalf("the deleted row is back: %+v", d.Row(k))
	}
	notFound(t, s, k)
}
