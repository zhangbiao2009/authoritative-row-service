package store

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

// negativeScore is the CHECK constraint of these tests: score >= 0.
func negativeScore(r *schema.Row) bool { return r.Vals[2] == int64(-1) }

// quarantine dirties rows 1..5 of one shard, sets row 3's score to -1, and runs one flush round against a
// DB whose CHECK rejects it. It returns the keys and the poison key.
func quarantine(t *testing.T, s *Store, d *fakedb.DB) (ks []schema.Key, poison schema.Key) {
	t.Helper()
	ks = dirtyRows(t, s, d, 5)
	poison = ks[2]
	update(t, s, poison, set(2, int64(-1)))
	d.Reject = negativeScore
	flushAll(s)
	return ks, poison
}

// wantQuarantined checks that only poison is quarantined, still dirty and queued, and that the DB holds
// every other row of ks.
func wantQuarantined(t *testing.T, s *Store, d *fakedb.DB, ks []schema.Key, poison schema.Key) {
	t.Helper()
	for i, k := range ks {
		if k == poison {
			continue
		}
		if r := d.Row(k); r.Vals[1] != "x" {
			t.Fatalf("row %d: DB %+v, want the update written", i+1, r)
		}
	}
	dbMatches(t, d, testRow(poison, 3))
	if got, want := bookkeeping(s, poison), "dirty 0x6 queued true queue 1 dirtyCount 1"; got != want {
		t.Fatalf("poison row: %s, want %s", got, want)
	}
	if got := s.Poisoned(); !slices.Equal(got, []schema.Key{poison}) {
		t.Fatalf("Poisoned() = %q, want [%q]", got, poison)
	}
	if n := s.m.poisoned.Load(); n != 1 {
		t.Fatalf("poisoned gauge %d, want 1", n)
	}
	checkInvariants(t, s, d)
}

// Acceptance (design.md 12.2 M6): the poison row is quarantined, and the other rows of its batch are
// written.
func TestPoisonRowQuarantined(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, oneShard)
	ks, poison := quarantine(t, s, d)
	wantQuarantined(t, s, d, ks, poison)
}

// Review Focus 3: a rejection because of another row (a foreign key) is quarantined the same way.
func TestCrossRowRejectionQuarantines(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, oneShard)
	ks := dirtyRows(t, s, d, 5)
	poison := ks[2]
	update(t, s, poison, set(2, int64(-1)))
	// The rows are written alone in key order; the third one is the poison row.
	order := slices.Sorted(slices.Values(ks))
	steps := make([]step, len(order))
	steps[slices.Index(order, poison)] = fail(db.ErrCrossRow)
	script(d, map[fakedb.Kind][]step{fakedb.UpdateBatch: {fail(db.ErrCrossRow)}, fakedb.Update: steps})
	flushAll(s)
	d.Hook = nil
	wantQuarantined(t, s, d, ks, poison)
}

func TestPoisonNotInNextBatch(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, oneShard)
	ks, poison := quarantine(t, s, d)
	update(t, s, ks[0], set(1, "z"))
	rec := script(d, nil)
	flushAll(s)
	batches := rec.of(fakedb.UpdateBatch)
	if len(batches) != 1 || !slices.Equal(batches[0].Keys, []schema.Key{ks[0]}) {
		t.Fatalf("batches %+v, want one of row 1 only", batches)
	}
	if n := len(rec.of(fakedb.Update)); n != 0 {
		t.Fatalf("%d solo writes before poisonRetryInterval, with no update of the poison row; want 0", n)
	}
	if got := s.Poisoned(); !slices.Equal(got, []schema.Key{poison}) {
		t.Fatalf("Poisoned() = %q, want [%q]", got, poison)
	}
}

func TestPoisonRetriedAfterUpdate(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, oneShard)
	_, poison := quarantine(t, s, d)
	r := update(t, s, poison, set(2, int64(5))) // the human corrects the value
	rec := script(d, nil)
	flushAll(s)
	if w := rec.of(fakedb.Update); len(w) != 1 || w[0].Key != poison {
		t.Fatalf("solo writes %+v, want one of the poison row", w)
	}
	if n := len(rec.of(fakedb.UpdateBatch)); n != 0 {
		t.Fatalf("%d batches, want 0: a poisoned row is written alone", n)
	}
	dbMatches(t, d, r)
	if got := bookkeeping(s, poison); got != clean || len(s.Poisoned()) != 0 || s.m.poisoned.Load() != 0 {
		t.Fatalf("after the retry: %s, Poisoned %q, gauge %d; want clean, none, 0", got, s.Poisoned(), s.m.poisoned.Load())
	}
	checkInvariants(t, s, d)
}

func TestPoisonRetriedAfterInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, oneShard)
		_, poison := quarantine(t, s, d)
		d.Reject = nil // the constraint was dropped; the value is unchanged
		rec := script(d, nil)
		time.Sleep(poisonRetryInterval - time.Second)
		flushAll(s)
		if n := len(rec.of(fakedb.Update)); n != 0 {
			t.Fatalf("%d solo writes before poisonRetryInterval, want 0", n)
		}
		time.Sleep(time.Second)
		flushAll(s)
		if n := len(rec.of(fakedb.Update)); n != 1 {
			t.Fatalf("%d solo writes after poisonRetryInterval, want 1", n)
		}
		if got := bookkeeping(s, poison); got != clean || len(s.Poisoned()) != 0 {
			t.Fatalf("after the retry: %s, Poisoned %q; want clean, none", got, s.Poisoned())
		}
		checkInvariants(t, s, d)
	})
}

// Review Focus 1.
func TestDeletePoisonedRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, oneShard)
	_, poison := quarantine(t, s, d)
	if err := s.Delete(context.Background(), poison, 0); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(s.Poisoned()) != 0 || s.m.poisoned.Load() != 0 {
		t.Fatalf("after the delete: Poisoned %q, gauge %d; want none, 0", s.Poisoned(), s.m.poisoned.Load())
	}
	flushAll(s)
	if got := bookkeeping(s, poison); got != clean {
		t.Fatalf("after the delete and a flush: %s, want %s", got, clean)
	}
	if !evict(t, s, poison) {
		t.Fatal("the deleted row's tombstone was not evicted")
	}
	checkInvariants(t, s, d)
}

func TestPoisonedRowNotEvicted(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d, oneShard)
	_, poison := quarantine(t, s, d)
	if evict(t, s, poison) {
		t.Fatal("a poisoned row was evicted: it is dirty (I10)")
	}
}
