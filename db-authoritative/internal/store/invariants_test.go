package store

import (
	"reflect"
	"testing"

	"rowcache/internal/db/fakedb"
)

// checkInvariants runs at a quiescent point (no operation running). For every entry: op is free
// (NoLockLeak); a loaded snapshot equals d.Row(key) exactly, absent meaning no row (CacheCoherent, I4). No
// statement answered ambiguous can still execute (StmtOnlyUnderOp, I5). d.Violation() is nil
// (OneWriterPerKey, NoResurrection, DbValsBelongToGen).
func checkInvariants(t *testing.T, s *Store, d *fakedb.DB) {
	t.Helper()
	var entries []*entry
	for _, sh := range s.shards {
		sh.mu.RLock()
		for _, e := range sh.m {
			entries = append(entries, e)
		}
		sh.mu.RUnlock()
	}
	for _, e := range entries {
		if !opFree(e) {
			t.Errorf("NoLockLeak: op of %q is held at a quiescent point", e.id)
		}
		snap := e.snap.Load()
		if snap == nil {
			continue
		}
		want := d.Row(e.id)
		switch {
		case snap == absent:
			if want != nil {
				t.Errorf("CacheCoherent: %q cached as absent, DB has %+v", e.id, want)
			}
		case want == nil || !reflect.DeepEqual(snap, want):
			t.Errorf("CacheCoherent: %q cached %+v, DB has %+v", e.id, snap, want)
		}
	}
	if n := d.Pending(); n != 0 {
		t.Errorf("StmtOnlyUnderOp (I5): %d statements answered ambiguous may still execute", n)
	}
	if err := d.Violation(); err != nil {
		t.Error(err)
	}
}
