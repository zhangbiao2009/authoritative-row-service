package store

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"rowcache/internal/db/fakedb"
	"rowcache/internal/schema"
)

// design.md 7.3: after the fence the row can only be the loaded one (not applied) or the loaded one with this
// write applied. Anything else means a broken assumption (another writer, a stale cache, a DB gone back): the
// answer is ErrInternal and the snapshot is dropped, never a definite answer that may be wrong.
func TestFenceFindsUnexpectedRow(t *testing.T) {
	loaded := func(k schema.Key) *schema.Row { return testRow(k, 1) } // gen 101, ver 3
	other := func(k schema.Key, gen, ver int64) *schema.Row {
		r := testRow(k, 1)
		r.Gen, r.Ver = gen, ver
		return r
	}
	cases := []struct {
		name  string
		kind  fakedb.Kind
		after func(k schema.Key) *schema.Row // the row the fence will read
		write func(s *Store, k schema.Key) error
	}{
		{"update finds ver+2", fakedb.Update, func(k schema.Key) *schema.Row { return other(k, 101, 5) },
			func(s *Store, k schema.Key) error {
				_, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}})
				return err
			}},
		{"update finds ver going back", fakedb.Update, func(k schema.Key) *schema.Row { return other(k, 101, 2) },
			func(s *Store, k schema.Key) error {
				_, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}})
				return err
			}},
		{"update finds another life", fakedb.Update, func(k schema.Key) *schema.Row { return other(k, 999, 4) },
			func(s *Store, k schema.Key) error {
				_, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}})
				return err
			}},
		{"delete finds another life", fakedb.Delete, func(k schema.Key) *schema.Row { return other(k, 999, 0) },
			func(s *Store, k schema.Key) error { return s.Delete(context.Background(), k, 0) }},
		{"delete finds the row updated", fakedb.Delete, func(k schema.Key) *schema.Row { return other(k, 101, 9) },
			func(s *Store, k schema.Key) error { return s.Delete(context.Background(), k, 0) }},
		{"create finds another life", fakedb.Insert, func(k schema.Key) *schema.Row { return other(k, 999, 0) },
			func(s *Store, k schema.Key) error {
				_, err := s.Create(context.Background(), newRow(k, 1, 7))
				return err
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := fakedb.New()
				s := newTestStore(t, d)
				k := key(t, s, 1)
				if c.kind != fakedb.Insert {
					d.Put(loaded(k))
				}
				script(d, map[fakedb.Kind][]step{c.kind: {behind(d, c.after(k))}})
				if err := c.write(s, k); !errors.Is(err, ErrInternal) {
					t.Fatalf("err = %v, want ErrInternal", err)
				}
				if r := entryOf(s, k).snap.Load(); r != nil {
					t.Fatalf("snapshot %+v kept, want it dropped", r)
				}
				checkInvariants(t, s, d)
			})
		})
	}
}
