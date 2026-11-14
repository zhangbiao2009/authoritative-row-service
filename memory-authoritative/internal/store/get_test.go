package store

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

func newTestStore(t *testing.T, d *fakedb.DB) *Store {
	t.Helper()
	sch, err := schema.New("items", []schema.Column{
		{Name: "id", Type: schema.Int8}, {Name: "name", Type: schema.Text}, {Name: "score", Type: schema.Int8},
	}, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.NumShards = 4
	s, err := New(sch, d, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func key(t *testing.T, s *Store, id int64) schema.Key {
	t.Helper()
	k, err := s.schema.EncodeKey([]any{id})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func testRow(k schema.Key, id int64) *schema.Row {
	return &schema.Row{Key: k, Gen: id + 100, Ver: 3, Vals: []any{id, "a", int64(10)}}
}

func TestGetLoadsOnceThenServesFromMemory(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	r1, err := s.Get(context.Background(), k)
	if err != nil || !reflect.DeepEqual(r1, testRow(k, 1)) {
		t.Fatalf("Get = %+v, %v; want %+v", r1, err, testRow(k, 1))
	}
	r2, err := s.Get(context.Background(), k)
	if err != nil || r2 != r1 {
		t.Fatalf("second Get = %p, %v; want the same snapshot %p", r2, err, r1)
	}
	if n := d.Selects(); n != 1 {
		t.Fatalf("Selects = %d, want 1", n)
	}
}

func TestGetMissingIsNegativelyCached(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	for range 2 {
		if _, err := s.Get(context.Background(), k); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	}
	if n := d.Selects(); n != 1 {
		t.Fatalf("Selects = %d, want 1", n)
	}
}

func TestGetOrCreateOneEntryPerKey(t *testing.T) { // I1
	s := newTestStore(t, fakedb.New())
	k := key(t, s, 1)
	got := make([]*entry, 64)
	var wg sync.WaitGroup
	for i := range got {
		wg.Go(func() { got[i] = s.getOrCreate(k) })
	}
	wg.Wait()
	for _, e := range got {
		if e != got[0] {
			t.Fatal("getOrCreate returned different entries for one key")
		}
	}
	n := 0
	for _, sh := range s.shards {
		n += len(sh.m)
	}
	if n != 1 {
		t.Fatalf("entries in map = %d, want 1", n)
	}
}

func TestNewRejectsBadNumShards(t *testing.T) {
	sch, err := schema.New("t", []schema.Column{{Name: "id", Type: schema.Int8}}, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, -4, 3, 6} {
		cfg := DefaultConfig()
		cfg.NumShards = n
		if _, err := New(sch, fakedb.New(), cfg); err == nil {
			t.Errorf("New accepted NumShards = %d", n)
		}
	}
}
