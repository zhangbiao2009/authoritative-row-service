package store

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"rowcache/internal/db"
	"rowcache/internal/db/fakedb"
	"rowcache/internal/schema"
)

func set(col int, v any) ColChange    { return ColChange{Col: col, Op: OpSet, Val: v} }
func incr(col int, d int64) ColChange { return ColChange{Col: col, Op: OpIncr, Val: d} }

func entryOf(s *Store, k schema.Key) *entry {
	sh := s.shardFor(k)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.m[k]
}

// opFree reports whether e.op is free, taking and releasing it if so (NoLockLeak).
func opFree(e *entry) bool {
	select {
	case e.op <- struct{}{}:
		<-e.op
		return true
	default:
		return false
	}
}

func TestUpdateSetAndIncr(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1)) // gen 101, ver 3, {1, "a", 10}
	got, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b"), incr(2, 5)}})
	want := &schema.Row{Key: k, Gen: 101, Ver: 4, Vals: []any{int64(1), "b", int64(15)}}
	if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(d.Row(k), want) {
		t.Fatalf("Update = %+v, %v; DB %+v; want %+v", got, err, d.Row(k), want)
	}
	if r, _ := s.Get(context.Background(), k); r != got {
		t.Fatalf("Get = %p, want the row Update returned %p", r, got)
	}
	if !opFree(entryOf(s, k)) {
		t.Fatal("op held after Update returned")
	}
}

func TestUpdateLoadsFirst(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	var kinds []fakedb.Kind
	d.Hook = func(_ context.Context, st *fakedb.Stmt) error {
		kinds = append(kinds, st.Kind)
		return nil
	}
	got, err := s.Update(context.Background(), k, Change{Cols: []ColChange{incr(2, 1)}})
	if err != nil || got.Vals[2] != int64(11) || got.Vals[1] != "a" {
		t.Fatalf("Update = %+v, %v", got, err)
	}
	if want := []fakedb.Kind{fakedb.Select, fakedb.Update}; !reflect.DeepEqual(kinds, want) {
		t.Fatalf("statements = %v, want %v", kinds, want)
	}
}

func TestUpdateMissingRow(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	for i := 0; i < 2; i++ {
		if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "x")}}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Update #%d: %v, want ErrNotFound", i, err)
		}
		if d.Updates() != 0 {
			t.Fatalf("Updates = %d, want 0", d.Updates())
		}
		if !opFree(entryOf(s, k)) {
			t.Fatal("op held")
		}
	}
}

func TestUpdateWithStaleGen(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1)) // gen 101
	if _, err := s.Update(context.Background(), k, Change{Gen: 999, Cols: []ColChange{set(1, "x")}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Update with another gen: %v, want ErrNotFound", err)
	}
	if d.Updates() != 0 {
		t.Fatalf("Updates = %d, want 0", d.Updates())
	}
	if r, err := s.Update(context.Background(), k, Change{Gen: 101, Cols: []ColChange{set(1, "x")}}); err != nil || r.Vals[1] != "x" {
		t.Fatalf("Update with the row's gen = %+v, %v", r, err)
	}
}

func TestUpdateRejectsMalformedChange(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	before, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatal(err)
	}
	for name, cols := range map[string][]ColChange{
		"no columns":     nil,
		"primary key":    {set(0, int64(9))}, // I11
		"out of range":   {set(3, "x")},
		"negative":       {set(-1, "x")},
		"same col twice": {set(1, "x"), set(1, "y")},
		"unknown op":     {{Col: 1, Op: 9, Val: "x"}},
	} {
		if _, err := s.Update(context.Background(), k, Change{Cols: cols}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	if d.Updates() != 0 {
		t.Fatalf("Updates = %d, want 0", d.Updates())
	}
	if after, _ := s.Get(context.Background(), k); after != before || !reflect.DeepEqual(d.Row(k), testRow(k, 1)) {
		t.Fatalf("row changed to %+v", after)
	}
}

func TestFailedIncrementSendsNothing(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	for name, c := range map[string]struct {
		score any
		cols  []ColChange
	}{
		"NULL":                  {nil, []ColChange{incr(2, 1)}},
		"overflow":              {int64(math.MaxInt64), []ColChange{incr(2, 1)}},
		"set then failing incr": {nil, []ColChange{set(1, "z"), incr(2, 1)}},
	} {
		id := int64(len(name))
		k := key(t, s, id)
		r := testRow(k, id)
		r.Vals[2] = c.score
		d.Put(r)
		if _, err := s.Update(context.Background(), k, Change{Cols: c.cols}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
		if got, _ := s.Get(context.Background(), k); got.Vals[1] != "a" || d.Row(k).Vals[1] != "a" {
			t.Errorf("%s: name changed: memory %+v, DB %+v", name, got, d.Row(k))
		}
	}
	if d.Updates() != 0 {
		t.Fatalf("Updates = %d, want 0", d.Updates())
	}
}

func TestUpdateLoadErrorSendsNothing(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	d.Hook = func(_ context.Context, st *fakedb.Stmt) error {
		if st.Kind == fakedb.Select {
			return errors.New("boom")
		}
		return nil
	}
	if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "x")}}); !errors.Is(err, db.ErrNotExecuted) {
		t.Fatalf("err = %v, want ErrNotExecuted", err)
	}
	if d.Updates() != 0 {
		t.Fatalf("Updates = %d, want 0", d.Updates())
	}
}

func TestUpdateCachesDBValue(t *testing.T) {
	d := fakedb.New()
	d.Normalize = func(_ int, v any) any {
		if s, ok := v.(string); ok {
			return strings.ToUpper(s)
		}
		return v
	}
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	got, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}}) // I7
	if err != nil || got.Vals[1] != "B" {
		t.Fatalf("Update = %+v, %v; want name B", got, err)
	}
	if r, _ := s.Get(context.Background(), k); r.Vals[1] != "B" || d.Row(k).Vals[1] != "B" {
		t.Fatalf("Get %+v, DB %+v; want name B", r, d.Row(k))
	}
}

func TestUpdateZeroRowsIsInternal(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	if _, err := s.Get(context.Background(), k); err != nil { // caches gen 101
		t.Fatal(err)
	}
	other := testRow(k, 1)
	other.Gen = 999
	d.Put(other) // behind the cache's back
	_, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "x")}})
	if !errors.Is(err, ErrInternal) {
		t.Fatalf("err = %v, want ErrInternal", err)
	}
	e := entryOf(s, k)
	if e.snap.Load() != nil || !opFree(e) {
		t.Fatal("snapshot not dropped, or op held")
	}
	if r, err := s.Get(context.Background(), k); err != nil || r.Gen != 999 {
		t.Fatalf("Get = %+v, %v; want gen 999", r, err)
	}
}
