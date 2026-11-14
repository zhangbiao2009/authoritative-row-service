# M1: Skeleton, Key Encoding, fakedb, Get + Load — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Go module with the schema abstraction and primary key encoding, a `db.DB` interface with an in-memory `fakedb`, and a sharded `Store` whose `Get` loads each key once and then serves it lock-free from memory.

**Architecture:** One `Store` per table holds `NumShards` shards; each shard maps an encoded `Key` to an `entry` whose `snap` is an immutable `*Row` (nil = not loaded, `absent` = does not exist). `Get` reads `snap` without locks; on a miss it queues on the entry's `op` lock, re-checks `snap`, and loads from `db.DB` (design.md 7.1, 7.2).

**Tech Stack:** Go 1.25, standard library only (`testing/synctest` for time and blocking).

**Spec:** [design.md](../../design.md) — sections 3, 4, 5.2, 5.3 (primary keys), 6, 7.1, 7.2, 12.2 (M1). Also [CLAUDE.md](../../CLAUDE.md) and [tla-mapping.md](../../tla-mapping.md).

## Global Constraints

- All paths below are relative to `memory-authoritative/`. Run every command from there.
- Work on branch `memory-authoritative-m1`, one commit per task. The user reviews the branch and merges it.
- `go.mod`: `module rowsvc`, `go 1.25`. No third-party dependencies in M1.
- Done means: `gofmt -l .` prints nothing, `go vet ./...` is clean, `go test -race ./...` passes.
- Lock rules (design.md 6.4): `mu` and `shard.mu` are leaf locks; while holding them no other lock, no channel operation, no IO, no logging. Only `op` may be held while taking other locks. Functions that require a lock held end in `Locked`.
- Comments: invariants by ID where the code maintains them (I1, I2, I3, I8, I11 in M1) and each lock's role; mark each TLA step with `// TLA: <Action>` (tla-mapping.md section 3: `Goc`, `G_read`, `L_lock`, `L_load`).
- **Deliberately not in M1** (added by the milestone that needs them; do not add them early): `entry.dead` and its checks, `dirty`, `queued`, `touch`/`lastAccess`/`nowSec`, `entryCount`, `Update`/`Create`/`Delete`, flush, eviction, the PostgreSQL implementation. Value validation beyond primary key columns is M7/M8.

### Decisions in this plan that the design does not fix

1. `Key` and `Row` live in package `schema`, not `store` as in the design.md 5.2 skeleton: `db.DB.Select` returns a `Row`, and `store` imports `db`, so `Row` in `store` would be an import cycle.
2. Composite key length prefix: 4-byte big-endian `uint32`, then the column's bytes. Single-column keys have no prefix (5.3). The encoding cannot change once live (5.1).
3. `schema.Type` defines only the types M1 uses: `Int2`, `Int4`, `Int8`, `Text`, `UUID`. M7 adds the rest of the 5.3 table.
4. The `db.DB` interface has only `Select`; later milestones add methods as they need them.

## Review Focus

1. Composite keys whose bytes shift between columns, e.g. `("a","bc")` vs `("ab","c")`, must encode differently. Test: `TestEncodeKeyComposite` (Task 1).
2. A Go `int` (not `int64`), a string, or `nil` passed for an integer key column must be rejected with an error, not panic or silently encode. Test: `TestEncodeKeyRejects` (Task 1).
3. A Get whose context is already cancelled on an unloaded key returns the context error and does not poison later Gets. Test: `TestGetWithCancelledContext` (Task 4).
4. A caller that modifies the `Vals` of a row returned by `fakedb` must not change the fakedb's stored row. Test: `TestSelect` (Task 2).
5. Many Gets queued behind a failing load return the cached error instead of each waiting for its own DB timeout. Test: `TestLoadErrorIsCachedForLoadErrTTL` (Task 4).

---

### Task 1: Module and schema with primary key encoding

**Files:**
- Create: `go.mod`
- Create: `internal/schema/schema.go`
- Test: `internal/schema/schema_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```go
  package schema
  type Key string                       // canonical primary key encoding; a map key
  type Row struct {                     // immutable once published (I2, I11)
      Key  Key
      Gen  int64
      Ver  int64
      Vals []any                        // by column ordinal; neither the slice nor its values may be modified
  }
  type Type uint8
  const ( Int2 Type = iota + 1; Int4; Int8; Text; UUID )
  type Column struct { Name string; Type Type }
  type Schema struct { Table string; Cols []Column; PK []int } // immutable after New (I11); PK = column ordinals in key order
  func New(table string, cols []Column, pk []int) (*Schema, error)
  func (s *Schema) EncodeKey(pk []any) (Key, error) // pk values in PK order
  ```

- [ ] **Step 1: Create the branch and the module**

```bash
git checkout -b memory-authoritative-m1
go mod init rowsvc && go mod edit -go=1.25
```

- [ ] **Step 2: Write the failing tests**

Go values per type: `Int2`/`Int4`/`Int8` take `int64`; `Text` takes `string`; `UUID` takes `[16]byte`.

```go
package schema

import (
	"strings"
	"testing"
)

func oneCol(t *testing.T, typ Type) *Schema {
	t.Helper()
	s, err := New("t", []Column{{Name: "id", Type: typ}}, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestEncodeKeySingleColumn(t *testing.T) {
	cases := []struct {
		name string
		typ  Type
		val  any
		want Key
	}{
		{"int8", Int8, int64(1), Key("\x00\x00\x00\x00\x00\x00\x00\x01")},
		{"int8 negative", Int8, int64(-1), Key("\xff\xff\xff\xff\xff\xff\xff\xff")},
		{"int4", Int4, int64(258), Key("\x00\x00\x00\x00\x00\x00\x01\x02")},
		{"int2 min", Int2, int64(-32768), Key("\xff\xff\xff\xff\xff\xff\x80\x00")},
		{"text", Text, "abc", Key("abc")},
		{"text empty", Text, "", Key("")},
		{"uuid", UUID, [16]byte{0: 1, 15: 16}, Key("\x01" + strings.Repeat("\x00", 14) + "\x10")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := oneCol(t, c.typ).EncodeKey([]any{c.val})
			if err != nil || got != c.want {
				t.Fatalf("EncodeKey(%v) = %q, %v; want %q", c.val, got, err, c.want)
			}
		})
	}
}

func TestEncodeKeyComposite(t *testing.T) {
	// PK order (n, tenant) differs from column order: encoding follows PK order.
	s, err := New("t", []Column{{Name: "tenant", Type: Text}, {Name: "n", Type: Int8}, {Name: "v", Type: Text}}, []int{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.EncodeKey([]any{int64(7), "ab"})
	want := Key("\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x07" + "\x00\x00\x00\x02ab")
	if err != nil || got != want {
		t.Fatalf("EncodeKey = %q, %v; want %q", got, err, want)
	}

	s2, err := New("t", []Column{{Name: "a", Type: Text}, {Name: "b", Type: Text}}, []int{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	k1, _ := s2.EncodeKey([]any{"a", "bc"})
	k2, _ := s2.EncodeKey([]any{"ab", "c"})
	if k1 == k2 {
		t.Fatalf("(a, bc) and (ab, c) both encode to %q", k1)
	}
}

func TestEncodeKeyRejects(t *testing.T) {
	s, err := New("t", []Column{{Name: "id", Type: Int4}, {Name: "name", Type: Text}}, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	for name, pk := range map[string][]any{
		"too few":        {},
		"too many":       {int64(1), int64(2)},
		"go int":         {1},
		"string for int": {"1"},
		"nil":            {nil},
		"int4 overflow":  {int64(1) << 31},
		"int4 underflow": {-(int64(1) << 31) - 1},
	} {
		if k, err := s.EncodeKey(pk); err == nil {
			t.Errorf("%s: EncodeKey(%v) = %q, want error", name, pk, k)
		}
	}
	if k, err := oneCol(t, Int2).EncodeKey([]any{int64(32768)}); err == nil {
		t.Errorf("int2 overflow: got %q, want error", k)
	}
	if k, err := oneCol(t, UUID).EncodeKey([]any{[]byte("0123456789abcdef")}); err == nil {
		t.Errorf("uuid as []byte: got %q, want error", k)
	}
}

func TestNewRejects(t *testing.T) {
	cols := []Column{{Name: "id", Type: Int8}, {Name: "name", Type: Text}}
	for name, pk := range map[string][]int{
		"no primary key": nil,
		"out of range":   {2},
		"negative":       {-1},
		"duplicate":      {0, 0},
	} {
		if _, err := New("t", cols, pk); err == nil {
			t.Errorf("%s: New accepted pk %v", name, pk)
		}
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./internal/schema/`
Expected: FAIL (build errors: `New`, `EncodeKey` undefined).

- [ ] **Step 4: Implement `schema.go`**

Types and signatures as in the Interfaces block. `New` copies `cols` and `pk` so the caller cannot change them later. `EncodeKey` encodes integers as `uint64(v)` big-endian (`binary.BigEndian.AppendUint64`), range-checking `Int2` and `Int4`; `Text` as its bytes; `UUID` as its 16 bytes. With one PK column, the key is that encoding; with several, each column's encoding is preceded by its length as a 4-byte big-endian `uint32`. Errors are plain `fmt.Errorf` values naming the column.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -race ./internal/schema/ && go vet ./... && gofmt -l .`
Expected: `ok`, no vet output, no gofmt output.

- [ ] **Step 6: Commit**

```bash
git add go.mod internal/schema
git commit -m "M1: schema and primary key encoding"
```

---

### Task 2: `db.DB` interface and `fakedb`

**Files:**
- Create: `internal/db/db.go`
- Create: `internal/db/fakedb/fakedb.go`
- Test: `internal/db/fakedb/fakedb_test.go`

**Interfaces:**
- Consumes: `schema.Key`, `schema.Row` (Task 1).
- Produces:
  ```go
  package db
  var ErrNoRows = errors.New("no rows")
  // DB is one table's storage. Implementations: fakedb (tests) and PostgreSQL (M7).
  type DB interface {
      // Select returns the row, or ErrNoRows. A late read is harmless, so it needs no gen/ver (7.2).
      Select(ctx context.Context, key schema.Key) (*schema.Row, error)
  }

  package fakedb
  type DB struct {
      // SelectHook, if set, runs at the start of every Select, without fakedb's lock held.
      // A non-nil result is returned instead of reading. Set it before concurrent use.
      SelectHook func(ctx context.Context, key schema.Key) error
      // unexported: mutex, rows map[schema.Key]*schema.Row, select counter
  }
  func New() *DB
  func (d *DB) Put(r *schema.Row)  // test setup: stores a copy (Vals cloned)
  func (d *DB) Selects() int       // number of Select calls so far, including ones the hook failed
  func (d *DB) Select(ctx context.Context, key schema.Key) (*schema.Row, error) // returns a copy (Vals cloned)
  ```

- [ ] **Step 1: Write the failing tests**

```go
package fakedb

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"rowsvc/internal/db"
	"rowsvc/internal/schema"
)

var _ db.DB = (*DB)(nil)

func TestSelect(t *testing.T) {
	d := New()
	ctx := context.Background()
	if _, err := d.Select(ctx, "k"); !errors.Is(err, db.ErrNoRows) {
		t.Fatalf("Select of missing key: err = %v, want ErrNoRows", err)
	}
	d.Put(&schema.Row{Key: "k", Gen: 5, Ver: 2, Vals: []any{int64(1), "a"}})
	r, err := d.Select(ctx, "k")
	want := &schema.Row{Key: "k", Gen: 5, Ver: 2, Vals: []any{int64(1), "a"}}
	if err != nil || !reflect.DeepEqual(r, want) {
		t.Fatalf("Select = %+v, %v; want %+v", r, err, want)
	}
	r.Vals[1] = "changed"
	if r2, _ := d.Select(ctx, "k"); r2.Vals[1] != "a" {
		t.Fatalf("modifying a returned row changed the stored row: %v", r2.Vals[1])
	}
	if n := d.Selects(); n != 3 {
		t.Fatalf("Selects = %d, want 3", n)
	}
}

func TestSelectHook(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1})
	boom := errors.New("boom")
	d.SelectHook = func(context.Context, schema.Key) error { return boom }
	if _, err := d.Select(context.Background(), "k"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if n := d.Selects(); n != 1 {
		t.Fatalf("Selects = %d, want 1", n)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/db/...`
Expected: FAIL (build errors: package `db` / `New` undefined).

- [ ] **Step 3: Implement `db.go` and `fakedb.go`**

As in the Interfaces block. `Select`: count the call under the mutex, release it, run the hook, return `ctx.Err()` if the context is done, then read under the mutex.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race ./internal/db/... && go vet ./... && gofmt -l .`
Expected: `ok`, no other output.

- [ ] **Step 5: Commit**

```bash
git add internal/db
git commit -m "M1: db.DB interface and fakedb"
```

---

### Task 3: Store, shards, entries, `Get` + load

**Files:**
- Create: `internal/store/store.go` — `Store`, `Config`, `DefaultConfig`, `New`, errors, `shard`, `entry`, `absent`, `shardFor`, `getOrCreate`, `lockOp`, `unlockOp`
- Create: `internal/store/get.go` — `Get`, `load`, `ensureLoadedLocked`
- Test: `internal/store/get_test.go`

**Interfaces:**
- Consumes: `schema.Schema`, `schema.Key`, `schema.Row` (Task 1); `db.DB`, `db.ErrNoRows`, `fakedb` (Task 2).
- Produces:
  ```go
  package store
  var ErrNotFound = errors.New("not found")
  type Config struct {
      NumShards  int           // power of 2
      LoadErrTTL time.Duration // after a load failure, how long the same error is returned directly
  }
  func DefaultConfig() Config // NumShards 256, LoadErrTTL 100ms (11.4)
  func New(sch *schema.Schema, d db.DB, cfg Config) (*Store, error) // error unless NumShards is a positive power of 2
  func (s *Store) Get(ctx context.Context, key schema.Key) (*schema.Row, error) // ErrNotFound if absent; the row is shared and read-only

  // unexported, used by Task 4 and later milestones:
  type Store struct { schema *schema.Schema; shards []*shard; db db.DB; cfg Config; seed maphash.Seed }
  type shard struct { mu sync.RWMutex; m map[schema.Key]*entry }
  type entry struct {
      id            schema.Key
      snap          atomic.Pointer[schema.Row] // nil = not loaded; absent = does not exist
      op            chan struct{}              // cap 1
      mu            sync.Mutex
      lastLoadErr   error                      // op held
      lastLoadErrAt time.Time                  // op held
  }
  var absent = &schema.Row{}
  func (s *Store) shardFor(key schema.Key) *shard // maphash.String(s.seed, string(key)) & (NumShards-1)
  func (s *Store) getOrCreate(key schema.Key) *entry
  func (e *entry) lockOp(ctx context.Context) error
  func (e *entry) unlockOp()
  func (s *Store) load(ctx context.Context, e *entry) error
  func (s *Store) ensureLoadedLocked(ctx context.Context, e *entry) (*schema.Row, error)
  ```
  `tryLockOp` is not needed until M2.

- [ ] **Step 1: Write the failing tests**

```go
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/store/`
Expected: FAIL (build errors: `New`, `Store` undefined).

- [ ] **Step 3: Implement `store.go` and `get.go`**

Follow design.md 7.1 (`getOrCreate`, `lockOp`, `unlockOp`) and 7.2 (`Get`, `load`, `ensureLoadedLocked`), minus everything listed under "Deliberately not in M1": no `touch`, no `dead` check in `load`, no `entryCount`. Keep from 7.2: the re-check of `snap` after taking `op` (this is what makes concurrent misses one load), the `LoadErrTTL` cache, and caching an error only when `ctx.Err() == nil`. `db.ErrNoRows` becomes `absent`. Write `snap` only under `mu` (I3).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race ./internal/store/ && go vet ./... && gofmt -l .`
Expected: `ok`, no other output.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "M1: Store with sharded entries, Get and load"
```

---

### Task 4: Load under concurrency, cancellation and errors

**Files:**
- Test: `internal/store/load_test.go`
- Modify: `internal/store/get.go` only if a test fails

**Interfaces:**
- Consumes: everything from Task 3, plus the test helpers `newTestStore`, `key`, `testRow` in `get_test.go`.
- Produces: nothing new.

These tests pin behaviour Task 3 should already have. If one passes on the first run, confirm it can fail: break the rule it guards with a temporary edit, see it fail, revert (this is the M1 part of tla-mapping.md section 6).

- [ ] **Step 1: Write the tests**

```go
package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

// Concurrent misses queue on op; only the first loads (TLA: L_lock, L_load; the re-check of snap).
func TestConcurrentMissesSelectOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		release := make(chan struct{})
		d.SelectHook = func(context.Context, schema.Key) error { <-release; return nil }
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		errs := make(chan error, 32)
		var wg sync.WaitGroup
		for range 32 {
			wg.Go(func() {
				_, err := s.Get(context.Background(), k)
				errs <- err
			})
		}
		synctest.Wait() // one Get is inside Select, the rest wait for op
		close(release)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if n := d.Selects(); n != 1 {
			t.Fatalf("Selects = %d, want 1", n)
		}
	})
}

func TestLoadErrorIsCachedForLoadErrTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		boom := errors.New("boom")
		var fail atomic.Bool
		fail.Store(true)
		d.SelectHook = func(context.Context, schema.Key) error {
			if fail.Load() {
				return boom
			}
			return nil
		}
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		if _, err := s.Get(context.Background(), k); !errors.Is(err, boom) {
			t.Fatalf("first Get: err = %v, want boom", err)
		}
		fail.Store(false)
		if _, err := s.Get(context.Background(), k); !errors.Is(err, boom) {
			t.Fatalf("Get within LoadErrTTL: err = %v, want the cached boom", err)
		}
		if n := d.Selects(); n != 1 {
			t.Fatalf("Selects within LoadErrTTL = %d, want 1", n)
		}
		time.Sleep(s.cfg.LoadErrTTL)
		if _, err := s.Get(context.Background(), k); err != nil {
			t.Fatalf("Get after LoadErrTTL: %v", err)
		}
		if n := d.Selects(); n != 2 {
			t.Fatalf("Selects = %d, want 2", n)
		}
	})
}

// The caller's own cancellation says nothing about the DB, so it is not cached.
func TestCancelledLoadIsNotCached(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	ctx, cancel := context.WithCancel(context.Background())
	var first atomic.Bool
	first.Store(true)
	d.SelectHook = func(ctx context.Context, _ schema.Key) error {
		if first.Swap(false) {
			cancel()
			return ctx.Err()
		}
		return nil
	}
	if _, err := s.Get(ctx, k); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if _, err := s.Get(context.Background(), k); err != nil {
		t.Fatalf("Get after a cancelled load: %v", err)
	}
	if n := d.Selects(); n != 2 {
		t.Fatalf("Selects = %d, want 2", n)
	}
}

func TestGetWithCancelledContext(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Get(ctx, k); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if _, err := s.Get(context.Background(), k); err != nil {
		t.Fatalf("Get after a cancelled Get: %v", err)
	}
}

// A Get that times out while queued for op leaves the queue (TLA: L_lock).
func TestGetTimesOutWhileQueuedForOp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		release := make(chan struct{})
		d.SelectHook = func(context.Context, schema.Key) error { <-release; return nil }
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		firstDone := make(chan error, 1)
		go func() {
			_, err := s.Get(context.Background(), k)
			firstDone <- err
		}()
		synctest.Wait() // the first Get holds op and is inside Select
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := s.Get(ctx, k); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued Get: err = %v, want DeadlineExceeded", err)
		}
		close(release)
		if err := <-firstDone; err != nil {
			t.Fatalf("first Get: %v", err)
		}
	})
}

// Many keys, many goroutines; run under -race.
func TestGetConcurrentKeys(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	keys := make([]schema.Key, 8)
	for id := range int64(8) {
		keys[id] = key(t, s, id)
		if id%2 == 0 {
			d.Put(testRow(keys[id], id))
		}
	}
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 200 {
				id := int64((g + i) % 8)
				r, err := s.Get(context.Background(), keys[id])
				switch {
				case id%2 == 0 && (err != nil || r.Gen != id+100):
					t.Errorf("Get(%d) = %+v, %v", id, r, err)
				case id%2 == 1 && !errors.Is(err, ErrNotFound):
					t.Errorf("Get(%d): err = %v, want ErrNotFound", id, err)
				}
			}
		})
	}
	wg.Wait()
	if n := d.Selects(); n != 8 {
		t.Fatalf("Selects = %d, want 8 (one per key)", n)
	}
}
```

- [ ] **Step 2: Run the tests**

Run: `go test -race -run 'Concurrent|LoadError|Cancelled|TimesOut' -v ./internal/store/`
Expected: all PASS. If any fails, fix `get.go` (not the test) and rerun.

- [ ] **Step 3: Show that the guarding tests can fail**

For each row, make the temporary edit, run `go test ./...`, confirm the named test fails, revert with `git checkout -- <file>`:

| Temporary edit | Must fail |
|---|---|
| `ensureLoadedLocked`: remove the re-check of `snap` (TLA mutation `LoadNoRecheck`) | `TestConcurrentMissesSelectOnce` |
| `ensureLoadedLocked`: cache the error even when `ctx.Err() != nil` | `TestCancelledLoadIsNotCached` |
| `ensureLoadedLocked`: remove the `LoadErrTTL` check | `TestLoadErrorIsCachedForLoadErrTTL` |
| `EncodeKey`: drop the length prefix for composite keys | `TestEncodeKeyComposite` |

Run: `git status --short`
Expected: only `internal/store/load_test.go` (and `get.go` if Step 2 needed a fix) changed.

- [ ] **Step 4: Commit**

```bash
git add internal/store
git commit -m "M1: tests for load under concurrency, cancellation and errors"
```

---

### Task 5: Milestone check

- [ ] **Step 1: Full check**

Run: `gofmt -l . && go vet ./... && go test -race -count=3 ./...`
Expected: no gofmt output, no vet output, every package `ok`.

- [ ] **Step 2: Review the comments**

Check by reading: the invariants maintained in M1 (I1, I2, I3, I8, I11) and each lock's role are commented where the code relies on them; `// TLA: Goc`, `G_read`, `L_lock`, `L_load` mark the steps; functions requiring `op` end in `Locked`.

- [ ] **Step 3: Correctness review, then simplification**

Run `/code-review` on the branch and fix what it finds; then `/simplify`, rejecting any change listed in CLAUDE.md "Correctness rules that look like complexity"; then repeat Step 1.

- [ ] **Step 4: Report**

To the user: the output of Step 1, the table from Task 4 Step 3 with each result, what review and simplification changed, and the acceptance criterion of M1 (design.md 12.2: "`-race` unit tests pass; primary key encoding covers integer, string, UUID, composite primary keys") with the tests that show it.
