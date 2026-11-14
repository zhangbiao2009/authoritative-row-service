# M1: Module, Schema, fakedb, Get + Load — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The Go module `rowcache` with the schema and primary key encoding, a `db.DB` interface with an in-memory `fakedb`, and a sharded `Store` whose `Get` loads each key once and then serves it lock-free from memory.

**Architecture:** Most of M1 is copied from `../memory-authoritative` (its state after M4) and trimmed to this design (design.md 3, decision 4 of 13.1). One `Store` per table holds `NumShards` shards; each shard maps an encoded `Key` to an `entry` whose `snap` is an immutable `*Row` (nil = not loaded, `absent` = does not exist). `Get` reads `snap` without locks; on a miss it queues on the entry's `op`, re-checks `snap`, and loads from `db.DB` (design.md 7.1, 7.2). There is no per-entry `mu`: `snap` is written only with `op` held (I3).

**Tech Stack:** Go 1.25, standard library only (`testing/synctest` for time and blocking).

**Spec:** [design.md](../../design.md) sections 3, 4, 5.2, 5.3 (primary keys), 6, 7.1, 7.2, 12.2 (M1). Also [CLAUDE.md](../../CLAUDE.md) and [tla-mapping.md](../../tla-mapping.md).

## Global Constraints

- All paths are relative to `db-authoritative/`; run every command from there. "Source" means the same path under `../memory-authoritative/`.
- Work on branch `db-authoritative-m1`, created from `main` once this plan is merged. One commit per task. The user reviews the branch and merges it.
- `go.mod`: `module rowcache`, `go 1.25`. No third-party dependencies in M1. Never import from `../memory-authoritative`, never change it.
- Done means: `gofmt -l .` prints nothing, `go vet ./...` is clean, `go test -race ./...` passes.
- Lock rules (design.md 6): `op` is the root lock and may take `shard.mu`; `shard.mu` is a leaf (no other lock, no channel operation, no IO, no logging while held). Functions that require `op` held end in `Locked`.
- Comments: invariants of this design by ID where the code maintains them (M1: I1, I2, I3, I8, I10) and each lock's role; `// TLA: <Action>` at `Goc`, `G_read`, `L_lock`, `L_load` (tla-mapping.md 3). Copied comments cite memory-authoritative's numbering and must be renumbered: its I11 (schema immutable, `Vals` by ordinal) is I10 here; its I3 (snap written under `mu`) becomes "written only with `op` held" (I3); `Add` is design.md 7.4 here (7.3 there); the key encoding is 5.3 in both, the dedup table 5.1 in both.
- **Deliberately not in M1** (added by the milestone that needs them): `entry.dead` and its checks, `touch` / `lastAccess` / `nowSec`, `entryCount` (M5); `tryLockOp`, `runSync`, `inflight`, `bgCtx`, writes, `ErrAlreadyExists` / `ErrOutcomeUnknown` / `ErrInternal` (M2, M3); `AmbiguousError`, sessions, faults (M3); the PostgreSQL implementation (M7).

### Decisions in this plan that the design does not fix

1. **`Key`, `Row` and `ErrInvalid` live in package `schema`**, not `store` as in the design.md 5.2 skeleton: `db.DB` returns `*Row`, and `store` imports `db`, so `Row` in `store` would be an import cycle. `store.ErrInvalid = schema.ErrInvalid`, so `errors.Is` works either way. This is what memory-authoritative's M1 did. Updating design.md 5.2 to match is the user's call.
2. **`db.DB` has only `Select` in M1**, and package `db` only `ErrNoRows` and `ErrNotExecuted` (the load error, 7.2). The rest of 5.2 is added by the milestone that first uses it (CLAUDE.md: no code for later milestones).
3. **One fakedb hook from the start**: `Hook func(ctx, *Stmt) error`, the final form of memory-authoritative's fakedb, so that M2-M6 extend `Stmt` instead of adding hooks. In M1, `Stmt` has only `Kind` and `Key`, and `Kind` only `Select`.
4. **`schema` is copied whole except `ColBit`** (there is no dirty mask here): the types `Int2` … `Float8` and `Add` with its tests (design.md 12.2 M1 lists `Add`). M7 adds the rest of the 5.3 type table.

## Review Focus

1. Composite keys whose bytes shift between columns, `("a","bc")` vs `("ab","c")`, must encode differently. Test: `TestEncodeKeyComposite` (Task 1, copied).
2. A Go `int`, a string, or `nil` passed for an integer key column is rejected with an error wrapping `schema.ErrInvalid`, never encoded or a panic. Test: `TestEncodeKeyRejects` (Task 1, copied, plus the `errors.Is` check).
3. A Get whose own ctx ends during the load returns the bare ctx error (it maps to `DEADLINE_EXCEEDED`, 7.9), not `ErrNotExecuted`, and is not cached. Test: `TestCancelledLoadIsNotCached` (Task 4).
4. A DB failure of a load returns an error that `errors.Is(err, db.ErrNotExecuted)`, so the server answers `UNAVAILABLE` (7.2); queued Gets get that cached error for `LoadErrTTL`. Tests: `TestLoadErrorIsNotExecuted`, `TestLoadErrorIsCachedForLoadErrTTL` (Task 4).
5. A caller that modifies the `Vals` of a row returned by `fakedb` must not change the stored row. Test: `TestSelect` (Task 2).

---

### Task 1: Module and schema

**Files:**
- Create: `go.mod`
- Create: `internal/schema/schema.go` (copy of the source, edited)
- Test: `internal/schema/schema_test.go` (copy of the source, edited)

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```go
  package schema
  var ErrInvalid = errors.New("invalid argument")
  type Key string
  type Row struct { Key Key; Gen int64; Ver int64; Vals []any } // immutable once published (I2)
  type Type uint8
  const ( Int2 Type = iota + 1; Int4; Int8; Text; UUID; Float4; Float8 )
  type Column struct { Name string; Type Type }
  type Schema struct { Table string; Cols []Column; PK []int } // immutable after New (I10)
  func New(table string, cols []Column, pk []int) (*Schema, error)
  func (s *Schema) EncodeKey(pk []any) (Key, error)       // pk values in PK order
  func (s *Schema) Add(col int, old, delta any) (any, error) // used from M2 (7.4)
  ```

- [ ] **Step 1: Create the branch and the module**

```bash
git checkout -b db-authoritative-m1 main
go mod init rowcache && go mod edit -go=1.25
```

- [ ] **Step 2: Copy the tests, then the code**

`cp ../memory-authoritative/internal/schema/schema_test.go internal/schema/`. Edit the test file: delete `TestColBit`; in `TestEncodeKeyRejects`, assert `errors.Is(err, ErrInvalid)` for every case, not only `err != nil`.

Run: `go test ./internal/schema/`
Expected: FAIL (build error: package has no non-test files).

- [ ] **Step 3: Copy and edit `schema.go`**

`cp ../memory-authoritative/internal/schema/schema.go internal/schema/`. Delete `ColBit`. Renumber the comments as in Global Constraints (I11 → I10; `Add` is 7.4; "called with mu held (6.4)" becomes "pure computation, called with `op` held"). No other change.

- [ ] **Step 4: Run the tests**

Run: `go test -race ./internal/schema/ && go vet ./... && gofmt -l .`
Expected: `ok`, no other output.

- [ ] **Step 5: Commit**

```bash
git add go.mod internal/schema
git commit -m "M1: module rowcache and schema, copied from memory-authoritative"
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
  var (
      ErrNoRows      = errors.New("no rows")      // Select found nothing
      ErrNotExecuted = errors.New("not executed") // definitely not executed, and never will be
  )
  // DB is one table's storage. Implementations: fakedb (tests) and PostgreSQL (M7). The only interface
  // in the Store (design.md 5.2).
  type DB interface {
      // Select returns the row, or ErrNoRows. A late read changes nothing: no gen condition, no fence (7.2).
      Select(ctx context.Context, key schema.Key) (*schema.Row, error)
  }

  package fakedb
  type Kind uint8
  const Select Kind = iota
  type Stmt struct { Kind Kind; Key schema.Key } // one statement, as the hook sees it
  type DB struct {
      // Hook, if set, runs for every statement before it executes, without the DB's lock held. An error is
      // returned instead and the statement does not execute. Set it before concurrent use.
      Hook func(ctx context.Context, st *Stmt) error
      // unexported: mutex, rows map[schema.Key]*schema.Row, select counter
  }
  func New() *DB
  func (d *DB) Put(r *schema.Row)             // test setup: stores a copy (Vals cloned)
  func (d *DB) Row(key schema.Key) *schema.Row // a copy, or nil; counts nothing, runs no hook
  func (d *DB) Selects() int                  // Select calls so far, including ones the hook failed
  func (d *DB) Select(ctx context.Context, key schema.Key) (*schema.Row, error) // returns a copy
  ```

- [ ] **Step 1: Write the failing tests** (`internal/db/fakedb/fakedb_test.go`)

```go
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
	if r2 := d.Row("k"); r2.Vals[1] != "a" {
		t.Fatalf("modifying a returned row changed the stored row: %v", r2.Vals[1])
	}
	if n := d.Selects(); n != 2 {
		t.Fatalf("Selects = %d, want 2", n)
	}
}

func TestSelectHook(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1})
	boom := errors.New("boom")
	var seen Stmt
	d.Hook = func(_ context.Context, st *Stmt) error { seen = *st; return boom }
	if _, err := d.Select(context.Background(), "k"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if seen != (Stmt{Kind: Select, Key: "k"}) {
		t.Fatalf("hook saw %+v", seen)
	}
	if n := d.Selects(); n != 1 {
		t.Fatalf("Selects = %d, want 1", n)
	}
}

func TestSelectWithDoneContext(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := d.Select(ctx, "k")
	if !errors.Is(err, db.ErrNotExecuted) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want ErrNotExecuted wrapping context.Canceled", err)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/db/...`
Expected: FAIL (build errors: package `db` / `New` undefined).

- [ ] **Step 3: Implement `db.go` and `fakedb.go`**

As in the Interfaces block. `Select`: count the call, run the hook (no lock held), then if `ctx` is done return `fmt.Errorf("%w: %w", db.ErrNotExecuted, ctx.Err())`, else read a clone under the mutex. Take `clone` from the source fakedb. Package comment of `fakedb`: an in-memory `db.DB` for tests (design.md 12.1); later milestones add writes, sessions and faults.

- [ ] **Step 4: Run the tests**

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
- Create: `internal/store/store.go`: package comment (locks), `ErrNotFound`, `ErrInvalid`, `Config`, `DefaultConfig`, `New`, `Store`, `shard`, `entry`, `absent`, `shardFor`, `getOrCreate`, `lockOp`, `unlockOp`
- Create: `internal/store/get.go`: `Get`, `load`, `ensureLoadedLocked`
- Test: `internal/store/get_test.go`

**Interfaces:**
- Consumes: Task 1 (`schema.Schema`, `Key`, `Row`, `ErrInvalid`); Task 2 (`db.DB`, `db.ErrNoRows`, `db.ErrNotExecuted`, `fakedb`).
- Produces:
  ```go
  package store
  var (
      ErrNotFound = errors.New("not found")
      ErrInvalid  = schema.ErrInvalid
  )
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
      snap          atomic.Pointer[schema.Row] // read lock-free; written only with op held (I3)
      op            chan struct{}              // cap 1
      lastLoadErr   error                      // op held
      lastLoadErrAt time.Time                  // op held
  }
  var absent = &schema.Row{}
  func (s *Store) shardFor(key schema.Key) *shard
  func (s *Store) getOrCreate(key schema.Key) *entry
  func (e *entry) lockOp(ctx context.Context) error
  func (e *entry) unlockOp()
  func (s *Store) load(ctx context.Context, e *entry) error
  func (s *Store) ensureLoadedLocked(ctx context.Context, e *entry) (*schema.Row, error)
  ```

- [ ] **Step 1: Copy the tests and add the M1 acceptance test**

`cp ../memory-authoritative/internal/store/get_test.go internal/store/`. Change the imports to `rowcache/...`. In `newTestStore`, remove the `t.Cleanup` that checks `d.Violation()` (fakedb's assertions come in M2). Rename `TestNewRejectsBadConfig` to `TestNewRejectsBadNumShards` and keep only its `NumShards` cases (`0, -4, 3, 6`). Add:

```go
// design.md 12.2 M1: a Get of a missing key leaves an absent entry (negative cache, 7.2).
func TestGetMissingLeavesAbsentEntry(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	if _, err := s.Get(context.Background(), k); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	sh := s.shardFor(k)
	sh.mu.RLock()
	e := sh.m[k]
	sh.mu.RUnlock()
	if e == nil || e.snap.Load() != absent {
		t.Fatalf("entry = %v, want an entry whose snap is absent", e)
	}
	d.Put(testRow(k, 1)) // only this service writes the table (section 2): the cache is not re-read
	if _, err := s.Get(context.Background(), k); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Get: err = %v, want ErrNotFound", err)
	}
	if n := d.Selects(); n != 1 {
		t.Fatalf("Selects = %d, want 1", n)
	}
}
```

Run: `go test ./internal/store/`
Expected: FAIL (build errors: `New`, `Store` undefined).

- [ ] **Step 2: Copy and trim `store.go` and `get.go`**

Copy both from the source, change the imports, then trim to the Interfaces block. Remove: `entry.mu`, `dirty`, `queued`, `shard.qmu` and `queue`, `markDirtyLocked`, `clearDirtyLocked`, `enqueue`, `dbCtx`, `backoff`, `tryLockOp`, `dirtyCount`, `inflight`, the errors other than `ErrNotFound` and `ErrInvalid`, and the `Config` fields other than `NumShards` and `LoadErrTTL` (with their checks in `New`). Rewrite the package comment for this design: PostgreSQL is authoritative and the Store is a cache of its rows (design.md 1, 3); the locks are `entry.op` (root, held across DB IO, may take `shard.mu`; I8) and `shard.mu` (leaf, the map only); there is no per-entry `mu`.

Then make `ensureLoadedLocked` follow design.md 7.2 exactly:
- `snap.Store(r)` with no lock other than `op` (I3).
- On a `Select` error: if `ctx.Err() != nil`, return `ctx.Err()` itself, not cached; otherwise `err = fmt.Errorf("%w: load: %v", db.ErrNotExecuted, err)`, cache it in `lastLoadErr` / `lastLoadErrAt`, return it.

Comments: I1 at `getOrCreate`, I2 at `Row` use in `Get`, I3 at `snap`, I8 in the package comment; `// TLA: Goc`, `G_read`, `L_lock`, `L_load` as in the source.

- [ ] **Step 3: Run the tests**

Run: `go test -race ./internal/store/ && go vet ./... && gofmt -l .`
Expected: `ok`, no other output.

- [ ] **Step 4: Commit**

```bash
git add internal/store
git commit -m "M1: Store with sharded entries, Get and load"
```

---

### Task 4: Load under concurrency, cancellation and errors

**Files:**
- Test: `internal/store/load_test.go` (copy of the source, edited)
- Modify: `internal/store/get.go` only if a test fails

**Interfaces:**
- Consumes: Task 3, and the helpers `newTestStore`, `key`, `testRow` in `get_test.go`.
- Produces: nothing new.

- [ ] **Step 1: Copy and adapt the tests**

`cp ../memory-authoritative/internal/store/load_test.go internal/store/`, change the imports. The source uses `script`, `step` and `blockOn` from its `faults_test.go`, which is not copied: replace each use with a direct `d.Hook` that acts on `st.Kind == fakedb.Select` (block on a channel, or return an error). Where a test asserts `errors.Is(err, boom)` for a load failure, assert `errors.Is(err, db.ErrNotExecuted)` instead (design.md 7.2). In `TestCancelledLoadIsNotCached`, also assert `!errors.Is(err, db.ErrNotExecuted)`. Add:

```go
// design.md 7.2: any failure of a read means "this request did nothing" (UNAVAILABLE).
func TestLoadErrorIsNotExecuted(t *testing.T) {
	d := fakedb.New()
	boom := errors.New("boom")
	d.Hook = func(context.Context, *fakedb.Stmt) error { return boom }
	s := newTestStore(t, d)
	k := key(t, s, 1)
	_, err := s.Get(context.Background(), k)
	if !errors.Is(err, db.ErrNotExecuted) || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want ErrNotExecuted mentioning boom", err)
	}
}
```

- [ ] **Step 2: Run the tests**

Run: `go test -race -v ./internal/store/`
Expected: all PASS. If one fails, fix `get.go` (not the test) and rerun.

- [ ] **Step 3: Show that the guarding tests can fail**

For each row: make the temporary edit, run `go test ./...`, confirm the named test fails, revert with `git checkout -- <file>`.

| Temporary edit | Must fail |
|---|---|
| `ensureLoadedLocked`: remove the re-check of `snap` (TLA `LoadNoRecheck`: not needed for correctness, tla-mapping.md 6.2, but it is what makes concurrent misses one SELECT) | `TestConcurrentMissesSelectOnce` |
| `ensureLoadedLocked`: cache the error even when `ctx.Err() != nil` | `TestCancelledLoadIsNotCached` |
| `ensureLoadedLocked`: return the `Select` error unwrapped | `TestLoadErrorIsNotExecuted` |
| `ensureLoadedLocked`: remove the `LoadErrTTL` check | `TestLoadErrorIsCachedForLoadErrTTL` |
| `EncodeKey`: drop the length prefix of composite keys | `TestEncodeKeyComposite` |

Run: `git status --short`
Expected: only `internal/store/load_test.go` (and `get.go` if Step 2 needed a fix).

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

By reading: I1, I2, I3, I8, I10 and each lock's role are commented where the code relies on them, with this design's numbers (no memory-authoritative numbering left: `grep -n 'I1[1-6]\|mu held' internal/` finds nothing that refers to the other design); `// TLA: Goc`, `G_read`, `L_lock`, `L_load` mark the steps; functions requiring `op` end in `Locked`; nothing from "Deliberately not in M1" is present.

- [ ] **Step 3: Correctness review, then simplification**

Run `/code-review` on the branch and fix what it finds; then `/simplify`, rejecting any change listed in CLAUDE.md "Correctness rules that look like complexity"; then repeat Step 1.

- [ ] **Step 4: Report**

To the user: the output of Step 1; the table of Task 4 Step 3 with each result; what review and simplification changed; and the acceptance criteria of M1 (design.md 12.2) with the tests that show them:

| Criterion | Tests |
|---|---|
| `-race` unit tests pass | Step 1 |
| key encoding covers integer, string, UUID and composite keys | `TestEncodeKeySingleColumn`, `TestEncodeKeyComposite` |
| Get of a missing key leaves an absent entry | `TestGetMissingLeavesAbsentEntry` |
| concurrent misses of one key send one SELECT | `TestConcurrentMissesSelectOnce`, `TestGetConcurrentKeys` |
