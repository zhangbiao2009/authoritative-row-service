# M2: Update (SET and INCR), Single-Key Flush, Flush Queue — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Store.Update` changes a row in memory and returns at once; a background flusher writes each dirty row back to `db.DB` with a conditional UPDATE, so that after the flusher catches up memory and the DB match exactly.

**Architecture:** `Update` does its whole read-modify-write inside the entry's leaf lock `mu`, publishes a new immutable `Row` with `Ver+1`, marks the changed columns dirty and, after releasing `mu`, puts the entry on its shard's flush queue (design.md 7.3). `flushShard` takes the queue, and for each entry takes `op` with `tryLockOp`, snapshots the row and dirty mask, clears dirty, writes the masked columns with `UPDATE ... WHERE key AND gen = ? AND ver < ?`, and on failure merges the mask back and re-enqueues (7.6, one row per DB call; batches are M6).

**Tech Stack:** Go 1.25, standard library only.

**Spec:** [design.md](../../design.md) — sections 4 (I3, I4, I5, I6, I8, I12, I13), 5.2, 6, 7.1, 7.3, 7.6, 8.3 (flush UPDATE), 11.4, 12.2 (M2). Also [CLAUDE.md](../../CLAUDE.md) and [tla-mapping.md](../../tla-mapping.md) (M2 rows of sections 3, 5, 6).

## Global Constraints

- All paths are relative to `memory-authoritative/`. Run every command from there.
- Work on branch `memory-authoritative-m2` (from `main` at `148e0ad`), one commit per task. The user reviews and merges.
- Done means: `gofmt -l .` prints nothing, `go vet ./...` is clean, `go test -race ./...` passes.
- Lock rules (6.4): `mu`, `shard.mu`, `shard.qmu` are leaf locks: while holding one, no other lock, no channel operation, no IO, no logging. Only `op` may be held while taking other locks (edges `op → mu`, `op → qmu`). `enqueue` is called after `mu` is released. Functions that require a lock held end in `Locked`.
- Increment computes the new value from the old one inside `mu`; flush writes absolute values; `col = col + delta` is never issued (I12).
- The flush write is always conditional (`gen =` and `ver <`); it never inserts (I6).
- Comments: invariants by ID where the code maintains them (I3, I4, I5, I12, I13 new in M2) and each new field's guarding lock; `// TLA: <Action>` for `U_mu`, `U_enq`, `F_take`, `F_scan`, `F_send`, `F_done`.
- **Deliberately not in M2** (do not add early): `entry.dead` and its checks, `touch`/`lastAccess`/`nowSec`, `entryCount` (M5); `Change.Gen` (M3); `dirtySince`, `poisoned`, `ErrBusy`/`MaxDirty`/`MaxDirtyAge`, `UpdateBatch`, worker pool, graceful shutdown, owner epoch and the 0-row epoch check, metrics (M6); value validation for SET (M7).

### Decisions in this plan that the design does not fix

1. **fakedb gets `UpdateHook` now.** The hook receives the statement as an `exec func()` and decides what happens: run it now, never run it (definite failure), or return an error and run it later (late execution). tla-mapping assigns the `NoVerCheck` and `NoMaskMerge` mutations to M2, and both need a failing or late flush to be caught. The rest of fault injection (ambiguous Select, sessions, cross-row switch) stays in M3; whether `SelectHook` changes to the same shape is decided there.
2. **`ColBit` lives in `schema`, exported.** The store, fakedb and the PG adapter (M7) all need the same meaning of the dirty mask (design 5.2 shows it in `store`).
3. **`Update` checks the shape of a `Change` itself** and returns `ErrInvalid` for: no columns, a column out of range, a primary key column (I13), a column twice, an unknown op. It computes the mask from `Cols`, so `Change` has no `Mask` field. Design 7.3 leaves these checks to the API layer (M8), but the store is what relies on them: a `Change` with no columns would count a row dirty that never becomes clean, and an out-of-range column panics. The types of SET values are still not checked until M7.
4. **`schema` gains `Float4` and `Float8`**, because INCR is defined on float columns too (7.3). A float4 result is rounded to float32 precision so memory holds what PG would store; a result that is ±Inf or NaN, or a float4 result beyond `math.MaxFloat32`, is `ErrInvalid`. The other types of 5.3 still come in M7. Floats cannot be primary key columns (`New` already refuses any type it cannot encode).
5. **One flusher goroutine**, `RunFlusher(ctx)`, flushes every shard once per `FlushInterval` until `ctx` is cancelled; the caller starts it. Each DB write is bounded by `DBTimeout`. Worker pool, batches and graceful shutdown are M6.
6. **`db.DB.Update` returns only an error.** Zero rows affected is `nil` (7.6). The affected-row count is added when M6 needs it for the epoch check.
7. **No failpoint hooks in `store` yet.** The windows M2 has are reached without one: the flush-snapshot window through `fakedb.UpdateHook` (`TestUpdateDuringFlushIsWrittenNext`), and the update-enqueue window (tla-mapping 3.1) only by the stress test's random timing. The first store failpoint, and with it the one mechanism CLAUDE.md asks for, is designed in M3, where most failpoints are (delete-db-done, flush-late with delete + create).

## Review Focus

1. A `Change` with no columns, a primary key column, a repeated column or a column out of range must return `ErrInvalid` and leave the row, `dirty`, `dirtyCount` and the queue untouched. Test: `TestUpdateRejectsMalformedChange` (Task 3).
2. An Update whose load fails must return the error and leave nothing dirty or queued; a later Update must work. Test: `TestUpdateLoadErrorHasNoEffect` (Task 3).
3. An Update of a key that does not exist in the DB returns `ErrNotFound` and queues nothing. Test: `TestUpdateMissingRow` (Task 3).
4. A float4 increment must leave in memory exactly what PG would store (float32 precision), or memory and DB differ after a reload. Test: `TestAddFloat` (Task 1).
5. A flush whose DB call hangs must not block Updates of the same key (they take only `mu`), must give up after `DBTimeout`, and the row must stay dirty. Test: `TestUpdateNotBlockedByHangingFlush` (Task 4).

---

### Task 1: Column bits, float types and increment arithmetic in `schema`

**Files:**
- Modify: `internal/schema/schema.go`
- Test: `internal/schema/schema_test.go`

**Interfaces:**
- Consumes: M1 `schema` (`Type`, `Schema`, `ErrInvalid`).
- Produces:
  ```go
  const ( Int2 Type = iota + 1; Int4; Int8; Text; UUID; Float4; Float8 ) // Float4/Float8 hold float64
  // ColBit returns the dirty-mask bit of column i: bit i for i < 63; bit 63 stands for every column >= 63.
  func ColBit(i int) uint64
  // Add returns old + delta for column col. Integer columns take an int64 delta, float columns a float64
  // delta. ErrInvalid (wrapped) if old is nil, the column type has no increment, the delta has the wrong Go
  // type, or the result is out of the column type's range. Pure computation: called with mu held (6.4).
  func (s *Schema) Add(col int, old, delta any) (any, error)
  ```

- [ ] **Step 1: Create the branch**

```bash
git checkout main && git checkout -b memory-authoritative-m2
```

- [ ] **Step 2: Write the failing tests**

Test helper next to `oneCol`: `floatCol(t, typ)` returns a schema whose column 0 is `f` of type `typ` and column 1 is `id Int8`, the primary key (`PK: []int{1}`), so the tests below call `Add(0, ...)`.

```go
func TestColBit(t *testing.T) {
	for i, want := range map[int]uint64{0: 1, 1: 2, 62: 1 << 62, 63: 1 << 63, 64: 1 << 63, 200: 1 << 63} {
		if got := ColBit(i); got != want {
			t.Errorf("ColBit(%d) = %#x, want %#x", i, got, want)
		}
	}
}

func TestAddInt(t *testing.T) {
	cases := []struct {
		typ        Type
		old, delta int64
		want       int64
		ok         bool
	}{
		{Int8, 10, 5, 15, true},
		{Int8, 10, -15, -5, true},
		{Int8, math.MaxInt64, 1, 0, false},
		{Int8, math.MinInt64, -1, 0, false},
		{Int4, math.MaxInt32 - 1, 1, math.MaxInt32, true},
		{Int4, math.MaxInt32, 1, 0, false},
		{Int4, math.MinInt32, -1, 0, false},
		{Int2, math.MaxInt16, 1, 0, false},
		{Int2, math.MinInt16, -1, 0, false},
	}
	for _, c := range cases {
		got, err := oneCol(t, c.typ).Add(0, c.old, c.delta)
		if c.ok && (err != nil || got != c.want) {
			t.Errorf("%v: Add(%d, %d) = %v, %v; want %d", c.typ, c.old, c.delta, got, err, c.want)
		}
		if !c.ok && !errors.Is(err, ErrInvalid) {
			t.Errorf("%v: Add(%d, %d) = %v, %v; want ErrInvalid", c.typ, c.old, c.delta, got, err)
		}
	}
}

func TestAddFloat(t *testing.T) {
	if got, err := floatCol(t, Float8).Add(0, 1.5, 0.25); err != nil || got != 1.75 {
		t.Errorf("float8 Add = %v, %v; want 1.75", got, err)
	}
	// float4 rounds to float32 precision, as PG stores it
	got, err := floatCol(t, Float4).Add(0, 0.0, 0.1)
	if want := float64(float32(0.1)); err != nil || got != want {
		t.Errorf("float4 Add = %v, %v; want %v", got, err, want)
	}
	for name, c := range map[string]struct {
		typ        Type
		old, delta float64
	}{
		"float8 overflow": {Float8, math.MaxFloat64, math.MaxFloat64},
		"float8 NaN":      {Float8, 1, math.NaN()},
		"float4 overflow": {Float4, math.MaxFloat32, math.MaxFloat32},
	} {
		if got, err := floatCol(t, c.typ).Add(0, c.old, c.delta); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Add = %v, %v; want ErrInvalid", name, got, err)
		}
	}
}

func TestAddRejects(t *testing.T) {
	// columns: 0 id int8 (pk), 1 n int4, 2 f float8, 3 s text
	s, err := New("t", []Column{{"id", Int8}, {"n", Int4}, {"f", Float8}, {"s", Text}}, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		col        int
		old, delta any
	}{
		"old NULL":             {1, nil, int64(1)},
		"text column":          {3, "a", int64(1)},
		"float delta on int":   {1, int64(1), 1.0},
		"int delta on float":   {2, 1.0, int64(1)},
		"go int delta":         {1, int64(1), 1},
		"old of wrong Go type": {1, "1", int64(1)},
	} {
		if got, err := s.Add(c.col, c.old, c.delta); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Add = %v, %v; want ErrInvalid", name, got, err)
		}
	}
}
```

Also add to `TestNewRejects`: a schema whose only primary key column is `Float8` is refused.

- [ ] **Step 3: Run to verify they fail**

Run: `go test ./internal/schema/`
Expected: FAIL to compile: `undefined: ColBit`, `undefined: Float4`, `s.Add undefined`.

- [ ] **Step 4: Implement `ColBit`, `Float4`/`Float8` and `Add` in `internal/schema/schema.go`**

Integer overflow: check before adding (`delta > 0 && old > max-delta`, `delta < 0 && old < min-delta`) with the type's bounds (`math.MaxInt16`/`MaxInt32`/`MaxInt64`). Floats: compute in float64; reject `math.IsInf` / `math.IsNaN`; for `Float4` reject `math.Abs(v) > math.MaxFloat32`, then round with `float64(float32(v))`. Errors wrap `ErrInvalid` and name table and column, like `encodeCol`. Update the `Type` comment: Float4/Float8 hold `float64`.

- [ ] **Step 5: Run the package tests**

Run: `go test -race ./internal/schema/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/schema && git commit -m "M2: column bits, float types and increment arithmetic"
```

---

### Task 2: `db.DB.Update` and fakedb's conditional write, hook and assertions

**Files:**
- Modify: `internal/db/db.go`, `internal/db/fakedb/fakedb.go`
- Test: `internal/db/fakedb/fakedb_test.go`

**Interfaces:**
- Consumes: `schema.ColBit`, `schema.Row`.
- Produces:
  ```go
  // db.DB gains:
  // Update writes the columns of r selected by mask, and ver = r.Ver, to the row with key r.Key, only if
  // that row exists with gen = r.Gen and ver < r.Ver (I6; design.md 8.3). It never inserts. Zero rows
  // affected is not an error (7.6).
  Update(ctx context.Context, r *schema.Row, mask uint64) error

  // fakedb.DB gains:
  // UpdateHook, if set, decides what happens to each Update. exec applies the statement to the stored
  // rows; call it at most once, at any time, from any goroutine — calling it after returning is a late
  // statement. Update returns what the hook returns. Set it before concurrent use.
  UpdateHook func(ctx context.Context, key schema.Key, exec func()) error
  func (d *DB) Updates() int                   // number of Update calls so far
  func (d *DB) Row(key schema.Key) *schema.Row // copy of the stored row or nil; for test checks, no counters
  func (d *DB) Violation() error               // first broken assertion, or nil
  ```
  Assertions, always on (tla-mapping section 5): **OneWriterPerKey (I5)**: an `Update` call for a key starting while another `Update` call for the same key has not returned (late execs after a call returned do not count). **DbVerMonotonic**: an applied write with the same gen must have a ver greater than the stored one. Each records the first violation.

- [ ] **Step 1: Write the failing tests**

```go
func TestUpdateIsConditional(t *testing.T) {
	ctx := context.Background()
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 5, Ver: 2, Vals: []any{int64(1), "a", int64(10)}})
	write := func(gen, ver int64, mask uint64, vals ...any) *schema.Row {
		t.Helper()
		if err := d.Update(ctx, &schema.Row{Key: "k", Gen: gen, Ver: ver, Vals: vals}, mask); err != nil {
			t.Fatal(err)
		}
		return d.Row("k")
	}
	// only the masked column and ver change
	got := write(5, 3, schema.ColBit(2), int64(1), "zzz", int64(11))
	want := &schema.Row{Key: "k", Gen: 5, Ver: 3, Vals: []any{int64(1), "a", int64(11)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after update: %+v, want %+v", got, want)
	}
	// another gen, an equal ver, an older ver: no effect
	for _, c := range []struct{ gen, ver int64 }{{6, 9}, {5, 3}, {5, 2}} {
		if got := write(c.gen, c.ver, schema.ColBit(2), int64(1), "a", int64(99)); !reflect.DeepEqual(got, want) {
			t.Fatalf("gen %d ver %d: %+v, want unchanged %+v", c.gen, c.ver, got, want)
		}
	}
	// a missing row is not inserted
	if err := d.Update(ctx, &schema.Row{Key: "other", Gen: 1, Ver: 1, Vals: []any{int64(2), "b", int64(0)}}, 1); err != nil || d.Row("other") != nil {
		t.Fatalf("update of a missing row: err %v, row %+v; want nil, nil", err, d.Row("other"))
	}
	if d.Updates() != 5 || d.Violation() != nil {
		t.Fatalf("Updates = %d, Violation = %v; want 5, nil", d.Updates(), d.Violation())
	}
}

func TestUpdateHookLateExec(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1, Ver: 1, Vals: []any{int64(0)}})
	var late func()
	d.UpdateHook = func(_ context.Context, _ schema.Key, exec func()) error {
		late = exec
		return context.DeadlineExceeded
	}
	err := d.Update(context.Background(), &schema.Row{Key: "k", Gen: 1, Ver: 2, Vals: []any{int64(7)}}, 1)
	if !errors.Is(err, context.DeadlineExceeded) || d.Row("k").Ver != 1 {
		t.Fatalf("err %v, ver %d; want DeadlineExceeded and the row unchanged", err, d.Row("k").Ver)
	}
	late()
	if r := d.Row("k"); r.Ver != 2 || r.Vals[0] != int64(7) {
		t.Fatalf("after the late exec: %+v", r)
	}
}

func TestOneWriterPerKeyIsAsserted(t *testing.T) {
	d := New()
	d.Put(&schema.Row{Key: "k", Gen: 1, Ver: 1, Vals: []any{int64(0)}})
	entered, release := make(chan struct{}), make(chan struct{})
	d.UpdateHook = func(_ context.Context, _ schema.Key, exec func()) error {
		entered <- struct{}{}
		<-release
		exec()
		return nil
	}
	var wg sync.WaitGroup
	for v := int64(2); v <= 3; v++ {
		wg.Go(func() { d.Update(context.Background(), &schema.Row{Key: "k", Gen: 1, Ver: v, Vals: []any{v}}, 1) })
	}
	<-entered
	<-entered // both calls are in progress at once
	close(release)
	wg.Wait()
	if d.Violation() == nil {
		t.Fatal("two concurrent writes of one key were not reported")
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/db/...`
Expected: FAIL to compile: `d.Update undefined`, `d.Row undefined`, `d.UpdateHook undefined`.

- [ ] **Step 3: Implement**

`db.go`: add `Update` to the interface with the comment above. `fakedb.go`: `Update` counts the call, registers the key as being written (a `map[schema.Key]bool` under `d.mu`; already present → record a violation), builds `exec` (under `d.mu`: if the stored row exists with `Gen` equal and `Ver < r.Ver`, check DbVerMonotonic, then store a copy of the stored row with the masked columns (`mask&schema.ColBit(i) != 0`) and `Ver` taken from `r`), then either calls the hook or checks `ctx.Err()` and runs `exec`; finally unregisters the key. Call the hook without `d.mu` held, as for `SelectHook`.

- [ ] **Step 4: Run the tests**

Run: `go test -race ./internal/db/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/db && git commit -m "M2: conditional Update in db.DB and fakedb, with UpdateHook and write assertions"
```

---

### Task 3: `Store.Update` in memory: dirty columns and the flush queue

**Files:**
- Modify: `internal/store/store.go` (errors, entry and shard fields, `enqueue`, `markDirtyLocked`, `clearDirtyLocked`, `tryLockOp`)
- Create: `internal/store/update.go`
- Test: `internal/store/update_test.go`; modify `internal/store/get_test.go` (`newTestStore` checks `d.Violation()` in `t.Cleanup`)

**Interfaces:**
- Consumes: `schema.Add`, `schema.ColBit`, `schema.ErrInvalid`.
- Produces:
  ```go
  var ErrInvalid = schema.ErrInvalid // the same error value, so errors.Is works across packages

  type ColOp uint8
  const ( OpSet ColOp = iota; OpIncr )
  type ColChange struct {
      Col int   // column ordinal; not a primary key column (I13); at most once per Change
      Op  ColOp
      Val any   // OpSet: the new value, never modified afterwards; OpIncr: the delta
  }
  type Change struct{ Cols []ColChange }

  // Update applies ch to the row in memory and returns the new row (shared, immutable). ErrNotFound if the
  // row does not exist; ErrInvalid if ch is malformed or an increment fails, in which case nothing changes.
  func (s *Store) Update(ctx context.Context, key schema.Key, ch Change) (*schema.Row, error)

  // entry gains (guarded by mu): dirty uint64 (dirty columns, schema.ColBit); queued bool (in the shard's queue)
  // shard gains: qmu sync.Mutex (leaf lock: guards queue only); queue []*entry (never blocks; queued keeps each entry in it at most once)
  // Store gains: dirtyCount atomic.Int64 (entries with dirty != 0)
  func (e *entry) tryLockOp() bool
  func (s *Store) markDirtyLocked(e *entry, mask uint64) // caller holds mu; mask != 0
  func (s *Store) clearDirtyLocked(e *entry)              // caller holds mu
  func (s *Store) enqueue(e *entry)                       // caller holds no lock (called after mu is released)
  ```

- [ ] **Step 1: Write the failing tests** (in `update_test.go`; the `items` schema is `id int8 pk, name text, score int8`)

Helpers in the test file: `set(col int, v any) ColChange`, `incr(col int, d int64) ColChange`, `queueLen(s) int` (sum of `len(sh.queue)` under `qmu`), `entryOf(s, key) *entry` (map lookup under `shard.mu`).

```go
func TestUpdateSetAndIncr(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	d.Put(testRow(k, 1)) // Gen 101, Ver 3, Vals {1, "a", 10}
	old, _ := s.Get(context.Background(), k)
	r, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b"), incr(2, 5)}})
	want := &schema.Row{Key: k, Gen: 101, Ver: 4, Vals: []any{int64(1), "b", int64(15)}}
	if err != nil || !reflect.DeepEqual(r, want) {
		t.Fatalf("Update = %+v, %v; want %+v", r, err, want)
	}
	if !reflect.DeepEqual(old, testRow(k, 1)) {
		t.Fatalf("the earlier snapshot changed (I2): %+v", old)
	}
	if g, _ := s.Get(context.Background(), k); g != r {
		t.Fatalf("Get after Update = %+v, want the same row", g)
	}
	e := entryOf(s, k)
	if e.dirty != schema.ColBit(1)|schema.ColBit(2) || !e.queued || queueLen(s) != 1 || s.dirtyCount.Load() != 1 {
		t.Fatalf("dirty %#x queued %v queue %d dirtyCount %d", e.dirty, e.queued, queueLen(s), s.dirtyCount.Load())
	}
	// a second update: dirty grows, the queue does not
	if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{incr(2, 1)}}); err != nil {
		t.Fatal(err)
	}
	if queueLen(s) != 1 || s.dirtyCount.Load() != 1 {
		t.Fatalf("after a second update: queue %d dirtyCount %d; want 1, 1", queueLen(s), s.dirtyCount.Load())
	}
	if d.Updates() != 0 {
		t.Fatal("Update wrote to the DB; it must only change memory")
	}
}

func TestUpdateLoadsFirst(t *testing.T)            // unloaded key: Update succeeds, Selects == 1, Ver == 4
func TestUpdateMissingRow(t *testing.T)            // no DB row: ErrNotFound; queueLen 0; dirtyCount 0
func TestUpdateLoadErrorHasNoEffect(t *testing.T)  // synctest. SelectHook returns boom: errors.Is(err, boom); snap nil, queue 0, dirtyCount 0;
                                                    // hook nil, time.Sleep(LoadErrTTL): the next Update succeeds

// Acceptance: an increment that fails leaves the whole Change without effect.
func TestFailedIncrementHasNoEffect(t *testing.T) {
	// two rows: score NULL, and score math.MaxInt64
	// for each: Update(Change{set(1, "changed"), incr(2, 1)}) -> errors.Is(err, ErrInvalid);
	// Get returns the identical snapshot pointer as before; dirty == 0; queueLen 0; dirtyCount 0
}

func TestUpdateRejectsMalformedChange(t *testing.T) {
	// cases: no columns; Col 0 (primary key); Col 3 (out of range); Col -1; the same column twice; Op 9
	// each: errors.Is(err, ErrInvalid); the row, dirty, dirtyCount and the queue unchanged
}

// N goroutines each increment the same column K times: the result is exactly N*K (in memory; Task 5 checks the DB).
func TestConcurrentIncrements(t *testing.T) {
	const N, K = 16, 200
	// start score 0; wg.Go N times: K × Update(incr(2, 1)), t.Error on error
	// Get: score == int64(N*K); Ver == 3 + N*K
}
```

`newTestStore` (in `get_test.go`) adds `t.Cleanup(func() { if err := d.Violation(); err != nil { t.Error(err) } })`.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/store/`
Expected: FAIL to compile: `s.Update undefined`, `undefined: ColChange`.

- [ ] **Step 3: Implement `Update` in `internal/store/update.go` and the helpers in `store.go`**

Follow design.md 7.3 without the `MaxDirty`, `dead`, `touch` and `ch.Gen` parts. Check the Change's shape and compute its mask before the loop (no lock needed). In the loop: `getOrCreate`; `// TLA: U_mu` lock `mu`; `nil` → unlock, `load`, `continue`; `absent` → unlock, `ErrNotFound`; else copy the row and clone `Vals` (5.2 warning), apply each column (`schema.Add` for `OpIncr`, unlocking and returning on error before `snap.Store`), `Ver+1`, `snap.Store` (linearization point), `markDirtyLocked`, `need := !e.queued; e.queued = true`, unlock; `// TLA: U_enq` `if need { s.enqueue(e) }`. `markDirtyLocked` / `clearDirtyLocked` / `enqueue` / `tryLockOp` as in design.md 7.1, minus `dirtySince`. Extend the package comment with `qmu` and the I3 rule for Update (writes `snap` with `mu` only; Row → Row only).

- [ ] **Step 4: Run the tests**

Run: `go test -race ./internal/store/`
Expected: PASS (M1 tests included).

- [ ] **Step 5: Commit**

```bash
git add internal/store && git commit -m "M2: Update in memory with dirty columns and the flush queue"
```

---

### Task 4: Single-key flush and the flusher

**Files:**
- Create: `internal/store/flush.go`
- Modify: `internal/store/store.go` (`Config.FlushInterval`, `Config.DBTimeout`, `DefaultConfig`)
- Test: `internal/store/flush_test.go`, `internal/store/invariants_test.go`
- Modify: `tla-mapping.md` row `F_send`: "`db.Update` per item (M2); `db.UpdateBatch` from M6"

**Interfaces:**
- Consumes: Task 2 `db.DB.Update`, `fakedb.UpdateHook`/`Row`/`Updates`; Task 3 `entry.dirty`/`queued`, `shard.queue`, `markDirtyLocked`, `clearDirtyLocked`, `enqueue`, `tryLockOp`.
- Produces:
  ```go
  // Config gains (defaults from design.md 11.4):
  FlushInterval time.Duration // 200ms
  DBTimeout     time.Duration // 3s: bound on each DB write of the flusher

  // RunFlusher flushes every shard once per FlushInterval until ctx is cancelled, then returns.
  func (s *Store) RunFlusher(ctx context.Context)
  // flushShard writes the shard's queued entries, one DB call per entry.
  func (s *Store) flushShard(ctx context.Context, sh *shard)
  func (s *Store) requeueAll(sh *shard, es []*entry)

  // invariants_test.go (test-only):
  // checkInvariants, at a quiescent point, for every entry: tryLockOp succeeds (L4 NoLockLeak); dirty != 0 → queued
  // (DirtyImpliesQueued); queued → in its shard's queue (QueuedIsTracked); a loaded row with dirty == 0 equals
  // d.Row(key) including Gen and Ver (QuiescentConsistent, I4); same gen → DB ver <= memory ver (DbNotAhead).
  func checkInvariants(t *testing.T, s *Store, d *fakedb.DB)
  // drain flushes all shards until no entry is dirty or queued (at most 100 rounds, else t.Fatal), then checks
  // dirtyCount == 0 (L3 EventuallyFlushed).
  func drain(t *testing.T, s *Store)
  ```

`flushShard`, per design.md 7.6 with one `db.Update` per entry instead of a batch: `// TLA: F_take` take the queue under `qmu`; for each entry `// TLA: F_scan` `tryLockOp` (fails → `retry`, `queued` stays true); under `mu` snapshot `snap` and `dirty`; nil, absent or clean → `queued = false`, unlock both, next; else `clearDirtyLocked`, `queued = false`, unlock `mu`, keep `op`; `// TLA: F_send` `db.Update` with a `DBTimeout` context; `// TLA: F_done` on error, under `mu`: `markDirtyLocked(e, mask)` (merge back: newer values are written next time), `need := !e.queued; e.queued = true`, unlock, `enqueue` if needed; `unlockOp` (I5: the next write of this key cannot start before this one returned). Finally `requeueAll(sh, retry)`.

- [ ] **Step 1: Write the failing tests** (`flush_test.go`; `flushAll(s)` = `flushShard(context.Background(), sh)` for every shard)

```go
func TestFlushWritesDirtyColumns(t *testing.T) {
	// load row 1, Update(set(1,"b"), incr(2,5)); flushAll
	// d.Row(k) equals the memory row (Gen 101, Ver 4, {1,"b",15}); d.Updates() == 1
	// e.dirty == 0, !e.queued, queueLen 0, dirtyCount 0; checkInvariants
}

func TestFlushCoalesces(t *testing.T) {
	// 10 Updates of the same key, then flushAll: d.Updates() == 1, DB score = start + 10
}

// Mutation NoMaskMerge: a failed flush must keep its columns dirty.
func TestFlushFailureKeepsColumnsDirty(t *testing.T) {
	// Update incr(2, 5); UpdateHook returns boom without exec; flushAll
	// e.dirty == ColBit(2), e.queued, queueLen 1, dirtyCount 1, DB unchanged
	// UpdateHook = nil; Update set(1, "b"); flushAll
	// DB equals memory: name "b" AND score 15; checkInvariants
}

func TestFlushRequeuesWhenOpBusy(t *testing.T) {
	// Update; e.lockOp(ctx) by the test; flushAll -> d.Updates() == 0, still dirty, queued, queueLen 1
	// e.unlockOp(); flushAll -> written; checkInvariants
}

// Mutation NoVerCheck: a flush that timed out and executes late must not overwrite a newer one.
func TestLateFlushDoesNotOverwriteNewer(t *testing.T) {
	// Update incr(2, 1) (Ver 4); hook saves exec, returns context.DeadlineExceeded; flushAll
	// hook nil; Update incr(2, 1) (Ver 5); flushAll -> DB Ver 5, score 12
	// call the saved exec (the late Ver 4 statement)
	// DB still Ver 5, score 12; checkInvariants; d.Violation() == nil (checked by newTestStore's cleanup)
}

// Review Focus 5. synctest: virtual time for DBTimeout.
func TestUpdateNotBlockedByHangingFlush(t *testing.T) {
	// hook: <-ctx.Done(); return ctx.Err()   (a DB that never answers)
	// Update; go flushAll; synctest.Wait()   (the flush holds op, waiting in the hook)
	// start := time.Now(); Update incr(2, 1) -> nil, and time.Since(start) == 0 (it did not wait for the flush)
	// the flush goroutine returns after DBTimeout; the row is dirty and queued; hook nil; flushAll; DB equals memory
}

// W_UpdateDuringFlush: an Update between the flush's snapshot and its write is written by the next flush.
func TestUpdateDuringFlushIsWrittenNext(t *testing.T) {
	// synctest. hook blocks on a channel, then exec(); Update set(1,"x"); go flushAll; synctest.Wait()
	// Update set(1,"y") -> requeued (queueLen 1); release; wait; flushAll -> DB name "y"; checkInvariants
}

func TestRunFlusher(t *testing.T) {
	// synctest: go s.RunFlusher(ctx); Update; time.Sleep(FlushInterval); synctest.Wait()
	// DB equals memory; cancel(); RunFlusher returns (wait on a done channel)
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/store/`
Expected: FAIL to compile: `s.flushShard undefined`, `s.RunFlusher undefined`.

- [ ] **Step 3: Implement `flush.go`, the two Config fields, `checkInvariants` and `drain`**

- [ ] **Step 4: Run the tests**

Run: `go test -race ./internal/store/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store tla-mapping.md && git commit -m "M2: single-key flush and the flusher"
```

---

### Task 5: Stress test and the acceptance criteria against the DB

**Files:**
- Test: `internal/store/stress_test.go`

**Interfaces:**
- Consumes: everything above; `checkInvariants`, `drain`.
- Produces: tests only.

- [ ] **Step 1: Write the tests**

```go
// Acceptance: N goroutines each increment the same column K times while the flusher runs; the final value is
// exactly N*K in memory and in the DB.
func TestConcurrentIncrementsMatchDB(t *testing.T) {
	const N, K = 16, 200
	// FlushInterval 1ms; go RunFlusher(ctx); N×K Update(incr(2,1)); cancel and wait for RunFlusher; drain
	// memory score == N*K == DB score; checkInvariants
}

// Acceptance: stress test; afterwards memory and DB match.
func TestStressUpdates(t *testing.T) {
	// 4 keys, 64 goroutines × 500 ops: 40% Get, 30% Update(set(1, random string)), 30% Update(incr(2, random in [-5,5]))
	// each op with a 5s context; any error -> t.Error (L1 EventuallyResponds)
	// FlushInterval 1ms; go RunFlusher(ctx)
	// UpdateHook (math/rand/v2): 70% exec() and nil; 20% boom without exec (definite failure);
	//   10% late: return context.DeadlineExceeded, run exec() from a goroutine after a random 0-2ms (tracked by a WaitGroup)
	// count failed and late flushes (atomic)
	// afterwards: cancel and wait for RunFlusher; wait for the late execs; hook nil; drain; checkInvariants
	// for every key: memory row == d.Row(key) (Gen, Ver, Vals); score == 10 + sum of the successful deltas (tracked per key)
	// non-vacuity: failed > 0 and late > 0
}
```

- [ ] **Step 2: Run them, three times, under the race detector**

Run: `go test -race -count=3 -run 'Stress|MatchDB' ./internal/store/`
Expected: PASS. If they fail, the code is wrong (systematic-debugging), not the test.

- [ ] **Step 3: Run the whole suite**

Run: `gofmt -l . && go vet ./... && go test -race ./...`
Expected: nothing from gofmt, vet clean, all packages PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/store/stress_test.go && git commit -m "M2: stress test and acceptance against the DB"
```

---

## After the tasks (CLAUDE.md workflow, steps 3-6)

`/code-review`, fix; `/simplify`, rerun the suite. Then the mutation checks — break, show a named test fails, revert:

| Mutation | Temporary patch | Must fail |
|---|---|---|
| `NoVerCheck` (tla-mapping 6) | drop `ver <` from fakedb's apply condition | `TestLateFlushDoesNotOverwriteNewer`, `TestStressUpdates` |
| `NoMaskMerge` (tla-mapping 6) | skip `markDirtyLocked` on flush error | `TestFlushFailureKeepsColumnsDirty`, `TestStressUpdates` |
| Increment outside `mu` | read `old` before `mu.Lock` and compute from it | `TestConcurrentIncrements` |
| No re-enqueue on flush error | merge the mask but leave `queued` false | `TestFlushFailureKeepsColumnsDirty` (checkInvariants: DirtyImpliesQueued) |
| `snap.Store` before the increment checks | store the new row first, then return `ErrInvalid` | `TestFailedIncrementHasNoEffect` |

Report each acceptance criterion of 12.2 M2 with the test that shows it.
