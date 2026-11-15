# M3: Create, Delete, Faults and the Fence — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create and Delete on top of M2's write path, a fakedb that can lose answers, execute statements late or never, reject, and fence sessions, and the one rule for an ambiguous outcome (fence the session, read the row, decide) for all three writes, so that every answer is exact under faults.

**Architecture:** design.md 7.3, 7.5, 7.6. Every write holds `op` from before its statement is sent until its outcome is settled (I5). On `*db.AmbiguousError`, `fenceAndReadLocked` terminates the statement's session until `FenceSession` confirms it, then reads the row until the read succeeds, stores it, and the caller decides from it (create: row with my gen, or my gen in the dedup table; update: same gen and `ver` = loaded `ver` + 1; delete: the loaded life is gone). Create loads first and writes the business row and its dedup record in one statement; Delete keeps the dedup record (I12).

**Tech Stack:** Go 1.25, standard library only (`testing/synctest`, `math/rand/v2`).

**Spec:** [design.md](../../design.md) sections 4 (I5, I12), 5.2, 7.3, 7.4 (the ambiguous branch), 7.5, 7.6, 8.3, 8.4, 10, 12.1 items 2-3, 12.2 (M3), 12.3, 13.2. Also [CLAUDE.md](../../CLAUDE.md) and [tla-mapping.md](../../tla-mapping.md) (the M3 rows of sections 3.1, 5, 6, 7, 8).

## Global Constraints

- All paths are relative to `db-authoritative/`; run every command from there.
- Work on branch `db-authoritative-m3` from `main` at `45fe606`; this plan is its first commit, then one commit per task. The user reviews and merges.
- Done means: `gofmt -l .` prints nothing, `go vet ./...` is clean, `go test -race ./...` passes.
- I5: `op` is never released while a statement of the key may still execute. After an ambiguous result, `op` stays held through the fence loop and the read loop; nothing in the server retries the write itself (7.3).
- After the fence, the decision is made only from the row read (and, for a create, the dedup table): never answer "failed" without the read, never answer "unknown" and release `op` while the statement may execute (0.4).
- Create loads first; INSERT is sent only when the snapshot is absent. Upsert is forbidden. Delete never deletes the dedup record (I12).
- Zero rows from Delete, like from Update, and `ErrDuplicate` from InsertDedup go to `unexpectedLocked` (7.3, 7.6).
- Comments: invariant IDs where the code maintains them (new in M3: I12); `// TLA: B_1`, `B_2`, `B_3` in `createLocked` and `deleteLocked`; `// TLA: B_2, the ambiguous branch (the connection fence)` and `// TLA: B_3 (the read)` in `fenceAndReadLocked` (design.md 7.3).
- **Deliberately not in M3**: `CheckOwner`, `ErrNotOwner`, `lostOwnership`, the owner check after the read, `errAbandoned`, cancelling `bgCtx` (M6); `dead`, `touch`, `tryLockOp` (M5); Porcupine (M4); PostgreSQL (M7); the SDK and its retry rules (M8).

### Decisions in this plan that the design does not fix

1. **No owner epoch yet** (as M2 decision 1). `fenceAndReadLocked` reads without the owner check, `InsertDedup` has no "not the owner" result, and `createLocked` has no `ErrNotOwner` case. Each place gets a one-line comment that M6 adds it; the superseded-fence failpoint test is M6.
2. **`backoff(attempt)` only waits**: 10 ms doubled per attempt, at most 1 s. Returning an error when `bgCtx` is cancelled, and with it `errAbandoned`, is M6 (shutdown).
3. **fakedb takes memory-authoritative's statement mechanism** (its `run`, `Stmt.Exec`, a new session per statement). A hook decides what happens: nil runs the statement normally; an error is returned to the caller, and the statement executes only if and when the hook calls `st.Exec` (now: executed, answer lost; later from any goroutine: late; never). M2's hook uses (return an error without calling `Exec`) keep their meaning.
4. **`FenceSession(ctx, id)` fences one session**; a `Fence` statement's `Session` is the session it terminates. A hook error means "not confirmed": the session is not fenced and its pending statement may still execute. On success, an `Exec` of that session's statement does nothing afterwards. "Executes now or is discarded" (12.1 item 2) is the hook calling the kept `Exec` before returning nil, or not.
5. **Rejection switch `SetRejecting(key, on)`**: while on, a write of that key that would change the row (an INSERT that would write it, an UPDATE or DELETE that matches it) is rejected with `db.ErrRejected` when it executes, on time or late, and changes nothing (the dedup record is rolled back with the INSERT).
6. **fakedb checks for the tests**: `RowCheck func(*schema.Row) error` (`DbValsBelongToGen`), `Pending() int` (statements answered ambiguous whose `Exec` has neither run nor been fenced: `StmtOnlyUnderOp`), `AppliedUpdates() []*schema.Row` (every row an Update wrote: `UpdateAnswerSound`), `Claimed`, `Deleted`, and `Violation()` gains `NoResurrection` (a row inserted for a (key, gen) whose row was deleted).
7. **`Create` checks its row** before taking `op` (memory-authoritative's `checkRow`, copied): one value per column, and the key matches the primary key values (I11); otherwise `ErrInvalid`. A `Gen` of 0 gets `newGen()`: non-zero, 63-bit, `math/rand/v2`.
8. **In the stress test, `CreateFailureIsFinal` is checked on first attempts only.** A retry with the same gen that times out queued behind its own first attempt answers `DEADLINE_EXCEEDED` although the first attempt lands: that is hole 7, which the SDK rule closes (M8).

## Review Focus

1. A Delete or Update that finds the row of another gen (`Change.Gen` / `gen` set) returns `ErrNotFound` and sends nothing; one that finds no row in the DB although the snapshot had it returns `ErrInternal` and drops the snapshot. Tests: `TestDeleteGenMismatch`, `TestDeleteZeroRowsIsInternal` (Task 4).
2. A create whose ambiguous INSERT was dropped by the fence, and whose dedup lookup after the fence fails, answers `ErrOutcomeUnknown`, not "not executed" and not OK. Test: `TestCreateAmbiguousDedupReadFails` (Task 3).
3. A statement kept pending by an ambiguous answer either lands before the fence returns or never: after the write answers, executing it late changes nothing and `Pending()` is 0. Test: `TestUpdateAmbiguousNotApplied` (Task 2).
4. A rejected INSERT leaves no dedup record, so a later create with the same gen still works. Test: `TestCreateRejected` (Task 3).
5. A `FenceSession` that cannot confirm is retried while `op` stays held, and the statement may still land in between; the final answer follows the read. Test: `TestUpdateFenceNotConfirmed` (Task 2).

---

### Task 1: The DB interface and a fakedb with faults

**Files:**
- Modify: `internal/db/db.go`
- Modify: `internal/db/fakedb/fakedb.go`
- Test: `internal/db/fakedb/fakedb_test.go`

**Interfaces:**
- Consumes: M2 `db`, `fakedb`.
- Produces:
  ```go
  package db
  var (
      ErrNoRows      = errors.New("no rows")      // Select found nothing; Update / Delete matched no row
      ErrNotExecuted = errors.New("not executed")
      ErrRejected    = errors.New("rejected")     // a constraint rejected it; the statement was rolled back
      ErrGenClaimed  = errors.New("gen already claimed")
      ErrDuplicate   = errors.New("duplicate key")
  )
  type SessionID uint64
  type AmbiguousError struct { Session SessionID; Err error } // Error(), Unwrap()
  func IsAmbiguous(err error) bool
  func SessionOf(err error) SessionID // 0 if err holds no *AmbiguousError
  type DB interface {
      Select(ctx, key) (*schema.Row, error)
      InsertDedup(ctx, r *schema.Row) (*schema.Row, error) // design.md 5.2 comment, without ErrNotOwner (M6)
      GenClaimed(ctx, key, gen int64) (bool, error)
      Update(ctx, r *schema.Row, cols []int) (*schema.Row, error)
      Delete(ctx, key, gen int64) error                    // ErrNoRows: no row matched; the dedup record stays
      FenceSession(ctx, id SessionID) error                // nil once the session can execute nothing more
  }

  package fakedb
  const ( Select Kind = iota; Update; Insert; Delete; GenClaimed; Fence )
  type Stmt struct {
      Kind    Kind
      Key     schema.Key   // not set for Fence
      Gen     int64        // Update, Insert, Delete, GenClaimed
      Session db.SessionID // the statement's own session; Fence: the session to terminate
      Exec    func()       // executes the statement; at most once; does nothing once its session is fenced
  }
  func Ambiguous(st *Stmt, err error) error // &db.AmbiguousError{Session: st.Session, Err: err}
  type DB struct {
      Hook      func(ctx context.Context, st *Stmt) error // decision 3
      Normalize func(col int, v any) any                  // also applied to InsertDedup's values
      RowCheck  func(r *schema.Row) error                 // every row Insert or Update writes; an error is a DbValsBelongToGen violation
  }
  func (d *DB) SetRejecting(key schema.Key, on bool) // decision 5
  func (d *DB) Claimed(key schema.Key, gen int64) bool
  func (d *DB) Deleted(key schema.Key, gen int64) bool
  func (d *DB) Pending() int                        // decision 6
  func (d *DB) AppliedUpdates() []*schema.Row       // copies
  // Violation: OneWriterPerKey (I5), NoResurrection, DbValsBelongToGen.
  ```

Semantics of each statement when it executes (design.md 8.3), under `d.mu`:
- `InsertDedup`: rejecting → `ErrRejected`, nothing written; gen in the dedup table → `ErrGenClaimed`, nothing written; a row with the key exists → the dedup record is written, `ErrDuplicate`; otherwise both are written (`Ver` 0, values through `Normalize`), `NoResurrection` and `RowCheck` checked, the stored row returned.
- `Delete`: no row with that gen → `ErrNoRows`; rejecting → `ErrRejected`; otherwise delete it and mark `(key, gen)` deleted. The dedup record stays.
- `Update`: as in M2, plus rejecting → `ErrRejected` when it would match, `RowCheck`, and the written row appended to `AppliedUpdates`.
- `GenClaimed`, `Select`: read. `FenceSession`: mark the session fenced.
- `OneWriterPerKey` counts `InsertDedup`, `Update` and `Delete` calls in progress per key, as M2 does.

- [ ] **Step 1: Create the branch and commit this plan**

```bash
git checkout -b db-authoritative-m3 main
git add docs/plans/2026-11-14-m3-create-delete.md
git commit -m "Add the implementation plan for milestone M3 of db-authoritative"
```

- [ ] **Step 2: Write the failing tests** (append to `fakedb_test.go`; `../memory-authoritative/internal/db/fakedb/fakedb_test.go` has tests of the same names to start from)

| Test | Asserts |
|---|---|
| `TestHookSeesEveryStatement` | each method's statement reaches the hook with its Kind, Key, Gen and a distinct non-zero Session; a Fence carries the target session |
| `TestHookErrorWithoutExecDoesNotExecute` | hook returns an error, never calls `Exec`: the caller gets the error, the row is unchanged |
| `TestLostAnswerExecutes` | hook calls `st.Exec()` then returns `Ambiguous(st, …)`: `db.IsAmbiguous(err)`, `db.SessionOf(err) == st.Session`, the row changed |
| `TestLateExecAndPending` | hook keeps `Exec` and returns `Ambiguous`: `Pending() == 1`; calling `Exec` later applies it and `Pending() == 0`; a second `Exec` does nothing |
| `TestFenceDropsPendingStatement` | ambiguous Update kept; `FenceSession(session)` nil; `Pending() == 0`; the kept `Exec` afterwards does nothing |
| `TestFenceNotConfirmed` | Fence hook returns an error: `FenceSession` returns it; the session is not fenced (the kept `Exec` still applies) |
| `TestInsertDedup` | insert → row (ver 0) and `Claimed`; same gen again → `ErrGenClaimed`; new gen while the row exists → `ErrDuplicate` and that gen `Claimed` |
| `TestDeleteKeepsDedupAndChecksGen` | Delete of another gen → `ErrNoRows`; of the row's gen → row gone, `Deleted`, still `Claimed` |
| `TestRejectingSwitch` | with `SetRejecting(k, true)`: Insert that would write → `ErrRejected`, not `Claimed`; Update and Delete that match → `ErrRejected`, row unchanged; an Update of another gen → `ErrNoRows` (not rejected) |
| `TestNoResurrectionIsAsserted` | (package fakedb, so it can reach `d.dedup`) Insert gen 5, Delete gen 5, remove the dedup record from `d.dedup` by hand, Insert gen 5 again: `Violation()` names NoResurrection |
| `TestRowCheck` | `RowCheck` rejecting a row: an Insert or Update that writes it sets `Violation()` |
| `TestAppliedUpdates` | two Updates applied, one that matched no row: `AppliedUpdates()` holds exactly the two written rows |
| `TestOneWriterPerKeyAcrossKinds` | an Update and a Delete of one key in progress at once (both held in the hook): `Violation()` is set |

Run: `go test ./internal/db/...`
Expected: FAIL (build errors).

- [ ] **Step 3: Implement**

`db.go` as in the Interfaces block (`AmbiguousError.Error` returns `"outcome ambiguous: " + e.Err.Error()`). `fakedb.go`: copy `run`, `Stmt`, `Ambiguous`, `write`, `checkRowLocked`, `dedup`, `deleted`, `clone` from the source fakedb and adapt to the semantics above: `FenceSession` takes one id; `SetCrossRow` becomes `SetRejecting` with `db.ErrRejected` and decision 5's rule; Delete with no matching row returns `db.ErrNoRows`; `Select` and `Update` move onto `run`. `Pending` tracking: when the hook returns an error that `db.IsAmbiguous` and `Exec` has not run, the statement is pending until its `Exec` runs or its session is fenced.

- [ ] **Step 4: Run the tests**

Run: `go test -race ./internal/db/... && go test -race ./... && go vet ./... && gofmt -l .`
Expected: `ok` everywhere (M1/M2 store tests unchanged), no other output.

- [ ] **Step 5: Commit**

```bash
git add internal/db
git commit -m "M3: DB interface for create, delete and fence; fakedb with sessions and faults"
```

---

### Task 2: The fence, and the ambiguous Update

**Files:**
- Modify: `internal/store/store.go`: `backoff`, `fenceAndReadLocked`
- Modify: `internal/store/update.go`: the `IsAmbiguous` case of `updateLocked`
- Create: `internal/store/script_test.go`: scripted-hook helpers
- Test: `internal/store/update_test.go` (append)

**Interfaces:**
- Consumes: Task 1.
- Produces:
  ```go
  func (s *Store) backoff(attempt int) // decision 2
  // fenceAndReadLocked settles a statement whose outcome is ambiguous (design.md 7.3, without the owner
  // check: M6). Requires op. Returns the row read after the fence (absent if none) and stores it in snap.
  func (s *Store) fenceAndReadLocked(e *entry, amb error) (*schema.Row, error)

  // script_test.go (test-only), copied from ../memory-authoritative/internal/store/create_test.go and adapted:
  type step func(st *fakedb.Stmt) error
  func lost(st *fakedb.Stmt) error            // executes, answers ambiguous
  func amb(late *func()) step                 // keeps Exec in *late, answers ambiguous
  func fail(err error) step                   // answers err wrapped, does not execute
  func blockOn(release chan struct{}) step
  func script(d *fakedb.DB, steps map[fakedb.Kind][]step) *recorder
  func (r *recorder) of(k fakedb.Kind) []fakedb.Stmt
  func (r *recorder) wantFences(t *testing.T, n int, k fakedb.Kind) // n Fence statements, each targeting the session of the first statement of kind k
  ```
  Leave out `landBefore` and `giveUp` (Task 3 adds a create version of `giveUp`).

- [ ] **Step 1: Write the failing tests** (`update_test.go`)

```go
// TLA: B_2 ambiguous, the statement executed (W_FenceThenApplied): OK with the DB's row.
func TestUpdateAmbiguousApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		rec := script(d, map[fakedb.Kind][]step{fakedb.Update: {lost}})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		got, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}})
		if err != nil || got.Ver != 4 || got.Vals[1] != "b" {
			t.Fatalf("Update = %+v, %v; want ver 4, b", got, err)
		}
		rec.wantFences(t, 1, fakedb.Update)
		checkInvariants(t, s, d)
	})
}

// TLA: B_2 ambiguous, the fence dropped the statement (W_FenceThenNotApplied): not executed, and the kept
// statement can never land (I5).
func TestUpdateAmbiguousNotApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		var late func()
		rec := script(d, map[fakedb.Kind][]step{fakedb.Update: {amb(&late)}})
		s := newTestStore(t, d)
		k := key(t, s, 1)
		d.Put(testRow(k, 1))
		if _, err := s.Update(context.Background(), k, Change{Cols: []ColChange{set(1, "b")}}); !errors.Is(err, db.ErrNotExecuted) {
			t.Fatalf("err = %v, want ErrNotExecuted", err)
		}
		rec.wantFences(t, 1, fakedb.Update)
		late() // too late: the session is fenced
		if r := d.Row(k); r.Ver != 3 || r.Vals[1] != "a" {
			t.Fatalf("DB row %+v changed after the answer", r)
		}
		if n := d.Pending(); n != 0 {
			t.Fatalf("Pending = %d, want 0", n)
		}
		checkInvariants(t, s, d)
	})
}
```

| Test | Script | Asserts |
|---|---|---|
| `TestUpdateAmbiguousExecutesAtFence` | Update `amb(&late)`; Fence: a step that calls `late()` then returns nil | OK, ver 4; one fence; `checkInvariants` |
| `TestUpdateFenceNotConfirmed` | Update `amb(&late)`; Fence: `fail(errors.New("not confirmed"))`, then a step that calls `late()` and fails again, then nil | OK (the statement landed between attempts); three fences, each targeting the Update's session; `checkInvariants` |
| `TestUpdateReadAfterFenceFails` | Update `amb(&late)`; Select: the load passes, then `fail(db.ErrNotExecuted)` twice, then normal | `ErrNotExecuted` (not applied); three Selects after the fence; `checkInvariants` |
| `TestUpdateRejected` | `d.SetRejecting(k, true)` | `errors.Is(err, db.ErrRejected)`; row unchanged; no fence |

Run: `go test ./internal/store/`
Expected: FAIL (build errors: `script`, `lost`, `Pending` … undefined, or wrong answers).

- [ ] **Step 2: Implement**

`fenceAndReadLocked` exactly as design.md 7.3 without the `CheckOwner` call and the `ErrNotOwner` case and with `backoff` returning nothing: loop `FenceSession(s.bgCtx, db.SessionOf(amb))` until nil; then loop `Select` (`ErrNoRows` → `absent`) until it succeeds; `e.snap.Store(r)`; return `r`. In `updateLocked` add the `IsAmbiguous` case of design.md 7.4. The `default` case's comment becomes `ErrNotExecuted or ErrRejected`.

- [ ] **Step 3: Run the tests**

Run: `go test -race ./internal/store/ && go vet ./... && gofmt -l .`
Expected: `ok`, no other output.

- [ ] **Step 4: Commit**

```bash
git add internal/store
git commit -m "M3: fence, read and decide for an ambiguous Update"
```

---

### Task 3: Create

**Files:**
- Create: `internal/store/create.go`: `Create`, `createLocked`, `checkRow`, `newGen`
- Modify: `internal/store/store.go`: `ErrAlreadyExists`
- Test: `internal/store/create_test.go`

**Interfaces:**
- Consumes: Tasks 1-2 (`fenceAndReadLocked`, `script`, `lost`, `amb`, `fail`, `blockOn`).
- Produces:
  ```go
  var ErrAlreadyExists = errors.New("already exists")
  // Create returns the row as stored. (nil, nil): this gen was created earlier and that life has been
  // deleted since (design.md 7.5).
  func (s *Store) Create(ctx context.Context, in *schema.Row) (*schema.Row, error)
  func (s *Store) createLocked(e *entry, in *schema.Row) (*schema.Row, error)
  func (s *Store) checkRow(in *schema.Row) error
  func newGen() int64
  // create_test.go helpers (test-only), used by Tasks 4-5:
  func newRow(k schema.Key, id, gen int64) *schema.Row // {Key: k, Gen: gen, Vals: {id, "a", int64(10)}}
  // giveUp creates in with a client that gives up while the INSERT is blocked; settle lets the INSERT run
  // and waits for the create to finish (s.inflight.Wait). Call it inside synctest.
  func giveUp(t *testing.T, s *Store, d *fakedb.DB, in *schema.Row) (settle func())
  ```

- [ ] **Step 1: Write the failing tests** (`create_test.go`)

| Test | Does | Asserts |
|---|---|---|
| `TestCreateInsertsRow` | Create of an absent key | the DB's row (ver 0) returned and cached; `Claimed(k, gen)`; `opFree` |
| `TestCreateNewGenIsNonZero` | `in.Gen == 0` | returned `Gen != 0`, and equals the DB row's gen |
| `TestCreateExistingIsAlreadyExists` | row with gen 101 exists; Create gen 7 (not claimed) | `ErrAlreadyExists`; no Insert sent |
| `TestCreateSameGenReturnsRow` | row with gen 101 exists; Create gen 101 | the cached row, no Insert sent |
| `TestCreateOtherLifeGenClaimed` (hole 6, unit) | row gen 200 exists; dedup holds gen 7 (Insert 7, Delete 7, Insert 200 directly on `d`); Create gen 7 | `(nil, nil)`; row 200 stays |
| `TestCreateLoadsFirst` | row in the DB, nothing cached | `ErrAlreadyExists`; one Select, no Insert |
| `TestCreateLoadErrorSendsNothing` | Select fails | `errors.Is(err, db.ErrNotExecuted)`; no Insert |
| `TestCreateRejectsBadRow` | wrong number of values; key not matching the PK value | `ErrInvalid`; nothing sent |
| `TestCreateGenClaimedIsSuccessWithNoRow` (hole 2, unit) | dedup holds gen 7, no row (Insert then Delete on `d`); Create gen 7 | `(nil, nil)`; Get → `ErrNotFound` |
| `TestCreateDuplicateIsInternal` | Get caches absent; `d.Put` a row behind the cache; Create | `ErrInternal`; snapshot dropped; next Get returns the Put row |
| `TestCreateRejected` | `SetRejecting(k, true)`; Create gen 7; switch off; Create gen 7 | first `ErrRejected` and not `Claimed`; second OK |
| `TestCreateAmbiguousApplied` (W_CreateAmb) | Insert `lost` | OK with the DB row; one fence |
| `TestCreateAmbiguousNotApplied` | Insert `amb(&late)` | `ErrNotExecuted`; `late()` afterwards changes nothing; not `Claimed`; `checkInvariants` |
| `TestCreateAmbiguousDedupReadFails` | Insert `amb(&late)` (the fence drops it); GenClaimed `fail(db.ErrNotExecuted)`; then a second Create with the same gen, no faults | first `ErrOutcomeUnknown` and `opFree`; the second answers OK with the row (the gen was never claimed) |
| `TestCreateClientGivesUp` | `giveUp`, then settle | `ErrOutcomeUnknown`, then Get returns the row |
| `TestUpdateWaitsForCreate` | Insert blocked; Update of the key started; release | the Update runs after the create and applies to the new row (ver 1) |

Run: `go test ./internal/store/`
Expected: FAIL (build errors).

- [ ] **Step 2: Implement**

`Create`: `checkRow`, then `lockOpLive`, then `runSync(… createLocked …)`. `createLocked` exactly as design.md 7.5 minus the `ErrNotOwner` case (decision 1). `checkRow` copied from `../memory-authoritative/internal/store/create.go`; `newGen` returns `rand.Int64N(math.MaxInt64) + 1`.

- [ ] **Step 3: Run the tests**

Run: `go test -race ./internal/store/ && go vet ./... && gofmt -l .`
Expected: `ok`, no other output.

- [ ] **Step 4: Commit**

```bash
git add internal/store
git commit -m "M3: Create with load first, dedup record and dedup lookup"
```

---

### Task 4: Delete, and holes 2 and 6

**Files:**
- Create: `internal/store/delete.go`: `Delete`, `deleteLocked`
- Test: `internal/store/delete_test.go`

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces:
  ```go
  // gen is optional (0 = no check); non-zero: only that life is deleted, a mismatch is ErrNotFound.
  func (s *Store) Delete(ctx context.Context, key schema.Key, gen int64) error
  func (s *Store) deleteLocked(e *entry, gen int64) error
  ```

- [ ] **Step 1: Write the failing tests** (`delete_test.go`)

| Test | Does | Asserts |
|---|---|---|
| `TestDeleteRow` | Delete of an existing row, gen 0 | nil; Get → `ErrNotFound`; DB has no row; `Claimed(k, 101)` still true (I12); `opFree` |
| `TestDeleteGenMismatch` | Delete with gen 999 | `ErrNotFound`; no Delete sent; row stays |
| `TestDeleteMissingIsNotFound` | no row | `ErrNotFound`; no Delete sent; `opFree` (LeakOpOnNotFound, delete half) |
| `TestDeleteZeroRowsIsInternal` | Get caches the row; `d.Put` gen 999 behind the cache; Delete gen 0 | `ErrInternal`; snapshot dropped |
| `TestDeleteRejected` | `SetRejecting(k, true)` | `ErrRejected`; row stays |
| `TestDeleteAmbiguousApplied` (W_DeleteAmb) | Delete `lost` | nil; one fence; absent cached |
| `TestDeleteAmbiguousNotApplied` | Delete `amb(&late)` | `ErrNotExecuted`; `late()` afterwards changes nothing; `checkInvariants` |
| `TestUpdateAfterDeleteIsNotFound` | Delete blocked; Update of the key started; release | Delete nil; Update `ErrNotFound`; nothing written by it |
| `TestCreateRetryAfterDeleteDoesNotResurrect` (hole 2) | `giveUp` on create gen 7, settle (it landed); Delete; Create gen 7 again | the retry answers `(nil, nil)`; Get → `ErrNotFound`; DB has no row; `d.Violation() == nil` |
| `TestCreateRetryFindsOtherLife` (hole 6) | `giveUp` on create gen 7, settle; Delete; Create gen 8 → OK; Create gen 7 again | the retry answers `(nil, nil)`; the row is still gen 8 |

Run: `go test ./internal/store/`
Expected: FAIL (build errors).

- [ ] **Step 2: Implement**

`Delete`: `lockOpLive`, then `runSync(… deleteLocked …)`. `deleteLocked` exactly as design.md 7.6 (no `touch`: M5).

- [ ] **Step 3: Run the tests**

Run: `go test -race ./internal/store/ && go vet ./... && gofmt -l .`
Expected: `ok`, no other output.

- [ ] **Step 4: Commit**

```bash
git add internal/store
git commit -m "M3: Delete, and the hole 2 and hole 6 tests"
```

---

### Task 5: Stress under every fault, the invariants, and the mutations

**Files:**
- Create: `internal/store/faults_test.go`: the random fault injector
- Modify: `internal/store/invariants_test.go`: `checkInvariants` also requires `d.Pending() == 0`
- Modify: `internal/store/stress_test.go`: `TestStressAll`; `TestStressUpdates` unchanged
- Modify: `internal/store/*.go` only if a test fails

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces (test-only):
  ```go
  // faults injects random faults into d while on (design.md 12.1 item 2); run inside synctest.
  //   Insert, Update, Delete: 55% normal; 10% executed, answer lost; 10% ambiguous, executed 0-50ms later
  //   (unless fenced first); 10% ambiguous, never executed; 15% not executed. Each after 0-500µs.
  //   Select, GenClaimed: 5% not executed. Fence: 20% not confirmed (the kept statement may still land).
  //   A flipper turns the rejection switch of a random key on or off every 1ms.
  type faults struct { /* counters: lost, late, never, fencesFailed, … */ }
  func startFaults(d *fakedb.DB, keys []schema.Key) *faults
  func (f *faults) stop() // no new faults, switches off, wait for the late Execs to run (or be no-ops)
  ```
  Adapt `../memory-authoritative/internal/store/faults_test.go`: keep its structure, drop eviction, `landBefore` and the cross-row bookkeeping it no longer needs.

- [ ] **Step 1: Write `TestStressAll`** (12.1 item 3, every fault)

Inside `synctest.Test`: 4 keys, initially absent; 64 goroutines × 100 operations, each a Get (30%), Create (20%), Update (30%: SET of column 1 with a unique value tagged with the gen the goroutine last saw, `Change.Gen` = that gen), or Delete (20%: gen 0 or the last seen gen). A Create uses a new gen, except that a goroutine whose last create answered `ErrOutcomeUnknown` retries that gen once (an SDK retry). Initial values of a create are tagged with its gen; `d.RowCheck` checks that column 1 of every written row carries the row's gen (`DbValsBelongToGen`). One in ten operations uses a 1 ms ctx, the rest 5 s.

Accepted answers: those of design.md 7.3 for each operation (`ErrNotFound`, `ErrAlreadyExists`, `ErrOutcomeUnknown`, `ErrRejected`, `ErrNotExecuted`, `context.DeadlineExceeded`, Create `(nil, nil)`); `ErrInternal`, an `*AmbiguousError` reaching the caller, or anything else fails the test.

At the end: `f.stop()`, `s.inflight.Wait()` (`BackgroundSettles`), `checkInvariants` (`CacheCoherent`, `NoLockLeak`, `StmtOnlyUnderOp` via `Pending() == 0`, `OneWriterPerKey`, `NoResurrection`, `DbValsBelongToGen` via `Violation()`), then:
- `UpdateAnswerSound`: every SET value answered OK is in `d.AppliedUpdates()`; none answered with a failure is.
- `CreateFailureIsFinal`: a first-attempt create answered with a failure never has its gen `Claimed` (decision 8).
- Non-vacuity (tla-mapping.md 8), asserted > 0: deletes answered OK (`W_Delete`); creates answered OK of a key after a delete of it (`W_Recreate`); answers lost, late Execs that landed, statements dropped by a fence (from the injector: `W_AmbStmtInFlight`, `W_FenceThenNotApplied`); ambiguous Inserts, Updates and Deletes each (`W_CreateAmb`, `W_DeleteAmb`, `W_FenceThenApplied`); `ErrOutcomeUnknown` answers (`W_ClientGaveUp`); `ErrRejected` answers (`W_Rejected`). Logged with `t.Logf`, not asserted: creates answered `(nil, nil)` (`W_DedupClaimed`), retries that found another life (`W_RetryFindsOtherLife`).

Run: `go test -race -run TestStressAll -count=5 ./internal/store/`
Expected: PASS 5/5, each run well under 10 s of wall time. If an assertion fails, find the cause (superpowers:systematic-debugging) and fix the code, not the test.

- [ ] **Step 2: Show that the guarding tests can fail**

For each row: make the temporary edit, run `go test ./internal/store/` (with `-run` and `-timeout 60s` where the edit leaks `op`), confirm a named test fails, revert with `git checkout -- <file>`.

| Temporary edit | TLA mutation / hole | Must fail |
|---|---|---|
| `fenceAndReadLocked`: skip the `FenceSession` loop | `NoConnFence` | `TestUpdateAmbiguousNotApplied` |
| `updateLocked`: on ambiguous, `return nil, ErrOutcomeUnknown` (op released by runSync) | `ReleaseOnAmbiguous` | `TestUpdateAmbiguousNotApplied` |
| `updateLocked`: after the fence, answer `ErrNotExecuted` without looking at the row | `AmbIsFailure` | `TestUpdateAmbiguousApplied` |
| `updateLocked`: drop `now.Ver == old.Ver+1` | `UpdateNoVerCompare` | `TestUpdateAmbiguousNotApplied` |
| fakedb `Delete`: also delete the dedup record | `DedupDeletedWithRow` | `TestCreateRetryAfterDeleteDoesNotResurrect` |
| `runSync`: skip `unlockOp` when `err` is `ErrNotFound` | `LeakOpOnNotFound` | `TestDeleteMissingIsNotFound` |
| `createLocked`: drop the `GenClaimed` lookup on another life | hole 6 | `TestCreateRetryFindsOtherLife` |
| `createLocked`: treat `ErrGenClaimed` from the INSERT as `ErrAlreadyExists` | hole 2 | `TestCreateRetryAfterDeleteDoesNotResurrect` |
| `createLocked`: send the INSERT without loading first | (load first, 7.5) | `TestCreateLoadsFirst` |
| `deleteLocked`: treat `ErrNoRows` as success | (zero rows, 7.6) | `TestDeleteZeroRowsIsInternal` |

Run: `git status --short`
Expected: only the test files of this task.

- [ ] **Step 3: Commit**

```bash
git add internal/store
git commit -m "M3: stress under every fault, invariants and mutation checks"
```

---

### Task 6: Milestone check

- [ ] **Step 1: Full check**

Run: `gofmt -l . && go vet ./... && go test -race -count=3 ./...`
Expected: no gofmt output, no vet output, every package `ok`.

- [ ] **Step 2: Review the comments and tla-mapping.md**

By reading: I5 and I12 are commented where the code relies on them; `// TLA: B_1`, `B_2`, `B_3` mark `createLocked`, `deleteLocked`, and `fenceAndReadLocked` carries its two marks; the M6 owner-check notes of decision 1 are present; nothing from "Deliberately not in M3" is present. If a name in tla-mapping.md no longer matches the code (for example the rejection switch is `SetRejecting`), update tla-mapping.md in this branch.

- [ ] **Step 3: Correctness review, then simplification**

Run `/code-review` on the branch and fix what it finds; then `/simplify`, rejecting any change listed in CLAUDE.md "Correctness rules that look like complexity"; then repeat Step 1.

- [ ] **Step 4: Report**

To the user: the output of Step 1; the table of Task 5 Step 2 with each result; the stress test's non-vacuity counters; what review and simplification changed; and the acceptance criteria of M3 (design.md 12.2) with the tests that show them:

| Criterion | Tests |
|---|---|
| stress test with every fault passes | `TestStressAll` |
| ambiguous then applied / not applied, for create, update and delete | `TestCreateAmbiguousApplied`, `TestCreateAmbiguousNotApplied`, `TestUpdateAmbiguousApplied`, `TestUpdateAmbiguousNotApplied`, `TestDeleteAmbiguousApplied`, `TestDeleteAmbiguousNotApplied` |
| hole 2 and hole 6, failing with the fix removed | `TestCreateRetryAfterDeleteDoesNotResurrect`, `TestCreateRetryFindsOtherLife` (Task 5 Step 2) |
| each of `NoConnFence`, `ReleaseOnAmbiguous`, `AmbIsFailure`, `UpdateNoVerCompare`, `DedupDeletedWithRow`, `LeakOpOnNotFound` makes a test fail | Task 5 Step 2 |
