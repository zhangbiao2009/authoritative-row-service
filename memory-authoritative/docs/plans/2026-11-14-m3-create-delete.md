# M3: Create and Delete, Until-Sure Retries, Dedup Table, Session Fence — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Store.Create` and `Store.Delete` return only once the DB outcome is definite and change memory just before returning. A timed-out, lost or late statement can never resurrect a deleted row, insert a row the client was told failed, or delete a newer life of the key. `Update` and `Delete` accept an optional gen.

**Architecture:** Create and Delete take the key's `op` and run the operation in a background goroutine (`runSync`). If the client's ctx expires first, the client gets `ErrOutcomeUnknown` while the goroutine finishes and releases `op` (design.md 7.4).
- `createLocked` loads first and decides from memory. It sends `InsertDedup`, which writes the business row and the dedup record in one statement, only when memory says the row does not exist.
- `insertUntilSure` and `deleteUntilSure` retry with the same parameters until the outcome is definite:
  - `unsure` remembers an ambiguous attempt, so a later "not executed" is not final (hole 4).
  - `inflight` remembers those attempts' sessions; they are fenced before a "rejected because of another row" is trusted (hole 5).
- A create retry that finds another life looks its gen up in the dedup table (hole 6).
- fakedb gains every write the design needs, a dedup table, sessions and fencing, a cross-row switch, and one hook through which tests inject faults at every statement.

**Tech Stack:** Go 1.25 (`testing/synctest`), standard library only.

**Spec:** [design.md](../../design.md). Relevant sections:
- section 4 (I5, I6, I7, I14);
- 5.1 (dedup table) and 5.2 (errors);
- 7.1, 7.3 (`Change.Gen`), 7.4 and 7.5;
- 8.2–8.4;
- 10 (gap list);
- 12.1 items 2, 3 and 6, and 12.2 (M3);
- 13.2 (holes 1, 2, 4, 5 and 6).

Also [CLAUDE.md](../../CLAUDE.md) and the M3 rows of [tla-mapping.md](../../tla-mapping.md) sections 3–8. The counterexamples the hole tests follow were regenerated from the model for this plan: `./run.sh hole1|hole2|hole4`, and `python3 suite.py holes --only hole5,hole6`, then `trace.py`.

## Global Constraints

- All paths are relative to `memory-authoritative/`. Run every command from there.
- Work on branch `memory-authoritative-m3` (from `main` at `b7715b1`), one commit per task. The user reviews and merges.
- Done means: `gofmt -l .` prints nothing, `go vet ./...` is clean, and `go test -race ./...` passes.
- Lock rules (6.4) as in M2. `op` is held for the whole of a create or delete, including every retry and the fence. `mu` is taken only for the `snap.Store` at the linearization point, never across DB IO.
- I7: Create and Delete change memory only after the DB outcome is definitely success, and before returning.
- I6: there is never an upsert. `InsertDedup` never modifies an existing row. `Delete` has `AND gen = ?`. Deleting a business row never deletes its dedup record (I14).
- Design 0.5 rules that must survive `/simplify`:
  - "not executed" after an ambiguous attempt is not final;
  - a cross-row rejection is final only once nothing is in flight;
  - the fence empties `inflight` and never resets `unsure`;
  - create loads first.
  - `insertUntilSure` and `deleteUntilSure` stay two functions, shaped as in design.md 7.4 and 7.5.
- Comments:
  - I7 and I14 where the code maintains them;
  - `// TLA: <Action>` for `X_lock`, `X_qtimeout`, `X_wait`, `FinishOp`, `B_c1`, `B_c2`, `B_d1`, `B_d2` and `FenceAndSend`;
  - each new field's guarding lock.
- **Deliberately not in M3** (do not add early):
  - `entry.dead`, `lockOpLive`'s dead loop, `touch`/`lastAccess`/`nowSec`, `entryCount`, `NegativeIdleTTL` (M5);
  - `bgCtx`, graceful shutdown, `ErrBusy`/`MaxDirty`, owner epoch (`ErrNotOwner`, the epoch condition, the 0-row epoch check), startup session cleanup, dedup-table cleanup, batches, metrics (M6);
  - PostgreSQL, the pgx error classification, `AllowCrossRowConstraints` (M7);
  - the SDK retry rule of hole 7 (M8).

### Decisions in this plan that the design does not fix

1. **Error classification in `db`.**
   - New sentinels:
     - `ErrNotExecuted`;
     - `ErrCrossRow` (rejected because of another row);
     - `ErrRejected` (rejected by the row's own data);
     - `ErrGenClaimed`;
     - `ErrDuplicate`.
   - One struct type, `*db.AmbiguousError{Session, Err}`, because `SessionOf` needs the session the attempt was sent on. It is the only error type, not a hierarchy.
   - The classifiers carry the design's names: `IsAmbiguous`, `NotExecuted`, `IsCrossRow` and `SessionOf`.
   - Contract: every error that `InsertDedup` and `Delete` return is classified by the DB implementation, and the store trusts it. Design 8.4's rule "an error we are not sure about is ambiguous" belongs to the PG adapter (M7).
   - `db.SessionID` is an opaque `uint64`. M7 may redefine it as (pid, backend_start); the store only stores the value and passes it on.
2. **fakedb has one `Hook` for every statement** (Select, Insert, Update, Delete, GenClaimed, Fence).
   - It replaces `SelectHook` and `UpdateHook` (M2 decision 1 left this to M3). The M1/M2 tests move to it.
   - The hook sees the statement and its `Exec`, and decides whether the statement executes now, later or never, and what the caller gets.
   - Every statement runs on its own session. `FenceSessions` marks sessions, and an `Exec` on a fenced session does nothing.
   - The cross-row switch (`SetCrossRow`) is checked when a statement executes, synchronously or late, so a late statement lands only if the switch is off by then.
   - A rejection by the row's own data is injected by the hook returning `db.ErrRejected`. fakedb has no predicate for it.
3. **fakedb's always-on assertions grow** (tla-mapping section 5). Each one is checked in `newTestStore`'s cleanup.
   - **OneWriterPerKey** now covers Insert, Update and Delete calls together.
   - New **NoResurrection**: a business row is inserted for a (key, gen) whose row was deleted earlier.
   - New **DbValsBelongToGen**, through an optional `RowCheck` that the stress test sets, which checks every row Insert or Update writes.
4. **No failpoint hooks in `store` in M3.**
   - Every gap M3 owns is a DB-call boundary, so fakedb's `Hook` reaches it:
     - delete-db-done: the Delete statement's hook calls `Exec` and then runs the Update before returning;
     - flush-snapshot and flush-late: the Update statement's hook;
     - every hole: the Insert/Delete hooks plus `SetCrossRow`.
   - Steps are ordered with `synctest.Wait()`. `fakedb.Hook` is the "one mechanism" of CLAUDE.md.
   - The first store failpoint is M5's evict-dead gap, which is not a DB call.
5. **Background work without `bgCtx` until M6.**
   - Each DB call made inside `runSync` gets a fresh `context.WithTimeout(context.Background(), DBTimeout)`; `FenceSessions` gets `OpFenceTimeout`.
   - The until-sure loops end only on a definite outcome. Tests stop their faults so that the loops settle (L2: `s.inflight.Wait()` returns).
   - `backoff(attempt)` sleeps 10ms·2^attempt, capped at 1s. These are unexported constants, because 11.4 has no setting for them.
   - `Config` gains `OpFenceTimeout` (11.4: 10s), and `New` rejects a value ≤ 0.
   - Until M5 (no `dead`), Create and Delete take `op` with `getOrCreate` plus `lockOp`; M5 turns that into `lockOpLive`.
6. **`Create` checks the shape of its row:** `len(Vals)` must equal the number of columns, and `Key` must equal `EncodeKey` of the primary key columns of `Vals` (I13). Otherwise it returns `ErrInvalid` without any DB call. `in.Ver` is ignored. The types of values are still not checked until M7.
7. **Gens.** `newGen()` returns `rand.Int64N(math.MaxInt64) + 1` (math/rand/v2): non-zero, 63-bit. Tests pass explicit gens, as the SDK will.
8. **Counterexample 1 of design 8.2** (a late flush overwriting a newer one) is M2's `TestLateFlushDoesNotOverwriteNewer` and is kept. M3 adds counterexamples 2 and 3.

## Review Focus

1. A Create whose `Vals` has the wrong length, or whose `Key` does not match the primary key columns of `Vals`, returns `ErrInvalid` and sends nothing. Test: `TestCreateRejectsBadRow` (Task 3).
2. A load that fails inside `createLocked`/`deleteLocked` returns the error, changes nothing and releases `op`, and the next Create of the key works. Test: `TestCreateLoadErrorHasNoEffect` (Task 3).
3. A fence that cannot be confirmed must not end the loop: back off, keep `inflight`, fence again. Test: `TestFenceNotConfirmedKeepsTrying` (Task 3).
4. A Delete arriving while a flush of the same key is writing must wait for it, not write at the same time (I5). Test: `TestDeleteWaitsForFlush` (Task 4).
5. A Delete whose client gives up still finishes in the background, releases `op` and leaves memory and the DB consistent. Test: `TestDeleteOutcomeUnknown` (Task 4).

---

### Task 1: Error classification in `db`, one statement hook in fakedb

**Files:**
- Modify: `internal/db/db.go`, `internal/db/fakedb/fakedb.go`
- Test: `internal/db/db_test.go` (new), `internal/db/fakedb/fakedb_test.go`
- Modify (hook migration, no behavior change): `internal/store/load_test.go`, `update_test.go`, `flush_test.go`, `stress_test.go`

**Interfaces:**
- Consumes: M2 `db.DB` (`Select`, `Update`), fakedb.
- Produces:
  ```go
  // package db
  var (
      ErrNotExecuted = errors.New("not executed")                   // definitely not executed (8.4)
      ErrCrossRow    = errors.New("rejected because of another row") // may succeed later (8.4, hole 5)
      ErrRejected    = errors.New("rejected by the row's own data")  // same parameters, same result (8.4)
      ErrGenClaimed  = errors.New("gen already claimed")             // InsertDedup: (key, gen) is in the dedup table
      ErrDuplicate   = errors.New("duplicate key")                   // InsertDedup: dedup record new, business row exists
  )
  type SessionID uint64 // the DB session a statement was sent on; opaque to the store
  // AmbiguousError: the statement may have executed, or may still execute, on Session (8.1, 8.4).
  type AmbiguousError struct { Session SessionID; Err error }   // Error(), Unwrap()
  func IsAmbiguous(err error) bool      // errors.As *AmbiguousError
  func SessionOf(err error) SessionID   // the session of the AmbiguousError in err; 0 if none
  func NotExecuted(err error) bool      // errors.Is ErrNotExecuted
  func IsCrossRow(err error) bool       // errors.Is ErrCrossRow

  // package fakedb
  type Kind uint8
  const ( Select Kind = iota; Insert; Update; Delete; GenClaimed; Fence ) // Insert..Fence used from Task 2
  // Stmt is one statement, as the hook sees it.
  type Stmt struct {
      Kind     Kind
      Key      schema.Key     // not set for Fence
      Gen, Ver int64          // Gen: Insert, Update, Delete, GenClaimed; Ver: Update
      Session  db.SessionID   // a new session per statement
      Sessions []db.SessionID // Fence: the sessions to terminate
      Exec     func()         // executes the statement; at most once (a second call does nothing)
  }
  // Hook, if set, runs for every statement before it executes, without the DB's lock held.
  //   nil:   the statement executes (Exec is called now unless the hook already called it) and its own result is returned.
  //   error: that error is returned. The statement executes only if and when the hook calls Exec:
  //          now (executed, response lost), later from any goroutine (late), or never.
  // Exec of a statement whose session was fenced does nothing. Set Hook before concurrent use.
  Hook func(ctx context.Context, st *Stmt) error
  // Ambiguous returns &db.AmbiguousError{Session: st.Session, Err: err}.
  func Ambiguous(st *Stmt, err error) error
  ```
  Without a hook: a statement whose ctx is already done is not executed, and returns `fmt.Errorf("%w: %w", db.ErrNotExecuted, ctx.Err())`. Otherwise it executes.

- [ ] **Step 1: Create the branch**

```bash
git checkout main && git checkout -b memory-authoritative-m3
```

- [ ] **Step 2: Write the failing tests**

```go
// internal/db/db_test.go
func TestClassify(t *testing.T) {
	amb := fmt.Errorf("insert: %w", &AmbiguousError{Session: 7, Err: context.DeadlineExceeded})
	if !IsAmbiguous(amb) || SessionOf(amb) != 7 || !errors.Is(amb, context.DeadlineExceeded) {
		t.Fatalf("ambiguous: IsAmbiguous %v, SessionOf %d", IsAmbiguous(amb), SessionOf(amb))
	}
	for _, err := range []error{ErrNotExecuted, ErrCrossRow, ErrRejected, errors.New("x")} {
		if IsAmbiguous(err) || SessionOf(err) != 0 {
			t.Errorf("%v classified as ambiguous", err)
		}
	}
	if !NotExecuted(fmt.Errorf("w: %w", ErrNotExecuted)) || NotExecuted(ErrCrossRow) {
		t.Error("NotExecuted")
	}
	if !IsCrossRow(fmt.Errorf("w: %w", ErrCrossRow)) || IsCrossRow(ErrRejected) {
		t.Error("IsCrossRow")
	}
}
```

In `fakedb_test.go`, change `TestUpdateHookLateExec` and `TestOneWriterPerKeyIsAsserted` to `Hook` (same assertions; the late one returns `Ambiguous(st, context.DeadlineExceeded)` and checks `db.IsAmbiguous(err)`), and add:

```go
func TestHookSeesEveryStatement(t *testing.T) {
	// Put row k; Hook records (st.Kind, st.Key, st.Gen, st.Ver, st.Session) and returns nil
	// Select(k), Update(k gen 5 ver 3) -> both succeed with their normal results
	// seen == [{Select k 0 0 s1}, {Update k 5 3 s2}], s1 != s2, both non-zero
}

func TestHookErrorWithoutExecDoesNotExecute(t *testing.T) {
	// Hook returns fmt.Errorf("%w", db.ErrNotExecuted) without Exec for an Update -> DB row unchanged;
	// errors.Is(err, db.ErrNotExecuted)
}

func TestNoHookCancelledCtxIsNotExecuted(t *testing.T) {
	// cancelled ctx, no hook: Update -> db.NotExecuted(err) && errors.Is(err, context.Canceled); row unchanged
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `go test ./internal/db/...`
Expected: FAIL to compile: `undefined: AmbiguousError`, `d.Hook undefined`, `undefined: Ambiguous`.

- [ ] **Step 4: Implement**

- `db.go`: the sentinels, `SessionID`, `AmbiguousError` and the four classifiers. The package comment states the contract of decision 1.
- `fakedb.go`: replace `SelectHook`/`UpdateHook` with `Hook`, adding a session counter (`atomic.Uint64`, starting at 1). A private `run(ctx, st, exec func())` is shared by every statement. It does the hook-or-ctx logic above, wraps `exec` so it runs at most once, and returns the hook's error or `nil`. Each statement method keeps its own result in a local that `exec` sets.
- Move the M1/M2 tests to `Hook`, filtering on `st.Kind`. `hang` (update_test.go) becomes a `Hook` that waits for `ctx.Done()` on `Update` statements.

- [ ] **Step 5: Run everything**

Run: `go test -race ./...`
Expected: PASS. Every M1/M2 test passes unchanged in what it asserts.

- [ ] **Step 6: Commit**

```bash
git add internal && git commit -m "M3: error classification in db; one statement hook in fakedb"
```

---

### Task 2: `InsertDedup`, `GenClaimed`, `Delete`, `FenceSessions` in `db.DB` and fakedb

**Files:**
- Modify: `internal/db/db.go`, `internal/db/fakedb/fakedb.go`
- Test: `internal/db/fakedb/fakedb_test.go`

**Interfaces:**
- Consumes: Task 1.
- Produces:
  ```go
  // db.DB gains (design.md 8.3):
  // InsertDedup inserts r (r.Ver == 0) and its dedup record (table, r.Key, r.Gen) in one statement (I14).
  // nil: both written; returns the stored row. ErrGenClaimed: the dedup record exists; nothing written.
  // ErrDuplicate: the dedup record was new and is now written, but a row with r.Key exists.
  InsertDedup(ctx context.Context, r *schema.Row) (*schema.Row, error)
  // GenClaimed reports whether (key, gen) is in the dedup table. A read.
  GenClaimed(ctx context.Context, key schema.Key, gen int64) (bool, error)
  // Delete deletes the row with key and gen (I6). Zero rows affected is nil. The dedup record stays (I14).
  Delete(ctx context.Context, key schema.Key, gen int64) error
  // FenceSessions terminates the sessions and returns nil once none of them can execute anything any
  // more (7.4, 8.3). An error means "not confirmed".
  FenceSessions(ctx context.Context, ids []db.SessionID) error

  // fakedb gains:
  func (d *DB) SetCrossRow(key schema.Key, on bool) // while on, Insert and Delete of key, when they execute, return
                                                    // db.ErrCrossRow and change nothing (dedup included)
  func (d *DB) Claimed(key schema.Key, gen int64) bool // dedup table lookup for test checks; counts nothing
  RowCheck func(r *schema.Row) error // optional; run on every row Insert or Update writes; an error is a
                                     // DbValsBelongToGen violation. Set before concurrent use.
  type Stats struct{ Late, LateEffective, GenClaimed, CrossRow, Fenced int }
  func (d *DB) Stats() Stats // Late: Execs after their call returned; LateEffective: of those, ones that changed
                             // a row or the dedup table; GenClaimed / CrossRow: results of those kinds;
                             // Fenced: sessions fenced
  ```
  fakedb semantics, all in `exec` under `d.mu`:
  - **Insert**: cross-row on → `ErrCrossRow`. Dedup has (key, gen) → `ErrGenClaimed`. Otherwise it writes the dedup record. Then, if a row with the key exists → `ErrDuplicate`; otherwise it stores a copy of `r` and returns a copy. It checks **NoResurrection**, failing if a row of (key, gen) was deleted before.
  - **Delete**: cross-row on → `ErrCrossRow`. If a row with the key and gen exists, it deletes the row and records (key, gen) as deleted; the dedup table is never touched.
  - **GenClaimed**: reads the dedup table.
  - **Fence**: marks `st.Sessions` fenced.
  - **OneWriterPerKey** counts Insert, Update and Delete calls of a key together.

- [ ] **Step 1: Write the failing tests**

```go
func TestInsertDedup(t *testing.T) {
	// insert k gen 100 -> row returned == stored copy (Ver 0); Claimed(k, 100)
	// insert k gen 100 again -> ErrGenClaimed, row unchanged
	// insert k gen 200 (row exists) -> ErrDuplicate; Claimed(k, 200) (the gen is burned); row still gen 100
	// Stats().GenClaimed == 1
}

func TestDeleteKeepsDedupAndChecksGen(t *testing.T) {
	// insert k gen 100; Delete(k, 999) -> nil, row still there; Delete(k, 100) -> nil, Row(k) == nil;
	// Claimed(k, 100) still true; insert k gen 100 -> ErrGenClaimed; Violation() == nil
}

func TestCrossRowSwitch(t *testing.T) {
	// SetCrossRow(k, true): insert -> db.IsCrossRow(err), Row nil, !Claimed; Stats().CrossRow == 1
	// a late Insert (hook saves Exec, returns Ambiguous) executed while on -> nothing; Stats().LateEffective == 0
	// SetCrossRow(k, false): the same kind of late Insert executed now -> row inserted; Late 2, LateEffective 1
}

func TestFencedSessionNeverExecutes(t *testing.T) {
	// hook saves the Insert's Exec and returns Ambiguous(st, ...); FenceSessions(ctx, []SessionID{st.Session}) == nil
	// saved Exec() -> Row(k) == nil, !Claimed; Stats().Fenced == 1
	// a Fence whose hook returns an error without Exec: FenceSessions returns that error; nothing fenced
}

func TestOneWriterPerKeyAcrossKinds(t *testing.T) {
	// a blocked Insert of k (hook waits on a channel) and a Delete of k in progress at once -> Violation() != nil
}

func TestNoResurrectionIsAsserted(t *testing.T) {
	// white-box: insert gen 100, delete it, then remove (k,100) from d.dedup (simulating DedupDeletedWithRow),
	// insert gen 100 -> Violation() mentions NoResurrection
}

func TestRowCheck(t *testing.T) {
	// RowCheck returns an error for Vals[1] == "bad": insert with "bad" -> Violation() != nil
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/db/...`
Expected: FAIL to compile: `d.InsertDedup undefined`, `d.SetCrossRow undefined`, …

- [ ] **Step 3: Implement** as specified above. Each method goes through Task 1's `run`. Late/LateEffective are counted inside the at-most-once `exec` wrapper; "after the call returned" is a flag the method sets before returning.

- [ ] **Step 4: Run**

Run: `go test -race ./internal/db/... && go vet ./...`
Expected: PASS; vet clean (`store` still compiles: the new methods are not called yet).

- [ ] **Step 5: Commit**

```bash
git add internal/db && git commit -m "M3: InsertDedup, GenClaimed, Delete, FenceSessions in db.DB and fakedb"
```

---

### Task 3: `Create`: runSync, load first, insertUntilSure, the fence and the dedup lookup

**Files:**
- Modify: `internal/store/store.go` (errors, `Config.OpFenceTimeout`, `Store.inflight`, `dbCtx`, `backoff`)
- Create: `internal/store/create.go`
- Test: `internal/store/create_test.go`; `get_test.go` (`TestNewRejectsBadConfig` gains `OpFenceTimeout 0`)

**Interfaces:**
- Consumes: Task 2 `db.DB`, the classifiers, fakedb `Hook`/`Ambiguous`/`SetCrossRow`/`Claimed`/`Stats`; M1 `ensureLoadedLocked`, `getOrCreate`, `lockOp`; M2 `clearDirtyLocked`, `checkInvariants`.
- Produces:
  ```go
  var (
      ErrAlreadyExists  = errors.New("already exists")
      ErrOutcomeUnknown = errors.New("outcome unknown") // the client's ctx expired; the operation goes on (7.4)
      ErrInternal       = errors.New("internal")        // an invariant is broken
  )
  // Config gains: OpFenceTimeout time.Duration // 10s (11.4)
  // Store gains:  inflight sync.WaitGroup      // runSync goroutines still running
  // Create inserts in (a row of this table; in.Gen 0 = pick one) once the DB has definitely done so, and
  // returns the stored row. (nil, nil): this gen's create took effect earlier and the row was deleted since.
  func (s *Store) Create(ctx context.Context, in *schema.Row) (*schema.Row, error)
  func runSync[T any](ctx context.Context, s *Store, e *entry, fn func() (T, error)) (T, error)
  func (s *Store) createLocked(e *entry, in *schema.Row) (*schema.Row, error)
  func (s *Store) insertUntilSure(r *schema.Row) (*schema.Row, error)
  func (s *Store) dbCtx() (context.Context, context.CancelFunc) // WithTimeout(Background, DBTimeout)
  func (s *Store) backoff(attempt int)
  func newGen() int64
  ```
  Follow design.md 7.4:
  - `Create`: decision 6 check; `getOrCreate`; `// TLA: X_lock` `lockOp(ctx)` (error → return it: `// TLA: X_qtimeout`, definitely not executed); `runSync`.
  - `runSync`: `inflight.Add(1)`; the goroutine (`// TLA: FinishOp`) defers `inflight.Done` and `unlockOp` and sends the result; `// TLA: X_wait` select.
  - `createLocked`: `// TLA: B_c1`, using `ensureLoadedLocked` with `dbCtx`.
  - `insertUntilSure`: `// TLA: B_c2` / `// TLA: FenceAndSend`, exactly as the skeleton with `backoff(attempt)`. The success path does `snap.Store(got)` + `clearDirtyLocked` under `mu`, I7.

- [ ] **Step 1: Write the failing tests** (`create_test.go`)

The `items` schema and `newTestStore` as before. `newRow(k, id, gen)` = `&schema.Row{Key: k, Gen: gen, Vals: []any{id, "a", int64(10)}}`. Scripted hooks match on `st.Kind == fakedb.Insert` (etc.) and count attempts. "amb" means: save `st.Exec`, return `fakedb.Ambiguous(st, context.DeadlineExceeded)`. "lost" means: call `st.Exec()`, then return `fakedb.Ambiguous(st, context.DeadlineExceeded)`.

```go
func TestCreateInsertsRow(t *testing.T) {
	// Create(newRow(k,1,0)) -> r: r.Gen != 0, r.Ver == 0, r.Vals as given; Get(k) returns r (same pointer)
	// d.Row(k) equals r; d.Claimed(k, r.Gen); checkInvariants
}
func TestCreateExistingIsAlreadyExists(t *testing.T)   // Create gen 0 twice -> second ErrAlreadyExists, one Insert stmt
func TestCreateSameGenIsIdempotent(t *testing.T)      // Create gen 100 twice -> same row both times, one Insert stmt
func TestCreateRejectsBadRow(t *testing.T)            // Review Focus 1: Vals of length 2; Key of id 2 with Vals id 1
                                                      // -> ErrInvalid; Hook saw no statement
// Mutation CreateNoLoad.
func TestCreateLoadsFirst(t *testing.T) {
	// DB has k gen 101 (not loaded); Create(newRow(k,1,7)) -> ErrAlreadyExists; !d.Claimed(k, 7) (gen not burned);
	// no Insert stmt; no ErrInternal
}
func TestCreateLoadErrorHasNoEffect(t *testing.T) {
	// Review Focus 2, synctest. Select hook returns db.ErrNotExecuted: Create -> errors.Is(err, db.ErrNotExecuted);
	// no Insert stmt; checkInvariants (op free); hook off; time.Sleep(LoadErrTTL); Create -> succeeds
}
func TestCreateQueueTimeout(t *testing.T) {
	// synctest. the test holds entry op; Create with a 1s ctx -> errors.Is(err, context.DeadlineExceeded) after 1s;
	// no statement sent; unlockOp
}
func TestCreateOutcomeUnknown(t *testing.T) {
	// synctest. Insert hook blocks on a channel, then nil. Create with a 1s ctx -> ErrOutcomeUnknown at 1s.
	// Get(k) meanwhile -> ErrNotFound (absent, linearized before the create); release; s.inflight.Wait() (L2);
	// Get(k) -> the row; checkInvariants
}
func TestCreateAmbiguousRetriesSameGen(t *testing.T) {
	// attempt 1 lost; attempt 2 normal -> ErrGenClaimed -> Select -> Create returns the row with gen 100;
	// both Insert stmts had Gen 100; memory == DB
}
func TestCreateNotExecutedWithoutAmbiguityIsFinal(t *testing.T) // attempt 1 returns ErrNotExecuted -> returned; 1 Insert stmt
func TestCreateRejectedByOwnDataIsFinal(t *testing.T)           // attempt 1 amb (never executed); attempt 2 returns
                                                                // db.ErrRejected -> errors.Is(err, db.ErrRejected); Get NotFound
// Mutation LoadNoRecheck: a Get queued behind a create must not reload over the created row.
func TestGetQueuedBehindCreateKeepsNewerRow(t *testing.T) {
	// synctest. Create's Select blocks on a channel; go Create; synctest.Wait()
	// go Get(k): it sees nil and queues on op; synctest.Wait(); release; wait for both
	// Get returned the same pointer Create returned; d.Selects() == 1 (the queued load did not read again)
}

// Hole 4, create (trace: hole4).
func TestCreateAmbiguousThenNotExecutedIsNotFinal(t *testing.T) {
	// attempt 1 lost (executed); attempt 2 returns db.ErrNotExecuted; attempt 3 normal -> ErrGenClaimed -> Select
	// Create returns the row with gen 100; memory == DB; 3 Insert stmts
}

// Hole 5, create (trace: hole5). SetCrossRow(k, true) first.
func TestCrossRowRejectionAfterAmbiguousIsFenced(t *testing.T) {
	// attempt 1 amb (Exec saved, A); later attempts normal (rejected by the switch)
	// Create -> db.IsCrossRow(err); one Fence stmt with Sessions == [A's session]
	// SetCrossRow(k, false); A() -> d.Row(k) == nil, !d.Claimed(k, 100); Get NotFound; checkInvariants
}
// The first fix of hole 5 cleared unsure at the fence (design 7.4); TLC's counterexample, step by step.
func TestFenceKeepsUnsure(t *testing.T) {
	// SetCrossRow on; attempt 1 amb (A saved); attempt 2 normal (rejected); Fence hook: SetCrossRow(k, false), A()
	// (A lands: row + dedup), then nil; attempt 3 returns db.ErrNotExecuted; attempt 4 normal -> ErrGenClaimed
	// Create returns the row with gen 100 (not an error); memory == DB
}
// Review Focus 3.
func TestFenceNotConfirmedKeepsTrying(t *testing.T) {
	// synctest. as the hole 5 test, but the first Fence returns an error without Exec, the second is normal
	// Create -> IsCrossRow; 2 Fence stmts, both with A's session; A() afterwards lands nothing
}
```

Holes 1, 2 and 6 need `Store.Delete`, so their tests are in Task 4.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/store/`
Expected: FAIL to compile: `s.Create undefined`, `undefined: ErrAlreadyExists`.

- [ ] **Step 3: Implement** `create.go`, the store.go additions, and the Config field with validation.

- [ ] **Step 4: Run**

Run: `go test -race ./internal/store/`
Expected: PASS (M1/M2 tests included).

- [ ] **Step 5: Commit**

```bash
git add internal/store && git commit -m "M3: Create with runSync, load first, insertUntilSure, fence and dedup lookup"
```

---

### Task 4: `Delete`, optional gen for `Update`, and the tests that need both

**Files:**
- Create: `internal/store/delete.go`
- Modify: `internal/store/update.go` (`Change.Gen`)
- Test: `internal/store/delete_test.go`; `update_test.go` (gen cases)

**Interfaces:**
- Consumes: Task 3 (`runSync`, `dbCtx`, `backoff`, errors, `Create`).
- Produces:
  ```go
  // Delete deletes the row once the DB has definitely done so. gen 0: whatever life is there; else only that
  // life, ErrNotFound on a mismatch (7.3, 7.5).
  func (s *Store) Delete(ctx context.Context, key schema.Key, gen int64) error
  func (s *Store) deleteLocked(e *entry, gen int64) error
  func (s *Store) deleteUntilSure(key schema.Key, gen int64) error
  // Change gains: Gen int64 // 0: no check; else ErrNotFound unless the row in memory has this gen
  ```
  Follow design.md 7.5:
  - `Delete`: `getOrCreate`, `// TLA: X_lock` `lockOp`, `runSync`.
  - `deleteLocked`: `// TLA: B_d1`.
  - `deleteUntilSure`: `// TLA: B_d2`, `// TLA: FenceAndSend`, the same rules as `insertUntilSure`.
  - The success path does `snap.Store(absent)` + `clearDirtyLocked` under `mu` (I7: an Update before that point is discarded, one after it sees absent).
  - `Update`: the gen check sits in the `absent` branch (7.3).

- [ ] **Step 1: Write the failing tests**

```go
func TestDeleteRow(t *testing.T)            // Create gen 100; Delete(k, 0) -> nil; Get NotFound; d.Row nil; Claimed(k,100); checkInvariants
func TestDeleteGenMismatch(t *testing.T)    // Delete(k, 999) -> ErrNotFound; row in memory and DB; Delete(k, 100) -> nil
// Mutation LeakOpOnNotFound.
func TestDeleteMissingIsNotFound(t *testing.T) // no row: Delete -> ErrNotFound; no Delete stmt; checkInvariants (op free)
func TestUpdateWithGen(t *testing.T)        // Change{Gen: 999} -> ErrNotFound, nothing dirty; Change{Gen: row.Gen} -> ok

// Mutation DeleteKeepsDirty; gap delete-db-done (tla-mapping 3.1).
func TestUpdateDuringDeleteIsDiscarded(t *testing.T) {
	// load row; Delete hook: st.Exec(), then s.Update(k, set(1,"x")) -> must succeed, then return nil
	// Delete -> nil; Get NotFound; entry dirty 0, dirtyCount 0; flushAll -> d.Updates() == 0; checkInvariants
}
// Mutation FlushNoOp; Review Focus 4.
func TestDeleteWaitsForFlush(t *testing.T) {
	// synctest. Update; Update-stmt hook blocks on a channel, then nil; go flushAll; synctest.Wait()
	// go Delete(k, 0); synctest.Wait() -> no Delete stmt yet; release; wait both -> Delete nil, d.Row nil
	// d.Violation() == nil (OneWriterPerKey, checked in cleanup)
}
// Review Focus 5.
func TestDeleteOutcomeUnknown(t *testing.T) {
	// synctest. Delete-stmt hook blocks, then nil; Delete with a 1s ctx -> ErrOutcomeUnknown; release;
	// s.inflight.Wait(); Get NotFound; d.Row nil; checkInvariants
}
func TestDeleteAmbiguousRetries(t *testing.T) // attempt 1 lost; attempt 2 normal (0 rows) -> nil; Get NotFound

// Design 8.2 counterexample 3. Mutation NoGenCheck (Delete without gen).
func TestLateDeleteDoesNotDeleteNewLife(t *testing.T) {
	// Create gen 100; Delete attempt 1 amb (A saved); attempt 2 normal -> Delete nil
	// Create gen 200 -> ok; A() -> d.Row(k).Gen == 200; memory == DB
}
// Hole 4, delete (trace: hole4, the delete form in 13.2).
func TestDeleteAmbiguousThenNotExecutedIsNotFinal(t *testing.T) {
	// attempt 1 lost (deleted); attempt 2 ErrNotExecuted; attempt 3 normal -> Delete nil; Get NotFound; memory == DB
}
// Hole 5, delete.
func TestDeleteCrossRowAfterAmbiguousIsFenced(t *testing.T) {
	// row gen 100; SetCrossRow(k, true); attempt 1 amb (A saved); later attempts normal (rejected)
	// Delete -> IsCrossRow; one Fence with A's session; SetCrossRow false; A() -> row still in DB and memory, equal
}

// Hole 1 (trace: hole1). Mutations ReleaseOnAmbiguous, SelectConfirms, DedupDeletedWithRow.
func TestLateInsertDoesNotResurrect(t *testing.T) {
	// Insert attempt 1 amb (A saved); attempt 2 normal -> Create(gen 100) returns the row
	// Delete(k, 100) -> nil; A() -> d.Row(k) == nil; Get NotFound; checkInvariants
}
// Hole 2 (trace: hole2).
func TestCreateRetryAfterDeleteDoesNotResurrect(t *testing.T) {
	// synctest. Insert hook blocks then nil; Create(gen 100) with a 1s ctx -> ErrOutcomeUnknown; release; inflight.Wait()
	// Delete(k, 0) -> nil; Create(gen 100) again -> (nil, nil); d.Row(k) == nil; Get NotFound
}
// Hole 6 (trace: hole6).
func TestCreateRetryFindsOtherLife(t *testing.T) {
	// as hole 2 up to the delete; Create(gen 200) -> ok; Create(gen 100) -> (nil, nil), not ErrAlreadyExists;
	// memory and DB still gen 200; Create(gen 300) -> ErrAlreadyExists, !d.Claimed(k, 300)
}

// Design 8.2 counterexample 2. Mutation NoGenCheck (flush UPDATE without gen); gap flush-late.
func TestLateFlushAcrossDeleteAndCreate(t *testing.T) {
	// Create gen 100; Update set(1,"old"); Update-stmt hook: save Exec (F), return Ambiguous; flushAll
	// hook off; Delete(k,0); Create(newRow gen 200, name "new"); F() -> d.Row(k): gen 200, ver 0, name "new"
}
// Mutation FlushUpsert; NoResurrection.
func TestLateFlushAfterDeleteDoesNotResurrect(t *testing.T) {
	// as above but no re-create: F() -> d.Row(k) == nil
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/store/`
Expected: FAIL to compile: `s.Delete undefined`, `unknown field Gen in struct literal of type Change`.

- [ ] **Step 3: Implement** `delete.go` and `Change.Gen`.

- [ ] **Step 4: Run**

Run: `go test -race ./internal/store/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store && git commit -m "M3: Delete, optional gen for Update, holes 1, 2, 6 and design 8.2 counterexamples 2, 3"
```

---

### Task 5: Stress test with fault injection; tla-mapping

**Files:**
- Test: `internal/store/stress_test.go`
- Modify: `tla-mapping.md`, for the names that differ from the design skeletons:
  - `X_lock`: "`getOrCreate` + `lockOp` (`lockOpLive` from M5)";
  - section 2.3 `blocked`: "`fakedb.SetCrossRow`";
  - section 3.1: the gaps delete-db-done, flush-snapshot and flush-late are reached through `fakedb.Hook`;
  - section 5: `NoResurrection` and `DbValsBelongToGen` are also fakedb assertions.

**Interfaces:**
- Consumes: everything above.
- Produces: tests only.

- [ ] **Step 1: Write the test**

```go
// Acceptance (design.md 12.2 M3): a stress test with fault injection; afterwards memory and the DB match
// row for row, and the history checks of tla-mapping section 5 hold.
func TestStressCreateDelete(t *testing.T) {
	// 4 keys, 64 workers × 300 ops, flusher at 1ms (startFlusher)
	// ops: 25% Get; 25% Update with Change.Gen = the gen this worker last saw for the key (0 if none):
	//      set(1, "g<gen>") or incr(2, d); 25% Create(newRow(k, id, fresh gen from an atomic counter), name "g<gen>");
	//      25% Delete(k, last seen gen or 0)
	// client ctx: 5ms on 10% of creates and deletes (gives ErrOutcomeUnknown and queue timeouts), else 5s
	// SDK-like: a Create answered ErrOutcomeUnknown is retried with the same gen, up to 3 times, with a 5s ctx
	// RowCheck: Vals[1] == fmt.Sprint("g", r.Gen)  (DbValsBelongToGen)
	// faults (while `faults` is on), on Insert/Update/Delete statements:
	//   55% normal; 10% lost; 10% amb + late Exec after 0-3ms (late WaitGroup); 10% amb never executed;
	//   15% ErrNotExecuted; plus a random 0-500µs sleep before deciding. Select/GenClaimed: 5% ErrNotExecuted.
	//   Fence: always normal. A flipper goroutine toggles SetCrossRow on a random key every 1ms.
	// allowed results: nil, ErrNotFound, ErrAlreadyExists, ErrOutcomeUnknown, ctx errors, db.ErrNotExecuted,
	//   db.ErrCrossRow; ErrInternal or anything else -> t.Error
	// history (recorded by the driver):
	//   - NoResurrection: deleted[gen] = seq when Delete(k, gen != 0) returns nil; a Get/Update that started
	//     after that seq never returns gen
	//   - CreateFailureIsFinal: gens whose every Create call ended in a definite failure (not nil, not
	//     ErrOutcomeUnknown) are never d.Claimed at the end
	// end: faults off, cross-row off on every key; s.inflight.Wait() (L2 BackgroundSettles); late.Wait();
	//   stop the flusher; drain; checkInvariants; for every key: memory absent <=> d.Row nil, else equal incl.
	//   Gen and Ver (DbMatchesAbsWhenIdle); the end-of-run history checks above
	// non-vacuity (tla-mapping section 8), all > 0: d.Stats().Late, LateEffective, GenClaimed, CrossRow, Fenced;
	//   driver counts: successful deletes, creates after a delete of the key, ErrOutcomeUnknown answers,
	//   queue timeouts, "ErrNotExecuted given to a statement of a (key, gen) that had an ambiguous verdict"
}
```

Tune the fault percentages, not the checks, until every non-vacuity counter is > 0 in three consecutive runs. Keep the test under 15s with `-race`.

- [ ] **Step 2: Run it three times under the race detector**

Run: `go test -race -count=3 -run 'Stress' ./internal/store/`
Expected: PASS (M2's `TestStressUpdates` included). If it fails, the code is wrong (systematic-debugging), not the test.

- [ ] **Step 3: Run the whole suite**

Run: `gofmt -l . && go vet ./... && go test -race ./...`
Expected: nothing from gofmt, vet clean, all packages PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/store/stress_test.go tla-mapping.md && git commit -m "M3: stress test with fault injection; tla-mapping names"
```

---

## After the tasks (CLAUDE.md workflow, steps 3-6)

`/code-review`, fix; `/simplify`, rerun the suite. Then the mutation and fix-revert checks: break the rule, show that a named test fails, revert. Each hole test must fail with its fix reverted (12.2 M3).

| Mutation / fix reverted | Temporary patch | Must fail |
|---|---|---|
| `NoGenCheck` | fakedb Update and Delete ignore gen | `TestLateFlushAcrossDeleteAndCreate`, `TestLateDeleteDoesNotDeleteNewLife`, stress (DbValsBelongToGen) |
| `FlushUpsert` | fakedb Update inserts a missing row | `TestLateFlushAfterDeleteDoesNotResurrect` |
| `DedupDeletedWithRow` (holes 1, 2 fix) | fakedb Delete also removes the dedup record | `TestLateInsertDoesNotResurrect`, `TestCreateRetryAfterDeleteDoesNotResurrect` |
| `LoadNoRecheck` | `ensureLoadedLocked` skips the `snap` re-check | `TestGetQueuedBehindCreateKeepsNewerRow`, M1 `TestConcurrentMissesSelectOnce` |
| `FlushNoOp` | `flushShard` does not take `op` | `TestDeleteWaitsForFlush` |
| `ReleaseOnAmbiguous` | `insertUntilSure` returns the ambiguous error | `TestLateInsertDoesNotResurrect`, `TestCreateAmbiguousRetriesSameGen` |
| `SelectConfirms` | after an ambiguous attempt, one `Select`: no row → return the error | `TestLateInsertDoesNotResurrect` |
| `CreateNoLoad` | `createLocked` skips `ensureLoadedLocked` (treats nil as absent) | `TestCreateLoadsFirst` |
| `DeleteKeepsDirty` | `deleteLocked` does not clear dirty | `TestUpdateDuringDeleteIsDiscarded` |
| `LeakOpOnNotFound` | `runSync` releases `op` only on success | `TestDeleteMissingIsNotFound` |
| hole 4 fix | drop the `NotExecuted && unsure` case (insert and delete) | `TestCreateAmbiguousThenNotExecutedIsNotFinal`, `TestDeleteAmbiguousThenNotExecutedIsNotFinal` |
| hole 5 fix | drop the `IsCrossRow && len(inflight) > 0` case | `TestCrossRowRejectionAfterAmbiguousIsFenced`, `TestDeleteCrossRowAfterAmbiguousIsFenced` |
| hole 5, first wrong fix | the fence also sets `unsure = false` | `TestFenceKeepsUnsure` |
| hole 6 fix | drop the `GenClaimed` lookup in `createLocked` | `TestCreateRetryFindsOtherLife` |
| `NoVerCheck` (M2, re-run) | drop `ver <` from fakedb's Update | `TestLateFlushDoesNotOverwriteNewer` |

Report as in M2: acceptance criteria with their tests, the table above with results, rulings, deferred minors, and what `/simplify` changed and skipped.
