# M4: Porcupine Linearizability Tests — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove per-key linearizability (correctness-properties.md C1, S1 `NoViolation`/Get, S2 `MemMatchesAbs`). Random concurrent Get / Update / Create / Delete run against the Store under injected DB faults, and Porcupine checks that every answer a client got fits one sequential order consistent with real time.

**Architecture:**
- The test records each operation's input, answer, call time and return time. Times come from a logical clock, so real-time order is kept under `synctest`.
- Porcupine checks the history per key against a sequential model of one row: exists, gen, ver, name, score.
- An operation whose outcome is unknown returns at the end of the history and may or may not have taken effect. A nondeterministic model allows both.
- The fault injection is the one the M3 stress test uses, moved into a shared helper.

**Tech Stack:** Go 1.25 (`testing/synctest`), `github.com/anishathalye/porcupine` v1.3.1 (test only).

**Spec:** [design.md](../../design.md) — sections 9 (linearization points), 12.1 item 4 (Porcupine), 12.2 (M4: "No violations over long random runs"). Also [correctness-properties.md](../../correctness-properties.md) sections 3 (C1, C2, C3) and 4.1–4.2 (S1, S2), [CLAUDE.md](../../CLAUDE.md), and the M4 rows of [tla-mapping.md](../../tla-mapping.md) section 5.

## Global Constraints

- All paths are relative to `memory-authoritative/`. Run every command from there.
- Work on branch `memory-authoritative-m4` (from `main` at `a2ece99`), one commit per task. The user reviews and merges.
- Done means: `gofmt -l .` prints nothing, `go vet ./...` is clean, `go test -race ./...` passes.
- Dependencies: Porcupine is on CLAUDE.md's list. It is used only in `_test.go` files. No other new dependency.
- No production code changes in M4. If a test finds a violation, that is a bug to fix with systematic-debugging and a ledger ruling, not a reason to weaken the model.
- Test-only code lives in `_test.go` files. No flags, environment variables or build tags.
- **Deliberately not in M4:**
  - eviction and reloads (M5);
  - crash and second instance (M6);
  - PostgreSQL (M7);
  - the SDK and its retries (M8).

### Decisions in this plan that the design does not fix

1. **A logical clock for `Call` and `Return`.** One `atomic.Int64` shared by all workers. Take a tick just before calling the Store and just after it returns. Under `synctest`, `time.Now()` stands still while goroutines run, which would make almost every operation look concurrent. Atomic ticks keep "A returned before B was called" exact.
2. **Unknown outcomes** (`ErrOutcomeUnknown`) get `Return = math.MaxInt64` (12.1 item 4). The model is a `porcupine.NondeterministicModel`: such an operation has two next states, "took effect" and "did not".
3. **Operations that definitely did not take effect and tell nothing about the row are left out of the history.** That covers:
   - a ctx error while queued for `op` or for a load;
   - `db.ErrNotExecuted` (a load or a create/delete that did not execute);
   - `db.ErrCrossRow` and `db.ErrRejected`.

   Keeping them would only add operations that every state accepts. Answers that do say something stay in: `ErrNotFound` and `ErrAlreadyExists`.
4. **No SDK-like retries in this workload.** Each gen gets exactly one Create call, so `(nil, nil)` ("took effect earlier, deleted since") cannot be a correct answer and the model rejects it. Retries and holes 2 and 6 stay covered by M3's tests; the SDK is M8.
5. **The model tracks `Ver` too.** The rules are: Create gives ver 0, each Update adds 1, and a preloaded row keeps its stored ver. Get and Update answers carry ver, so an update applied twice or lost shows up even when values repeat.
6. **The initial rows come from a synthetic first operation per key.** `Model.Init` has no key, so each partition starts from `{loaded: false}`. An `init` operation at `Call -2, Return -1` sets that key's preloaded row, or absent. Half the keys are preloaded in fakedb (gen 101+, ver 3) and half start absent, so loads of both kinds happen.
7. **Fault injection is shared.** M3's `TestStressCreateDelete` builds its fault hook inline. Task 2 moves it, unchanged, into `faults_test.go`, so that M3's test and M4's test use the same faults.
8. **"Long random runs" means `-count`.** One run of `TestLinearizable` is short: 8 keys, 32 workers × 150 operations. The acceptance run is `go test -race -count=200 -run TestLinearizable`. There is no rounds flag.
9. **Porcupine's timeout.** Each check uses `CheckOperationsVerbose` with a 30s timeout. Both `Illegal` and `Unknown` fail the test: a check that timed out proved nothing. On failure the test writes Porcupine's HTML visualization to a temp file and logs its path.

## Review Focus

1. A Get or Update that a non-overlapping, earlier-acknowledged Update or Delete should have influenced, but that sees the older state, must be rejected. Real-time order must survive `synctest` (decision 1). Test: `TestModelRejectsStaleRead` (Task 1).
2. An unknown Create must be accepted whether it took effect or not, and an unknown Delete likewise. Test: `TestModelUnknownOutcomes` (Task 1).
3. An `Update` with `Change.Gen` of an older life must be answered `ErrNotFound` once the row is another life. The model must reject applying it. Test: `TestModelGenMismatchIsNotFound` (Task 1).
4. A check that times out (`Unknown`) must fail the test, not pass. Test: `TestCheckFailsOnTimeout` (Task 3), on the function `verdict` that `checkLinearizable` uses to decide.
5. The values recorded for an answer must be copied when the call returns, so a later Update cannot change what the history says. Rows are immutable (I2), but the recorder must store fields, not the `*Row`. Test: `TestRecorderCopiesAnswers` (Task 3).

---

### Task 1: The sequential model and its unit tests

**Files:**
- Modify: `go.mod`, `go.sum` (Porcupine v1.3.1)
- Create: `internal/store/model_test.go`

**Interfaces:**
- Consumes: `schema.Key`, `schema.Row`; `ErrNotFound`, `ErrAlreadyExists`, `ErrOutcomeUnknown`; M2/M3 test helpers `set`, `incr`.
- Produces:
  ```go
  // rowState is one key in the sequential model (correctness-properties.md 4.1, abs).
  type rowState struct {
      loaded           bool   // false only before the key's init operation
      exists           bool
      gen, ver         int64
      name             string // column 1
      score            int64  // column 2
  }
  type opKind uint8
  const ( opInit opKind = iota; opGet; opUpdate; opCreate; opDelete )
  type opInput struct {
      kind  opKind
      key   schema.Key
      init  rowState     // opInit
      cols  []ColChange  // opUpdate (OpSet on column 1 with a string, OpIncr on column 2 with an int64)
      gen   int64        // opUpdate: Change.Gen; opCreate: the new life's gen; opDelete: the gen argument
      name  string       // opCreate
      score int64        // opCreate
  }
  type opOutput struct {
      err     error    // nil, ErrNotFound, ErrAlreadyExists or ErrOutcomeUnknown; anything else is illegal
      row     rowState // the answer's row when err == nil and the operation returns one (Get, Update, Create)
      deleted bool     // Create answered (nil, nil): illegal in this workload (decision 4)
  }
  // rowModel partitions by key and steps one row; ErrOutcomeUnknown gives two next states.
  var rowModel = porcupine.NondeterministicModel{...}
  func stepRow(s rowState, in opInput, out opOutput) []rowState
  ```
  `stepRow` rules, one per operation:
  - **init**: `{loaded}` + `in.init`, from `!s.loaded` only.
  - **Get**: `exists` → `out.row` equals `s`. `!exists` → `ErrNotFound`. The state is unchanged.
  - **Update**:
    - `!exists`, or `in.gen != 0 && in.gen != s.gen` → `ErrNotFound`, state unchanged.
    - Otherwise apply the columns: SET name, INCR score. Then `ver+1`, and `out.row` must equal the new state.
  - **Create**:
    - `nil`: `!exists`, and `out.row == {exists, in.gen, ver 0, in.name, in.score}`, which becomes the new state.
    - `ErrAlreadyExists`: `exists`, state unchanged.
    - `ErrOutcomeUnknown`: `!exists` → the new life or unchanged; `exists` → unchanged.
  - **Delete**:
    - `nil`: `exists && (in.gen == 0 || in.gen == s.gen)` → absent.
    - `ErrNotFound`: the negation of that, state unchanged.
    - `ErrOutcomeUnknown`: when the condition holds → absent or unchanged; otherwise unchanged.
  - Anything else → no next state.

  `DescribeOperation` and `DescribeState` give short strings for the visualization.

- [ ] **Step 1: Create the branch and add the dependency**

```bash
git checkout main && git checkout -b memory-authoritative-m4
go get github.com/anishathalye/porcupine@v1.3.1
```

- [ ] **Step 2: Write the failing tests** (`model_test.go`)

`op(kind, call, ret, in, out)` builds a `porcupine.Operation`. `check(t, ops) porcupine.CheckResult` runs `porcupine.CheckOperationsVerbose(rowModel.ToModel(), ops, 30*time.Second)`. Key `"k"`; `r(gen, ver, name, score)` is an existing `rowState`.

```go
func TestModelAcceptsSequentialHistory(t *testing.T)
	// init absent; Create gen 7 -> row(7,0,"a",10); Update set name "b" -> row(7,1,"b",10);
	// Update incr 5 -> row(7,2,"b",15); Get -> row(7,2,"b",15); Delete 7 -> nil; Get -> ErrNotFound: Ok
func TestModelRejectsStaleRead(t *testing.T)
	// init row(101,3,"a",10); Update incr 1 [1,2] -> row(101,4,"a",11); Get [3,4] -> row(101,3,"a",10): Illegal
	// the same Get at [1,2] (overlapping the update): Ok
func TestModelRejectsLostIncrement(t *testing.T)
	// two concurrent Update incr 1 both answered row(101,4,"a",11): Illegal
func TestModelUnknownOutcomes(t *testing.T)
	// init absent; Create gen 7 [1, MaxInt64] -> ErrOutcomeUnknown; then Get [3,4] -> ErrNotFound: Ok;
	// Get [3,4] -> row(7,0,"a",10): Ok; Get [3,4] -> row(8,0,"a",10): Illegal
	// init row(101,3,"a",10); Delete [1,MaxInt64] -> unknown; Get [3,4] -> NotFound: Ok; -> row(101,3,...): Ok
func TestModelGenMismatchIsNotFound(t *testing.T)
	// init row(101,3,..); Update{gen 99} -> ErrNotFound: Ok; Update{gen 99} -> row(101,4,..): Illegal;
	// Delete gen 99 -> nil: Illegal
func TestModelRejectsCreateOverExisting(t *testing.T)
	// init row(101,3,..); Create gen 7 -> nil row(7,0,..): Illegal; -> ErrAlreadyExists: Ok; -> deleted: Illegal
func TestModelPartitionsByKey(t *testing.T)
	// two keys, interleaved histories, each linearizable on its own: Ok; and Partition returns 2 partitions
```

- [ ] **Step 3: Run to verify they fail**

Run: `go test ./internal/store/ -run TestModel`
Expected: FAIL to compile: `undefined: rowModel`, `undefined: rowState`.

- [ ] **Step 4: Implement `model_test.go`** with the interfaces above. `rowState` is comparable, so `Equal` is `==`.

- [ ] **Step 5: Run**

Run: `go test -race ./internal/store/ -run TestModel`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/store/model_test.go && git commit -m "M4: sequential row model for Porcupine, with unit tests"
```

---

### Task 2: Move the fault injection of the M3 stress test into a shared helper

**Files:**
- Create: `internal/store/faults_test.go`
- Modify: `internal/store/stress_test.go` (`TestStressCreateDelete` uses the helper; its assertions do not change)

**Interfaces:**
- Consumes: fakedb `Hook`, `Stmt`, `Ambiguous`, `SetCrossRow`, `Claimed`, `Deleted`; test steps `lost`, `fail`.
- Produces:
  ```go
  // faults injects the M3 stress faults into d while on: on Insert/Update/Delete 55% normal, 10% lost,
  // 10% ambiguous + late Exec after up to 100ms, 10% ambiguous never executed, 15% not executed, after a
  // random 0-500µs; Select and GenClaimed 5% not executed; cross-row flips on a random key every 1ms.
  type faults struct {
      on   atomic.Bool
      late sync.WaitGroup
      // witnesses (tla-mapping.md section 8)
      ambThenNotExecuted, notExecutedAfterFence, lateInsertOfDeleted, retryFindsOtherLife atomic.Int64
      ...
  }
  func startFaults(d *fakedb.DB, keys []schema.Key) *faults
  // stop turns the faults off, stops the flipper, clears every cross-row switch and waits for late
  // statements.
  func (f *faults) stop()
  ```

- [ ] **Step 1: Move the code.** The hook, the flipper and the witness counters move from `TestStressCreateDelete` into `faults_test.go`. The test calls `f := startFaults(d, ks)` and, where it used to stop them, `f.stop()`. Everything it asserts stays as it is.

- [ ] **Step 2: Run the M3 stress test three times**

Run: `go test -race -count=3 -run 'TestStressCreateDelete' -v ./internal/store/`
Expected: PASS. Every non-vacuity counter in the logged map is > 0, as before.

- [ ] **Step 3: Commit**

```bash
git add internal/store && git commit -m "M4: share the M3 stress fault injection"
```

---

### Task 3: Record histories and check them with Porcupine

**Files:**
- Create: `internal/store/linearizability_test.go`

**Interfaces:**
- Consumes: Task 1 `rowModel`, `opInput`, `opOutput`, `rowState`; Task 2 `startFaults`; `startFlusher`, `drain`, `checkInvariants`.
- Produces:
  ```go
  // recorder of a history; safe for concurrent use.
  type history struct { clock atomic.Int64; mu sync.Mutex; ops []porcupine.Operation }
  func (h *history) call() int64                               // clock tick
  func (h *history) add(client int, in opInput, call int64, out opOutput) // ret = tick, or MaxInt64 if unknown
  func answer(r *schema.Row, err error) opOutput               // copies the fields of r (Review Focus 5)
  // checkLinearizable fails t on Illegal or Unknown and writes the visualization to a temp file.
  func checkLinearizable(t *testing.T, ops []porcupine.Operation, timeout time.Duration)
  func verdict(res porcupine.CheckResult) error                 // nil only for Ok
  ```

- [ ] **Step 1: Write the tests**

```go
// Acceptance (design.md 12.2 M4): random concurrent operations under injected faults are linearizable per key.
func TestLinearizable(t *testing.T) {
	// synctest. 8 keys: keys 0-3 preloaded in fakedb (gen 101+i, ver 3, name "a", score 10), keys 4-7 absent;
	// one opInit per key at Call -2, Return -1.
	// startFlusher; f := startFaults(d, keys); 32 workers × 150 ops, each on a random key:
	//   30% Get; 30% Update (50% set(1, one of "a".."e"), 50% incr(2, -5..5); Change.Gen = the gen this
	//   worker last saw for the key, or 0, each half the time); 20% Create(fresh gen, name "c", score 0-9);
	//   20% Delete(gen last seen or 0, each half the time)
	//   client ctx: 5ms on 20% of Create/Delete, else 5s
	//   record per decision 3: ErrOutcomeUnknown with Return MaxInt64; drop the no-effect errors; any other
	//   error is t.Error
	// end: wg.Wait; f.stop(); s.inflight.Wait(); stop the flusher; drain; checkInvariants
	// checkLinearizable(t, h.ops, 30*time.Second)
	// non-vacuity, each > 0: unknown outcomes; Gets that returned a row; Gets answered ErrNotFound;
	//   Updates answered ErrNotFound because of Change.Gen; successful Creates; successful Deletes;
	//   pairs of overlapping operations on one key (count while building: an op whose Call is before
	//   another's Return on the same key)
}

// Review Focus 4.
func TestCheckFailsOnTimeout(t *testing.T) {
	// verdict(porcupine.Ok) == nil; verdict(porcupine.Illegal) != nil; verdict(porcupine.Unknown) != nil
}

// Review Focus 5.
func TestRecorderCopiesAnswers(t *testing.T) {
	// answer(row, nil) for a *schema.Row; then a new Update of that key; the recorded rowState is unchanged
}
```

`checkLinearizable` fails the test exactly when `verdict` returns an error: `func verdict(res porcupine.CheckResult) error`. Only `Ok` passes; a timeout (`Unknown`) proved nothing.

- [ ] **Step 2: Run**

Run: `go test -race -count=3 -v -run 'TestLinearizable|TestCheckFailsOnTimeout|TestRecorderCopiesAnswers' ./internal/store/`
Expected: PASS; one run of `TestLinearizable` under 10s. If Porcupine reports Illegal, the Store is wrong (systematic-debugging; the visualization path is in the log), not the model. Shrink only the workload if checks time out.

- [ ] **Step 3: Update `tla-mapping.md` section 5.** In the rows `NoViolation: Get not linearizable` and `MemMatchesAbs`, name `TestLinearizable` (and `model_test.go` for the model).

- [ ] **Step 4: Run the whole suite**

Run: `gofmt -l . && go vet ./... && go test -race ./...`
Expected: clean; all packages PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/linearizability_test.go tla-mapping.md && git commit -m "M4: Porcupine linearizability test under injected faults"
```

---

## After the tasks (CLAUDE.md workflow, steps 3-6)

1. `/code-review`, then fix. Then `/simplify`, then run the suite again.
2. The acceptance run (decision 8): `go test -race -count=200 -run TestLinearizable ./internal/store/`. It must show no violations, and I report the wall time.
3. Show that Porcupine actually catches bugs. For each planted bug: apply a temporary patch, show that `TestLinearizable` fails (at most 5 runs, `-count=5`), and revert.

| Planted bug | Temporary patch | What Porcupine should see |
|---|---|---|
| Increment outside `mu` | read `old` before `mu.Lock`, compute from it | two increments answered with the same value (lost update) |
| Create publishes before the DB | `snap.Store` of the new row before `insertUntilSure`, kept on failure | a Get sees a life whose create failed or is still unknown |
| Delete publishes before the DB | the tombstone before `deleteUntilSure`, kept on failure | a Get answers NotFound for a row that still exists |
| `Change.Gen` ignored | drop the gen check in `Update` | an Update of an older life applied to the new one |
| `LoadNoRecheck` | `ensureLoadedLocked` skips the `snap` re-check | a load overwrites a row created or updated meanwhile |

If one of these is not caught within 5 runs, I report it. Whether to change the workload to reach it is then your decision.
