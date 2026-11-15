# TLA+ to Go mapping

How the model in [tla/RowCache.tla](tla/RowCache.tla) corresponds to the Go implementation described in
[design.md](design.md). Use it in three ways:

- **While writing code**: each operation is one Go function (as in design.md section 7), not one function per TLA
  action. Mark where each action's step begins with a comment `// TLA: <Action>`, so a reviewer can line the code up
  with the model.
- **While writing tests**: section 5 says where each property is checked, section 6 which test must catch each planted
  bug, section 7 how each design hole is reproduced, section 8 which counters prove a test is not vacuous.
- **Before "simplifying" anything**: if a check or branch appears in section 6, a TLA mutation proved it is needed.
  The two checks in 6.2 are not needed by the model and are kept anyway.

Names in the Go column are the ones in the design.md skeletons. If the implementation renames something, update this
file in the same commit. The properties are stated in [alternative-design.md](alternative-design.md) 11.2; the TLC
results are in its 11.3 and in design.md 12.3.

---

## 1. Where the model and the code differ on purpose

The table "Where the code differs from the model on purpose" in design.md 12.3 is the list. Do not "fix" the code to
match the model, or the model to match the code, without asking. Three smaller differences that only matter when
reading the two side by side:

| Model | Go | Why it is safe |
|---|---|---|
| `B_1` loads, then runs `B_1` again on the loaded snapshot | `ensureLoadedLocked` returns the row and the function continues | Same steps, `op` held throughout |
| `B_1` reads `dedup` directly when a create finds another life | `db.GenClaimed`, a read; on error the create answers `ErrNotExecuted` | Nothing was sent, so nothing is in flight |
| Every `"zero"` result is a violation (the live instance always holds the epoch) | `ErrNoRows` from Update / Delete → `unexpectedLocked`, which checks the owner first; `ErrNotOwner` from `InsertDedup` → `lostOwnership` | design.md 12.3, row "Superseded instance notices" |

---

## 2. Variables

### 2.1 Per-entry state (`internal/store`)

| TLA | Go | Notes |
|---|---|---|
| `mapE` | `shard.m[key]` | `0` = no entry in the map |
| `snap[e]` | `entry.snap` | `NIL` = `nil`, `ABSENT` = the `absent` sentinel, `RowR(g,v,vl)` = `*Row{Gen: g, Ver: v, Vals: vl}` |
| `dead[e]` | `entry.dead` | |
| `opH[e]` | `entry.op` (buffered chan, cap 1) | the holder is whoever sent into it: `CL(c)` = `load`, `BG(c)` = the `runSync` goroutine, `EV` = `tryEvict`. `ZB` never holds `op` |
| `nextEntry`, `eEp` | none | model bookkeeping for "a new entry after eviction or crash" |

### 2.2 Clients and background goroutines

| TLA | Go | Notes |
|---|---|---|
| `pc[c]`, `cop[c]`, `ce[c]`, `cret[c]` | program counter, arguments and local `e` of a `Get`/`Update`/`Create`/`Delete` call | `cret` = "go back to the top of the loop after `load`" |
| `ops[c]` | the test driver | |
| `lastSeen[c]` | the gen a caller remembers and passes as `Change.Gen` or the `gen` of `Delete` | |
| `cwait[c]` | `runSync`'s `select` is still waiting on `done` | `FALSE` = the caller got `ErrOutcomeUnknown` |
| `bpc[c]`, `be[c]`, `bop[c]` | the `runSync` goroutine running `updateLocked` / `createLocked` / `deleteLocked` on `e` with its arguments | |
| `bres[c]` | the value sent on `done` | section 4.2 |
| `bgr[c]` | the gen the statement was built with: `old.Gen` in `updateLocked`, `r.Gen` in `deleteLocked`, `nr.Gen` in `createLocked` | |
| `bver[c]` | `old.Ver` in `updateLocked` | compared after the fence |
| `vpc`, `ve` | program counter and entry of `tryEvict` | |
| `unkGen[c]`, `retry[c]` | SDK (M8): gen of a create whose outcome is unknown; "this call is a retry" | |

### 2.3 DB and environment (`fakedb`, real PG)

| TLA | Go | Notes |
|---|---|---|
| `db` | the business row: `fakedb` state / PG row (`ex`, `gen`, `ver`, `vals`) | |
| `dedup` | `row_service_gen_log` rows of this key | |
| `dbEpoch` | `row_service_epoch.epoch` | |
| `epoch` | the epoch this Store took over at startup (7.8 step 2) | the model bumps it at `Crash`; Go takes it when the new Store starts |
| `blocked` | `fakedb`'s per-key rejection switch, flipped by the test | |
| `net` | statements sent and not yet executed: `fakedb`'s pending statements, each on a session | `src` = the session's sender; `live = FALSE`: the caller has stopped waiting; `pre`: was in flight when the epoch was bumped |
| `wait[p]` | a write call of `db.DB` is in progress for the `runSync` goroutine `p` | |
| `resp[p]` | that call's return value | section 4.1 |
| `up` | the Store is running | `Crash` = the test discards the Store |
| `stmtTO`, `drops`, `cto`, `crashes`, `flips`, `qto`, `zsent`, `nextGen`, `nextVal` | `fakedb` fault settings and the test driver | |

### 2.4 Ghost variables (the test harness, not the service)

| TLA | Go |
|---|---|
| `abs`, `absKnown`, `seen` | Porcupine's sequential model and the recorded history (M4) |
| `preCrash`, `preAlt` | the crash-recovery test's two acceptable states per key: as at the crash, or with the one write in progress applied (M6) |
| `deletedGens`, `insSent`, `delSent`, `failedGens`, `okUpd`, `failedUpd`, `appliedUpd` | history recorded by the test from the answers, and `fakedb`'s log of statements that took effect |
| `violation` | `t.Fatal` in tests; `ErrInternal` in the service |

---

## 3. Actions

Each row is one atomic step of the model. In Go a step is a stretch of one function; mark its start with
`// TLA: <Action>`.

| TLA action | Go (design.md section) | The step |
|---|---|---|
| `ClientStart` | test driver / SDK call | |
| `Goc` | `getOrCreate` (7.1) | called by `Get` and `lockOpLive` |
| `G_read` | `Get` (7.2) | `e.snap.Load()`: linearization point; `nil` → `load`, then back to the top. No `dead` check (dead protocol rule 3) |
| `L_lock` | `load` (7.2) | `e.lockOp(ctx)` |
| `L_load` | `load` → `ensureLoadedLocked` (7.2) | `dead` check; re-check `snap`; `db.Select`; `snap.Store`; deferred `unlockOp` |
| `X_lock` | `lockOpLive` (7.1), then `runSync` starts the goroutine | `dead` → `unlockOp`, look up again |
| `X_qtimeout` | `lockOpLive` returns `ctx.Err()` | timed out while queued for `op`: definitely not executed |
| `X_wait` | `runSync`'s `select` | `done` → result; `ctx.Done()` → `ErrOutcomeUnknown`, the goroutine keeps running |
| `B_1` | `updateLocked` / `createLocked` / `deleteLocked` up to the DB write (7.4-7.6) | `ensureLoadedLocked`; decide from the snapshot: create finds a row → same gen: return it; `db.GenClaimed` → `(nil, nil)`; else `ErrAlreadyExists`. Update / delete finds absent or another gen → `ErrNotFound`. Else send one statement: `db.InsertDedup`, `db.Update`, `db.Delete` |
| `B_2` | the `switch` on the write's result, and the fence loop of `fenceAndReadLocked` (7.3) | success → `snap.Store(got)` (linearization point); section 4.1 for the rest; ambiguous → `db.FenceSession` until confirmed |
| `B_3` | the read loop of `fenceAndReadLocked`, then the decision in the `*Locked` function (7.3) | `db.Select`, `db.CheckOwner`, `snap.Store`; create: `now.Gen == nr.Gen` or `db.GenClaimed`; update: same gen and `now.Ver == old.Ver+1`; delete: absent or another gen |
| `FinishOp` (helper of `B_1`-`B_3`) | the end of the `runSync` goroutine (7.1) | `e.unlockOp()`, then `done <- result`: `op` is free by the time the caller has its answer. Not for `errAbandoned` |
| `V_evict` | `tryEvict` (7.7) | `tryLockOp`; `!dead && eligible` → `dead = true` |
| `V_remove` | `tryEvict` | under `shard.mu`: delete from the map; deferred `unlockOp` |
| `DbExec`, `DbTimeout`, `DbDrop` | `fakedb` (and real PG) | execute (response maybe lost: ambiguous); time out, stay pending and maybe execute later (ambiguous); definitely not execute |
| `EnvFlip` | the test flips `fakedb`'s rejection switch | |
| `Crash` | the crash-recovery test discards the Store (M6) | |
| `ZombieSend` | second-instance test: the old Store keeps running (M6) | |
| `Bump` | startup step 2: epoch takeover (7.8) | |
| `FenceDrop` | startup step 3: terminate the previous sessions (7.8) | |
| `Restart` | startup steps 4-5 (7.8) | |
| `Done` | none | end of a test run |

The model's fence (`B_2`) drops the pending statements of the session. `fakedb.FenceSession` executes each now or
drops it; executing it now is the same as a `DbExec` just before the fence, which the model allows.

### 3.1 Gaps: where failpoints go

A gap is the space between two steps of one goroutine, where another goroutine can run (design.md section 10).
Failpoint hooks are nil in production, one mechanism for all of them. A gap at a DB call is reached through `fakedb`'s
statement hook (it runs the inserted operation before or after the statement executes); the others need a hook in
`store`.

| Gap | Between | What the failpoint test inserts | Exercises | From |
|---|---|---|---|---|
| write-in-flight | `B_1` sends → `B_2` result | a Get, and an Update queued for `op` | `W_GetDuringWrite`; the Get sees the old row, the Update waits | M2 |
| amb-pending | ambiguous result → fence | the pending statement executes, or is dropped by the fence | `W_AmbStmtInFlight`, `W_FenceThenApplied`, `W_FenceThenNotApplied`, `W_CreateAmb`, `W_DeleteAmb`; mutants `NoConnFence`, `ReleaseOnAmbiguous`, `AmbIsFailure`, `UpdateNoVerCompare` | M3 |
| fence-retry | `FenceSession` not confirmed → next attempt | the pending statement executes between the attempts | `op` stays held; the answer follows the final read | M3 |
| client-gave-up | `X_wait` times out → the goroutine settles | another request for the key, queued for `op` | `W_ClientGaveUp`; it runs only after the write settled | M3 |
| lookup-op | `Goc` → `X_lock` / `L_lock` | an eviction of that entry | dead protocol rule 2 | M5 |
| evict-dead | `V_evict` → `V_remove` | an Update and a Get for the key | mutant `EvictNoDead` | M5 |
| evict-sample | sampling → `tryEvict` | a Get / Update | not a model step: the model picks and marks atomically; the `eligible` re-check makes Go equivalent | M5 |
| superseded-fence | old Store's ambiguous update → its read after the fence | a new Store starts (its startup fence drops the statement) and updates the row | not in the model: the owner check after the read (design.md 12.3) | M6 |
| shutdown-settling | fence or read loop → `bgCtx` cancelled | shutdown | `errAbandoned` keeps `op`; the next start settles the statement | M6 |

---

## 4. Results and answers

### 4.1 DB results

| TLA `resp` | Meaning | Go (design.md 5.2, 8.4) | Handled by |
|---|---|---|---|
| `"ok"` | executed; `row` is what `RETURNING` gave | `err == nil`, the returned `*Row` (Delete: none) | `snap.Store(got)` |
| `"amb"` | may have executed, or may still | `db.IsAmbiguous(err)`; session `db.SessionOf(err)` | `fenceAndReadLocked` |
| `"fail"` | definitely not executed | `db.ErrNotExecuted` | passed through |
| `"rej"` | rejected by a constraint, rolled back | `db.ErrRejected` | passed through |
| `"claimed"` | this gen is already in the dedup table | `db.ErrGenClaimed` | `(nil, nil)` |
| `"conflict"` | dedup record new, business row exists | `db.ErrDuplicate` | `unexpectedLocked` |
| `"zero"` | no row matched, or the epoch did not match | `db.ErrNoRows` (Update, Delete); `db.ErrNotOwner` (InsertDedup) | `unexpectedLocked`; `lostOwnership` |
| `"dup"` | plain INSERT conflict, only without `FixDedup` | none: Go always uses `InsertDedup` | |

### 4.2 Answers (`bres`, and what the SDK reports)

| TLA | Go | Status (7.9) |
|---|---|---|
| `"ok"` | `nil` error (Create may return `(nil, nil)`: success with no row) | `OK` |
| `"exists"` | `ErrAlreadyExists` | `ALREADY_EXISTS` |
| `"notfound"` | `ErrNotFound` | `NOT_FOUND` |
| `"rejected"` | `db.ErrRejected` | `INVALID_ARGUMENT` |
| `"fail"` | `db.ErrNotExecuted`, including "not applied after the fence" | `UNAVAILABLE` |
| `"internal"` | `ErrInternal` | `INTERNAL` |
| `"deadline"` (`X_qtimeout`) | `ctx.Err()` from `lockOpLive` | `DEADLINE_EXCEEDED` |
| `X_wait` timeout, `cwait = FALSE` | `ErrOutcomeUnknown` | `OUTCOME_UNKNOWN` |

`Failure(res)` in the model is every answer above except `"ok"`, `"internal"` and the `X_wait` timeout.

---

## 5. Properties: where each is checked in Go

`checkInvariants` below means a test-only helper in `internal/store` (in a `_test.go` file) that runs at a quiescent
point (no operation running), takes each entry's `op` with `tryLockOp`, and compares with `fakedb`.

| TLA property | Go check | From |
|---|---|---|
| `NoViolation`: Get not linearizable | Porcupine: the linearizability test, per key | M4 |
| `NoViolation`: post-crash state implausible | crash-recovery test: the first state loaded per key is `preCrash` or `preAlt` (section 2.4); every acknowledged write is there | M6 |
| `NoViolation`: unexpected result | no test may ever see `ErrInternal` | M2 |
| `MemMatchesAbs` | Porcupine, on the rows Get, Update and Create answer | M4 |
| `CacheCoherent` (I4) | `checkInvariants`: every loaded entry in the map equals its `fakedb` row exactly (gen, ver, values), with the normalizer on | M2 |
| `StmtOnlyUnderOp` (I5) | `checkInvariants`: `fakedb` has no pending statement of the current Store | M3 |
| `OneWriterPerKey` (I5) | `fakedb` asserts: per key, at most one write call of the current Store in progress | M2 |
| `DbMatchesAbs` | end of every stress test: memory equals the DB for every key; in the second-instance test, the new Store's cache equals the DB | M2, M6 |
| `UpdateAnswerSound` | each update writes a value no other update writes; an update answered OK is in `fakedb`'s log of applied statements; one answered with any failure (not `ErrOutcomeUnknown`) is not, checked after the pending statements have drained; Porcupine (12.1 item 4) | M3, M4 |
| `CreateFailureIsFinal` | history: a create answered with a failure never has its gen in `fakedb`'s dedup table, checked after draining | M3 |
| `NoResurrection` | history: once a delete of gen g returned success, g never appears again in `fakedb` or in a Get; `fakedb` asserts no row is inserted for a (key, gen) whose row was deleted | M3 |
| `CreateDurable` | end of every stress test: a confirmed create still exists unless a delete of it was sent; crash-recovery test | M3, M6 |
| `LiveEntryInMap` (I1) | `checkInvariants`: entries in the map are not dead; the evict-dead failpoint test | M5 |
| `DbValsBelongToGen` | stress updates carry `Change.Gen` and write values tagged with that gen; `fakedb` asserts every applied write keeps the tags equal to the row's gen | M3 |
| `FencedRestart` (I13) | crash-recovery test: once the new Store's startup returns, no pending statement of an old session can still execute | M6 |
| `DbVerMonotonic` | holds by construction in `fakedb`, which computes `ver = ver + 1` itself; against PG, the SQL of 8.3 (M7) | M2, M7 |
| `EventuallyResponds` | every test operation returns before its deadline | M2 |
| `BackgroundSettles` | `s.inflight.Wait()` returns once faults stop | M3 |
| `NoLockLeak` | end of every stress test: `tryLockOp` succeeds on every entry | M2 |

---

## 6. Planted bugs

### 6.1 Mutations: which test must catch each

Each mutation breaks one rule of the design. In the milestone that writes the code involved, break the rule with a
temporary patch, run the tests, confirm at least one fails, and revert. Record the result in the milestone summary.
Do not add flags or build tags for this.

| Mutation | The rule in Go | Must be caught by | From |
|---|---|---|---|
| `NoConnFence` | `fenceAndReadLocked` reads only after `FenceSession` confirmed | amb-pending failpoint: the statement lands after the read; `checkInvariants` | M3 |
| `ReleaseOnAmbiguous` | an ambiguous result goes to `fenceAndReadLocked` with `op` held | amb-pending failpoint: the statement lands after `op` is released; `checkInvariants` | M3 |
| `AmbIsFailure` | after the fence the row is read and the answer decided from it | amb-pending failpoint where the statement executed: the answer must be OK and the cache equal the DB | M3 |
| `UpdateNoVerCompare` | `updateLocked` requires `now.Ver == old.Ver+1` | amb-pending failpoint where the fence dropped the update: the answer must be "not executed" (`UpdateAnswerSound`) | M3 |
| `DedupDeletedWithRow` | `Delete` (in `fakedb`, and the SQL of 8.3) keeps the dedup record | hole 2 test | M3 |
| `LeakOpOnNotFound` | `runSync` releases `op` on every return of `fn` | update / delete of a missing key, then another request for the key; `NoLockLeak` end check | M3 |
| `EvictNoDead` | `tryEvict` sets `dead` before removing the entry | evict-dead failpoint | M5 |

Two rules the model cannot plant, with the test that must catch each (design.md 12.2):

| Rule in Go | Must be caught by | From |
|---|---|---|
| the owner check after the read in `fenceAndReadLocked` | superseded-fence failpoint: the old Store answers `OUTCOME_UNKNOWN`, and OK without the check | M6 |
| `errAbandoned` keeps `op` | shutdown-settling failpoint: the key stays locked until exit; the next start settles the statement | M6 |

### 6.2 Kept although the model does not need them

The model passes with either removed (`suite.py claims`: `redundant_NoGenCheck`, `redundant_LoadNoRecheck`). No test is
expected to fail without them. Keep them (design.md 0.4); do not "simplify" them away.

| Check | Why it stays |
|---|---|
| `AND gen = ?` in UPDATE and DELETE | a second line of defence if an invariant is ever broken |
| `ensureLoadedLocked` re-checks `snap` after `op` is taken | saves a query for requests queued behind a load |

---

## 7. Design holes: reproduction tests

Build each test from the model's counterexample: `cd tla && python3 suite.py claims --only <job>`, then
`python3 trace.py < out/suite/<job>.out`. Each test must fail with its fix removed and pass with it.

| Hole (design.md 13.2) | Fix in Go | Test (claims job) | From |
|---|---|---|---|
| 1 late INSERT resurrects a deleted row | none needed: I5 | none of its own; the amb-pending tests cover I5 (`hole1_late_insert_no_dedup`) | — |
| 2 SDK retry resurrects a deleted row | `InsertDedup` writes row + dedup in one statement; `Delete` keeps dedup; create loads first | create ambiguous and applied, caller got `ErrOutcomeUnknown`; delete; retry with the same gen → success with no row, the row stays absent (`hole2_retry_no_dedup`) | M3 |
| 3 old process's statement lands after restart | startup fence (7.8 step 3) | crash with an UPDATE pending; new Store; the UPDATE never lands after the load (`hole3_no_startup_fence`) | M6 |
| 4 ambiguous, then "not executed", taken as final | none needed | none (`hole4_amb_then_not_executed`) | — |
| 5 retry rejected because of another row | none needed | the stress test with the rejection switch flipping (`hole5_cross_row`, `ext_crossrow`) | M3 |
| 6 retry finds another life | `db.GenClaimed` in `createLocked` | create A ambiguous, lands; delete; create B; retry A → success with no row (`hole6_no_dedup_lookup`) | M3 |
| 7 SDK reports "not executed" | the SDK keeps the outcome unknown | retry times out queued behind its own first attempt → the caller never sees `DEADLINE_EXCEEDED` (`hole7_sdk_rule_off`) | M8 |
| 8 second instance | owner epoch on every write | second-instance variant of the crash-recovery test (`hole8_no_epoch`) | M6 |

---

## 8. Non-vacuity: proving the tests reach the hard cases

The model's `W_*` witnesses prove TLC reached each situation. The Go equivalent: `fakedb` and the test driver count
these events, and each stress test asserts the relevant counters are above zero.

| Witness | Counter |
|---|---|
| `W_Delete`, `W_Recreate` | deletes that succeeded; creates of a key after its delete |
| `W_Update` | updates answered OK |
| `W_Evict`, `W_Reload` | evictions; loads of a key that was evicted |
| `W_AmbStmtInFlight` | ambiguous results whose statement was still pending and then executed |
| `W_FenceThenApplied`, `W_FenceThenNotApplied` | updates found applied / not applied after the fence |
| `W_CreateAmb`, `W_DeleteAmb` | fences after a create / a delete |
| `W_NotExecuted` | writes answered `db.ErrNotExecuted` by the DB |
| `W_DedupClaimed`, `W_RetryOfDeletedGen` | `ErrGenClaimed` results; create retries with a gen that was deleted |
| `W_GetDuringWrite` | Gets answered while a write of the same key was in flight |
| `W_ClientGaveUp`, `W_QueueTimeout` | `ErrOutcomeUnknown` answers; timeouts while queued for `op` |
| `W_Rejected`, `W_AmbWhileBlocked` | `db.ErrRejected` results; ambiguous results while the rejection switch was on |
| `W_RetryFindsOtherLife` | create retries that found another life with their gen claimed |
| `W_RetryQueuedBehindSelf` | SDK retries queued behind their own first attempt (M8) |
| `W_Crash`, `W_CrashWithStmtInFlight`, `W_CrashWithWritePending` | crash-recovery runs; with a statement pending; with a write in progress |
| `W_ZombieStmt`, `W_ZombieStoppedByEpoch` | writes from the old Store; of those, ones the epoch stopped |

Paths the model does not have get counters too: fences not confirmed, reads after a fence that failed, `GenClaimed`
and `CheckOwner` failures (the fakedb faults of design.md 12.1 item 2).
