# TLA+ to Go mapping

How the model in [tla/RowService.tla](tla/RowService.tla) corresponds to the Go implementation described in
[design.md](design.md). Use it in three ways:

- **While writing code**: each operation is one Go function (as in design.md section 7), not one function per TLA
  action. Mark where each action's step begins with a comment `// TLA: <Action>`, so a reviewer can line the code up
  with the model.
- **While writing tests**: section 5 says where each invariant is checked, section 6 which test must catch each planted
  bug, section 7 how each design hole is reproduced, section 8 which counters prove a test is not vacuous.
- **Before "simplifying" anything**: if a check or branch appears in section 6, a TLA mutation proved it is needed.

Names in the Go column are the ones in the design.md skeletons. If the implementation renames something, update this
file in the same commit.

---

## 1. Where the model and the code differ on purpose

The model is a small abstraction. These differences are known and fine. Do not "fix" the code to match the model, or
the model to match the code, without asking.

| Model | Go | Why it is safe |
|---|---|---|
| One key | Many keys, shards, tables | All correctness issues are per key (RowService.tla header) |
| `Goc` is one atomic step | `getOrCreate`: RLock lookup, then Lock + double check | Equivalent: the double check makes it linearizable |
| Update sets one column | `Change` with several columns, SET and INCR | Same path, same `mu` critical section (I12) |
| Flush sends one item per round | `flushShard` writes a batch in one transaction | Each item still holds its own `op`; per-key behaviour is the same |
| `"rej"` only means "rejected because of another row" | Rejection by the row's own data (CHECK, NOT NULL, length) is the `default` branch of the until-sure loops | Same parameters on every attempt, so the earlier attempt was rejected too (7.4) |
| Not modeled: poison rows (7.9), backpressure (11.1), `lastLoadErr` cache, backoff, `nowSec`, `dirtySince`, metrics | Present | They only return errors earlier or change timing; none may change which branch a step takes |
| Eviction may happen at any moment | TTL / capacity policy, sampling, `eligible` re-check | The model covers every timing; policy only narrows it |
| Superseded instance detects the new epoch: not a step | 0-row UPDATE/DELETE triggers an epoch check (7.6); periodic check (7.8) | The model checks the effect (`EpochOk`) of every statement; stopping is liveness, not safety |
| SDK behaviour (`retry`, `unkGen`, `NotFinal`, `SdkAnswer`) is part of the client process | `client/` package (M8) | |
| No TCP server, no protocol | `internal/server` (M8) | |

---

## 2. Variables

### 2.1 Per-entry state (`internal/store`)

| TLA | Go | Notes |
|---|---|---|
| `mapE` | `shard.m[key]` | `0` = no entry in the map |
| `snap[e]` | `entry.snap` | `NIL` = `nil`, `ABSENT` = the `absent` sentinel, `RowR(g,v,vl)` = `*Row{Gen: g, Ver: v, Vals: vl}` |
| `dirty[e]` | `entry.dirty` | set of columns becomes a `uint64` bitmap (`colBit`) |
| `queued[e]` | `entry.queued` | |
| `dead[e]` | `entry.dead` | |
| `opH[e]` | `entry.op` (buffered chan, cap 1) | the holder is whoever sent into it: `CL(c)` = `load`, `BG(c)` = the `runSync` goroutine, `FL` = `flushShard`, `EV` = `tryEvict` |
| `queue` | `shard.queue` | |
| `nextEntry`, `eEp` | none | model bookkeeping for "a new entry after eviction or crash" |

### 2.2 Clients and background goroutines

| TLA | Go | Notes |
|---|---|---|
| `pc[c]`, `cop[c]`, `ce[c]`, `cret[c]` | program counter, arguments and local `e` of a `Get`/`Update`/`Create`/`Delete` call | `cret` = "go back to the top of the loop after `load`" |
| `lastSeen[c]` | the gen a caller remembers and passes as `Change.Gen` or the `gen` of `Delete` | |
| `cwait[c]` | `runSync`'s `select` is still waiting on `done` | `FALSE` = the caller got `ErrOutcomeUnknown` |
| `bpc[c]`, `be[c]`, `bop[c]` | the `runSync` goroutine running `createLocked` / `deleteLocked` on `e` with its arguments | |
| `bres[c]` | the value sent on `done` | |
| `bgr[c]` | `r.Gen` read in `deleteLocked` and passed to `deleteUntilSure` | |
| `bamb[c]` | `unsure` in `insertUntilSure` / `deleteUntilSure` | never reset inside one operation |
| `bfl[c]` | `len(inflight) > 0` | emptied by `FenceSessions` |
| `fbatch`, `fretry`, `fitem` | `batch`, `retry`, `items` in `flushShard` | `fitem.r`, `fitem.m` = `flushItem.row`, `flushItem.mask` |
| `fpc`, `vpc`, `ve` | program counters of `flushShard` and `tryEvict` | |
| `unkGen[c]`, `retry[c]` | SDK (M8): gen of a create whose outcome is unknown; "this call is a retry" | |

### 2.3 DB and environment (`fakedb`, real PG)

| TLA | Go | Notes |
|---|---|---|
| `db` | the business row: `fakedb` state / PG row (`ex`, `gen`, `ver`, `vals`) | |
| `dedup` | `row_service_gen_log` rows of this key | |
| `dbEpoch` | `row_service_epoch.epoch` | |
| `epoch` | the epoch this Store took over at startup (7.8 step 2) | the model bumps it at `Crash`; Go takes it when the new Store starts |
| `net` | statements sent and not yet executed: `fakedb`'s pending (possibly late) statements | `live = FALSE`: the caller has stopped waiting; `pre`: was in flight when the epoch was bumped |
| `wait[p]` | a `db.DB` call is in progress for executor `p` | |
| `resp[p]` | the `db.DB` call's return value, see section 4 | |
| `blocked` | `fakedb`'s cross-row-constraint switch, flipped by the test | |
| `up` | the Store is running | `Crash` = the test discards the Store |
| `stmtTO`, `drops`, `cto`, `crashes`, `flips`, `qto`, `zsent`, `nextGen`, `nextVal`, `ops` | fault budgets and workload: `fakedb` fault settings and the test driver | |

### 2.4 Ghost variables (the test harness, not the service)

| TLA | Go |
|---|---|
| `abs`, `absKnown`, `seen` | Porcupine's sequential model and the recorded history (M4) |
| `deletedGens`, `insSent`, `delSent`, `failedGens`, `preCrash` | history recorded by the test from operation results and `fakedb`'s statement log |
| `violation` | `t.Fatal` in tests; `ErrInternal` in the service |

---

## 3. Actions

Each row is one atomic step of the model. In Go a step is a stretch of one function; mark its start with
`// TLA: <Action>`.

| TLA action | Go (design.md section) | The step |
|---|---|---|
| `ClientStart` | test driver / SDK call | |
| `Goc` | `getOrCreate` (7.1) | |
| `G_read` | `Get` (7.2) | `e.snap.Load()`: linearization point; `nil` → `load`, then back to the top. No `dead` check (dead protocol rule 3) |
| `L_lock` | `load` (7.2) | `e.lockOp(ctx)` |
| `L_load` | `load` → `ensureLoadedLocked` (7.2) | `dead` check; re-check `snap`; `db.Select`; `snap.Store` under `mu`; `unlockOp` |
| `U_mu` | `Update` (7.3) | the whole `mu` critical section: `dead` → `Gosched`, retry; `nil` → unlock `mu`, `load`; absent or gen mismatch → `ErrNotFound`; else build the new Row, `snap.Store` (linearization point), `markDirtyLocked`, set `queued` |
| `U_enq` | `Update` → `enqueue` (7.1) | after `mu.Unlock` |
| `X_lock` | `Create` / `Delete` → `lockOpLive` (7.1), then `runSync` starts the goroutine (7.4) | |
| `X_qtimeout` | `lockOpLive` returns `ctx.Err()` | timed out while queued for `op`: definitely not executed |
| `X_wait` | `runSync`'s `select` | `done` → result; `ctx.Done()` → `ErrOutcomeUnknown`, the goroutine keeps running |
| `FinishOp` | `runSync` goroutine | `defer e.unlockOp()`; `done <- result` |
| `B_c1` | `createLocked` (7.4) | `ensureLoadedLocked`; a row is there → same gen: return it; `db.GenClaimed` → `(nil, nil)`; else `ErrAlreadyExists`. Absent → first `db.InsertDedup` |
| `B_c2` | `insertUntilSure` loop body, then the end of `createLocked` (7.4) | classify the result (section 4); success → under `mu`: `snap.Store(got)`, `clearDirtyLocked` (linearization point) |
| `B_d1` | `deleteLocked` (7.5) | `ensureLoadedLocked`; absent or gen mismatch → `ErrNotFound`; else first `db.Delete` |
| `B_d2` | `deleteUntilSure` loop body, then the end of `deleteLocked` (7.5) | success → under `mu`: `snap.Store(absent)`, `clearDirtyLocked` (linearization point) |
| `F_take` | `flushShard` (7.6) | take `sh.queue` under `qmu` |
| `F_scan` | `flushShard` loop over `batch` | `tryLockOp` (fails → `retry`); under `mu`: dead / nil / absent / clean → `queued = false`, `unlockOp`; else snapshot row + mask, `clearDirtyLocked`, `queued = false`, keep `op` |
| `F_send` | `flushShard` | M2: `db.Update` per item, each right after its `F_scan`; `requeueAll(retry)` after the shard's items. From M6: `requeueAll(retry)`, then `db.UpdateBatch` |
| `F_done` | `flushShard` after `UpdateBatch` | on error: merge the mask back, re-enqueue if not queued; `unlockOp` |
| `V_evict` | `tryEvict` (7.7) | `tryLockOp`; under `mu`: `!dead && dirty == 0 && eligible` → `dead = true` |
| `V_remove` | `tryEvict` | under `shard.mu`: delete from the map; `unlockOp` |
| `DbExec`, `DbTimeout`, `DbDrop` | `fakedb` (and real PG) | execute; time out and maybe execute later; definitely not execute |
| `EnvFlip` | test flips `fakedb`'s cross-row switch | |
| `FenceAndSend` | `db.FenceSessions(inflight)`, then the next attempt (7.4, 7.5) | |
| `Crash` | crash-recovery test discards the Store (M6) | |
| `ZombieSend` | second-instance test: the old Store keeps running (M6) | |
| `Bump` | startup step 2: epoch takeover (7.8) | |
| `FenceDrop` | startup step 3: terminate the previous sessions (7.8) | |
| `Restart` | startup steps 4-5 (7.8) | |

### 3.1 Gaps: where failpoints go

A gap is the space between two steps of one goroutine, where another goroutine can run (design.md section 10). Each
gap below is a separate step boundary in the model. Failpoint hooks are nil in production.

| Gap | Between | What the failpoint test inserts | Exercises |
|---|---|---|---|
| update-enqueue | `U_mu` → `U_enq` | a flush round | `QueuedIsTracked` |
| delete-db-done | `B_d2`: DB DELETE succeeded → `snap.Store(absent)` | an Update | `W_UpdateDuringDelete`, `DeleteKeepsDirty` |
| flush-snapshot | `F_scan` → `F_send` | an Update and a Delete | `W_UpdateDuringFlush`, `FlushNoOp` |
| flush-late | `F_send` times out, executes later | a delete + create | `NoGenCheck`, `NoVerCheck`, `FlushUpsert` |
| evict-dead | `V_evict` → `V_remove` | an Update | `EvictNoDead`, `UpdateIgnoresDead` |
| evict-sample | idle sampling → `tryEvict` | a Get / Update | not a model step: the model picks and marks atomically; the `eligible` re-check is what makes Go equivalent |

---

## 4. DB results

| TLA `resp` | Meaning | Go (8.4) |
|---|---|---|
| `"ok"` | executed (0 rows affected also counts for UPDATE/DELETE) | `err == nil` |
| `"amb"` | may or may not have executed | `db.IsAmbiguous(err)`; session in `db.SessionOf(err)` |
| `"fail"` | definitely not executed | `db.NotExecuted(err)` |
| `"rej"` | rejected because of another row | `db.IsCrossRow(err)` |
| `"claimed"` | this gen is already in the dedup table | `db.ErrGenClaimed` |
| `"conflict"` | dedup record new, business row exists | `db.ErrDuplicate` → `ErrInternal` (cannot happen after "load first") |
| `"dup"` | only without the dedup fix | none |

---

## 5. Properties: where each is checked in Go

`checkInvariants` below means a test-only helper in `internal/store` (in a `_test.go` file) that walks the shards at a
quiescent point (no flush round running), takes each entry's locks, and compares with `fakedb`.

| TLA property | Go check | From |
|---|---|---|
| `NoViolation`: Get not linearizable | Porcupine | M4 |
| `NoViolation`: post-crash state implausible | crash-recovery test: the reloaded state satisfies `Plausible` (correctness-properties.md) | M6 |
| `NoViolation`: dedup conflict | no test may ever see `ErrInternal` | M3 |
| `NoResurrection` | history: once a delete of gen g returned success, g never appears again in `fakedb` or in a Get | M3 |
| `MemMatchesAbs` | Porcupine (Get and Update results) | M4 |
| `QuiescentConsistent`, `CleanIsDurable` | `checkInvariants`: entry free (`tryLockOp` succeeds), loaded, `dirty == 0` → equal to the `fakedb` row, including gen and ver | M2 |
| `DbMatchesAbsWhenIdle` | end of every stress test: graceful shutdown, then memory equals the DB for every key | M2 |
| `CreateDurable` | crash-recovery test: a confirmed create still exists unless its delete was sent | M6 |
| `OneWriterPerKey` (I5) | `fakedb` asserts: per key, at most one write call in progress from one Store (late statements whose caller gave up do not count) | M2 |
| `DirtyImpliesQueued`, `QueuedIsTracked` | `checkInvariants`: `dirty != 0` → `queued`; `queued` → in `shard.queue` | M2 |
| `LiveEntryInMap` (I1) | `checkInvariants`: entries in the map are not dead; queued entries that are not dead are the map's entry for their key | M5 |
| `NoDirtyEviction` (I10) | `checkInvariants`: a dead entry has `dirty == 0` | M5 |
| `DbValsBelongToGen` | stress updates carry `Change.Gen` and write values tagged with that gen; `fakedb` asserts every applied write keeps tags equal to the row's gen | M3 |
| `DbNotAhead` | `checkInvariants`: same gen → DB ver ≤ memory ver | M2 |
| `FencedRestart` (I15) | crash-recovery test: after the new Store's startup returns, no pending statement of an old session can still apply | M6 |
| `CreateFailureIsFinal` | history: a create answered with a failure never has its gen in `fakedb`'s dedup table, checked after all late statements have drained | M3 |
| `DbVerMonotonic` | `fakedb` asserts on every applied write, always on: same gen → ver does not decrease; values change → ver increases | M2 |
| L1 `EventuallyResponds` | every test operation returns before its deadline | M2 |
| L2 `BackgroundSettles` | `s.inflight.Wait()` returns once faults stop | M3 |
| L3 `EventuallyFlushed` | once faults stop, `dirtyCount` reaches 0 | M2 |
| L4 `NoLockLeak` | end of every stress test: `tryLockOp` succeeds on every entry | M2 |

---

## 6. Planted bugs: which test must catch each

Each mutation breaks one rule of the design. For each milestone that writes the code involved, break the rule with a
temporary patch, run the tests, confirm at least one fails, and revert. Record the result in the milestone summary.
Do not add flags or build tags for this.

| Mutation | The rule in Go | Must be caught by | From |
|---|---|---|---|
| `NoVerCheck` | flush UPDATE has `AND ver < ?` | flush-late failpoint; `DbVerMonotonic` | M2 |
| `NoGenCheck` | UPDATE / DELETE have `AND gen = ?` | flush-late failpoint across delete + create; `DbValsBelongToGen` | M3 |
| `FlushUpsert` | `UpdateBatch` never inserts | flush-late failpoint after a delete; `NoResurrection` | M3 |
| `DedupDeletedWithRow` | `db.Delete` leaves the dedup record | hole 1 and hole 2 tests | M3 |
| `NoMaskMerge` | `flushShard` merges the mask back on error | flush failure, then `checkInvariants` | M2 |
| `EvictDirty` | `tryEvict` requires `dirty == 0` | stress test with a very short IdleTTL; evict-dead failpoint | M5 |
| `EvictNoDead` | `tryEvict` sets `dead` | evict-dead failpoint (the Update is lost) | M5 |
| `UpdateIgnoresDead` | `Update` checks `dead` | evict-dead failpoint | M5 |
| `LoadNoRecheck` | `ensureLoadedLocked` re-checks `snap` | load racing a create (a Get queued behind it overwrites the created row) | M3 |
| `FlushNoOp` | `flushShard` takes `op` (`tryLockOp`) | flush-snapshot failpoint with a Delete | M3 |
| `ReleaseOnAmbiguous` | until-sure loops do not give up | hole 1 test | M3 |
| `SelectConfirms` | no "confirm by one SELECT" | late INSERT / late DELETE tests | M3 |
| `CreateNoLoad` | `createLocked` loads first | create on an unloaded key whose DB row is another life | M3 |
| `DeleteKeepsDirty` | `deleteLocked` clears dirty | delete-db-done failpoint | M3 |
| `LeakOpOnNotFound` | `runSync` always releases `op` | `NoLockLeak` end check | M3 |

---

## 7. Design holes: reproduction tests

Build each test from the model's counterexample: `cd tla && ./run.sh <scenario> | python3 trace.py` (holes 5-8 are
generated by `suite.py holes`). Each test must fail with its fix reverted and pass with it.

| Hole (design.md 13.2) | Fix in Go | Test | From |
|---|---|---|---|
| 1 late INSERT resurrects a deleted row | `InsertDedup` writes row + dedup in one statement; `Delete` keeps dedup | create times out and executes late after a delete | M3 |
| 2 client retry resurrects a deleted row | same | create succeeds but response lost; delete; retry with the same gen → success with no row, row stays absent | M3 |
| 3 old process's late UPDATE after restart | startup fence (7.8 step 3) | crash with an UPDATE in flight; new Store; the UPDATE never lands | M6 |
| 4 ambiguous, then "not executed", taken as final | `unsure` | ambiguous attempt, then a not-executed one; the loop keeps going | M3 |
| 5 cross-row rejection after an ambiguous attempt | `inflight` + `FenceSessions` | ambiguous attempt; cross-row rejection; switch flips; the earlier attempt must not land after "failed" | M3 |
| 6 retry finds another life | `db.GenClaimed` in `createLocked` | create A ambiguous, lands; delete; create B; retry A → success with no row | M3 |
| 7 SDK reports "not executed" | SDK keeps the outcome unknown | retry times out queued behind its own first attempt → caller never sees DEADLINE_EXCEEDED | M8 |
| 8 second instance | owner epoch on every write | second-instance variant of the crash-recovery test | M6 |

---

## 8. Non-vacuity: proving the tests reach the hard cases

The model's `W_*` witnesses prove TLC actually reached each situation. The Go equivalent: `fakedb` and the test driver
count these events, and each stress test asserts the relevant counters are above zero.

| Witness | Counter |
|---|---|
| `W_Delete`, `W_Recreate` | deletes that succeeded; creates of a key after its delete |
| `W_Evict`, `W_Reload` | evictions; loads of a key that was evicted |
| `W_LateStmt`, `W_EffectiveLateStmt` | statements executed after their caller stopped waiting; of those, ones that changed the DB |
| `W_LateInsertOfDeletedGen`, `W_RetryOfDeletedGen`, `W_DedupClaimed` | INSERTs arriving for a deleted gen; `ErrGenClaimed` results |
| `W_AmbThenNotExecuted`, `W_NotExecutedAfterFence` | "not executed" after an ambiguous attempt, before and after a fence |
| `W_CrossRowAfterAmb`, `W_FencedAttempt` | cross-row rejections after an ambiguous attempt |
| `W_RetryFindsOtherLife` | create retries that find another life with the gen claimed |
| `W_FlushFailed`, `W_UpdateDuringFlush`, `W_UpdateDuringDelete` | failed flushes; updates landing in those windows |
| `W_ClientGaveUp`, `W_QueueTimeout` | `ErrOutcomeUnknown` answers; timeouts while queued for `op` |
| `W_Crash`, `W_CrashWithStmtInFlight`, `W_OldUpdateAtRestart` | crash-recovery runs with statements in flight |
| `W_ZombieStmt`, `W_ZombieStoppedByEpoch` | writes from the old Store rejected by the epoch |
