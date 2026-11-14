---
aliases: [Row Service Correctness Properties, correctness properties]
tags: [design, tla+, verification, linearizability, mutation-testing]
created: 2026-11-12
---

# Row Service Correctness Properties

> [!info] What this document is
> The acceptance criteria for the In-Memory Authoritative Row Service ([design.md](design.md)): what the service promises, the precise properties that make those promises true, the faults they must survive, and how each property is checked. The TLA+ model in [tla/](tla/) implements every checkable item here, and the implementation tests (design.md 12.1) reuse the same list.
>
> It merges the draft "Row Service Correctness Properties (Draft)" with design.md. The draft's method is kept: numbered properties, liveness under fairness, an explicit environment model, a mutation catalogue and vacuity checks. Its premises are replaced by the design's: random gens with equality checks, a dedup table, retry-until-sure, linearization points, column masks and the startup fence. Section 10 lists every difference and the reason for it.

---

## 1. How to use this document

- Every property has an ID, a plain statement, the reason it matters, and the TLA+ definition that checks it (`tla/RowService.tla`).
- A property is only as good as the faults the model can produce. Section 6 lists every action in the model. **Leaving an action out of `Next` assumes that event never happens.** Check that table first when reviewing a model.
- Checking that the design passes is not enough. Two further suites show that the properties are not vacuous:
  - **Mutations** (section 7): each plants one known bug, and each must produce a counterexample.
  - **Reachability witnesses** (section 8): each claims a situation never happens, and each must be violated.
- To run everything:

  ```
  cd tla
  python3 suite.py quick        # after any change to the model: 92 runs, about 13 minutes (section 9.1)
  python3 suite.py liveness     # live.cfg + 2 liveness mutants, about half an hour
  ./run.sh base; ./run.sh base_2col; ./run.sh fixed          # exhaustive, minutes to hours (section 9)
  ```

  `quick` is `regress` + `holes` + `ext` + `mutants` + `witnesses` + `sim`; each can also be run on its own.

---

## 2. Scope and assumptions

The model covers **one key**: every correctness question in the design is per key (design.md 2, 9). Anything that needs more than one key is out of the model and covered by other tests (section 11).

| ID | Assumption | Why it is needed, and who guarantees it |
|---|---|---|
| A1 | Clients talk to one instance at a time: when a new instance starts, clients stop using the old one. | Needed for C1 only. Guaranteed by deployment (design.md 11.3). The properties about the DB (S3-S10, S18) **no longer depend on it**: the model lets a superseded instance keep sending statements (`ZombieSend`), and the owner epoch (I16) makes them harmless. What is not modeled is what the old instance answers to its own clients before it notices, which is bounded by `EpochCheckInterval`. |
| A2 | The service is the only writer of its tables, and its DB account is used by nothing else. | The startup fence terminates every session of that account (design.md 2, 7.8). |
| A3 | Only per-key linearizability is required; there are no cross-key transactions. | design.md 1. |
| A4 | PostgreSQL semantics: a statement executes at most once and atomically; the WHERE clause of a conditional UPDATE/DELETE is evaluated atomically with the write; a statement can only execute inside its own session; once `pg_terminate_backend` has finished, that session's uncommitted work is rolled back and nothing more from it executes. **The death of the client process does not end its statements.** | This is the root of holes 1 and 3. Verified on PG 16: a client killed with `kill -9` while its UPDATE was running left the backend `active`, and the UPDATE committed 3 s later. With the startup fence run first, it never committed. |
| A5 | A gen is unique among the lives of one (table, primary key). | Gens are non-zero 63-bit random numbers drawn from `crypto/rand` (design.md 7.4, 7.10). This is probabilistic: about k × 1.1e-19 per create, where k is the number of earlier lives of that key. The model assumes uniqueness. |
| A6 | A late or duplicate INSERT never arrives after its dedup record has been cleaned up (`GenLogRetention`, 24 h). | design.md 5.1. |
| A7 | Faults are finite: the DB is not unavailable forever, and statements eventually finish or fail. | Needed only for liveness (section 5). The model bounds every fault with a budget. |
| A8 | PostgreSQL never loses a transaction it acknowledged and never goes back in time under a running service: `synchronous_commit = on` for the service's sessions, no promotion of an asynchronous standby, no restore without restarting the service. | Otherwise a confirmed create or delete can vanish from the DB while memory keeps it, and S3, S4 and S9 fail for a reason no design above PG can remove. Guaranteed by the DB deployment (design.md 2); the service detects a changed timeline and stops (7.8). The model's DB never loses a write. |
| A9 | The epoch condition of a write is evaluated when the statement runs, and a statement that began before the epoch was bumped may still pass it. | The second half is why the startup fence is still needed after the epoch was added. The model takes the worst case: every statement in flight at the bump passes (`pre`), and only the fence removes it. |

---

## 3. What clients are promised

These are the promises in plain words. Sections 4 and 5 make them precise.

| ID | Promise |
|---|---|
| C1 | **Per-key linearizability.** Every operation takes effect atomically at one instant between its request and its response. A read returns the state at some instant during the read. |
| C2 | **Create / Delete.** "Success": it took effect and is durable. "Failure": it did not take effect and never will. "Unknown" (`ErrOutcomeUnknown`): it may or may not have taken effect, and the service keeps working until it knows. A create retried with the same gen is idempotent: it is never executed twice, and a row deleted in between is never resurrected. These are promises about the **call** the SDK returns from, not about one request: when an earlier request of the call was ambiguous, a later request that "was not executed" does not make the call a failure. |
| C3 | **Update.** "Success": the new value is visible to every later read at once and reaches the DB within the flush window. An update is never applied to a different life of the row. |
| C4 | **Every request is answered.** It succeeds, fails, or returns "unknown" when the caller's deadline expires. No request hangs forever. |
| C5 | **Crash.** A crash may lose only updates that had not been flushed yet. It never loses a confirmed create or delete. It never resurrects a deleted row. It never shows a state that no sequence of operations could produce. **Starting a second instance costs the same as a crash of the first one, and no more**: the first instance can no longer change the DB. |
| C6 | **Bounded loss.** The updates a crash can lose are those of the last flush window, and at most those of the last `MaxDirtyAge` when the DB cannot be written. |

What may legitimately happen is just as important. Reviewers should not flag these:

- A read concurrent with an update may return the new value **before** the updater receives its acknowledgement. The linearization point is `snap.Store`, which comes before the response (design.md 9).
- While a create's outcome is unknown, reads return "not found". The create has not taken effect yet, even if its INSERT has already reached the DB; it takes effect when memory is updated (I7).
- During a flush the entry is already clean in memory while the DB has not been written yet. op is held throughout, and a failure merges the mask back (design.md 7.6).

---

## 4. Safety properties (must hold in every reachable state)

### 4.1 The correctness criterion: an abstract sequential state

The model keeps a ghost variable `abs`, the state a sequential, single-threaded service would have. It records whether the row exists, its gen, and its column values. Each operation changes `abs` at its linearization point (design.md 9):

| Operation | Linearization point |
|---|---|
| Update | `snap.Store` inside `mu` |
| Create | `snap.Store(row)` after the DB has definitely succeeded |
| Delete | swapping in the tombstone after the DB has definitely succeeded |
| Get | `snap.Load` |

Memory and the DB are then compared against `abs`.

The draft compared against "the last value acknowledged to a client" (`acked`) instead. That is wrong for this design, for three reasons:

- The linearization point comes before the acknowledgement, so a concurrent read can legally see a value that has not been acknowledged yet.
- An update acknowledged while a read is in flight can legally be seen by that read.
- An "unknown" outcome has no acknowledged value at all.

An `acked`-based check would report false counterexamples. Worse, a model bent to satisfy it would hide the window between the write and the acknowledgement.

### 4.2 Properties

| ID | Name (TLA+) | Statement | Catches |
|---|---|---|---|
| S1 | Get linearizable (`NoViolation`, assertion `GetNotLinearizable`) | The value a Get returns is a value `abs` held at some instant during that Get. | Stale reads; reads of a value that was never committed |
| S2 | `MemMatchesAbs` | When the key's entry is loaded and live, its snapshot (existence, gen, values) equals `abs`. | Lost updates, a load overwriting newer memory, applying an operation twice |
| S3 | `DbMatchesAbsWhenIdle` | When everything is idle and memory holds no newer data, the DB equals `abs`. | Divergence between memory and the DB that survives until the next load |
| S4 | `CleanIsDurable` (I4) | When the entry is clean and nobody holds op, the DB row equals the snapshot (including gen and ver), **even while late statements are still in flight**. `QuiescentConsistent` is the weaker form that also requires no statement in flight. | Lost flushes, a failed flush not merged back, a create/delete given up while its statement was still pending |
| S5 | `DbNotAhead` | For the life memory holds, the DB ver is never greater than the memory ver. | A write memory never made reaching the DB, such as the previous process's late UPDATE (hole 3) |
| S6 | `DbVerMonotonic` (action property) | Within one life, the DB ver never decreases, and no value changes without the ver increasing. | A stale UPDATE overwriting a newer one (design.md 8.2 example 1) |
| S7 | `DbValsBelongToGen` | Every column value in the DB was written by the life that currently owns the row. | Data of an old life written into a new one (8.2 example 2) |
| S8 | `NoResurrection` | A life whose delete has taken effect never reappears, in the DB or in memory. | Late INSERTs, create retries, upserts (holes 1, 2) |
| S9 | `CreateDurable` | While `abs` says the row exists, the DB holds that life, unless its DELETE has been sent. | A confirmed create disappearing (8.2 example 3) |
| S10 | Post-crash plausibility (`NoViolation`, assertion `PostCrashStateImplausible`) | The first state read from the DB after a restart could have been produced by the operations so far: no deleted life, no life whose INSERT was never sent, no values from another life, and an existing life is gone only if its DELETE was sent. Unflushed updates may be missing. | Crash recovery producing impossible states (C5) |
| S11 | `OneWriterPerKey` (I5) | At most one write statement per key is awaiting its result. | Two writers racing in the DB, e.g. a flush concurrent with a delete |
| S12 | `NoDirtyEviction` (I10) | An entry being evicted holds no dirty data, and nothing makes it dirty afterwards. | Evicting unflushed data, updates applied to an entry already being removed |
| S13 | `LiveEntryInMap` (I1) | In the current process, every entry that is not dead is in the map. | Two live entries for one key, an eviction that skips the dead protocol |
| S14 | `DirtyImpliesQueued`, `QueuedIsTracked` | A dirty entry is always in the flush pipeline. | Dirty data that would never be flushed (the safety half of L3) |
| S15 | `FencedRestart` (I15) | Once the service is up, no statement that a previous process sent before the fence is still in flight. | Starting to load before the fence has finished |
| S16 | Dedup never conflicts (`NoViolation`, assertion `DedupConflict`) | Never "dedup record written but business row not inserted". | Creating without loading first, which burns a gen (I14, design.md 7.4) |
| S17 | Dirty budget (backpressure) | **Not in the model** (it needs many keys). The bound is soft: the check `dirtyCount > MaxDirty` is not atomic with the update, so the count can exceed `MaxDirty` by up to the number of concurrent updaters. An implementation test must use that bound, not `<= MaxDirty`. | Unbounded memory growth while the DB is down |
| S18 | `CreateFailureIsFinal` (C2) | A create whose call ended in "failed" is never inserted: not before the answer, and not after it. The model keeps the set of gens answered "failed" (`failedGens`) and the dedup table; both only grow, and they never intersect. | A failure answered while an attempt can still land (holes 5, 7), a retry answered "exists" although it took effect (hole 6) |
| S19 | Single writer to the DB (I16) | A statement sent by a superseded instance changes nothing. There is no invariant of its own: such a statement taking effect violates S4, S5, S9 or S10, whichever comes first. | A second instance (hole 8) |
| S20 | Bounded loss window (C6) | **Not in the model** (it needs real time). While the oldest dirty row that is not a quarantined poison row is older than `MaxDirtyAge`, no update is acknowledged. The age is sampled once per flush round, so the bound an implementation test must use is `MaxDirtyAge + FlushInterval`. | Acknowledging updates for the whole length of a DB outage |

S1-S17 say nothing about what create and delete **answer**; before S18 was added, the answer of a background create (`bres`) appeared in no property, which is how hole 6 stayed invisible. Delete has no counterpart of S18 yet: the model does not retry deletes (section 6).

---

## 5. Liveness properties (must eventually happen)

Liveness holds only under fairness. `FairSpec` gives **weak fairness** to:

- every step of an operation that has started;
- the flusher;
- the second half of an eviction already under way;
- DB execution;
- the epoch bump, the fence and the restart.

It gives **no fairness** to:

- faults: timeouts, dropped statements, crashes, clients giving up, requests timing out in the queue, the other row of a cross-row constraint changing, statements of a superseded instance;
- starting new operations or new evictions.

Every fault is bounded by a budget, which models A7. Without these assumptions nothing can be promised. For example, if the DB is down forever, a create with an ambiguous outcome keeps retrying.

| ID | Name (TLA+) | Statement | Catches |
|---|---|---|---|
| L1 | `EventuallyResponds` | Every client request eventually gets an answer. | Requests blocked forever on op or on a lost result |
| L2 | `BackgroundSettles` | Every create/delete running in the background (`runSync`) eventually reaches a definite outcome. | Retry loops that never end once the DB is back |
| L3 | `EventuallyFlushed` | Dirty data is eventually flushed, or lost in a crash. | Failed flushes never retried, dirty bits left on an entry the flusher skips |
| L4 | `NoLockLeak` | op is never held forever. | A path that returns without releasing op; such an entry can never be evicted again |

"An idle clean entry is eventually evicted" (the draft's EventuallyEvicted) is not checked directly. The model bounds the number of entries, so eviction is eventually disabled on purpose. It follows from L4 plus the eviction condition: clean, op free, and an IdleTTL policy that design.md 7.7 leaves to configuration.

TLC also checks for deadlock in every safety run. The `Done` action marks the legitimate final states, so a deadlock report is a real bug.

---

## 6. Environment model

| TLA+ action | Real-world event | Stresses | If it were missing |
|---|---|---|---|
| `ClientStart`, `Goc` | A request arrives and looks up its entry. A Get or Delete may carry a gen seen earlier. | everything | - |
| `G_read`, `L_lock`, `L_load` | Lock-free read, and a load split into "queue on op" and "SELECT + store". Writes can interleave between the steps. | S1, S2 | Load-vs-write races |
| `U_mu`, `U_enq` | Update inside `mu`, then enqueue after releasing `mu` | S2, S12, S14 | Updates racing eviction or flush |
| `X_lock`, `X_wait` | Create/delete hands off to `runSync`. **The client may give up first** ("unknown"). | C2, L1 | The client timing out while the background continues |
| `B_c1`, `B_c2` | createLocked + insertUntilSure: load first, INSERT with dedup, retry with the same gen | S8, S16, L2 | - |
| `B_d1`, `B_d2` | deleteLocked + deleteUntilSure | S8, S9, L2 | - |
| `F_take`, `F_scan`, `F_send`, `F_done` | The flusher: tryLock op, clear dirty, write the mask's columns, merge back on failure | S4, S6, S11, L3 | Flush ordering and failure handling |
| `V_evict`, `V_remove` | Eviction with the dead protocol, at any moment | S12, S13 | Eviction races |
| `DbExec` | A statement executes. **Possibly late**, after its sender stopped waiting. **Its response may be lost.** | S4-S9 | Hole 1 (late INSERT), 8.2 examples 1-3 |
| `DbTimeout` | The Go-side ctx expires. The statement stays in flight and may execute later. | S4-S9 | "Timeout means it was not executed", the most common wrong assumption |
| `DbDrop` | The statement definitely did not execute: connection refused, `statement_timeout`, and so on | L2, hole 4 | Hole 4 ("ambiguous, then definitely not executed") |
| SDK retry in `ClientStart` | The SDK retries an ambiguous create with the same gen | S8 | Hole 2 |
| `Crash` | The process dies, **or is superseded**: a newer instance starts while it is still running. Memory of that process stops counting. **Statements already sent stay in flight.** | C5, S10, S15 | Hole 3 (late UPDATE from the old process) |
| `Bump`, `FenceDrop`, `Restart` | The new instance takes over the owner epoch, the startup fence terminates the old sessions, then the service is up | S15, S19 | - |
| `ZombieSend` | A superseded instance, after the new one is up, sends a statement from its stale memory. The model does not keep that memory: it sends the worst statements instead (a flush with a ver above any real one, an INSERT, a DELETE, for any gen handed out). | S19 | Hole 8 (two instances writing) |
| `EnvFlip` | Another row changes, so a cross-row constraint (unique index, foreign key) starts or stops rejecting an INSERT / DELETE of this key. `DbExec` then answers "rejected" and rolls the statement back, dedup record included. | S4, S18 | Hole 5: "rejected by the data" would always be the same verdict for the same statement |
| `X_qtimeout` | A create/delete request times out while still queued for op (`DEADLINE_EXCEEDED`) | S18 | Hole 7 |
| SDK retry while busy (`RetryWhileBusy` in `ClientStart`) | The SDK retries while its first attempt is still running in the background; the retry queues behind it | S18 | Hole 7: the model retried only after the first attempt had settled |
| `Done` | All work finished (a legitimate final state, not a deadlock) | - | False deadlock reports |

**Not modeled**, and covered elsewhere (section 11):

- what a superseded instance answers to its own clients before it notices (A1);
- a DB that loses acknowledged writes (A8);
- cleanup of the dedup table while a late INSERT is still in flight (A6);
- SDK retry of a delete with gen (the rule of hole 7 applies to it unchanged; S18 has no delete counterpart);
- a failed or unconfirmed session fence (the loop backs off and fences again, design.md 7.4);
- the loss window in real time (S20);
- values PG normalizes on write (design.md 13.3);
- data rejection of an UPDATE (poison rows, design.md 7.9);
- INCR, which is the same path as SET inside `mu` (I12);
- more than two columns (`base_2col` covers column masks);
- several keys and the dirty budget (S17);
- the TCP protocol and SDK framing;
- gen collisions (A5).

---

## 7. Mutation catalogue

Each mutation breaks exactly one rule of the design. The constant `Mut` selects it; `"none"` is the design itself. Every mutation runs in the environment where the design passes (that of `fixed.cfg`: 2 clients, all faults, all fixes), with all invariants and S6 enabled. **Every one must produce a counterexample.**

`python3 suite.py mutants` (2026-11-12, and again on 2026-11-13 after holes 5-8 were added): **15/15 caught**. The trace lengths below are from the first run; with several workers TLC does not always report the shortest counterexample, so they vary by a state or two between runs.

| Mut | Rule broken (design.md) | Caught first by | Trace (states) |
|---|---|---|---|
| `NoVerCheck` | flush UPDATE needs `AND ver < ?` (I6, 8.2 ex. 1) | S6 `DbVerMonotonic` | 25 |
| `NoGenCheck` | UPDATE/DELETE need `AND gen = ?` (I6, 8.2 ex. 2, 3) | `QuiescentConsistent` | 24 |
| `FlushUpsert` | no upsert (I6) | S8 `NoResurrection` | 24 |
| `DedupDeletedWithRow` | the dedup record outlives the row (I14) | S8 `NoResurrection` | 17 |
| `NoMaskMerge` | a failed flush merges its mask back (7.6) | S4 `CleanIsDurable` | 17 |
| `EvictDirty` | never evict dirty entries (I10) | S12 `NoDirtyEviction` | 12 |
| `EvictNoDead` | eviction sets dead before removing (dead protocol, 7.7) | S13 `LiveEntryInMap` | 6 |
| `UpdateIgnoresDead` | Update re-checks dead inside `mu` (7.3) | S12 `NoDirtyEviction` | 12 |
| `LoadNoRecheck` | load re-checks snap after taking op (7.2) | S2 `MemMatchesAbs` | 17 |
| `FlushNoOp` | the flusher holds op while writing (I5, 7.6) | `QuiescentConsistent` | 14 |
| `ReleaseOnAmbiguous` | an ambiguous create/delete keeps op and retries until sure (7.4) | `QuiescentConsistent` | 8 |
| `SelectConfirms` | one SELECT cannot settle an ambiguous outcome (7.4) | `QuiescentConsistent` | 9 |
| `CreateNoLoad` | create loads before inserting (7.4) | S16 `DedupConflict` | 14 |
| `DeleteKeepsDirty` | a delete discards dirty data at its linearization point (7.5) | S14 `DirtyImpliesQueued` | 20 |
| `LeakOpOnNotFound` | every path releases op | deadlock | 14 |

The seven fixes are mutations too: switch them off and the original holes come back. `python3 suite.py holes` (2026-11-13): all 9 scenarios fail without their fix and pass with it. `hole1`-`hole4` are files; `hole5`-`hole8` are generated by `suite.py` (`NEW_HOLES`).

| Fix switch off | Scenario | Caught first by | Trace (states) |
|---|---|---|---|
| `FixDedup` | `hole1` (late INSERT) | S8 `NoResurrection` | 17 |
| `FixDedup` | `hole2` (create retry) | S8 `NoResurrection` | 19 |
| `FixFence` | `hole3` (late UPDATE of the old process) | S5 `DbNotAhead` | 22 |
| `FixFence` | `hole3_insert` (late INSERT of the old process) | `QuiescentConsistent` | 13 |
| `FixUnsureRetry` | `hole4` (ambiguous, then not executed) | S3 `DbMatchesAbsWhenIdle` | 9 |
| `FixCrossRow` | `hole5` (ambiguous, then rejected because of another row, then that row changes) | `QuiescentConsistent` | 13 |
| `FixRetryAnswer` | `hole6` (a create retry finds another life) | S18 `CreateFailureIsFinal` | 27 |
| `FixSdkRetry` | `hole7` (a retry times out queued behind its own first attempt) | S18 `CreateFailureIsFinal` | 11 |
| `FixEpoch` | `hole8` (a superseded instance writes after the new one is up) | S10 `PostCrashStateImplausible` | 11 |

The four new switches are also switched off one at a time in the `ext` environments (2 clients, every earlier fault; section 9), where each must be caught again: `off_FixCrossRow`, `off_FixRetryAnswer`, `off_FixSdkRetry`, `off_FixEpoch`: **4/4 caught**, by the same properties.

One wrong fix was caught this way before it reached the design: the first fix for hole 5 cleared the "an earlier attempt may have executed" flag when it fenced the earlier attempts. `ext_crossrow` refuted it in 2 seconds (15 states): ambiguous A, B rejected because of another row, A lands, fence, C not executed, "failed" returned while the row exists. The fix now keeps two flags (`bamb` and `bfl` in the model, `unsure` and `inflight` in design.md 7.4).

**Liveness-only mutations.** These run under `FairSpec` with invariants and deadlock checking switched off, to show that the temporal properties are not vacuous:

| Mut | Must be caught by | Result |
|---|---|---|
| `LeakOpOnNotFound` | L4 `NoLockLeak` | caught: lasso of 12 states, 4 s |
| `DeleteKeepsDirty` | L3 `EventuallyFlushed` | caught: lasso of 26 states, 4 min |

Both were run again on 2026-11-13 with the fixes of holes 5-8 on: caught again (13 and 27 states; 4 s and 4 min).

---

## 8. Reachability witnesses

A property that passes because the model never reaches the dangerous situation proves nothing. Each `W_x` in `RowService.tla` claims that a situation never occurs, and TLC **must** report it as violated in the `fixed` environment. This replaces the draft's single "never deleted" check.

`python3 suite.py witnesses` (2026-11-12): **17/17 reached**.

| Witness | Situation that must be reachable | Trace |
|---|---|---|
| `W_Crash` | a crash | 3 |
| `W_Evict` | an eviction | 4 |
| `W_ClientGaveUp` | a client gives up before the outcome is known | 5 |
| `W_Reload` | a second entry is allocated (reload after eviction or crash) | 7 |
| `W_CrashWithStmtInFlight` | crash while a statement is in flight | 7 |
| `W_LateStmt` | a statement still pending after its sender timed out | 7 |
| `W_EffectiveLateStmt` | such a statement would change the DB if it ran now | 7 |
| `W_DedupClaimed` | an INSERT is answered "this gen is already claimed" | 9 |
| `W_AmbThenNotExecuted` | "not executed" after an earlier ambiguous attempt (hole 4's situation) | 9 |
| `W_Delete` | a delete takes effect | 14 |
| `W_RetryOfDeletedGen` | a create retried with a gen that was already deleted (hole 2's situation) | 14 |
| `W_OldUpdateAtRestart` | an old process's UPDATE in flight at restart (hole 3's situation) | 16 |
| `W_LateInsertOfDeletedGen` | an INSERT of a deleted gen in flight (hole 1's situation) | 16 |
| `W_FlushFailed` | a flush fails or is ambiguous | 16 |
| `W_UpdateDuringDelete` | an update succeeds while a delete is in flight | 16 |
| `W_UpdateDuringFlush` | an update succeeds while a flush is in flight | 17 |
| `W_Recreate` | the row exists again after a delete | 21 |

The situations of holes 5-8 need their faults, so their witnesses run in the `ext` environments (section 9). `python3 suite.py witnesses` (2026-11-13): **17/17 and 9/9 reached**.

| Witness | Environment | Situation that must be reachable | Trace |
|---|---|---|---|
| `W_QueueTimeout` | `ext_sdk` | a request times out while queued for op | 5 |
| `W_ZombieStmt` | `ext_epoch` | a statement of a superseded instance in flight while the new instance is up | 6 |
| `W_RetryQueuedBehindSelf` | `ext_sdk` | an SDK retry queued for op behind its own first attempt (hole 7's situation) | 7 |
| `W_ZombieStoppedByEpoch` | `ext_epoch` | such a statement would change the DB, and only the epoch condition stops it (hole 8's situation) | 7 |
| `W_CrossRowAfterAmb` | `ext_crossrow` | "rejected because of another row" while an earlier attempt may be in flight (hole 5's situation) | 10 |
| `W_FencedAttempt` | `ext_crossrow` | the same, with an earlier statement in flight that would still change the DB | 10 |
| `W_NotExecutedAfterFence` | `ext_crossrow` | "not executed" after a fence (the situation that refuted the first fix for hole 5) | 12 |
| `W_RetryNotExecuted` | `ext_sdk` | an SDK retry answered "not executed" | 13 |
| `W_RetryFindsOtherLife` | `ext_sdk` | a create retry finds another life while the dedup table holds its gen (hole 6's situation) | 25 |

---

## 9. Model configurations and results

| Config | Spec | Environment | Expected | Result |
|---|---|---|---|---|
| `hole1`-`hole8`, `hole3_insert` | Spec | each hole's minimal scenario, fix off | fail | fail (all 9) |
| `*_fixed` | Spec | the same scenarios, fix on | pass | pass (all 9) |
| `base` | Spec | hole 4 fixed; holes 1-3's preconditions removed; 2 clients x 3 ops | pass | pass |
| `base_2col` | Spec | as `base`, two columns (column masks) | pass | pass |
| `fixed` | Spec | the faults of holes 1-4 and all fixes; 2 clients x 3 ops, 1 crash | pass | pass before holes 5-8; **not re-run with the fixes of holes 5-8 on** (see below) |
| `ext_crossrow`, `ext_sdk`, `ext_epoch` | Spec | the scale of `live`, every earlier fault, all fixes, plus the fault of hole 5 / holes 6-7 / hole 8 | pass | pass |
| `ext_all` | Spec | the same with the faults of holes 5-8 all enabled | pass | pass |
| `regress` (12 scenarios) | Spec | the model at `6b9291e` against the changed model with the switches of holes 5-8 off | same | same in all 12 |
| `base_big`, `fixed_big` | Spec, random simulation | 3 clients, two columns, 2 of each fault (`fixed_big`: including those of holes 5-8) | no violation | no violation |
| `live` | FairSpec | the faults of holes 1-4 and all fixes; 2 clients x 2 ops, 1 crash | pass | pass (re-run on 2026-11-13 with the fixes of holes 5-8 on: 9,938,605 distinct states, depth 51, 34 min with 16 workers on a machine shared with other runs) |
| mutants (15) | Spec | the `fixed` environment | caught | 15/15 caught |
| fix switches of holes 5-8 (4) | Spec | the `ext` environments | caught | 4/4 caught |
| witnesses (17 + 9) | Spec | the `fixed` environment; the `ext` environments | violated | 26/26 violated |
| liveness mutants (2) | FairSpec | the `live` environment | caught | 2/2 caught |

The long runs, with every property of this document enabled (2026-11-12 to 11-13, 48-core machine; the worker count is TLC's `-workers`):

| Config | Distinct states | Depth | Time | Result |
|---|---|---|---|---|
| `base` | 274,895,794 | 71 | 20 min (28 workers) | no error |
| `base_2col` | 39,658,214 | 62 | 3 min (12 workers) | no error |
| `fixed` | 1,667,861,866 | 74 | 2 h 03 min (28 workers) | no error |
| `live` | 9,634,994 | 50 | 27 min (16 workers), including 5 min of temporal checking | no error |
| `base_big` | about 1.38 billion states checked (random, depth <= 300) | - | 25 min (10 workers) | no violation |
| `fixed_big` | about 1.38 billion states checked (random, depth <= 300) | - | 25 min (10 workers) | no violation |

The state counts of `base`, `base_2col` and `fixed` equal those of the runs before the properties of this document were added: `Mut = "none"` and the new properties do not change the model's behavior.

The runs made after holes 5-8 were added (2026-11-13, same machine, as part of `python3 suite.py quick`: 8 workers per run, 24 for `ext_all` and `base_2col`, while a liveness run was using 16 cores):

| Run | Distinct states | Depth | Time | Result |
|---|---|---|---|---|
| `ext_crossrow` | 19,688,687 | 53 | 6 min | no error |
| `ext_sdk` | 6,396,480 | 51 | 2 min | no error |
| `ext_epoch` | 23,144,974 | 52 | 7 min | no error |
| `ext_all` | 117,928,805 | 54 | 12 min | no error |
| `regress`: `base_2col` | 39,658,214 in both models (139,174,924 generated in both) | 62 | 6 min each | same |
| `regress`: `fixed_small` (the environment of `live`, safety only) | 4,827,667 in both models (14,320,947 generated in both) | 50 | 1 min each | same |
| `regress`: the 10 hole scenarios | the 5 that pass: same counts; the 5 that fail: same property violated | - | seconds | same |
| `sim` (`fixed_big`) | about 39 million states checked (random, depth <= 300) | - | 2 min (time-boxed) | no violation |
| the whole of `quick` | 92 runs | - | 13 min | 92/92 as expected |

What this does and does not establish for the long runs:

- `base` and `base_2col` have every switch of holes 5-8 off, so by `regress` they are the model that was checked before; `base_2col` was compared directly.
- `fixed.cfg` now has the fixes of holes 5-8 switched on. With them off it is, by `regress`, the model whose 1,667,861,866 states were checked on 2026-11-13. With them on it has **not** been run at full scale (about two hours); the same configuration at the scale of `live` (`ext_*` with the new faults, `fixed_small` without) passes.

### 9.1 Checking a change to the model in minutes

`base` takes 20 minutes and `fixed` two hours. They are the wrong tool for checking a change: `python3 suite.py quick` takes about 13 minutes on the same machine and answers the questions a change raises, one part per question.

| Part | Question | How |
|---|---|---|
| `regress` | Did the change alter anything it was not meant to alter? | Every new fault and every new fix has a switch. With all of them off, the changed model must be the previous model, state for state. `suite.py` takes the previous version from git (`--ref`, default `6b9291e`), runs both versions on the same scenarios, and compares the number of states generated and of distinct states. The changed model is viewed through `VIEW OldVars`, the variables the previous model had, so new ghost variables do not split states. (The depth is printed but not compared: with several workers TLC reports 18 or 19 for the same small model from one run to the next.) |
| `holes` | Is each fault real, and does its fix work in the smallest scenario? | Fix off: a counterexample. Fix on: pass. |
| `ext` | Does each fix hold together with every earlier fault? | The scale of `live.cfg`, all earlier faults, plus one new fault at a time, then all new faults together. Exhaustive. |
| `mutants`, `witnesses` | Do the properties still catch planted bugs? Does the model reach the new situations? | As in sections 7 and 8; the new fix switches and witnesses run in the `ext` environments. |
| `sim` | Is there anything at a scale exhaustive search cannot reach? | Random simulation of `fixed_big` (3 clients, two columns, every fault twice), time-boxed. |

**Why `regress` can stand in for the long runs.** If the changed model with its switches off has exactly the previous model's state graph, every result obtained for the previous model still holds for it, including the two-hour `fixed` run, without repeating it. Equal counts in every scenario are strong evidence of that, not a proof: the comparison would miss a change that alters behavior while leaving both numbers unchanged in all scenarios. `regress --full` repeats the comparison on `base` and `fixed` themselves when certainty is wanted.

**What `regress` does not cover** is the model with the new switches on. That is the job of `ext` (exhaustive, small) and `sim` (large, random). The exhaustive run of `fixed.cfg` with the new fixes on remains the last word; it belongs before a release, not after every edit.

**Why small scenarios are enough to find a wrong fix.** Every hole so far has a counterexample of at most 27 states, found in at most 2 seconds, and breadth-first search reports short counterexamples first. A fix that is wrong fails early in a small environment: the first version of the fix for hole 5 was refuted by `ext_crossrow` in 2 seconds with a 15-state trace, long before the environment was explored (design.md 13.2).

**How to keep a change checkable this way:**

1. Put every new fault behind a budget constant and every new fix behind a boolean constant; 0 and FALSE must mean "as before".
2. Put new variables into `extV`, not into the old groups, unless they are a function of old variables while the switches are off (`bfl` equals `bamb` then).
3. With the switches off, no old action may read a new variable.
4. Then run `python3 suite.py quick`. A `DIFF` line from `regress` means rule 1, 2 or 3 was broken, or an old action was changed by accident (a changed guard, or a variable listed as `UNCHANGED` that the action used to change).
5. After the change is accepted, set `BASELINE_REF` in `suite.py` to its commit, so the next change is compared against it.

---

## 10. Differences from the draft

| Draft | Here | Why |
|---|---|---|
| S1 `NoLostAck` and S7 `ReadYourAck`, against the last acknowledged value | S1-S3, against the abstract state at linearization points | The acknowledgement comes after the linearization point; "unknown" has no acknowledged value (4.1) |
| gen = (generation, version) from a PG sequence, compared lexicographically; S3 `DbGenMonotonic`, S10 `GenNeverReused` | S6 (ver monotonic **within one life**), S7, S8, A5 | The design uses random gens compared for equality (design.md 8.5). A sequence fixes none of the four holes: an INSERT has no old row to compare against, a server-assigned gen defeats idempotent retries, and the old process reuses the same version numbers |
| "No tombstone rows or tombstone table" | dedup table (S8, S16) | Without a record of deleted lives, a late or retried INSERT resurrects the row (holes 1, 2) |
| Create/delete fail when the DB is unavailable | definite failure only when nothing ambiguous happened before; otherwise retry until sure, and the client gets "unknown" (C2, L2) | Hole 4, and the memory/DB divergence of `ReleaseOnAmbiguous` and `SelectConfirms` |
| S8 `CleanIsDurable` at all times; mutation `CleanBeforeFlushDone` treated as a bug | S4 qualifies it with "op is free" | The design clears dirty when the flush is sent and merges back on failure. That is correct because op is held throughout |
| dirty as a boolean | column mask (`base_2col`) | Merge-back bugs are invisible with a boolean |
| `Read` consults a pending-flush queue on a miss (mutation `ReadFromDbWhenDirty`) | dropped | Dirty entries are never evicted (I10), so a miss never has pending data |
| `FlushTimeoutButApplied`: applied immediately, reported failed | `DbTimeout` + late `DbExec` + lost responses, for **every** statement kind | Lateness matters only when other things happen in between |
| `Crash` clears memory and queues | `Crash` keeps statements in flight; `FenceDrop`, `Restart` | Hole 3 |
| - (not in the draft) | client giving up, SDK retry, op lock, dead protocol, load-first, dedup, fence | Each one is the precondition or the fix of a hole |
| S9 `DirtyBudget` as `<= MaxDirty` | S17, soft bound, outside the model | The check is not atomic; a single-key model cannot exercise it |
| One vacuity check ("never deleted") | 17 witnesses (section 8) | Each hole's precondition must be shown reachable |
| Liveness L1-L3 under weak fairness | L1-L4, fairness spelled out | Kept from the draft. L4 replaces EventuallyEvicted (section 5) |
| Mutation table with booleans | 15 mutations plus the fix switches, all run in one environment | Kept from the draft, extended to every "deliberate design" rule in design.md 0.5 |

---

## 11. What this does not prove

TLC checks the model exhaustively, but only within its bounds, and only the model, not the Go code. The rest is covered by the tests in design.md 12.1:

- **Larger bounds**: covered by `base_big` and `fixed_big` (random simulation) plus long-running stress tests. The faults of holes 5-8 are checked exhaustively only at the scale of `live` (section 9).
- **The Go implementation matching the model**: `go test -race`, a failpoint-driven fake DB that injects late execution, lost responses, drops, rejections that depend on another row, and a second instance (it should reproduce every counterexample in section 7), and Porcupine linearizability checking.
- **Out-of-model behavior**: multi-key behavior and the dirty budget (S17), the loss window in real time (S20), poison rows, INCR overflow, values PG normalizes (design.md 13.3), the protocol layer, SDK retry of delete, PG version differences, and gen randomness (A5).
- **Deployment assumptions**: A2 and A8 must hold in production, and A1 for linearizable answers across an instance switch. The model assumes them.
