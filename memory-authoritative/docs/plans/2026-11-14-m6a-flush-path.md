# M6a: The Flush Path — Batch Flush, Poison Rows, Backpressure, Metrics — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The flusher writes dirty rows in batches of one transaction each, quarantines rows the DB definitely rejects without holding up the rest of the batch, and Update pushes back with `ErrBusy` when dirty rows are too many or too old. The Store reports the metrics of 11.2 that belong to code that exists now.

**Architecture:** `db.DB` gains `UpdateBatch`, which writes the whole batch or nothing. `flushShard` collects items up to `FlushBatchMax`, sends each batch, and on a definite rejection writes the batch's rows one by one to find the poison rows. `RunFlusher` becomes a pool of workers with per-shard exponential backoff. After each shard, the worker records the oldest `dirtySince` of the shard's non-poisoned dirty rows. Update reads a flag derived from it, and `dirtyCount`.

**Tech Stack:** Go 1.25, `testing/synctest`, `log/slog`, fakedb.

**Spec:** `memory-authoritative/design.md`: 7.6 (flush), 7.9 (poison rows), 8.3 (batch writes, sorting by key), 8.4 (error classes), 11.1 (backpressure), 11.2 (metrics), 11.4 (`FlushBatchMax`, `MaxDirty`, `MaxDirtyAge`), 12.2 (M6). Also `tla-mapping.md` row `F_send`, and the section 6 mutations `NoVerCheck`, `FlushUpsert`, `NoMaskMerge` and `FlushNoOp`, re-run on the batch path.

**Scope:** M6 is split in two (agreed with the user). M6a is the flush path. **M6b** is the rest:
- the owner epoch, and the epoch check on zero-row writes;
- `flush_noop_rows`, which needs writes to report zero rows, a change M6b makes for the epoch check anyway;
- startup session cleanup, graceful shutdown and the service-lifetime ctx (`bgCtx`);
- dedup table cleanup;
- the crash recovery tests (holes 3 and 8);
- the epoch, startup and dedup-table metrics.

The admin interface of 7.9 point 4 is here as Store methods. Exposing them over the network is M8.

## Global Constraints

- Go 1.25. Dependencies: the standard library only (`log/slog` for the poison log). No new modules.
- The CLAUDE.md commands must be clean before each task is reported done: `gofmt -l .`, `go vet ./...`, `go test -race ./...`.
- Leaf locks (`mu`, `shard.mu`, `shard.qmu`): no other lock, no channel operation, no IO and no logging while one is held (6.4). `time.Now()` under `mu` is allowed (in-memory, as `markDirtyLocked` does).
- Never upsert (8.3). Every flush write is conditional: `gen = ? AND ver < ?` (I6). Values are absolute (I12).
- `dirty` is cleared only by the flusher with `op` held until the write returns, and merged back on failure (I4). Poison rows stay dirty and are never evicted (I10, 7.9).
- Failpoints: only `Store.failpoint`, nil in production. Test-only code in `_test.go` or fakedb.
- design.md, the TLA model and correctness-properties.md are not changed without the user's approval. tla-mapping.md rows are filled in as the code lands.

## Decisions (where the spec leaves a choice)

1. **`UpdateBatch` is all or nothing.** `UpdateBatch(ctx, ws []db.Write) error` with `type Write struct { Row *schema.Row; Mask uint64 }`. Its contract:
   - It is one transaction of the conditional UPDATEs of `db.Update`.
   - `ErrRejected` / `ErrCrossRow` mean no row was written.
   - An ambiguous error means all of them or none.

   The Store sorts `ws` by key before the call (8.3), so fakedb can check the order.
2. **Definite rejection** in a flush means `errors.Is(err, db.ErrRejected) || db.IsCrossRow(err)` (8.4, 7.9). Anything else (ambiguous, not executed) is an ordinary failure. On a definite rejection the batch's rows are written one by one with `db.Update`:
   - a row rejected alone is poisoned;
   - a row that fails any other way is merged back, together with every row not yet tried, and the shard stops for this round.
3. **Ordinary failure stops the shard for the round.** After a failed batch (or solo write), the batch's rows are merged back, and the entries not yet scanned are put back on the queue untouched. `flushShard` reports the failure. `RunFlusher` then skips that shard for `FlushInterval << (failures-1)`, at most `flushBackoffMax = 5 * time.Second`, and resets after a success (7.6: "exponential backoff to this shard"). The backoff state lives in the worker, not in `shard`, so tests that call `flushShard` directly see no backoff.
4. **Workers:** `RunFlusher` runs `min(flushWorkers, NumShards)` goroutines, with `flushWorkers = 8`, a constant to tune in the M7 load test. Shard `i` belongs to worker `i % W` (7.6). Each worker has its own `FlushInterval` ticker. 11.4 has no config row for W, so it is not a config option.
5. **`dirtySince`** (5.2, `mu`) is set by `markDirtyLocked` when `dirty` goes from 0 to non-zero. On a failed write the flusher merges it back as the earlier of the item's `since` and the entry's current `dirtySince` (7.6 skeleton).
6. **Oldest dirty row, per shard.**
   - **Record (flusher).** After `flushShard` finishes a shard, it copies the shard's queue under `qmu`. Then it takes each entry's `mu` in turn, and stores the minimum `dirtySince` over entries with `dirty != 0 && !poisoned` in `shard.oldestDirty atomic.Int64` (UnixNano, 0 = none).
   - **Why the queue is enough.** Every dirty entry is in the queue, or is held by this same worker, which has finished. A shard in backoff keeps its last value. That value is still exact, because only that shard's worker can make those rows clean.
   - **Read.** `oldestDirty()` returns the minimum over the shards.
   - **Publish.** After each round, the worker stores `s.tooOld` (`atomic.Bool`) = `MaxDirtyAge > 0 && oldest != 0 && time.Since(oldest) > MaxDirtyAge`. Update reads only that bool. The refresh happens once per round, as 11.1 says.
7. **Backpressure in Update** (11.1), in this order:
   1. Before the loop: if `tooOld`, return `ErrBusy`, counted as `BusyAge`. This covers every update, including updates to rows that are already dirty.
   2. Inside `mu`, after the `ErrNotFound` checks and before building the new row: if `e.dirty == 0 && dirtyCount >= MaxDirty`, return `ErrBusy`, counted as `BusyCount`. A row that is already dirty does not grow the backlog, so it is let through (11.1). The limit is approximate under concurrency.

   `ErrBusy` changes nothing: no `snap`, `dirty` or queue change.
8. **Poison quarantine** (7.9 points 2 and 3). New entry fields:
   - `poisoned bool`, guarded by `mu`, as in 5.2;
   - `poisonVer int64` and `poisonRetryAt time.Time`, accessed only with `op` held, like `lastLoadErr`.

   A row rejected alone has `poisoned`, `poisonVer = r.Ver` and `poisonRetryAt = now + poisonRetryInterval` set (`poisonRetryInterval = time.Minute`, a constant). It is merged back dirty, and logged with `slog.Warn` after `mu` is released.

   In `F_scan`, a poisoned entry is written alone, after the shard's batches, when its `snap.Ver != poisonVer` (an update since: 7.9 "every new update also triggers a retry") or `now >= poisonRetryAt`. Otherwise it goes back on the queue untouched. A successful solo write clears `poisoned`.

   `deleteLocked` clears `poisoned` along with the dirty columns, since the row is gone. Create cannot meet a poisoned entry: it only creates over `absent`.
9. **Admin** (7.9 point 4):
   - `Poisoned() []schema.Key` lists the quarantined keys (a scan).
   - `DiscardPoisoned(ctx, key) error` is the human's "drop my changes and reload": it does the following.
     1. It takes `op` with `lockOpLive`.
     2. In one `mu` section: if not poisoned, it returns `ErrNotFound`. Otherwise it clears the dirty columns and `poisoned`, and sets `dead`.
     3. It removes the entry from the map, sharing the removal code with `tryEvict` in `removeLocked(sh, e)`.

     Discard goes through the dead protocol, so that the next access loads the DB's row. It does not add a new `snap` transition. "Correct the data and retry" is an ordinary Update.
10. **Metrics** (11.2) as `func (s *Store) Stats() Stats`, a plain struct. Counters are cumulative; latencies are a total plus a count, so an exporter can compute rates and means. The internal counters are `atomic.Int64` fields of an unexported `metrics` struct in `Store`. Get's hits and misses are per shard, next to `shard.mu`, which Get already writes, so the hot path adds no shared cache line. Tombstones and negative-cache entries are both `absent` in memory, so they are one number, `Absent`, counted by a scan in `Stats`.
11. **Config** (11.4):
    - `FlushBatchMax` 500, must be > 0;
    - `MaxDirty` 100,000, must be > 0. 11.4 leaves it TBD; the value is a tenth of the `MaxEntries` default;
    - `MaxDirtyAge` 30s; 0 disables it; must be >= 0.
12. **`ErrBusy`** is added to the store errors (5.2). In the shared test classifier it counts as "definitely no effect" (12.1).

## Review Focus

1. **A poisoned row that is then deleted:** it must leave quarantine (`Poisoned()` empty, `Stats().Poisoned` 0) and become evictable. Test `TestDeletePoisonedRow` (Task 3).
2. **Every dirty row poisoned, DB otherwise healthy:** no `ErrBusy` by age, however long. That is the acceptance criterion "a poison row alone does not trigger it". Test `TestPoisonAloneDoesNotTriggerBusy` (Task 4).
3. **A cross-row rejection in a batch** (`ErrCrossRow`, such as a foreign key): treated like `ErrRejected`, quarantined, with the other rows written. Test `TestCrossRowRejectionQuarantines` (Task 3).
4. **An update during a failed batch:** the newer value and the earlier `dirtySince` must both survive the merge-back. Otherwise the age limit resets every round and never fires while the DB is down. Test `TestBusyWhenDirtyTooOld` (Task 4), with an update between two failed rounds.
5. **`ErrBusy` has no effect:** no `snap`, `dirty`, `queued` or `dirtyCount` change, and it is never answered for a Get, a Create or a Delete. Test `TestBusyChangesNothing` (Task 4).

---

### Task 1: `db.UpdateBatch` and fakedb

**Files:**
- Modify: `internal/db/db.go` (type `Write`, method `UpdateBatch` on `DB`)
- Modify: `internal/db/fakedb/fakedb.go`
- Test: `internal/db/fakedb/fakedb_test.go`

**Interfaces:**
- Produces:
  - `type db.Write struct { Row *schema.Row; Mask uint64 }`;
  - `UpdateBatch(ctx context.Context, ws []db.Write) error` on `db.DB`;
  - fakedb's `Kind` gains `UpdateBatch`, and `Stmt.Keys []schema.Key` holds the batch's keys in order;
  - `fakedb.DB.Reject func(r *schema.Row) bool`, a CHECK constraint: set before concurrent use. A row it rejects fails `Update` or `UpdateBatch` with `db.ErrRejected`, and nothing of that call is written. It is checked when the statement executes, under `d.mu`.
  - `d.Updates()` counts `Update` and `UpdateBatch` calls.

- [ ] **Step 1: Write the failing tests** in `fakedb_test.go`:
  - `TestUpdateBatchAppliesEach`: three rows, one of them with a stale ver. The other two are written, the stale one is unchanged, and `err == nil`.
  - `TestUpdateBatchRejectedWritesNothing`: `Reject` matches the second row. The call returns `errors.Is(err, db.ErrRejected)`, and rows one and three are unchanged.
  - `TestUpdateBatchLateExecIsAtomic`: the hook returns `Ambiguous` and keeps `Exec`. Before `Exec` no row has changed; after it, all have, and `Stats().Late == 1`.
  - `TestUpdateBatchOneWriterPerKey`: a batch containing key k, blocked in the hook, plus a concurrent `Update` of k, gives `Violation() != nil`.
  - `TestUpdateBatchUnsorted`: a batch whose keys are not ascending gives `Violation() != nil`, naming the order (8.3).
  - `TestUpdateRejected`: a single `Update` that `Reject` matches gives `ErrRejected`, with the row unchanged.
- [ ] **Step 2:** `go test ./internal/db/fakedb/` fails to compile (no `UpdateBatch`, `Reject`).
- [ ] **Step 3: Implement.**
  - `UpdateBatch` registers `write(key)` for every key and runs one `Stmt{Kind: UpdateBatch, Keys}`.
  - Its exec checks `Reject` on every row first. If one matches, it applies nothing, and the call returns `db.ErrRejected`, wrapped with the key. The exec reports the rejection to `run` through a captured variable, because `run` returns nil after `Exec`. Otherwise it applies each row with `applyLocked`.
  - `Update` checks `Reject` the same way.
  - `run` is not changed: a Hook that returns an error still decides alone.
- [ ] **Step 4:** `go test -race ./internal/db/...` passes.
- [ ] **Step 5: Commit** "M6a: db.UpdateBatch; fakedb batches and the Reject check".

### Task 2: Batch flush, workers, backoff, `dirtySince`, oldest dirty row

**Files:**
- Modify: `internal/store/flush.go`, `internal/store/store.go` (Config, `entry.dirtySince`, `shard.oldestDirty`, `markDirtyLocked`)
- Modify tests that script `fakedb.Update` for flushes, switching them to `fakedb.UpdateBatch`: `flush_test.go`, `delete_test.go`, `stress_test.go`, `update_test.go` (`hang`)
- Test: `internal/store/flush_test.go`

**Interfaces:**
- Consumes: Task 1.
- Produces:
  - `Config.FlushBatchMax int` (default 500) and its validation;
  - `type flushItem struct { e *entry; row *schema.Row; mask uint64; since time.Time }` (7.6);
  - `func (s *Store) flushShard(ctx context.Context, sh *shard) (failed bool)`;
  - `func (s *Store) oldestDirty() time.Time` (zero = none);
  - `s.tooOld atomic.Bool`, refreshed by `RunFlusher` after each round (decision 6).

  Later tasks add `Poisoned`/`MaxDirtyAge` to these paths.

- [ ] **Step 1: Write the failing tests:**
  - `TestFlushBatches`: one shard, `FlushBatchMax` 2, five dirty rows. One `flushAll` gives 3 `UpdateBatch` statements of sizes 2, 2 and 1, each with ascending `Keys`, and the DB matches memory.
  - `TestFlushBatchFailureMergesBack`: the hook fails every `UpdateBatch` with `ErrNotExecuted`. Afterwards every row is still dirty and queued, `dirtyCount` is unchanged, and `checkInvariants` passes. With the hook removed, `drain` writes them all.
  - `TestFlushStopsShardAfterFailedBatch`: `FlushBatchMax` 2, six dirty rows, and the hook fails batches. One `flushAll` sends exactly 1 `UpdateBatch`, and the queue holds all six.
  - `TestFlushBacksOffWhileDBFails` (synctest): `RunFlusher` with `FlushInterval` 200ms and every batch failing. Over 10s of virtual time there are between 3 and 12 `UpdateBatch` calls; without backoff there would be about 50. After the hook is cleared, the row is flushed within `flushBackoffMax + FlushInterval`.
  - `TestOldestDirtyKeepsEarliest` (synctest): row dirty at t0, with every batch failing. After a round, an update at t0+1s and another failed round, `oldestDirty() == t0`.
  - Existing flush tests pass, with `fakedb.Update` replaced by `fakedb.UpdateBatch` where they script the flush.
- [ ] **Step 2:** run `-run 'Flush|OldestDirty'`. Expect failures: no `FlushBatchMax`, `oldestDirty`; one call per row.
- [ ] **Step 3: Implement**, following the 7.6 skeleton.
  - Per item, `F_scan` as now, plus `since := e.dirtySince`.
  - Send at `FlushBatchMax` items and at the end; sort by key before sending. `// TLA: F_send` is on the `UpdateBatch` call and `// TLA: F_done` on the merge-back.
  - Merge back: `markDirtyLocked`, then `dirtySince = min(since, dirtySince)`, `queued`, and enqueue after `mu`.
  - Release each item's `op` after its batch has returned.
  - After a failure, requeue the unscanned rest (decision 3).
  - Then record `sh.oldestDirty` (decision 6).
  - `RunFlusher` has the workers and the backoff (decisions 3 and 4).
  - `markDirtyLocked` sets `dirtySince`.
- [ ] **Step 4:** `go test -race ./...` passes. That includes `TestStressUpdates`, `TestStressCreateDelete` and `TestLinearizable`, whose flushes are now batches through the fault injector.
- [ ] **Step 5: Commit** "M6a: batch flush, flush workers and per-shard backoff".

### Task 3: Poison row quarantine and the admin methods

**Files:**
- Modify: `internal/store/flush.go`, `store.go` (`poisoned`, `poisonVer`, `poisonRetryAt`, the poisoned gauge, `removeLocked`), `evict.go` (`tryEvict` uses `removeLocked`), `delete.go` (clears `poisoned`)
- Create: `internal/store/poison.go` (`Poisoned`, `DiscardPoisoned`)
- Test: `internal/store/poison_test.go`

**Interfaces:**
- Consumes: Task 1 (`Reject`, `ErrRejected`), Task 2 (`flushShard`, `flushItem`).
- Produces:
  - `func (s *Store) Poisoned() []schema.Key`;
  - `func (s *Store) DiscardPoisoned(ctx context.Context, key schema.Key) error` (`ErrNotFound` if `key` is not poisoned);
  - `func (s *Store) removeLocked(sh *shard, e *entry)`, which needs `op` held and `dead` set;
  - `entry.poisoned`, which Task 4 reads.

- [ ] **Step 1: Write the failing tests:**
  - `TestPoisonRowQuarantined` (acceptance). `Reject` matches score == -1. Five rows in one batch, one of them updated to -1; then one `flushAll`.
    - The other four are in the DB with their new values.
    - The poison row is not in the DB, and is still dirty and queued.
    - `Poisoned()` is `[k]`, `Stats().Poisoned` is 1, and `checkInvariants` passes.
  - `TestPoisonNotInNextBatch`: after quarantine, update another row and flush. The `UpdateBatch` statement's `Keys` does not contain the poison key, and there is no solo `Update` of it before `poisonRetryInterval`.
  - `TestPoisonRetriedAfterUpdate`: update the poison row to a valid value; the next `flushAll` writes it with a solo `Update`, after which it is no longer poisoned or dirty, and the DB matches.
  - `TestPoisonRetriedAfterInterval` (synctest): with the value unchanged and `Reject` cleared, no write happens before `poisonRetryInterval`; one happens after it, and clears the quarantine.
  - `TestCrossRowRejectionQuarantines`: the hook answers the batch, and then the poison row's solo `Update`, with `db.ErrCrossRow`. The result is the same as `TestPoisonRowQuarantined` (Review Focus 3).
  - `TestDeletePoisonedRow`: quarantine, then `Delete`. Afterwards `Poisoned()` is empty, `Stats().Poisoned` is 0, the entry is clean, and `evict` removes it (Review Focus 1).
  - `TestDiscardPoisoned`: quarantine, then `DiscardPoisoned`. A `Get` returns the DB's row, with `Selects` up by 1; `dirtyCount` is 0, and `checkInvariants` passes. `DiscardPoisoned` of a key that is not poisoned returns `ErrNotFound`.
  - `TestPoisonedRowNotEvicted`: `evict` of a poisoned row returns false.
- [ ] **Step 2:** `-run 'Poison|Discard'` fails (no `Poisoned`, ...).
- [ ] **Step 3: Implement** decisions 2, 8 and 9.
  - Log the poison with `slog.Warn("flush rejected by the DB; row quarantined", "table", s.schema.Table, "key", k, "err", err)` with no lock held.
  - Keep the gauge in `metrics.poisoned`, changed only with `mu` held, wherever `poisoned` flips.
- [ ] **Step 4:** `go test -race ./...` passes.
- [ ] **Step 5: Commit** "M6a: poison row quarantine (7.9)".

### Task 4: Backpressure by count and by age

**Files:**
- Modify: `internal/store/store.go` (`ErrBusy`, `MaxDirty`, `MaxDirtyAge`, validation), `update.go`, `flush.go` (`tooOld` refresh), `faults_test.go` (`classify`: `ErrBusy` = no effect)
- Test: `internal/store/backpressure_test.go`

**Interfaces:**
- Consumes: Task 2 (`oldestDirty`, `tooOld`, `flushShard`), Task 3 (`poisoned` excluded from `oldestDirty`).
- Produces: `ErrBusy`; `Config.MaxDirty`, `Config.MaxDirtyAge`.

- [ ] **Step 1: Write the failing tests:**
  - `TestBusyWhenDirtyTooOld` (acceptance, synctest). `MaxDirtyAge` 2s, every batch failing, `RunFlusher` running. Update row a once, and row b every 500ms.
    - Before 2s, every update succeeds.
    - After 2s + `flushBackoffMax` + `FlushInterval`, an update of a, of b, or of a new clean row c returns `ErrBusy`.
    - After the hook is cleared and the rows are flushed, the next round's update succeeds again.
    - The update of b between failed rounds pins Review Focus 4.
  - `TestPoisonAloneDoesNotTriggerBusy` (acceptance, synctest). `MaxDirtyAge` 2s, `Reject` on one row's value, `RunFlusher` running. After 10s, updates of other rows still succeed, and `Stats().OldestDirtyAge < 2s`.
  - `TestBusyWhenTooManyDirty`: `MaxDirty` 2, no flusher. Updates of clean rows a and b succeed; an update of clean row c returns `ErrBusy`; a further update of dirty row a succeeds. `Stats().BusyCount == 1`.
  - `TestBusyChangesNothing`: in both busy cases, the row's `snap`, `bookkeeping` and `dirtyCount` are as before the call. `Get`, `Create` and `Delete` succeed meanwhile (Review Focus 5).
  - `TestMaxDirtyAgeZeroDisables` (synctest): `MaxDirtyAge` 0, every batch failing; after 1 minute, updates succeed.
  - `TestNewRejectsBadConfig` gains `FlushBatchMax 0`, `MaxDirty 0` and `MaxDirtyAge -1s`.
- [ ] **Step 2:** `-run 'Busy|MaxDirty|NewRejects'` fails.
- [ ] **Step 3: Implement** decisions 6, 7, 11 and 12.
- [ ] **Step 4:** `go test -race ./...` passes.
- [ ] **Step 5: Commit** "M6a: backpressure by count and by age (11.1)".

### Task 5: Metrics

**Files:**
- Create: `internal/store/stats.go` (`Stats`, `metrics`, `(*Store).Stats`)
- Modify: the counting sites in `get.go`, `store.go` (`lockOp` wait), `create.go`, `delete.go`, `evict.go`, `flush.go`, `update.go`
- Test: `internal/store/stats_test.go`

**Interfaces:**
- Consumes: everything above.
- Produces:

```go
type Stats struct {
	Entries, Absent, Dirty, Poisoned        int64         // gauges; Absent = tombstones + negative-cache entries
	OldestDirtyAge                          time.Duration // of rows not poisoned, as of each shard's last flush; 0 if none
	BusyCount, BusyAge                      int64         // updates rejected by MaxDirty, by MaxDirtyAge
	FlushBatches, FlushRows, FlushFailures  int64         // batch statements, rows in them, batches that failed
	FlushTime                               time.Duration // total time in UpdateBatch
	Hits, Misses                            int64         // Gets answered from memory, Gets that loaded
	LoadErrors                              int64
	OpWaits                                 int64         // lockOp calls that had to wait
	OpWaitTime                              time.Duration
	Ambiguous, OutcomeUnknown, GenClaimed   int64         // create/delete attempts; ErrOutcomeUnknown answers; ErrGenClaimed
	Internal                                int64         // ErrInternal answers: must stay 0
	Fences, FencesUnconfirmed, Evictions    int64
}
func (s *Store) Stats() Stats
```

- [ ] **Step 1: Write the failing test** `TestStats`: a scripted sequence where each step moves exactly the counters it names, and nothing else. Each `Stats()` is compared field by field with the expected struct.
  1. A Get that loads: `Misses` 1, `Entries` 1.
  2. The same Get again: `Hits` 1.
  3. A Get of a missing key: `Absent` 1.
  4. An Update: `Dirty` 1.
  5. A flush: `FlushBatches` 1, `FlushRows` 1, `FlushTime > 0`.
  6. A failed flush: `FlushFailures` 1.
  7. A Select error: `LoadErrors` 1.
  8. An ambiguous Create attempt: `Ambiguous` 1.
  9. A create answered `ErrGenClaimed`: `GenClaimed` 1.
  10. An evict: `Evictions` 1.
  11. A Get blocked behind a held `op`: `OpWaits` 1.
  12. A fence: `Fences` 1.

  `OutcomeUnknown` and `FencesUnconfirmed` use the M3 tests' scripts. `TestPoisonRowQuarantined` and `TestBusyWhenTooManyDirty` already assert `Poisoned` and `BusyCount`.
- [ ] **Step 2:** fails to compile.
- [ ] **Step 3: Implement** decision 10. Each counter is one `Add` at its site; `lockOp` measures time only when its first non-blocking try fails.
- [ ] **Step 4:** `go test -race ./...` passes.
- [ ] **Step 5: Commit** "M6a: Store.Stats (11.2)".

### Task 6: Mapping, acceptance, mutations

- [ ] **Step 1:** Update `tla-mapping.md`:
  - `F_send` (the batch, sorted; `requeueAll(retry)` before; definite rejection → row-by-row);
  - `F_done` (merge-back with `dirtySince`);
  - section 1: the model's single-row flush against the Go batch. The batch is one transaction, all or nothing, and every row in it is the row's own `F_send`/`F_done`; poison quarantine and backpressure are already listed as not modeled.

  Commit "M6a: tla-mapping for the batch flush".
- [ ] **Step 2: Acceptance** (12.2 M6, the flush-path half):
  - `go test -race -count=50 -run 'PoisonRowQuarantined|BusyWhenDirtyTooOld|PoisonAloneDoesNotTriggerBusy' ./internal/store/`;
  - `go test -race -count=100 -run 'TestStressCreateDelete$|TestStressUpdates$' ./internal/store/`;
  - `go test -race -count=200 -run 'TestLinearizable$' ./internal/store/`.

  All pass.
- [ ] **Step 3: Mutations**, each a temporary patch, a failing named test, then a revert:

| Mutation | Patch | Must fail |
|---|---|---|
| `NoVerCheck` | fakedb `applyLocked` drops `cur.Ver >= r.Ver` | `TestLateFlushDoesNotOverwriteNewer` |
| `FlushUpsert` | `applyLocked` inserts a missing row | the flush-late-after-delete test of M3 |
| `NoMaskMerge` | no `markDirtyLocked` on a failed batch | `TestFlushBatchFailureMergesBack` |
| `FlushNoOp` | `flushShard` skips `tryLockOp` | the M3 flush-snapshot failpoint test |
| `SinceNotKept` | the merge-back does not restore `since` | `TestOldestDirtyKeepsEarliest`, `TestBusyWhenDirtyTooOld` |
| `PoisonCountsForAge` | `oldestDirty` includes poisoned rows | `TestPoisonAloneDoesNotTriggerBusy` |
| `NoIsolation` | a definite rejection is handled as an ordinary failure | `TestPoisonRowQuarantined` |
| `BatchNotAtomic` | fakedb applies the rows before the `Reject` check | `TestUpdateBatchRejectedWritesNothing` |
| `BusyIgnoresDirtyRow` | the count check ignores `e.dirty == 0` | `TestBusyWhenTooManyDirty` |
