---
aliases: [Alternative Design, Write-Through Row Service, DB-authoritative baseline]
tags: [design, alternative, trade-off, write-through, postgresql]
created: 2026-11-13
---

# Alternative Design: DB-Authoritative Write-Through Cache

> [!info] What this document is
> A comparison between the design in [design.md](../memory-authoritative/design.md) (memory is authoritative, updates are written behind) and a simpler alternative (PostgreSQL stays authoritative, every write goes to the DB before it is acknowledged, memory is only a cache). It records a design review discussion of 2026-11-13 so that the choice can be revisited later.
>
> **Where things are.** Each design has its own directory, so that both can be implemented and compared: this one is `db-authoritative/`; the other is [`memory-authoritative/`](../memory-authoritative/), which holds `design.md`, `correctness-properties.md` and the TLA+ model in `tla/`. Section, invariant and hole numbers below refer to those documents.
>
> **Status of the alternative: a sketch.** It has no TLA+ model and no benchmark. Every throughput or latency figure below is an estimate to be replaced by the measurement in section 8.

---

## 1. Summary

| | design.md (write-behind) | This alternative (write-through) |
|---|---|---|
| Who is authoritative | Memory. The DB is a near-real-time mirror. | The DB. Memory is a cache that can be dropped at any time. |
| Update returns | After memory is changed (microseconds) | After the DB accepted the statement (one round trip, expected 0.2-1 ms) |
| DB writes per row | At most one per flush interval, however many updates | One per update |
| A service crash loses | Updates not yet flushed: 200 ms normally, up to `MaxDirtyAge` (30 s) while the DB cannot be written | Nothing |
| A PG or host crash loses | Nothing the DB confirmed | With `synchronous_commit = off`: up to about 600 ms of updates. With `on`: nothing |
| Worst outcome of a bug or a broken assumption | The authoritative copy is silently and permanently wrong | A cache entry is stale until it is reloaded |
| Mechanisms needed | All of design.md | About half: the whole write-behind half disappears (section 4) |

**Decision rule.** Choose the alternative if both hold:

1. An update latency of about 1 ms at p99 is acceptable.
2. No single row needs more than roughly a thousand updates per second.

Otherwise keep design.md. Reads do not enter the decision: the cache hit path is identical in both designs.

The workload stated on 2026-11-13 was "reads above ten thousand per second on one machine, updates a few thousand per second". That update rate is expected to be within reach of the alternative, but this has not been measured.

---

## 2. The one difference that matters

In design.md an update is acknowledged before the DB has it. Memory is therefore ahead of the DB, and that single fact creates a chain of obligations:

- Unflushed data must not be lost, so there is a dirty bitmap, a flush queue, a mask that is merged back on failure, a rule that dirty entries are never evicted (I10), and backpressure (11.1).
- The DB writes happen later and on other connections, so their order is not controlled. This needs `ver` as a fencing token and forbids `col = col + delta` (I12, 8.2 example 1).
- The DB can reject a value only after the client was told "success", so there are poison rows, a quarantine and an admin interface (7.9).
- After a crash the memory is gone while the old process's SQL is still in flight, which is hole 3 and the reason for the startup fence (7.8).
- Memory and the DB must match byte for byte, or a value changes when an entry is evicted and reloaded.

In the alternative nothing is acknowledged before the DB has accepted it. Memory is never ahead of the DB, every entry is disposable, and none of the obligations above exists.

The alternative does **not** remove the other source of complexity in design.md: a create or delete sent over an unreliable network can time out, execute late, or be retried by the client. Holes 1 and 2 come from that, not from memory being authoritative, and they remain (section 5).

---

## 3. The alternative in outline

Everything not mentioned here stays as in design.md: the external protocol and SDK (7.10), dynamic schemas and primary key encoding (5.3), entries, shards, immutable snapshots, the `op` and `mu` locks, the dead protocol and sampled eviction (7.7), tombstones and the negative cache.

### 3.1 One rule for every write

Create, delete **and update** follow the rule that design.md applies to create and delete only (I7):

1. Take the key's `op` lock.
2. Send one statement to the DB and wait for a definite outcome.
3. Store the row the DB returned into the snapshot, inside `mu`. This is the linearization point.
4. Release `op` and answer the client.

```sql
-- update: the DB increments ver; an increment can be written as a delta, because the statement runs exactly once
UPDATE items SET score = score + $1, ver = ver + 1
WHERE id = $2 AND gen = $3
RETURNING id, gen, ver, name, score
```

- `gen` in the WHERE clause is the gen of the loaded snapshot (update loads first, as create does today).
- Zero rows affected is now an error, not a normal case: while `op` is held and the entry is loaded, the row must be there. Treat it as `ErrInternal`, drop the entry and reload.
- The cache stores exactly what `RETURNING` gives, so it cannot differ from the DB in rounding, padding or JSON formatting.
- A constraint violation is returned to the caller synchronously as an ordinary error.

Updates use a connection pool whose sessions have `synchronous_commit = off`; creates and deletes use a second pool with the default `on`. With pgx this is `ConnConfig.RuntimeParams["synchronous_commit"] = "off"` on the first pool. Whether `off` is needed at all is a measurement question (section 8).

### 3.2 Ambiguous outcomes: fence the connection, then read

When a statement times out or its connection breaks, the outcome is unknown. design.md retries until sure and relies on gen/ver and the dedup table to make late statements harmless. The alternative uses what design.md 14.8 calls path 1, which design.md could not afford on the update path because an update there must not wait:

1. Terminate the backend that ran the statement and confirm that it is gone. The backend pid is known before the statement is sent (`conn.PgConn().PID()`); identify the session by (pid, `backend_start`) because pids are reused.

   ```sql
   SELECT pg_terminate_backend($1, 5000);
   SELECT count(*) FROM pg_stat_activity WHERE pid = $1 AND backend_start = $2;  -- repeat until 0
   ```

2. The statement has now either committed or will never execute (design.md 7.8, correctness-properties.md A4). SELECT the row, store it, and decide the answer from what is there.
3. `op` stays held throughout. If the client's deadline expires first, it gets `OUTCOME_UNKNOWN` and the work continues in the background, as `runSync` does today.

This gives one invariant that replaces several mechanisms: **a statement for a key is in flight only while that key's `op` is held.** Late statements do not exist once `op` is released, so:

- 8.2 examples 1 to 3 and hole 1 cannot occur inside one process.
- Hole 4 cannot occur: there is no "definite failure after an ambiguous attempt" to interpret, only a read of the settled state.
- The `unsure` flag and the split of definite failures into "not executed" and "rejected by data" (8.4) are not needed.

The startup fence (7.8) is kept. It extends the same invariant across a restart, and it is a few lines of code that are already designed and verified.

### 3.3 Eviction

Any entry whose `op` is free may be evicted. There is no dirty condition. The dead protocol is unchanged.

---

## 4. Mechanism by mechanism

| design.md mechanism | In the alternative |
|---|---|
| Dirty bitmap, flush queue, flusher, mask merge-back (7.6) | Gone |
| Backpressure on dirty rows, `MaxDirty` and `MaxDirtyAge` (11.1) | Gone. The connection pool and `MaxInflightPerConn` limit load |
| Poison rows, quarantine, admin interface (7.9) | Gone. Constraint errors are returned synchronously |
| "Dirty entries are never evicted" (I10) | Gone |
| `ver` as a fencing token, `AND ver < ?` (I6, 8.2 example 1) | Gone. `ver` remains as a counter the DB increments, for observation only |
| Flush writes absolute values, no `col = col + delta` (I12) | Gone. A delta in SQL is correct |
| Retry until sure, the `unsure` and `inflight` flags, three kinds of definite failure, the session fence used only after a rejection caused by another row (7.4, 7.5, 8.4, holes 4 and 5) | Replaced by one rule: "fence the connection, then read", on every ambiguous outcome (3.2) |
| Owner epoch in every write (I16, hole 8) | Optional. A second instance makes cache entries stale; it cannot corrupt the authoritative copy. Keeping the epoch is cheap and still stops the old instance |
| Late statements blocked by gen conditions (8.2 examples 2, 3; hole 1) | Cannot occur (3.2). `gen` in WHERE stays as a cheap second line of defence |
| Hole 3 (old process's late UPDATE diverges memory and DB forever) | Cannot diverge the authoritative copy. The startup fence is kept so that the cache is not stale either |
| Exact value agreement between memory and DB | Automatic: the cache holds the `RETURNING` row |
| Flush interval, batch size, HOT tuning to limit write amplification (8.5) | No flush settings. HOT and autovacuum tuning matter more, see section 6 |
| `gen` as the identity of one life; optional `expected_gen` (7.3, 7.5) | Unchanged |
| Dedup table, create loads first (I14, hole 2) | Unchanged, see section 5 |
| Startup fence, dedicated DB account (I15, 7.8) | Unchanged |
| `op`, `mu`, immutable snapshots, lock-free Get, dead protocol, eviction sampling | Unchanged. Update now also takes `op` |
| External protocol, SDK, schema loading | Unchanged |

Invariants of design.md section 4: I10 and I12 disappear. I4 simplifies to "while `op` is free, a loaded entry equals the DB row". I5 becomes the stronger statement in 3.2. I7 extends to update. The others are unchanged.

For the TLA+ model: the flusher, `dirty`, masks and "a crash loses unflushed updates" can be deleted. Create, delete, the dedup table, client retries, eviction and the fence stay. The alternative needs its own (smaller) model before it is implemented.

---

## 5. What does not get simpler

These come from clients retrying creates and deletes over an unreliable network, which is the same in both designs:

- **Hole 2 and the dedup table.** A create whose response was lost is retried by the SDK with the same gen. If someone deleted the row in between, only a durable record of "this gen was already inserted" stops the retry from inserting it again.
- **SDK retry rules.** Once any attempt of one logical operation had an unknown outcome, a later answer obtained before `op` was acquired (`DEADLINE_EXCEEDED`, `UNAVAILABLE`, `BUSY`) says nothing about the earlier attempt. A retried create must also be answered "success" when its gen is in the dedup table but the row now belongs to another life. Both points were fixed in design.md on 2026-11-13 (holes 6 and 7 of its 13.2) and apply here unchanged.
- **Single instance.** A linearizable cache still needs one instance per key. What changes is the consequence of a second instance: stale reads, where design.md needs the owner epoch (hole 8) to keep two authorities from overwriting each other.
- **Only this service writes the tables.** Another writer makes cache entries stale. The DB stays correct.

---

## 6. Costs of the alternative

- **Update latency.** One DB round trip per update instead of a memory write. Expected 0.2 to 1 ms in one data centre; to be measured.
- **Hot keys.** Updates of one key are serialized by `op` across the round trip, so one key takes at most 1 / round-trip-time updates per second. At 0.3 ms that is about 3,000 per second on a single key. design.md has no such limit and writes a hot row once per flush interval.
- **No write coalescing.** Every update produces WAL and a dead tuple. At 3,000 updates per second that is about 260 million row versions per day. HOT updates (fillfactor, no index on updated columns) and aggressive autovacuum settings become requirements, not optimizations.
- **DB outage.** Updates fail while the DB is unavailable. design.md keeps accepting them until `MaxDirty` or `MaxDirtyAge` (30 s by default) is reached. Cached reads work in both.
- **PG crash with `synchronous_commit = off`.** PG may lose the last moments of acknowledged updates (up to three times `wal_writer_delay`, 600 ms with defaults), and the cache then holds values the DB no longer has. The service must drop the whole cache when PG has restarted: compare `pg_postmaster_start_time()` on every new connection with the value seen at startup, and on a change clear every entry (or exit and let the supervisor restart it). With `synchronous_commit = on` this case does not arise.
- **The stated reason for a custom TCP protocol weakens.** design.md 7.10 chose it because an update costs a few microseconds and protocol overhead is a large share. With a DB round trip per update that argument no longer holds, and gRPC would be an acceptable simplification. This is optional.

---

## 7. Effect on the review findings of 2026-11-13

The review of design.md found five gaps. Four of them were fixed in design.md on the same day and are now its holes 5-8 (13.2), each reproduced and checked in the TLA+ model; the fourth finding was deliberately left open (13.3). The table records what each finding was, what design.md does about it now, and what becomes of it in the alternative.

| # | Finding in design.md | Now in design.md | In the alternative |
|---|---|---|---|
| 1 | "Rejected by data" is treated as final after an ambiguous attempt, on the argument that the same parameters give the same result. Foreign keys, non-primary unique indexes, exclusion constraints and triggers depend on DB state, so the earlier statement can still land later. Also, SQLSTATE 23505 from a non-primary unique index is mapped to `ErrInternal`. | Fixed (hole 5): such tables are refused unless enabled per table; with the option, the sessions of the ambiguous attempts are terminated before the rejection is trusted | Gone: the ambiguous statement is fenced before anything is decided (3.2), and a unique violation is an ordinary synchronous error |
| 2 | SDK retries: a retried create can be answered `ALREADY_EXISTS` although it took effect; a retry that times out while queued behind the first attempt is reported as "did not take effect". No property checks the responses of create and delete. | Fixed (holes 6 and 7; property S18) | The same fixes apply unchanged |
| 3 | "One instance at a time" is guaranteed only by deployment, and a violation silently corrupts the authoritative copy. | Fixed (hole 8): an owner epoch checked by every write | Still an assumption, but a violation causes stale reads, not corruption. The epoch check becomes optional |
| 4 | Values PG normalizes (float4, numeric scale, char(n) padding, timestamp precision, jsonb) differ between memory and DB, so a Get can change after eviction with no write. | Not fixed (13.3) | Gone: the cache holds the `RETURNING` row |
| 5 | The loss window grows without bound while the DB is slow; `synchronous_commit = on` and "restart the service after a PG failover or restore" are unstated assumptions. | Fixed: `MaxDirtyAge` caps the window; the PostgreSQL assumptions are stated in section 2 | The loss window is bounded by PG, not by load. The failover and restore assumption remains, and the PG crash rule of section 6 is added |

The fixes cost design.md three more mechanisms (the per-operation session fence, the owner epoch, the age limit). That widens the difference in size between the two designs a little; it does not change the decision rule of section 1.

---

## 8. How to decide: measure

Run against the PG instance and hardware the service will use, at the target rate rather than at maximum throughput, and look at latency.

```sql
-- setup.sql: the sample table of design.md 5.1
CREATE TABLE items (id bigint PRIMARY KEY, row_gen bigint NOT NULL DEFAULT 0,
                    row_ver bigint NOT NULL DEFAULT 0, name text, score bigint NOT NULL DEFAULT 0)
  WITH (fillfactor = 80);
INSERT INTO items (id, name) SELECT g, 'n' || g FROM generate_series(1, 1000000) g;
```

```sql
-- update.sql: uniform keys. For a skewed load use: \set id random_zipfian(1, 1000000, 1.2)
\set id random(1, 1000000)
UPDATE items SET score = score + 1, row_ver = row_ver + 1 WHERE id = :id RETURNING *;
```

```sh
# 3000 updates/s for 5 minutes, per-transaction log for percentiles
PGOPTIONS='-c synchronous_commit=off' pgbench -n -f update.sql -c 32 -j 8 -R 3000 -T 300 -P 10 -l <db>
# repeat without PGOPTIONS to see whether synchronous_commit=on is already good enough
# hot key: replace the \set line with "\set id 1" and run with -c 1 to get the single-key ceiling
```

Read the results as follows:

| Observation | Conclusion |
|---|---|
| p99 at the target rate is within the latency the business accepts, with `synchronous_commit = on` | Use the alternative with `on`: no update is ever lost and the PG crash rule of section 6 is not needed |
| Acceptable only with `off` | Use the alternative with `off` and implement the PG crash rule |
| The single-key ceiling is below what the hottest row needs | Keep design.md, or see the hybrid in section 9 |
| p99 is not acceptable in any setting | Keep design.md |

Also watch `pg_stat_user_tables.n_tup_hot_upd / n_tup_upd` and table size during the run; a low HOT ratio or steady growth means the vacuum settings need work before the result can be trusted.

---

## 9. Variants

- **No cache at all.** If point reads by primary key straight from PG meet the read latency target, the service becomes a stateless proxy: no entries, locks, eviction, single-instance assumption or startup fence; several instances can run. Only gen, the dedup table and the SDK retry rules remain. Worth one more `pgbench` run with a `SELECT ... WHERE id = :id` script at the read rate.
- **Updates without `op`.** To lift the hot-key ceiling, updates of one key could run concurrently and let PG's row lock order them, installing a returned row only if its `ver` is newer. This reintroduces races with load and delete (a load that read the old row must not overwrite a newer installed one) and must be model-checked. Do not start here.
- **Hybrid.** Both designs share the protocol, schema layer, entry and lock structure, eviction, dedup table and fence. A table, or a set of columns, could be switched to write-behind later if it proves too hot for write-through. Starting with the alternative and adding write-behind where measurement demands it is cheaper than the reverse.

---

## 10. If the alternative is adopted

1. Run the measurement of section 8 and record the numbers in design.md section 1, together with the target rates and latencies, which design.md does not state today.
2. Write the TLA+ model for section 3 (start from `../memory-authoritative/tla/RowService.tla`, put the copy in `db-authoritative/tla/`, delete the flusher and dirty state, add the connection fence) and carry over the properties of correctness-properties.md that still apply, including S18 on what create answers. Check each step with `python3 suite.py quick` (correctness-properties.md 9.1).
3. Keep the fixes for holes 6 and 7 (the dedup lookup in `createLocked`, the SDK rule after an ambiguous attempt); they are independent of this choice.
4. Rewrite design.md sections 7.3, 7.6, 7.9, 8.2, 8.4 and 11.1 accordingly; sections 5.3, 6, 7.2, 7.7, 7.8 and 7.10 need little or no change.
