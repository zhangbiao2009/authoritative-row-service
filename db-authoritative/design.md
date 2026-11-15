---
aliases: [DB-Authoritative Row Cache Design, Write-Through Row Service, rowcache]
tags: [design, concurrency, go, write-through, postgresql, linearizability]
created: 2026-11-14
---

# DB-Authoritative Row Service: Design Document

> [!info] Purpose of this document
> This is the implementation design of the DB-authoritative row service: PostgreSQL is authoritative, every write reaches the DB before it is acknowledged, and memory is a cache of rows. It is handed to Claude Code for implementation and is meant to be read on its own. Section 0 is the working agreement for the implementer.
>
> **Related documents.** [alternative-design.md](alternative-design.md) is the record of how this design was chosen: the comparison with the memory-authoritative design, its costs, how to measure, and the full TLA+ results (its section 11). This document turns that sketch into a design that can be implemented, and records the decisions of 2026-11-14 (13.1) that go beyond it. Where the two disagree, this document is the one to follow. The other design, [../memory-authoritative/design.md](../memory-authoritative/design.md), is referred to only where code is copied from it (M1) or where a part is identical by decision (the external protocol, 7.9).
>
> **Status.** The protocol is modeled in [tla/RowCache.tla](tla/RowCache.tla) and checked with TLC (12.3). Nothing has been measured: every latency or throughput figure is an estimate (alternative-design.md 8).

---

## 0. Working agreement for the implementer (Claude Code)

1. Read the whole document first, especially **section 4 (invariants)**, **section 6 (locks)** and **7.3 (the one rule for every write)**.
2. Proceed by the **milestones in 12.2**, one at a time. Every milestone must pass `go test -race` and the tests required for it.
3. Write the invariants of section 4, and the role of each lock, into code comments.
4. The following are **deliberate design choices**. Each was shown necessary by the TLA+ model (or is kept on purpose, as said); do not remove them for the sake of "simplification":
   - A write (create, update, delete) holds the key's `op` from before its statement is sent until the outcome is settled. **`op` is never released while a statement of this key may still execute** (I5).
   - On an ambiguous outcome: **terminate the session the statement was sent on and confirm it is gone, then read the row, then decide the answer from what is there** (7.3). Never skip the fence, never answer "failed" without the read, never release `op` and answer "unknown" while the statement may still execute. Nothing is retried inside the server.
   - After the fence, an update took effect only if the row has the same gen and `ver` = the loaded `ver` + 1. "The row is there" is not enough. The read after the fence is trusted only if the owner check that follows it passes (7.3).
   - `op` is not released by a write abandoned at shutdown (`errAbandoned`, 7.3), and not by a panic.
   - Zero rows affected by an UPDATE or DELETE, or a primary key conflict on INSERT, cannot happen while `op` is held and the entry is loaded: check the owner epoch, then treat it as `ErrInternal` and drop the snapshot (7.3).
   - The snapshot holds exactly the row the DB returned (`RETURNING`, or the read after a fence), never a row assembled in Go.
   - DB writes are conditional: UPDATE and DELETE carry `AND gen = <gen of the loaded snapshot>` (kept as a second line of defence; the model passes without it, 12.3). **Upsert is forbidden.**
   - Create loads first; inserting the business row and writing the dedup record are one statement; deleting the business row does not delete the dedup record (I12). A create whose gen finds another life looks the gen up in the dedup table before answering `ALREADY_EXISTS`.
   - At startup: take over the owner epoch, then terminate the previous process's sessions and confirm they are gone, and only then load data (I13, I14, 7.8).
   - Every write carries the owner-epoch condition; an instance that finds another epoch stops at once (I14).
   - Load re-checks `snap` after taking `op` (saves a query; the model passes without it).
   - Tombstones (logical delete) and the dead protocol (physical delete) stay separate.
   - Requests on one connection are processed concurrently and answered out of order (7.9).
   - The SDK never reports a "not executed" status as the final result of a call in which an earlier attempt was ambiguous (7.9).
5. If something in the design does not make sense or is missing, or the design, the model and the code disagree: stop and ask. Do not change this document, the model or the properties on your own.

---

## 1. Background and requirements

- **PostgreSQL is the authoritative copy** of the data. The service is a cache in front of it that serves reads from memory and makes every write synchronously in the DB.
- The data is **a row (several columns)** of a business table, not plain key-value.
- Four operations:

| Operation | Returns | Note |
|---|---|---|
| Get | On a cache hit, from memory; on a miss, after loading from the DB | Reads are the frequent operation: above 10,000/s on one machine (stated on 2026-11-13) |
| Update | After the DB has **definitely** applied it | A few thousand per second (stated); one DB round trip each |
| Create | After the DB has **definitely** inserted it | |
| Delete | After the DB has **definitely** deleted it | |

- An Update changes **one or a few columns** of a row, by assignment (SET) or increment (INCR, adding a delta to the old value, 7.4).
- The service manages **several tables** whose structure is **dynamic**: it reads the structure from PG's system catalogs at runtime. Each table must have the gen and ver columns (names configurable, 5.1).
- Primary keys may be integers, strings, UUIDs, or composite (5.3).
- **A service crash loses nothing that was acknowledged.** Every acknowledged write is committed in PG with `synchronous_commit = on` (section 2).
- **Not tolerable**:
  - A confirmed create disappearing
  - Data whose delete has been confirmed reappearing
  - An update answered "ok" that did not take effect, or an update answered "failed" that did (or does later)
  - Data of an old life written into a new life re-created with the same primary key
  - A Get returning a value that is not the current one (the cache is linearizable, section 9)
- External protocol: a custom TCP protocol with protobuf message bodies, identical to the memory-authoritative design (7.9). Whether gRPC replaces it is decided before M8 (13.4).
- Language: Go 1.25.

**When this design is the right one** (alternative-design.md 1): an update latency of about 1 ms at p99 is acceptable, and no single row needs more than about a thousand updates per second (updates of one key are serialized across the DB round trip, 6.3). Neither has been measured yet.

---

## 2. Assumptions

- **These tables are written only by this service.** Another writer makes cache entries stale until they are reloaded; the DB stays correct.
- **One instance at a time.** v1 is a single-instance deployment. A second instance cannot corrupt the DB, but it makes the cache of the other one stale and can fool the update rule of 7.3; the owner epoch (I14) makes the older instance stop. Multi-instance routing is later work (11.3).
- **The DB account is used by this service only**: at startup the service terminates every other session of that account (7.8). The DB also holds a dedup table and an owner-epoch table managed by this service (5.1).
- **A transaction PostgreSQL has acknowledged is never lost, and the DB never goes back in time under a running service:**
  - The service's sessions run with `synchronous_commit = on`, set at the role level (`ALTER ROLE <service account> SET synchronous_commit = on`) so a changed server default cannot switch it off. Decided on 2026-11-14 (13.1): the model covers only this case, and with `off` a PG crash could drop acknowledged writes and leave the cache ahead of the DB.
  - A standby that can be promoted must be synchronous.
  - After a failover or restore, restart the service. The service also detects a changed timeline and stops (7.8).
- **Memory holds only the working set**: entries idle for `IdleTTL` are dropped; above `MaxEntries` entries are evicted by approximate LRU; cold rows are loaded on first access. Any entry whose `op` is free may be evicted (7.7).
- **PostgreSQL 14 or later**, in docker, connected directly (no PgBouncer), driver `github.com/jackc/pgx/v5`.

---

## 3. Overall structure

```
Service
 ├── TCP server                  external protocol: length prefix + protobuf, multiplexed (7.9)
 ├── tables map[tableName]*Store loaded at startup from the configured table list (5.3)
 │    └── Store (one table)
 │         ├── schema            immutable at runtime
 │         └── shards[N]         N a power of 2 (e.g. 256), chosen by maphash(key) & (N-1)
 │              ├── mu (RWMutex) protects the map structure; leaf lock
 │              └── m  map[Key]*entry
 ├── evictor                     drops idle entries; evicts by approximate LRU above MaxEntries
 └── DB interface                PostgreSQL (pgx) + fakedb for tests
```

There is no flusher, no flush queue and no dirty state: memory is never ahead of the DB.

Each key has one **slot** (entry) in memory. Every write replaces the immutable snapshot pointer in that slot, always while holding the key's `op`:

```
nil    → Row / absent   (load)
absent → Row            (create)
Row    → Row'           (update)
Row    → absent         (delete, tombstone)
```

**Directory layout.** `db-authoritative/` is its own Go module (`module rowcache`), independent of `memory-authoritative/` (decided 2026-11-14, 13.1). Code that is the same in both designs is copied in M1 and then owned here; nothing is imported across the two modules.

```
db-authoritative/
  go.mod                    module rowcache
  design.md, alternative-design.md, tla/
  api/rowcache/v1           .proto and generated Go code (M8)
  cmd/server                entry point: config, owner epoch and startup fence (7.8), schemas, Store, TCP server
  internal/server           TCP server: framing, connections, dispatch, Status mapping (M8)
  internal/schema           schema, type conversion and validation, primary key encoding, increment arithmetic
                            (copied from memory-authoritative/internal/schema in M1)
  internal/store            entry, shard, Get/Create/Update/Delete, eviction
  internal/db               DB interface, error classification, PostgreSQL implementation (M7)
  internal/db/fakedb        fault-injecting fake DB (tests)
  client                    Go SDK (M8)
  deploy                    docker compose: PG for development and integration tests
```

---

## 4. Core invariants

All code must keep these invariants, and they must be written in code comments. The last column names the TLA+ property that checks it, where there is one (12.3); the memory-authoritative counterpart is in parentheses.

| ID | Invariant | Checked by |
|---|---|---|
| I1 | Each key has **at most one** entry in the map. An entry that is not dead is always in the map; a dead entry stays in the map only briefly before it is removed. (I1) | `LiveEntryInMap` |
| I2 | A published `*Row`, including its `Vals` slice and every value in it, is **never modified**. A change creates a new Row and replaces the pointer. (I2, I11) | |
| I3 | `snap` is written only while holding the entry's `op`. This covers every transition, including Row → Row. (I3, stronger: there is no `mu`) | |
| I4 | **While `op` is free, a loaded snapshot equals the DB row exactly**: existence, gen, ver and every value. While `op` is held, the snapshot is the row as it was before the holder's statement, until the holder stores the result. (I4) | `CacheCoherent` |
| I5 | **A write statement of this instance for a key is in flight only while its sender holds that key's `op`.** Hence at most one write per key is awaiting its result, and no statement of this instance lands after `op` is released. (I5, replaces the gen/ver fencing) | `StmtOnlyUnderOp`, `OneWriterPerKey` |
| I6 | DB writes are conditional: `UPDATE ... WHERE <pk> AND gen = ?`, `DELETE ... WHERE <pk> AND gen = ?`; INSERT relies on the primary key and the dedup table. The DB increments `ver` (`ver = ver + 1`). **Upsert is forbidden.** (I6) | `DbVerMonotonic`, `DbValsBelongToGen` |
| I7 | A write changes the snapshot only after its DB outcome is definite. The new snapshot is the row the DB returned; storing it is the linearization point and happens before the answer. (I7, extended to update) | `MemMatchesAbs`, `UpdateAnswerSound` |
| I8 | `op` is the only lock that may be held while another is acquired, and the only other lock is `shard.mu`, a leaf lock. (I8) | |
| I9 | `dead` is read and written only while holding `op`. Lock-free readers never read it. (I9) | |
| I10 | The schema is immutable at runtime. `Row.Vals` is indexed by the schema's column ordinals. (I11) | |
| I11 | Primary key columns cannot be changed by update: `Row.Key` and the primary key values in `Vals` always agree. (I13) | |
| I12 | A given (table, primary key, gen) is inserted at most once in its lifetime: the business row and the dedup record are written by the same statement, and deleting the business row does not delete the dedup record. (I14) | `NoResurrection`, `CreateFailureIsFinal` |
| I13 | Before the service loads any data, every session of the previous process has exited, so no statement it sent can still land. (I15) | `FencedRestart` |
| I14 | A write changes the DB only while the owner-epoch row holds this instance's epoch: every INSERT, UPDATE and DELETE carries that condition. A starting instance takes the epoch over before its startup fence; an instance that finds another epoch stops at once. (I16) | `DbMatchesAbs` under `ext_epoch` |

Memory-authoritative's I10 (dirty entries are not evicted) and I12 (absolute values in flushes) have no counterpart: there is no dirty state and no flush.

---

## 5. Data structures

### 5.1 Table structure

Each business table gets two columns (names configurable, default `row_gen` / `row_ver`; the SQL below writes them as gen / ver):

```sql
ALTER TABLE items
  ADD COLUMN row_gen BIGINT NOT NULL DEFAULT 0,  -- incarnation: a new value on each create, compared for equality
  ADD COLUMN row_ver BIGINT NOT NULL DEFAULT 0;  -- version: the DB adds 1 on each update
ALTER TABLE items SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.01);  -- every update is a DB write (8.5)
```

- Existing rows have gen 0. gen only needs to differ from the other lives of the same key; new rows get a non-zero random gen.
- `ver` tells whether an ambiguous update took effect (7.3). It is also returned to clients for observation. Unlike the memory-authoritative design, `ver` never goes backwards: nothing acknowledged is lost.
- Do not index gen, ver, or frequently updated columns: every update must be HOT where possible (8.5).

A **dedup table** records which (table, primary key, gen) have been inserted (I12; holes 2 and 6 of 13.2):

```sql
CREATE TABLE row_service_gen_log (
  tbl        text        NOT NULL,                -- business table name
  pk         bytea       NOT NULL,                -- canonical encoding of the primary key (Key, 5.3)
  gen        bigint      NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tbl, pk, gen)
);
CREATE INDEX ON row_service_gen_log (created_at);  -- for cleanup by retention period
```

- Each create writes one record in the same statement as the business row (8.3). Deleting the business row does not delete it.
- A background task deletes records older than `GenLogRetention` (24 h) every `GenLogCleanInterval`, in small batches. A duplicate INSERT can come only from an SDK retry: statements of this instance never outlive `op` (I5), those of a previous process are fenced at startup (I13), and the SDK stops retrying one call after `MaxRetryDuration` (10 min, 7.9). 24 hours is ample. The model never forgets a gen; this retention rule is what makes forgetting safe (12.3).
- `pk` uses the canonical key encoding of 5.3, which therefore cannot change once in production.

An **owner-epoch table** with exactly one row (I14; hole 8):

```sql
CREATE TABLE row_service_epoch (
  id    boolean PRIMARY KEY DEFAULT true CHECK (id),   -- exactly one row
  epoch bigint  NOT NULL
);
INSERT INTO row_service_epoch (epoch) VALUES (0);
```

- A starting instance adds 1 and keeps the result as its epoch (7.8). Nothing else writes this table.
- Every write contains `AND EXISTS (SELECT 1 FROM row_service_epoch WHERE epoch = <mine>)` (8.3).

The examples use a sample table `items(id, gen, ver, name, score)`.

### 5.2 Go structures

`Key`, `Row` and `ErrInvalid` live in package `schema`, which both `db` and `store` import: `db.DB` returns rows, and `store` imports `db`, so defining them in `store` would be an import cycle (decided 2026-11-14, 13.1). The skeletons in this document write `Key` and `Row` for `schema.Key` and `schema.Row`.

```go
package schema

var ErrInvalid = errors.New("invalid argument") // increment of NULL, result out of range, a bad key value, etc. (5.3, 7.4)

// Key is the canonical encoding of the primary key (5.3), usable directly as a map key.
type Key string

// Row is immutable once published (I2).
type Row struct {
	Key  Key
	Gen  int64 // incarnation: tells the lives of one key apart
	Ver  int64 // version within one life, incremented by the DB
	Vals []any // business column values by schema ordinal; neither the slice nor its values may be modified
}
```

```go
package store

var (
	ErrNotFound       = errors.New("not found")
	ErrAlreadyExists  = errors.New("already exists")
	ErrOutcomeUnknown = errors.New("outcome unknown") // the client's deadline expired before the outcome was settled
	ErrInvalid        = schema.ErrInvalid              // the same value, so errors.Is works whichever package wrapped it
	ErrInternal       = errors.New("internal")         // an invariant was violated: log it and alert
)
// Errors from package db are passed through, wrapped with %w: db.ErrNotExecuted ("did not take effect"),
// db.ErrRejected (a constraint rejected it), db.ErrNotOwner (this instance has been superseded). The server
// maps them to Status (7.9).

// absent is the sentinel for "definitely does not exist" (tombstone or negative cache).
var absent = &schema.Row{}

type entry struct {
	id schema.Key

	// Read lock-free; written only while holding op (I3).
	// nil = not loaded; absent = does not exist; other = the row, equal to the DB row while op is free (I4).
	snap atomic.Pointer[schema.Row]

	// op: the per-key lock, held across DB IO. A channel of capacity 1 so that waiting can be cancelled by ctx.
	// Serializes load, create, update, delete and eviction of this key. While it is held, the only other lock
	// that may be taken is shard.mu (I8). A write statement of this key is in flight only while op is held (I5).
	op chan struct{}

	// Guarded by op (I9).
	dead          bool // removed from the map, or being removed
	lastLoadErr   error
	lastLoadErrAt time.Time

	// Last access time on the coarse clock, for idle TTL and approximate LRU.
	lastAccess atomic.Int64
}

type shard struct {
	mu sync.RWMutex // leaf lock: protects only the map structure
	m  map[schema.Key]*entry
}

type Store struct {
	schema     *schema.Schema // immutable (I10)
	shards     []*shard
	db         db.DB
	cfg        Config
	nowSec     atomic.Int64   // coarse clock, updated once per second
	entryCount atomic.Int64
	inflight   sync.WaitGroup // writes running in runSync
	bgCtx      context.Context // service lifetime; cancelled at the end of shutdown
}
```

> [!warning] Shallow copy pitfall
> `nr := *old` copies only the struct; `Vals` still shares its array with the old row. Use `nr.Vals = slices.Clone(old.Vals)` before replacing elements. Individual values (`[]byte`, `json.RawMessage`) are immutable; replace them whole.

**The DB interface** (`internal/db`). Implementations: `fakedb` (tests) and PostgreSQL (M7). It is the only interface in the Store.

```go
package db

var (
	ErrNoRows      = errors.New("no rows")          // Select found nothing; Update / Delete matched no row
	ErrNotExecuted = errors.New("not executed")     // definitely not executed, and never will be
	ErrRejected    = errors.New("rejected")         // a constraint rejected it; the statement was rolled back
	ErrGenClaimed  = errors.New("gen already claimed")
	ErrDuplicate   = errors.New("duplicate key")
	ErrNotOwner    = errors.New("not the owner")    // the epoch row holds another instance's epoch
)

// SessionID identifies the DB session a statement was sent on (8.3).
type SessionID uint64

// AmbiguousError: the statement sent on Session may have executed, or may still execute (8.1).
// It is returned only once the statement was written to a connection, so Session is always known.
type AmbiguousError struct {
	Session SessionID
	Err     error
}

func IsAmbiguous(err error) bool      // errors.As(err, *AmbiguousError)
func SessionOf(err error) SessionID   // the Session of the *AmbiguousError in err

type DB interface {
	// Select returns the row, or ErrNoRows.
	Select(ctx context.Context, key schema.Key) (*schema.Row, error)

	// InsertDedup inserts r (gen r.Gen, ver 0) together with its dedup record, in one statement (I12).
	// Returns the stored row, with defaults the DB filled in. ErrGenClaimed: the dedup record exists and
	// nothing was written. ErrDuplicate: the dedup record was new but the business row conflicted on the
	// primary key. ErrNotOwner: nothing was written.
	InsertDedup(ctx context.Context, r *schema.Row) (*schema.Row, error)

	// GenClaimed reports whether (key, gen) is in the dedup table.
	GenClaimed(ctx context.Context, key schema.Key, gen int64) (bool, error)

	// Update sets the columns cols of the row with key r.Key and gen r.Gen to r.Vals[c], and ver = ver + 1.
	// Returns the row as stored (RETURNING). ErrNoRows: no row matched, which includes "not the owner".
	Update(ctx context.Context, r *schema.Row, cols []int) (*schema.Row, error)

	// Delete deletes the row with key and gen. ErrNoRows: no row matched, which includes "not the owner".
	// The dedup record stays (I12).
	Delete(ctx context.Context, key schema.Key, gen int64) error

	// FenceSession terminates the session and returns nil once it can execute nothing any more (8.3).
	// An error means "not confirmed".
	FenceSession(ctx context.Context, id SessionID) error

	// CheckOwner returns nil if the epoch row holds this instance's epoch, ErrNotOwner if it holds another.
	CheckOwner(ctx context.Context) error
}
```

Every error a call returns is classified by the implementation (8.4): `*AmbiguousError`, `ErrNotExecuted`, `ErrRejected`, or one of the specific errors above. The Store trusts the classification. An error after the statement was written to a connection that the implementation is not sure about is an `*AmbiguousError`; any failure before that (acquiring a pool connection, connecting, `AfterConnect`) is `ErrNotExecuted`. The implementation applies `DBTimeout` to each call itself (the Store passes `bgCtx`, which has no deadline).

### 5.3 Dynamic schema, multiple tables, and primary keys

**Loading the schema**

- The managed tables are listed in the configuration; requests for other tables are refused.
- At startup the service reads from the system catalogs (`pg_attribute`, `pg_index`, `pg_constraint`, `pg_trigger`): column names, types (OID and `format_type`), NOT NULL, defaults, generated columns, primary key columns, constraints and triggers.
- It refuses to load a table, with a clear error, if:
  - the gen or ver column is missing or is not `BIGINT NOT NULL`;
  - there is no primary key, or a primary key column has an unsupported type, or is `GENERATED ALWAYS AS IDENTITY`;
  - any column has a type outside the list below;
  - a foreign key on this table has `ON DELETE` / `ON UPDATE` `CASCADE`, `SET NULL` or `SET DEFAULT`: changes to the parent would modify this table behind the cache.
- The schema is immutable afterwards (I10). SQL is generated from the schema, identifiers escaped with `pgx.Identifier{...}.Sanitize()`, and cached per (table, statement kind, column set).

**Supported types (v1)**

| PG type | Go representation | Protocol representation (`Value`, 7.9) |
|---|---|---|
| bool | bool | `bool_v` |
| int2 / int4 / int8 | int64 (range checked) | `int_v` (sint64) |
| float4 / float8 | float64 | `float_v` (double) |
| numeric | decimal string | `numeric_v` |
| text / varchar(n) / char(n) | string (length checked) | `text_v` |
| bytea | []byte | `bytes_v` |
| uuid | [16]byte | `uuid_v` |
| date | time.Time | `date_v` (days since 1970-01-01) |
| timestamp / timestamptz | time.Time | `time_v` (Unix microseconds) |
| json / jsonb | json.RawMessage (raw bytes) | `json_v` |
| NULL of any type | nil | `is_null = true` |

Any other type refuses the table. Values are immutable: the API layer copies `[]byte` and `json.RawMessage` on receipt; callers must not modify returned values.

**Primary keys**

- Encoded as a `Key` (a Go string of bytes), used as the map key and as the shard hash input (`hash/maphash`).
- int2/int4/int8: 8-byte big-endian; text/varchar: UTF-8 bytes; uuid: 16 bytes; composite: the columns' encodings in order, each as length prefix + content. The encoding is deterministic.
- **The caller supplies the primary key on create.** DB-generated keys (serial / identity) are not supported: a create retried after an ambiguous outcome must name the same row. `serial` and `GENERATED BY DEFAULT AS IDENTITY` are accepted; the supplied value is written.

**Other objects on the table**

- **Defaults** come back through `INSERT ... RETURNING`.
- **Generated columns** (`GENERATED ALWAYS AS`) are not loaded or exposed in v1.
- **Values PG normalizes** (float4, numeric scale, char(n) padding, timestamp precision, jsonb) are cached as PG stored them, because the snapshot is the `RETURNING` row (I7). A Get never changes after an eviction and reload. (This is memory-authoritative's open issue 13.3, which does not arise here.)
- **CHECK constraints, unique indexes, foreign keys, exclusion constraints, triggers that raise errors** are accepted. A rejection is a synchronous error of the write that caused it (8.4); with the fence of 7.3 nothing of a rejected write can land later, so no option and no special handling are needed. (Memory-authoritative refuses tables with constraints on other rows by default; that reason does not exist here.)
- **Triggers that modify rows**: a `BEFORE ROW` trigger's changes to the row itself are in `RETURNING` and therefore in the cache. A trigger that writes another table managed by this service breaks "only this service writes these tables". Triggers found when the schema is loaded are logged with a warning; the business side confirms they write no managed table.

**Table structure changes**

- v1: restart the service. There is nothing to flush; restarting only drops the cache.
- Adding a nullable column, or one with a default, is compatible until the restart (INSERT gives it its default; UPDATE does not touch it). Dropping, renaming or retyping a column requires stopping the service first.
- In the background, every `SchemaCheckInterval`, compare the catalogs with the loaded schema and alert on a difference.

**Multiple tables**: one `Store` per table, each with its schema and shards. The evictor is shared and processes shards in turn. IdleTTL and MaxEntries can be overridden per table (11.4).

---

## 6. Lock design

### 6.1 The role of each lock

| Lock | Protects | Scope | Hold time | Across IO | Role |
|---|---|---|---|---|---|
| `shard.mu` | the map structure (lookup, insert, delete) | many keys | very short | no | leaf lock |
| `e.op` | everything that changes the entry: `snap`, `dead`, the load error cache; and the order of DB writes for the key (I5) | 1 key | long | **yes** | root, may take `shard.mu` |
| `e.snap` (atomic) | lock-free readers see a whole row | 1 key | — | — | not a lock |

### 6.2 Lock order

**Only `op` may take another lock while held, and the only other lock is `shard.mu`.** The dependency graph has one edge, `op → shard.mu`; there is no cycle and no deadlock. A goroutine never holds two `op` locks.

### 6.3 Rationale

- **There is no `mu`** (decided 2026-11-14, 13.1). In the memory-authoritative design `mu` let an update change memory without waiting for a flush in progress. Here every write waits for the DB anyway and holds `op` for it, so `op` already serializes every change of the entry. Readers need no lock (atomic snapshot). The TLA+ model has no `mu` either.
- **Updates of one key are serialized across the DB round trip.** This is what makes I5 hold and lets 7.3 decide an ambiguous outcome by reading. The cost is a ceiling of about 1 / round-trip time updates per second for a single key (alternative-design.md 6). Updates of different keys run in parallel. Do not try to lift the ceiling by letting updates of one key run concurrently: that variant reintroduces races with load and delete and is not modeled (alternative-design.md 9).
- **`shard.mu` must be a leaf lock**: its scope is large; if it waited on anything, every key of the shard would queue behind it.
- **Readers take no locks**: immutable snapshots with atomic publication. Even an RWMutex read lock would contend on one cache line for hot keys.
- **Lookup is "release first, then validate", not crabbing**: find the entry under `shard.mu`, release it, take `op`, then check `dead`. Taking `op` while holding `shard.mu` would stall the whole shard behind one key's DB IO. Go's GC keeps the pointer valid in the gap.

### 6.4 Prohibitions (code review checklist)

- While holding `shard.mu`: take no other lock, no channel operation, no IO, no logging, no callbacks.
- While holding `op`: never wait for another entry's `op`.
- After acquiring `op`, check `dead` first.
- `op` is released on every path that returns, including errors and "not found" (the `LeakOpOnNotFound` mutation, 12.3): `runSync` releases it after `fn` returns, `load` with a `defer`. The two exceptions are deliberate: a write abandoned at shutdown (`errAbandoned`) and a panic keep it (7.1, 7.3).
- Functions that require `op` held end in `Locked`.

---

## 7. Operation flows (code skeletons)

> These skeletons fix the flow and the lock use; the implementer fills in details. They are the target shape: one function per operation as here, not one per TLA+ action. Mark where each action's step begins with `// TLA: <Action>` (names from [tla/RowCache.tla](tla/RowCache.tla)). `Store` is one table; `id` is the encoded key.

### 7.1 Helper functions

```go
func (e *entry) lockOp(ctx context.Context) error {
	select {
	case e.op <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *entry) tryLockOp() bool {
	select {
	case e.op <- struct{}{}:
		return true
	default:
		return false
	}
}

func (e *entry) unlockOp() { <-e.op }

// touch records the access time on the coarse clock, writing only when it changed.
func (s *Store) touch(e *entry) {
	if now := s.nowSec.Load(); e.lastAccess.Load() != now {
		e.lastAccess.Store(now)
	}
}

// TLA: Goc (the double lookup is one atomic step in the model)
func (s *Store) getOrCreate(id Key) *entry {
	sh := s.shardFor(id)
	sh.mu.RLock()
	e := sh.m[id]
	sh.mu.RUnlock()
	if e != nil {
		return e
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e = sh.m[id]; e == nil { // double check
		e = &entry{id: id, op: make(chan struct{}, 1)}
		e.lastAccess.Store(s.nowSec.Load()) // not idle from birth
		sh.m[id] = e
		s.entryCount.Add(1)
	}
	return e
}

// lockOpLive acquires op on an entry that is not dead (dead protocol rule 2, 7.7).
// TLA: X_lock; a ctx error while queued is X_qtimeout.
func (s *Store) lockOpLive(ctx context.Context, id Key) (*entry, error) {
	for {
		e := s.getOrCreate(id)
		if err := e.lockOp(ctx); err != nil {
			return nil, err // timed out while queued: this request did nothing
		}
		if !e.dead {
			return e, nil
		}
		e.unlockOp()
	}
}

// runSync runs fn in a background goroutine that already holds op, and releases op only when fn returns,
// that is, when the outcome is settled. If the client's ctx expires first, the client gets ErrOutcomeUnknown
// and fn keeps running. Used by Create, Update and Delete.
func runSync[T any](ctx context.Context, s *Store, e *entry, fn func(*entry) (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	s.inflight.Add(1)
	go func() {
		defer s.inflight.Done()
		// No defer for op or the answer: a panic in fn crashes the process with op held, instead of
		// releasing op and sending a zero result (which Create would read as success).
		v, err := fn(e)
		if errors.Is(err, errAbandoned) {
			done <- result{err: ErrOutcomeUnknown} // op stays held until the process exits (7.3, 7.8)
			return
		}
		// Release op first, then answer: a caller that has its answer always finds op free (TLA: FinishOp).
		e.unlockOp()
		done <- result{v, err}
	}()
	select {
	case r := <-done:
		return r.v, r.err
	case <-ctx.Done(): // TLA: X_wait, the client giving up
		var zero T
		return zero, ErrOutcomeUnknown
	}
}
```

### 7.2 Get and load

```go
// TLA: G_read
func (s *Store) Get(ctx context.Context, id Key) (*Row, error) {
	for {
		e := s.getOrCreate(id)
		s.touch(e)
		if r := e.snap.Load(); r != nil { // linearization point; dead is not checked (7.7, rule 3)
			if r == absent {
				return nil, ErrNotFound
			}
			return r, nil // shared immutable row
		}
		if err := s.load(ctx, e); err != nil {
			return nil, err
		}
		// Go round again: if the entry was evicted meanwhile, getOrCreate returns a new one.
	}
}

// load: concurrent misses queue on op, which amounts to singleflight.
// TLA: L_lock, L_load
func (s *Store) load(ctx context.Context, e *entry) error {
	if err := e.lockOp(ctx); err != nil {
		return err
	}
	defer e.unlockOp()
	if e.dead {
		return nil // the caller looks the map up again
	}
	_, err := s.ensureLoadedLocked(ctx, e)
	return err
}

// ensureLoadedLocked requires op. It is also the first step of every write (TLA: B_1, the load branch).
func (s *Store) ensureLoadedLocked(ctx context.Context, e *entry) (*Row, error) {
	if r := e.snap.Load(); r != nil {
		return r, nil // someone loaded it while we were queued (kept to save a query, 12.3)
	}
	// Fail fast after a recent load failure, so queued requests do not time out one after another.
	if e.lastLoadErr != nil && time.Since(e.lastLoadErrAt) < s.cfg.LoadErrTTL {
		return nil, e.lastLoadErr
	}
	r, err := s.db.Select(ctx, e.id)
	switch {
	case errors.Is(err, db.ErrNoRows):
		r = absent
	case err != nil:
		if ctx.Err() != nil {
			return nil, ctx.Err() // the client gave up: DEADLINE_EXCEEDED; not cached
		}
		// Any failure of a read, ambiguous ones included, means "this request did nothing": UNAVAILABLE.
		err = fmt.Errorf("%w: load: %v", db.ErrNotExecuted, err)
		e.lastLoadErr, e.lastLoadErrAt = err, time.Now()
		return nil, err
	}
	e.lastLoadErr = nil
	e.snap.Store(r)
	return r, nil
}
```

- A late read changes nothing, so SELECT needs no gen condition and no fence.
- A lookup of a missing key leaves an absent entry (negative cache). Absent entries use the shorter `NegativeIdleTTL` (7.7).

### 7.3 The one rule for every write

Create, Update and Delete all follow the same steps (alternative-design.md 3.1, 3.2):

1. Take the key's `op` (`lockOpLive`) and hand the rest to `runSync`.
2. Load if needed, and decide from the snapshot what can be decided without the DB (not found, gen mismatch, already exists). While `op` is held the snapshot is the DB row (I4).
3. Send **one** statement and wait for its result.
4. By result:

| Result | Meaning | What to do |
|---|---|---|
| Success | The statement took effect | Store the row the DB returned (`RETURNING`; absent for a delete). **Linearization point.** Answer OK |
| `ErrNotExecuted` | Definitely not executed (8.4) | Snapshot unchanged. Answer "not executed" (`UNAVAILABLE`) |
| `ErrRejected` | A constraint rejected it; rolled back | Snapshot unchanged. Answer `INVALID_ARGUMENT` |
| `ErrNoRows` (update, delete), `ErrDuplicate` (create) | Cannot happen while `op` is held and the entry is loaded | `unexpectedLocked`: check the epoch, drop the snapshot, `ErrInternal` |
| `ErrNotOwner` (create) | Superseded | Stop the instance (7.8); answer `UNAVAILABLE` |
| `*AmbiguousError` | May have executed, or may still | `fenceAndReadLocked`, then decide by the table below |

After an ambiguous outcome, the session the statement was sent on is terminated and confirmed gone; from then on the statement has either executed or never will. The row is then read, the owner epoch is checked (below), the row is stored, and the answer follows from it:

| Operation | It took effect if | It did not (and never will: "not executed") if | Anything else |
|---|---|---|---|
| Create | the row is there with this create's gen, **or** the row is absent and the gen is in the dedup table | the row is absent and the gen is not in the dedup table (or was generated by this create, 7.5) | another life is there: `unexpectedLocked` |
| Update | the row is there with the loaded gen **and** `ver` = loaded `ver` + 1 | the row is there with the loaded gen and the loaded `ver` | absent, another gen, or any other `ver`: `unexpectedLocked` |
| Delete | the row is absent | the row is there with the loaded gen and the loaded `ver` | another gen, or another `ver`: `unexpectedLocked` |

With `op` held, the row can only be the loaded one or the loaded one with this write applied (I5). Anything else means an assumption of section 2 is broken (another writer, a DB gone back in time, a superseded instance before M6's owner check) or an invariant is: the answer is `ErrInternal`, the snapshot is dropped, and it is alerted, rather than a definite answer that may be wrong. One case stays undetectable: another writer's update that leaves the loaded gen and `ver` + 1 looks exactly like this update applied; only section 2's rule and the owner epoch (I14) exclude it.

`op` stays held throughout. If the client's deadline expires first, it gets `OUTCOME_UNKNOWN` from `runSync` and the work finishes in the background.

```go
// fenceAndReadLocked settles a statement whose outcome is ambiguous (I5). Requires op.
// It terminates the statement's session and confirms it is gone, then reads the row and stores it in the
// snapshot, so that I4 holds again when op is released. Both steps are retried until they succeed: until the
// fence is confirmed the statement may still execute, so op must not be released.
// If the statement did not take effect, the row read equals the snapshot already held.
func (s *Store) fenceAndReadLocked(e *entry, amb error) (*Row, error) {
	// TLA: B_2, the ambiguous branch (the connection fence)
	for attempt := 0; ; attempt++ {
		if err := s.db.FenceSession(s.bgCtx, db.SessionOf(amb)); err == nil {
			break
		}
		if err := s.backoff(attempt); err != nil {
			return nil, errAbandoned
		}
	}
	// TLA: B_3 (the read)
	for attempt := 0; ; attempt++ {
		r, err := s.db.Select(s.bgCtx, e.id)
		if errors.Is(err, db.ErrNoRows) {
			r, err = absent, nil
		}
		if err == nil {
			err = s.db.CheckOwner(s.bgCtx) // after the read: see below
		}
		switch {
		case err == nil:
			e.snap.Store(r)
			return r, nil
		case errors.Is(err, db.ErrNotOwner):
			s.lostOwnership()
			e.snap.Store(nil)
			return nil, ErrOutcomeUnknown
		}
		if err := s.backoff(attempt); err != nil {
			return nil, errAbandoned
		}
	}
}

// errAbandoned: the service is shutting down (backoff returns an error once bgCtx is cancelled, 7.8) while
// a statement may still execute. runSync then answers ErrOutcomeUnknown and does NOT release op, so no load
// can cache the row before the statement lands (I5). The restart fence (I13) settles it.
var errAbandoned = errors.New("abandoned at shutdown")

// unexpectedLocked handles a result that cannot happen while op is held and the entry is loaded: an UPDATE
// or DELETE that matched no row, an INSERT that conflicted on the primary key, or a row after a fence that is
// neither the loaded one nor the loaded one with this write applied (the last column of the table above).
// Either this instance has been superseded (I14), or an invariant is broken. TLA: B_2, the "zero" /
// "conflict" / "dup" branch.
func (s *Store) unexpectedLocked(e *entry, what error) error {
	if err := s.db.CheckOwner(s.bgCtx); errors.Is(err, db.ErrNotOwner) {
		s.lostOwnership() // stop the instance (7.8)
		return err
	}
	e.snap.Store(nil) // the next access reloads
	return fmt.Errorf("%w: %v", ErrInternal, what)
}
```

`backoff(attempt)` waits with exponential backoff (capped) and returns an error only when `bgCtx` is cancelled.

**Why the owner check after the read.** The decision table assumes that nobody else wrote the row while `op` was held. A newer instance breaks that: it can terminate this instance's session in its startup fence (so the statement did not run), load the row and update it, after which this instance's read shows gen unchanged and `ver` + 1, and it would answer OK to an update that never ran (or OK to a delete someone else made). The model does not show this, because its superseded instance only sends statements and never answers a client. The epoch only ever moves forward, so if this instance is still the owner *after* the read, it was the owner during the read, no newer instance had started, and the read reflects only this instance's statement. Otherwise the answer is `ErrOutcomeUnknown` and the instance stops (7.8).

### 7.4 Update

```go
type ColOp uint8

const (
	OpSet  ColOp = iota // assignment
	OpIncr              // new value = old value + delta
)

type ColChange struct {
	Col int   // schema ordinal; not a primary key column (I11), not repeated within one Change
	Op  ColOp
	Val any   // OpSet: the new value, copied by the API layer; OpIncr: the delta (int64 or float64)
}

// Change is built and its values validated by the API layer (types, NOT NULL, lengths, ranges; 5.3, 7.9).
// The Store checks its shape itself (checkChange), because it relies on it: an out-of-range column would
// panic, and a Change with no columns would only bump ver.
type Change struct {
	Cols []ColChange
	Gen  int64 // optional: the gen the client expects; 0 = no check
}

// Update returns the row as the DB stored it. The API layer takes the new values of incremented columns from it.
func (s *Store) Update(ctx context.Context, id Key, ch Change) (*Row, error) {
	// The write may go on after the caller has its answer (ErrOutcomeUnknown), so it must not read the
	// caller's memory: copy the slice first. The values themselves are immutable (I2).
	ch.Cols = slices.Clone(ch.Cols)
	if err := s.checkChange(ch); err != nil { // shape of the Change: columns, primary key (I11), repeats
		return nil, err
	}
	e, err := s.lockOpLive(ctx, id)
	if err != nil {
		return nil, err // timed out while queued: not executed
	}
	return runSync(ctx, s, e, func(e *entry) (*Row, error) { return s.updateLocked(e, ch) })
}

func (s *Store) updateLocked(e *entry, ch Change) (*Row, error) {
	// TLA: B_1
	old, err := s.ensureLoadedLocked(s.bgCtx, e)
	if err != nil {
		return nil, err
	}
	if old == absent || (ch.Gen != 0 && old.Gen != ch.Gen) {
		return nil, ErrNotFound
	}
	nr, cols, err := s.applyChange(old, ch)
	if err != nil {
		return nil, err // ErrInvalid: nothing was sent
	}
	got, err := s.db.Update(s.bgCtx, nr, cols) // WHERE pk AND gen = old.Gen; ver = ver + 1
	// TLA: B_2
	switch {
	case err == nil:
		e.snap.Store(got) // linearization point (I7)
		s.touch(e)
		return got, nil
	case errors.Is(err, db.ErrNoRows):
		return nil, s.unexpectedLocked(e, err)
	case db.IsAmbiguous(err):
		now, ferr := s.fenceAndReadLocked(e, err)
		if ferr != nil {
			return nil, ferr
		}
		// TLA: B_3 (the table of 7.3)
		switch {
		case now != absent && now.Gen == old.Gen && now.Ver == old.Ver+1:
			return now, nil
		case now != absent && now.Gen == old.Gen && now.Ver == old.Ver:
			return nil, fmt.Errorf("%w: update not applied after fence", db.ErrNotExecuted)
		}
		return nil, s.unexpectedLocked(e, fmt.Errorf("after the fence: row %+v, loaded gen %d ver %d", now, old.Gen, old.Ver))
	default:
		return nil, err // ErrNotExecuted or ErrRejected: nothing changed
	}
}

// applyChange builds the row to write: SET columns take the given value; INCR columns take old + delta,
// computed by schema.Add. old is the DB row (I4), so the value written is the one the DB would compute.
// Pure computation. ErrInvalid if an old value is NULL or a result is out of the column's range; then
// nothing is sent. cols lists the changed ordinals.
func (s *Store) applyChange(old *Row, ch Change) (*Row, []int, error)
```

- **INCR writes the absolute value** (decided 2026-11-14, 13.1): the new value is computed in Go from the snapshot, and the SQL is always `SET col = $n`. This is correct because, while `op` is held, the snapshot is the DB row (I4) and nobody else writes it (I5, I14, section 2). It gives SET and INCR one SQL shape, keeps the NULL and overflow checks of `schema.Add` (copied in M1), and matches the model, which writes values. The snapshot and the answer still come from `RETURNING`: for a float4 column, for example, the cached value is PG's rounded value.
- An update reaching a key while a create or delete holds `op` waits for it, and then sees the new state. (In the memory-authoritative design such an update succeeded and was then discarded; here it simply runs after.)
- **`Change.Gen` is optional** (0 = no check). It tells lives of one key apart: with Gen = 100, an update meant for a deleted life does not change a new life with gen 200 (`ErrNotFound`). Clients that never reuse primary keys need not use it. Delete has the same option (7.6).
- **An update's answer is exact** (`UpdateAnswerSound`): OK means applied; `NOT_FOUND`, `INVALID_ARGUMENT`, `UNAVAILABLE` and `DEADLINE_EXCEEDED` mean not applied, and it never will be. Only `OUTCOME_UNKNOWN` (the client's deadline expired first) leaves it open; the work then settles in the background, and the next Get shows the result.

### 7.5 Create

```go
// Create returns the full row as stored (with DB defaults). (nil, nil) means: this gen was created earlier
// and that life has been deleted since (an SDK retry, hole 2).
func (s *Store) Create(ctx context.Context, in *Row) (*Row, error) {
	// The create may go on after the caller has its answer (ErrOutcomeUnknown), so it must not read the
	// caller's row: a changed Key would be inserted under another key's op (I5, I11). Copy it first.
	in = &Row{Key: in.Key, Gen: in.Gen, Vals: slices.Clone(in.Vals)}
	if err := s.checkRow(in); err != nil { // one value per column; Key matches the primary key values (I11)
		return nil, err
	}
	e, err := s.lockOpLive(ctx, in.Key)
	if err != nil {
		return nil, err // timed out while queued: not executed
	}
	return runSync(ctx, s, e, func(e *entry) (*Row, error) { return s.createLocked(e, in) })
}

func (s *Store) createLocked(e *entry, in *Row) (*Row, error) {
	// TLA: B_1. Load first: INSERT is sent only when the row definitely does not exist, so the dedup record
	// and the business row are always written together (I12).
	cur, err := s.ensureLoadedLocked(s.bgCtx, e)
	if err != nil {
		return nil, err
	}
	if cur != absent {
		if in.Gen != 0 && cur.Gen == in.Gen {
			return cur, nil // a retry whose earlier attempt succeeded
		}
		if in.Gen != 0 {
			// Another life. If the dedup table holds this gen, this is a retry of a create that took effect
			// and whose life was deleted since (hole 6): success with no row.
			claimed, err := s.db.GenClaimed(s.bgCtx, e.id, in.Gen)
			switch {
			case err != nil:
				return nil, fmt.Errorf("%w: dedup lookup: %v", db.ErrNotExecuted, err) // a read: nothing done
			case claimed:
				return nil, nil
			}
		}
		return nil, ErrAlreadyExists
	}
	nr := *in
	fresh := nr.Gen == 0 // a gen generated here has never been used before
	if fresh {
		nr.Gen = s.newGen() // non-zero 63-bit random (the SDK normally supplies it, 7.9)
	}
	got, err := s.db.InsertDedup(s.bgCtx, &nr)
	// TLA: B_2
	switch {
	case err == nil:
		e.snap.Store(got) // linearization point (I7)
		s.touch(e)
		return got, nil
	case errors.Is(err, db.ErrGenClaimed):
		return nil, nil // inserted earlier, deleted since; the tombstone stays
	case errors.Is(err, db.ErrDuplicate):
		return nil, s.unexpectedLocked(e, err)
	case errors.Is(err, db.ErrNotOwner):
		s.lostOwnership()
		return nil, err
	case db.IsAmbiguous(err):
		now, ferr := s.fenceAndReadLocked(e, err)
		if ferr != nil {
			return nil, ferr
		}
		// TLA: B_3 (the table of 7.3)
		if now != absent {
			if now.Gen == nr.Gen {
				return now, nil
			}
			// The load saw no row and op is held (I5): another life here is a broken assumption.
			return nil, s.unexpectedLocked(e, fmt.Errorf("after the fence: row %+v, created gen %d", now, nr.Gen))
		}
		// The row is absent, so this INSERT did not execute (it writes the row and the dedup record together,
		// and op is held). The gen can still be claimed by an earlier create with the same gen whose life was
		// deleted since (an SDK retry, hole 2); a gen generated here cannot. The lookup is retried until it
		// succeeds, like the read: the outcome is settled in the DB, so the answer must be definite.
		if !fresh {
			for attempt := 0; ; attempt++ {
				claimed, cerr := s.db.GenClaimed(s.bgCtx, e.id, nr.Gen)
				if cerr == nil {
					if claimed {
						return nil, nil
					}
					break
				}
				if err := s.backoff(attempt); err != nil {
					return nil, errAbandoned
				}
			}
		}
		return nil, fmt.Errorf("%w: create not applied after fence", db.ErrNotExecuted)
	default:
		return nil, err // ErrNotExecuted or ErrRejected
	}
}
```

- A Get during the create that sees nil queues for `op`; one that sees absent answers not-found (linearized before the create). It never sees a row the DB has not committed.
- **Why the dedup table is still needed** (hole 2): the SDK retries an ambiguous create with the same gen. If the first attempt succeeded and someone deleted the row meanwhile, the retry finds the row absent and would insert it again. Only a durable record of the gen stops it. The fence does not help: the first attempt was settled long ago; the duplicate is a new request.
- **Why load first**: an INSERT sent while the DB holds another life writes the dedup record (inside the same statement it is the business row that conflicts) and "burns" the gen. After loading first, INSERT is sent only when the row is absent, so `ErrDuplicate` cannot happen (`unexpectedLocked` treats it as a broken invariant).
- With `ON CONFLICT (pk) DO NOTHING` naming the primary key (8.3), a conflict on any other unique index is a `23505` error, `ErrRejected`, and rolls the dedup record back with the statement.

### 7.6 Delete

```go
// gen is optional (0 = no check). Non-zero: only that life is deleted; a mismatch is ErrNotFound (7.4).
func (s *Store) Delete(ctx context.Context, id Key, gen int64) error {
	e, err := s.lockOpLive(ctx, id)
	if err != nil {
		return err
	}
	_, err = runSync(ctx, s, e, func(e *entry) (struct{}, error) {
		return struct{}{}, s.deleteLocked(e, gen)
	})
	return err
}

func (s *Store) deleteLocked(e *entry, gen int64) error {
	// TLA: B_1
	r, err := s.ensureLoadedLocked(s.bgCtx, e)
	if err != nil {
		return err
	}
	if r == absent || (gen != 0 && r.Gen != gen) {
		return ErrNotFound
	}
	err = s.db.Delete(s.bgCtx, e.id, r.Gen)
	// TLA: B_2
	switch {
	case err == nil:
		e.snap.Store(absent) // linearization point (I7)
		s.touch(e)           // the tombstone's NegativeIdleTTL counts from now
		return nil
	case errors.Is(err, db.ErrNoRows):
		return s.unexpectedLocked(e, err)
	case db.IsAmbiguous(err):
		now, ferr := s.fenceAndReadLocked(e, err)
		if ferr != nil {
			return ferr
		}
		// TLA: B_3 (the table of 7.3)
		switch {
		case now == absent:
			return nil
		case now.Gen == r.Gen && now.Ver == r.Ver:
			return fmt.Errorf("%w: delete not applied after fence", db.ErrNotExecuted)
		}
		return s.unexpectedLocked(e, fmt.Errorf("after the fence: row %+v, loaded gen %d ver %d", now, r.Gen, r.Ver))
	default:
		return err // ErrNotExecuted or ErrRejected (a row of another table references this one)
	}
}
```

- **Zero rows is not "already deleted".** In the memory-authoritative design a DELETE matching no row counted as success, because a late attempt could have deleted it. Here no earlier statement can be in flight (I5), so zero rows means superseded or a broken invariant (`unexpectedLocked`).
- The entry is not removed from the map; the snapshot becomes a tombstone: requests already holding the pointer see "deleted" at once, it serves as a negative cache, and physical removal stays in one place (7.7).
- The dedup record is not deleted (I12; the mutation `DedupDeletedWithRow` is caught by `NoResurrection`).

### 7.7 Eviction (physical delete, the dead protocol)

**Any entry whose `op` is free may be evicted** (alternative-design.md 3.3): with `op` free its snapshot equals the DB row (I4), so a later access is just a miss that loads the same row. There is no dirty condition. Which entries are evicted affects the hit rate only.

| Trigger | Condition | Approach |
|---|---|---|
| Idle sweep | `now - lastAccess > IdleTTL` (absent or unloaded entries: `NegativeIdleTTL`) | Each shard is sampled every `EvictInterval` |
| Capacity | `entryCount > MaxEntries` | Sample, evict the least recently accessed, until below 0.9 × MaxEntries |

Sampling, not a full scan or an LRU list: a full scan holds `shard.mu` for long (and a queued writer blocks new readers); an LRU list turns every hit into a write. Sampling bounds each hold of `shard.mu`; this is what Redis does.

```go
const sampleK = 64

// idleSweepShard: Redis-style sampled expiry. If more than 25% of a sample was expired, sample again.
func (s *Store) idleSweepShard(sh *shard) {
	now := s.nowSec.Load()
	expired := func(e *entry) bool { return s.idleExpired(e, now) }
	for round := 0; round < s.cfg.MaxSweepRounds; round++ {
		sampled := 0
		cands := make([]*entry, 0, sampleK)
		sh.mu.RLock()
		for _, e := range sh.m { // map iteration starts at a random point: the first K are a random sample
			if expired(e) {
				cands = append(cands, e)
			}
			if sampled++; sampled == sampleK {
				break
			}
		}
		sh.mu.RUnlock() // release before touching any op
		for _, e := range cands {
			s.tryEvict(sh, e, expired)
		}
		if len(cands)*4 <= sampled {
			return
		}
	}
}

func (s *Store) idleExpired(e *entry, now int64) bool {
	ttl := s.cfg.IdleTTLSec
	if r := e.snap.Load(); r == nil || r == absent {
		ttl = s.cfg.NegativeIdleTTLSec
	}
	return now-e.lastAccess.Load() > ttl
}

// tryEvict runs the dead protocol. It re-checks eligible after taking op: the entry may have been used
// since it was sampled. TLA: V_evict, V_remove
func (s *Store) tryEvict(sh *shard, e *entry, eligible func(*entry) bool) bool {
	if !e.tryLockOp() {
		return false // a load or write is in progress
	}
	defer e.unlockOp()
	if e.dead || !eligible(e) {
		return false
	}
	e.dead = true // holding op (I9)
	sh.mu.Lock()  // op -> shard.mu, the one allowed edge (6.2)
	if sh.m[e.id] == e {
		delete(sh.m, e.id)
		s.entryCount.Add(-1)
	}
	sh.mu.Unlock()
	return true
}
```

Capacity eviction (`capacityEvictShard`) is the same: sample K under RLock, release, sort by `lastAccess`, `tryEvict` each with an `eligible` that returns true, until the shard's quota is met.

**The three rules of the dead protocol:**

1. **The remover** holds `op`, sets `dead = true`, then removes the entry from the map (the mutation `EvictNoDead`, which skips setting dead, is caught by `LiveEntryInMap`).
2. **Everyone who takes `op`** checks `dead` first; if set, releases `op` and looks the map up again (`lockOpLive`, `load`).
3. **Lock-free readers** do not check `dead`. They read the snapshot as it was at removal, which is the current row; the linearization point is the moment of removal.

A request that found the entry just before removal waits for `op` (held by the evictor for a few microseconds), sees `dead`, and looks up again. There is no spinning.

`s.nowSec` is updated once per second by a ticker; TTL precision is seconds.

### 7.8 Startup and graceful shutdown

**Startup**

1. Load the configuration and open **one** connection to PG.
2. **Take over the owner epoch** (I14):

   ```sql
   UPDATE row_service_epoch SET epoch = epoch + 1 RETURNING epoch;   -- autocommit; the result is this instance's epoch
   SELECT clock_timestamp();                                         -- separate statement, after the commit: the fence cutoff
   ```

   From the commit on, any statement that starts with an older epoch changes nothing.
3. **Terminate the sessions the previous process left** (I13). On the same connection:

   ```sql
   SELECT pg_terminate_backend(pid, 5000)      -- PG 14+: waits up to 5 s for the backend to exit
   FROM pg_stat_activity
   WHERE usename = current_user
     AND backend_type = 'client backend'
     AND backend_start <= $1                   -- the cutoff from step 2
     AND pid <> pg_backend_pid();
   ```

   Then query `pg_stat_activity` until no session with `backend_start <= cutoff` remains; if any remain after `StartupFenceTimeout`, refuse to start.
4. Open the connection pool, load the schemas, start the Stores and the background tasks (eviction, dedup cleanup, epoch and timeline check, schema check).
5. The TCP server starts accepting connections.

**Why the startup fence is still needed** (hole 3). A statement the old process sent just before it crashed can still be executing in PG, or still be on its way. If it lands after the new process has loaded the row, the cache holds a state the DB no longer has, and I4 is broken until the entry is evicted; worse, the update rule of 7.3 could take that statement's `ver` + 1 for its own. The DB itself stays right. The model reproduces it without the fence (`StmtOnlyUnderOp`, 10 states). A PG statement executes only in its session, so once the session is terminated and gone, the statement has either committed (and step 4 loads it) or never will. "The new process could bind the port, so the old one is dead" does not replace this: a backend notices its client died only when it next talks to it, so a running statement still commits (verified on PG 16, ../memory-authoritative/design.md 7.8).

**Why the epoch comes first.** A statement checks the epoch when it runs. An old statement that started before step 2 committed passes the check; only step 3 stops it. A statement that starts after step 2 committed changes nothing, whichever session it runs in; that covers everything the old instance sends after its pool reconnects, which step 3 cannot (hence the cutoff instead of "no other session").

**A superseded instance** finds out by `ErrNotOwner` from a create, by `unexpectedLocked` after an UPDATE or DELETE that matched no row, by the owner check after a fence (7.3), or by the periodic check (every `EpochCheckInterval`: `SELECT epoch FROM row_service_epoch`). `lostOwnership` then stops it **at once**: it closes all client connections and exits. It loses nothing (every acknowledged write is in the DB), but for up to `EpochCheckInterval` it may answer Gets from its own cache, which can be stale (13.3).

**A DB that went back in time**: the periodic check also reads `timeline_id` from `pg_control_checkpoint()` and compares it with the value read at startup; on a change the instance stops in the same way. This is a detector; restarting after a failover or restore remains an operations rule (section 2). M7 verifies that the service account may call the function.

**Graceful shutdown**

1. The TCP server stops accepting connections; new requests on existing connections get `UNAVAILABLE`.
2. Wait for writes in progress to settle (`s.inflight.Wait()`), up to `ShutdownTimeout`. `bgCtx` is not cancelled yet.
3. Cancel `bgCtx`: a write still settling gives up with `errAbandoned` and keeps its `op` (7.3), so requests for that key wait until they time out. Close the pool and all connections, exit.

Nothing needs to be flushed and nothing acknowledged is lost. A write abandoned in step 3 was not acknowledged (its client got or gets `OUTCOME_UNKNOWN`), and the next start's fence settles its statement.

### 7.9 External API: custom TCP protocol + protobuf

**The same protocol as the memory-authoritative design** (decided 2026-11-14, 13.1, until the protocol question of 13.4 is settled before M8). Framing, `Hello`, multiplexing with out-of-order responses, the message definitions, the server implementation notes and the SDK interface are those of [../memory-authoritative/design.md 7.10](../memory-authoritative/design.md), and are copied from there in M8, with the differences below. The differences are complete: anything else in that section applies as written, with its section references read as the corresponding ones here.

- `package rowcache.v1`, file `api/rowcache/v1/rowcache.proto`.
- `STATUS_BUSY` is removed: there is no dirty-row backpressure (11.1). The SDK rules below list the remaining statuses.
- `UpdateResponse.incr_results` and every returned row come from the DB (`RETURNING`).
- **ver**: the sentence "ver is for observation only … updates lost in a crash make ver go backwards" does not apply. Here ver never goes backwards within a life (nothing acknowledged is lost); it is still for observation only, clients must not build on it.
- **Request ctx**: "Get/Update requests queued waiting for load stop waiting; Create/Delete requests that entered runSync keep executing" becomes: a Get stops waiting for its load when the connection drops; Create, Update and Delete stop only while still queued for `op` (`DEADLINE_EXCEEDED`); once in `runSync` they keep executing until settled, and their client gets `OUTCOME_UNKNOWN`.
- The status table and the SDK rules are replaced by the ones below.
- The rationale "an update costs a few microseconds, so protocol overhead dominates" no longer holds for updates; it still holds for cache hits. See 13.4.

**Status semantics in this design**

| Status | Meaning | Took effect | SDK handling |
|---|---|---|---|
| `OK` | Success | Yes | — |
| `NOT_FOUND` | Does not exist, or expected_gen does not match | No | Returned to the caller |
| `ALREADY_EXISTS` | Create: another life holds the key, and the dedup table does not hold the request's gen (7.5) | No | Returned to the caller |
| `OUTCOME_UNKNOWN` | The request's deadline expired before its write was settled; the server settles it in the background | **Ambiguous** | Create, Delete with gen: retry with the same gen. Update, Delete without gen: returned to the caller as `ErrOutcomeUnknown` |
| `INVALID_ARGUMENT` | Invalid values (type, NOT NULL, length, range, INCR of NULL or overflow, primary key column, repeated column), or the DB rejected the write by a constraint | No | Returned to the caller |
| `SCHEMA_MISMATCH` | Stale table_id / schema_version | No | Describe again, retry once |
| `DEADLINE_EXCEEDED` | Timed out before anything started: a write queued for `op`, or a Get waiting for its load | **This request**: no | Returned to the caller, unless an earlier attempt of the same call was ambiguous |
| `UNAVAILABLE` | Shutting down or superseded (7.8); a read failed (a load, or the dedup lookup of 7.5); the write was not executed, including "not applied after the fence" (7.3) | **This request**: no | Get, Create, Delete: retry after backoff. Update: returned to the caller |
| `INTERNAL` | A broken invariant or other server bug | Writes: treated as ambiguous | Returned to the caller |

Store errors map as: `ctx.Err()` → `DEADLINE_EXCEEDED`; `ErrOutcomeUnknown` → `OUTCOME_UNKNOWN`; `ErrNotFound`, `ErrAlreadyExists`, `ErrInvalid`, `ErrInternal` → their status; `db.ErrRejected` → `INVALID_ARGUMENT`; `db.ErrNotExecuted` (which the Store also uses for every failed read, 7.2, 7.5) and `db.ErrNotOwner` → `UNAVAILABLE`. Any other error is a bug and maps to `INTERNAL`.

**SDK rules** (the same as the memory-authoritative design; holes 6 and 7 apply here unchanged):

- **Only idempotent requests are retried automatically**: Get, Create (same gen), Delete with gen.
- **Create** generates a non-zero random gen from `crypto/rand` (or the OS-seeded `math/rand/v2`), never a time-seeded generator. On `OUTCOME_UNKNOWN`, or a connection dropped after sending, it retries with the same gen until it gets an answer, the caller's ctx expires, or `MaxRetryDuration` (10 min, an SDK constant) has passed since the first attempt; then `ErrOutcomeUnknown`. The cap keeps every retry far inside `GenLogRetention` (5.1), even for a ctx without a deadline. `OK` with no row means: this create took effect earlier and that life has been deleted since; the SDK returns success.
- **After an ambiguous attempt, "not executed" is never the final answer** (hole 7). Once any attempt of a Create or Delete call ended in `OUTCOME_UNKNOWN` or a dropped connection, the call ends with a status that judges the data (`OK`, `ALREADY_EXISTS`, `NOT_FOUND`, `INVALID_ARGUMENT`), or with `ErrOutcomeUnknown` when the caller's ctx expires. `DEADLINE_EXCEEDED` and `UNAVAILABLE` on a retry are retried after backoff. The typical case: the retry queues for `op` behind its own first attempt and times out there; a moment later the first attempt succeeds.
- **Delete with gen** (`client.WithExpectedGen`): retried like create; a retry answered `NOT_FOUND` means the earlier attempt (or someone else) deleted it, and counts as success. **Delete without gen is not retried**: a retry could delete a life someone created in between.
- **Update is never retried automatically, on any status**: a retry of INCR could add twice, a retry of SET could overwrite a newer value. The model relies on this: each answer of an update is its final answer (`UpdateAnswerSound`). On a dropped connection or `OUTCOME_UNKNOWN` the caller gets `ErrOutcomeUnknown`; every other status is returned as is.
- `UNAVAILABLE` on Get, Create, Delete: retry after backoff (also capped by `MaxRetryDuration`). When the caller's ctx expires first: return the last status for Get, and for a Create or Delete that never had an ambiguous attempt; otherwise `ErrOutcomeUnknown`.

---

## 8. DB writes

### 8.1 The root problem, and this design's answer

A Go-side timeout or a broken connection means only that we stopped waiting. The statement may still be queued for a row lock, or still be in the network, and execute later. Besides success and failure, a write has a third outcome: **unknown**.

There are two ways to deal with "unknown" (../memory-authoritative/design.md 14.8): (1) wait until the outcome is certain, by terminating the session and confirming it is gone; or (2) let the storage reject stale statements with fencing tokens (gen/ver, the dedup table). The memory-authoritative design takes path 2 on every write, because its updates must not wait. **This design takes path 1 on every ambiguous outcome**, which it can afford because every write waits for the DB anyway, and ambiguous outcomes are rare.

The result is one invariant in place of several mechanisms: **a write statement of this instance for a key is in flight only while that key's `op` is held** (I5). Once `op` is released there is no late statement, so:

- Memory-authoritative's 8.2 examples 1 to 3 (an old value, a late UPDATE into a new life, a late DELETE of a new life) and hole 1 (a late INSERT after a delete) cannot occur within one process.
- Hole 4 ("ambiguous, then a definite failure taken as final") cannot occur: there is no retry whose failure would need interpreting, only a read of the settled state.
- Hole 5 (a retry rejected because of another row) cannot occur: the ambiguous attempt is fenced before anything is decided, and a rejection is an ordinary synchronous error.
- `ver` is not a fencing token; it only tells whether the one ambiguous update ran. `AND gen = ?` stays in UPDATE and DELETE as a second line of defence.

**Server-side timeouts still matter**: with `statement_timeout` shorter than the Go-side timeout, PG cancels a slow statement and reports it (`57014`, not executed) before the Go side gives up, so most slow statements end in a definite failure instead of a fence (8.4).

### 8.2 What each remaining mechanism blocks

| Mechanism | Blocks | Hole (13.2) | Without it, the model shows |
|---|---|---|---|
| Hold `op`, fence, then read (7.3) | Late statements of this process; misreading an ambiguous outcome | 1, 4, 5 do not occur | `NoConnFence` → `StmtOnlyUnderOp`; `ReleaseOnAmbiguous` → `CacheCoherent`; `AmbIsFailure` → `CacheCoherent`; `UpdateNoVerCompare` → `UpdateAnswerSound` |
| Dedup table, create loads first (I12) | An SDK retry of a create re-inserting a deleted row | 2 | `NoResurrection` |
| Dedup lookup when a create finds another life (7.5) | A retry answered `ALREADY_EXISTS` although it took effect | 6 | `CreateFailureIsFinal` |
| SDK rule after an ambiguous attempt (7.9) | "Not executed" of a retry reported as the call's result | 7 | `CreateFailureIsFinal` |
| Startup fence (I13) | Statements of the previous process landing after the load | 3 | `StmtOnlyUnderOp` |
| Owner epoch (I14) | A second instance writing behind this one's cache | 8 | `DbMatchesAbs`, `CacheCoherent`, `UpdateAnswerSound` |

### 8.3 SQL templates (PostgreSQL)

For the sample table `items(id, gen, ver, name, score)`; the real SQL is generated from the schema (5.3).

```sql
-- load, and the read after a fence (7.3)
SELECT id, gen, ver, name, score FROM items WHERE id = $1

-- create: the dedup record and the business row in one statement (I12).
-- ON CONFLICT DO NOTHING does not modify existing rows, so it is allowed (DO UPDATE is forbidden).
WITH owner AS (
  SELECT 1 FROM row_service_epoch WHERE epoch = $6               -- this instance's epoch (I14)
), claim AS (
  INSERT INTO row_service_gen_log (tbl, pk, gen)
  SELECT 'items', $1, $2 WHERE EXISTS (SELECT 1 FROM owner)
  ON CONFLICT DO NOTHING
  RETURNING 1                         -- no row if this gen was inserted before
), ins AS (
  INSERT INTO items (id, gen, ver, name, score)
  SELECT $3, $2, 0, $4, $5 WHERE EXISTS (SELECT 1 FROM claim)
  ON CONFLICT (id) DO NOTHING         -- names the primary key: any OTHER unique index raises 23505 (8.4)
  RETURNING id, gen, ver, name, score
)
SELECT EXISTS (SELECT 1 FROM owner) AS owner, EXISTS (SELECT 1 FROM claim) AS claimed, ins.*
FROM (SELECT 1) AS one LEFT JOIN ins ON TRUE   -- always exactly one row
-- $1 the canonical key encoding, $3 the primary key itself (several parameters for a composite key).
--   owner = false                             -> ErrNotOwner, nothing written
--   claimed = true, business columns present  -> inserted
--   claimed = false                           -> ErrGenClaimed
--   claimed = true, business columns null     -> ErrDuplicate (cannot happen after loading first)
-- An error raised by the business INSERT (another unique index, a foreign key, a CHECK) rolls the dedup record back.

-- has this gen been inserted before? (7.5)
SELECT 1 FROM row_service_gen_log WHERE tbl = 'items' AND pk = $1 AND gen = $2

-- update: absolute values (7.4); the DB increments ver
UPDATE items SET name = $1, score = $2, ver = ver + 1
WHERE id = $3 AND gen = $4
  AND EXISTS (SELECT 1 FROM row_service_epoch WHERE epoch = $5)
RETURNING id, gen, ver, name, score          -- no row -> ErrNoRows

-- delete
DELETE FROM items WHERE id = $1 AND gen = $2
  AND EXISTS (SELECT 1 FROM row_service_epoch WHERE epoch = $3)   -- RowsAffected 0 -> ErrNoRows

-- owner check (unexpectedLocked, and every EpochCheckInterval)
SELECT epoch FROM row_service_epoch

-- fence one session (FenceSession, 7.3). A session is identified by (pid, backend_start), since pids are
-- reused. Both are read once per connection in the pool's AfterConnect hook
-- (SELECT pid, backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()) and kept with the connection,
-- so they are known even when the connection is broken. SessionID is a handle the adapter maps to them.
-- Run on another pooled connection:
SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity WHERE pid = $1 AND backend_start = $2;
SELECT count(*) FROM pg_stat_activity WHERE pid = $1 AND backend_start = $2;   -- repeat until 0
-- An error, or sessions still there after OpFenceTimeout, means "not confirmed".
```

- **Absolutely forbidden**: `INSERT ... ON CONFLICT DO UPDATE` (upsert).
- Each statement runs in autocommit, on its own: one statement per write, no explicit transaction.
- The connection a statement used is not reused after an ambiguous outcome: pgx closes it, and the fence terminates its backend.

### 8.4 Error classification (pgx)

| Error | Classification | Notes |
|---|---|---|
| `pgconn.SafeToRetry(err) == true`; any failure before the statement was written to a connection (pool acquire, connect, `AfterConnect`) | `ErrNotExecuted` | Nothing was sent, so there is no session to fence |
| `*pgconn.PgError` `23505` on the business table's primary key | `ErrDuplicate` | Cannot happen with `ON CONFLICT (pk) DO NOTHING` and load-first; a broken invariant |
| `*pgconn.PgError` `23505` on any other unique index, `23503` (foreign key), `23P01` (exclusion), `23502` (NOT NULL), `23514` (CHECK), class `22` (data), `P0001` and other errors raised by a trigger | `ErrRejected` | The statement was rolled back. Reaches the client as `INVALID_ARGUMENT` |
| `*pgconn.PgError` `57014` (cancelled, including statement_timeout), `55P03` (lock_timeout), `40001`, `40P01` | `ErrNotExecuted` | Rolled back |
| Any other `*pgconn.PgError` | `ErrNotExecuted` | The server answered with an error, so the statement ended and was rolled back |
| ctx timeout or cancellation, broken connection, `io.EOF`, other network errors, anything not recognized | `*AmbiguousError` with the connection's session | May have executed, or may still |

The split matters less than in the memory-authoritative design: after any definite error nothing is in flight, so "not executed" and "rejected" differ only in the status the client sees. What must be right is the boundary to ambiguous: **an error is definite only if the server reported it for this statement**; when unsure, it is ambiguous, and the fence settles it at the cost of one terminated connection.

Server-side timeouts, set at the role level:

```sql
ALTER ROLE <service account> SET statement_timeout = '2s';   -- less than DBTimeout (3s)
ALTER ROLE <service account> SET lock_timeout = '1s';
ALTER ROLE <service account> SET client_connection_check_interval = '1s';   -- PG 14+
ALTER ROLE <service account> SET synchronous_commit = on;                   -- section 2
```

These reduce ambiguous outcomes; they cannot remove them (a COMMIT can succeed and its response be lost).

### 8.5 Other effects of PostgreSQL

1. **Write amplification.** Every update is a DB write: a new row version, WAL, a dead tuple. At 3,000 updates/s that is about 260 million row versions per day (alternative-design.md 6). HOT updates and autovacuum tuning are **requirements**: `fillfactor` 70-90, no index on any updated column (including gen and ver), a low `autovacuum_vacuum_scale_factor` per table. Watch `pg_stat_user_tables.n_tup_hot_upd / n_tup_upd`.
2. **RowsAffected** in PG is the number of rows matched, so 0 vs 1 is reliable.
3. **READ COMMITTED** is enough: every write is a single-row statement by primary key, serialized per key by `op`.
4. **Connection pool**: `pgxpool`, sized by `MaxDBConns`. It bounds concurrent writes and loads; a request waiting for a connection waits within its deadline. No PgBouncer (8.5 of the other design lists what would change with one).
5. **gen** is a non-zero 63-bit random number from the SDK (or from `newGen` when absent); no sequence, no round trip.

---

## 9. Linearizability and linearization points

Every operation takes effect at one instant between its request and its response; the order of those instants is a valid single-threaded execution, and an operation sent after another's response is ordered after it.

| Operation | Linearization point |
|---|---|
| Get (hit) | `snap.Load()` |
| Get of an entry being removed | the moment of removal |
| Create, Update, Delete (took effect) | `snap.Store` of the row the DB returned (or read after the fence), while holding `op`, before answering |
| Create, Update, Delete (answered "not found", "already exists", "rejected", "not executed") | the moment the snapshot was read under `op`; the abstract state did not change |

All linearization points are operations on the same atomic pointer, so they form one global order. Between the DB commit and `snap.Store`, a Get still returns the old row; both instants lie inside the write's interval, so this is linearizable.

**A crash loses nothing.** After a crash the first state loaded is the state at the crash, or that state with the one write in progress applied (the model's `Plausible`); no acknowledged write is missing.

---

## 10. Gap list

The gaps between releasing one lock and acquiring the next are where concurrency bugs collect. Each has a remedy:

| Gap | What can happen | Remedy |
|---|---|---|
| Release `shard.mu` → take `op` | The entry is evicted | Check `dead`, look up again (7.7) |
| Waiting for `op` | Someone loaded it | Re-check `snap` (saves a query) |
| Statement sent → result | Timeout or lost response; the statement may execute later | Keep `op`; fence the session; read; decide (7.3) |
| DB commit → `snap.Store` | A Get reads the old row | Fine: both are inside the write's interval (9) |
| `runSync` releases `op` → answers | The next request takes `op` | Fine: release first, then answer; the answer is already settled |
| Client attempt ambiguous → its retry answered "not executed" | The first attempt succeeds | SDK rule (7.9) |
| Client attempt ambiguous → its retry finds another life | The first attempt succeeded, its life was deleted | Dedup lookup before `ALREADY_EXISTS` (7.5) |
| Create confirmed → row deleted → SDK retry arrives | The retry would re-insert | Dedup table (I12) |
| Process crash → new process loads | The old process's statement lands | Startup fence (I13, 7.8) |
| New instance starts → old one notices | The old one keeps writing and serving | Owner epoch; the old one stops (I14, 7.8) |
| `tryEvict` sets `dead` → removes from map | Another request finds the dead entry | It takes `op` after the evictor, sees `dead`, looks up again |
| Request sent → response | The connection drops | Create retries with the same gen; delete with gen retries; update and delete without gen return `ErrOutcomeUnknown` (7.9) |

New code that introduces a gap adds it here with its remedy.

---

## 11. Operations

### 11.1 Load and overload

- There is no dirty-row backpressure: nothing accumulates in memory. Concurrency is bounded by `MaxDBConns` (writes and loads), `MaxInflightPerConn` (per client connection, propagated back through the TCP window) and `MaxConns`.
- A slow DB slows writes, and requests time out in the queue for a pool connection or for `op` (`DEADLINE_EXCEEDED`). A hot key queues on its `op`; its waiting requests time out the same way.
- **While the DB is unavailable**: writes fail (`UNAVAILABLE`), cached Gets keep working, misses fail. The memory-authoritative design accepts updates for up to `MaxDirtyAge` during an outage; this one does not (alternative-design.md 6).

### 11.2 Metrics

- Entries, tombstones, negative-cache entries; hit rate; evictions
- Latency percentiles per operation, and `op` wait time (hot keys)
- Ambiguous outcomes and fences (`FenceSession` calls, failures, time to confirm); outcomes after a fence (applied / not applied); `ErrOutcomeUnknown` count
- Definite failures by kind (not executed, rejected)
- Load errors
- `ErrInternal` count (must be 0; alert on any)
- Owner epoch: this instance's epoch; stops caused by another epoch or a changed timeline (alert)
- Dedup table: rows, cleanup lag, `ErrGenClaimed` count
- Startup: sessions terminated, time waited
- PG: HOT ratio, dead tuples, autovacuum runs of the managed tables
- Protocol: connections, in-flight requests, count per Status, protocol errors

### 11.3 Single instance and multiple instances

**v1: one instance at a time.** A second instance cannot corrupt the DB, because every write goes there synchronously; it can make the other instance's cache stale and fool its update rule (7.3), which is why the owner epoch is kept: the instance that started last owns the DB, and the older one stops when it notices. The deployment must still stop the old instance before the new one accepts connections if reads across the switch must be linearizable.

**Later: multiple instances.** A linearizable cache still needs one owner per key: route by key, with a lease, and an epoch per key range. What is easier than in the memory-authoritative design: a new owner loses nothing on takeover. The variant without a cache (alternative-design.md 9) would allow any number of instances.

### 11.4 Configuration

| Config | Meaning | Reference value |
|---|---|---|
| `Tables` | Managed tables; IdleTTL, NegativeIdleTTL, MaxEntries can be overridden per table | - |
| `GenColumn` / `VerColumn` | Names of the gen and ver columns | `row_gen` / `row_ver` |
| `NumShards` | Shards per table, a power of 2 | 256 |
| `IdleTTL` | Idle time after which an entry is dropped | TBD |
| `NegativeIdleTTL` | Idle TTL of tombstones and negative-cache entries | 1-5 min |
| `EvictInterval` | Interval of idle sampling | 1s |
| `MaxSweepRounds` | Maximum consecutive sampling rounds | 16 |
| `MaxEntries` | Entry limit per table (high-water mark) | TBD |
| `MaxDBConns` | Size of the PG connection pool | TBD (measure) |
| `EpochTable` | Owner-epoch table | `row_service_epoch` |
| `EpochCheckInterval` | Periodic epoch and timeline check; bounds how long a superseded instance keeps serving | 1s |
| `OpFenceTimeout` | How long one `FenceSession` waits for the session to go before reporting "not confirmed" (7.3 then backs off and retries) | 10s |
| `LoadErrTTL` | After a load failure, how long the same error is returned directly | 100ms |
| `DBTimeout` | Go-side timeout of a DB call | 3s |
| `StatementTimeout` / `LockTimeout` | PG server-side timeouts, set on the role; less than `DBTimeout` | 2s / 1s |
| `SchemaCheckInterval` | Interval of the catalog comparison | 1min |
| `ListenAddr` | TCP listen address | `:7070` |
| `MaxFrameBytes` | Maximum frame size | 4MB |
| `MaxInflightPerConn` | In-flight requests per connection | 1024 |
| `MaxConns` | Maximum client connections | 1000 |
| `DefaultRequestTimeout` | Timeout of a request without `timeout_ms`; greater than `DBTimeout` | 5s |
| `IdleConnTimeout` | Idle client connections are closed after it | 5min |
| `GenLogTable` | Dedup table | `row_service_gen_log` |
| `GenLogRetention` | Retention of dedup records; much longer than any client's retry time | 24h |
| `GenLogCleanInterval` | Interval of dedup cleanup | 10min |
| `StartupFenceTimeout` | Maximum wait at startup for the old sessions to exit (7.8) | 30s |
| `ShutdownTimeout` | Maximum wait for writes in progress at shutdown (7.8) | 10s |

---

## 12. Test plan and milestones

### 12.1 Testing techniques

1. **`go test -race`** on every test. It finds data races only.
2. **Fault-injecting fake DB (`fakedb`)**, copied from memory-authoritative in M1 and changed to this design's DB interface:
   - Real SQL semantics: conditional UPDATE / DELETE by gen and epoch, `ver = ver + 1`, `RETURNING`, the primary key, the dedup table, the epoch row.
   - Every statement runs on a session. Faults: delay; definitely not executed; rejected; **ambiguous, and executed later at a random moment** (the statement stays pending on its session); executed, response lost.
   - **`FenceSession`**: each pending statement of the session either executes now or is discarded; after it returns, none executes. This is the fault the whole design rests on.
   - Failures of the calls that settle and decide, so that every retry and give-up path can be tested: `FenceSession` "not confirmed" (a pending statement may still execute afterwards, until a later fence succeeds), `Select` failing after a fence, `GenClaimed` failing, `CheckOwner` failing.
   - A per-key switch that makes writes rejected (a constraint on another row), which a test flips between steps.
   - A value normalizer (for example, rounding a float4 column), so a test can show that the cache holds the DB's value.
   - A second Store on the same fakedb can take the epoch over.
   - Records every statement that took effect, in order, and checks per-statement invariants (`Violation()`).
3. **High-contention stress test**: few keys (4), many goroutines (64), random Get / Create / Update / Delete with faults. At the end, with `op` free everywhere, every loaded entry equals its DB row exactly (I4), and every answer agrees with the DB: an update answered OK is in the history of applied statements, an update answered "failed" is not.
4. **Porcupine linearizability** (`github.com/anishathalye/porcupine`), checked per key. Model state `{exists, gen, cols}`; INCR computed by the model, and the returned value checked. An operation answered `OUTCOME_UNKNOWN` returns at the end of the history and may have happened or not. **Every other failure answer means "did not happen"**, for updates too: this is stronger than the memory-authoritative model, and it is the property `UpdateAnswerSound`.
5. **Crash recovery**: during a stress test, discard the Store and start a new one on the same fakedb (with its startup fence). Check: **every acknowledged write is there**, not only creates and deletes. **Second-instance variant**: keep driving the old Store after the new one took the epoch; check that the old one stops within `EpochCheckInterval` and that no statement of it changes the DB.
6. **Failpoint tests**: hooks at the gaps (nil in production; one mechanism for all, no build tags) to force the interleavings of the model's counterexamples and witnesses, step by step.
7. **`testing/synctest`** for time: TTLs, `LoadErrTTL`, backoff, `EpochCheckInterval`.
8. **Protocol tests** (M8): fuzz the frame decoder; drop connections at random end to end and check the SDK rules of 7.9.
9. **TLA+ model** (M0, done): 12.3.

### 12.2 Milestones

| Milestone | Content | Acceptance criteria |
|---|---|---|
| M0 | TLA+ model (`tla/RowCache.tla`) | Done (12.3) |
| M1 | Module `rowcache` (Go 1.25). Copy and trim from memory-authoritative: `internal/schema` (schema, key encoding, `Add`), `internal/db` (this design's interface, 5.2) and `fakedb` without faults. shard / entry, `getOrCreate`, Get and load | `-race` unit tests pass; key encoding covers integer, string, UUID and composite keys; Get of a missing key leaves an absent entry; concurrent misses of one key send one SELECT |
| M2 | Update (SET and INCR): `lockOpLive`, `runSync`, `updateLocked`, `applyChange`, `unexpectedLocked`; no faults yet | Stress test passes and every entry equals its DB row afterwards; N goroutines each increment one column K times: the value is exactly N×K in memory and in the DB; INCR of NULL or out of range: nothing sent, `ErrInvalid`; with the normalizer on, the cache holds the DB's value; an update with a stale gen gets `ErrNotFound` |
| M3 | Create and Delete (load first, dedup table, dedup lookup on another life); fakedb gains faults and `FenceSession`; `fenceAndReadLocked` for all three writes | Stress test with every fault passes. Failpoint tests: ambiguous then applied, and ambiguous then not applied, for each of create, update and delete (`W_FenceThenApplied`, `W_FenceThenNotApplied`, `W_CreateAmb`, `W_DeleteAmb`); hole 2 and hole 6, failing with the fix removed. For each mutation `NoConnFence`, `ReleaseOnAmbiguous`, `AmbIsFailure`, `UpdateNoVerCompare`, `DedupDeletedWithRow`, `LeakOpOnNotFound`: a temporary patch that plants it makes a test fail |
| M4 | Porcupine linearizability tests | No violation over long random runs with faults, including the strong update answers of 12.1 item 4 |
| M5 | Eviction: idle sampling, capacity eviction, dead protocol | Failpoint tests (evict between lookup and `op`; between sampling and `tryEvict`); stress test with a very short IdleTTL so eviction interleaves heavily; the `EvictNoDead` mutation makes a test fail |
| M6 | Owner epoch, startup fence and graceful shutdown (7.8), `lostOwnership`, dedup cleanup, metrics, crash recovery test | Crash recovery test passes, including a statement still pending at the crash (hole 3: fails without the fence) and the second-instance variant (hole 8: fails without the epoch); a failpoint test where the new instance's fence stops the old instance's ambiguous update and then updates the row itself: the old instance answers `OUTCOME_UNKNOWN`, and OK without the owner check of 7.3; shutdown with a write still settling gives `OUTCOME_UNKNOWN`, keeps its `op`, and the next start settles it |
| M7 | PostgreSQL: pgx, pgxpool with the AfterConnect session hook, role settings (8.4), schema from the catalogs with the refusal rules of 5.3, type mapping and validation, error classification (8.4), SQL of 8.3, `FenceSession`, startup fence, timeline check, schema drift check | Integration tests against PG 14+ in docker (testcontainers-go) pass: a statement blocked on a row lock and then fenced never commits; an ambiguous update after the fence is decided correctly both ways; a duplicate INSERT is blocked by the dedup table; a write with an old epoch matches no row; a unique-index violation is `ErrRejected` and leaves no dedup record; a cascading foreign key is refused; float4, numeric(p,s), char(n) and jsonb values in the cache equal the DB's; HOT ratio measured |
| M8 | External protocol and SDK (7.9), after the decision of 13.4 | End-to-end tests (SDK → TCP → Store → PG); frame decoder fuzzing; under random connection drops the SDK follows 7.9, including hole 7; measure update and Get latency and throughput |

Each mutation and hole row is reproduced with the same method as in the memory-authoritative milestones: the test is built step by step from the model's counterexample (`python3 tla/trace.py`), it fails with the fix removed or the bug planted by a temporary patch, and passes after.

### 12.3 TLA+ model (M0)

**Files** (`tla/`): `RowCache.tla` (the model), `suite.py` (`python3 suite.py quick|main|full|ext|claims|mutants|witnesses|sim|liveness|all`; every configuration is generated into `out/suite/`), `trace.py` (condenses a counterexample). Java 11+ and `tla2tools.jar` in `~/.local/share/tla/` (or `TLA2TOOLS`). After any change to the model: `python3 suite.py quick` (48 runs, about 9 minutes). Changing the model needs the user's approval.

**What is modeled**: one key; the entry in the shard map and the dead protocol; the snapshot, `op`, lock-free Get and load; create, delete and update through `runSync`, which continues after the client gives up; one statement per write; on an ambiguous outcome the fence, then the read and the decision of 7.3; eviction of any entry whose `op` is free; the SDK retrying create with the same gen, also while its first attempt is still running; a request timing out while queued for `op`. Environment: a DB that executes late, loses responses, does not execute, or rejects because of another row; crash and restart with statements in flight; a superseded instance that keeps sending statements.

**Properties**: `NoViolation` (Get linearizable; the state after a crash explained by "nothing lost"), `MemMatchesAbs`, `CacheCoherent` (I4), `StmtOnlyUnderOp` (I5), `DbMatchesAbs`, `UpdateAnswerSound`, `CreateFailureIsFinal`, `NoResurrection`, `CreateDurable`, `OneWriterPerKey`, `LiveEntryInMap`, `DbValsBelongToGen`, `FencedRestart`, `DbVerMonotonic`; liveness `EventuallyResponds`, `BackgroundSettles`, `NoLockLeak`.

**Results** (2026-11-13, details in alternative-design.md 11.3): `main` passes (100 million distinct states), `full` passes (588 million), the `ext` environments pass, simulation finds nothing, `live` passes; all 7 mutants are caught, all 25 witnesses are reached, all 10 claims about the holes hold.

| Mutant | Planted bug | Caught by | Milestone |
|---|---|---|---|
| `NoConnFence` | Reads after an ambiguous outcome without terminating the session | `StmtOnlyUnderOp` | M3 |
| `ReleaseOnAmbiguous` | Answers "unknown" and releases `op` | `CacheCoherent` | M3 |
| `AmbIsFailure` | After the fence, answers "failed" without reading | `CacheCoherent` | M3 |
| `UpdateNoVerCompare` | After the fence, takes "the row is there" as "applied" | `UpdateAnswerSound` | M3 |
| `EvictNoDead` | Eviction removes the entry without setting `dead` | `LiveEntryInMap` | M5 |
| `DedupDeletedWithRow` | Deleting the row deletes its dedup record | `NoResurrection` | M3 |
| `LeakOpOnNotFound` | Update or delete of a missing row keeps `op` | deadlock, `NoLockLeak` | M3 |

Two checks are **not needed** by the model and are kept on purpose (0.4): `AND gen = ?` in UPDATE and DELETE (`redundant_NoGenCheck`), and the re-check of `snap` after load takes `op` (`redundant_LoadNoRecheck`).

**Where the code differs from the model on purpose**:

| Model | Code | Why it is safe |
|---|---|---|
| One key | Many keys, shards, tables | Every property is per key; keys share only `shard.mu`, a leaf lock |
| `Goc` is one atomic step | RLock lookup, then Lock and double check | The double check makes it equivalent |
| Update sets one column to a value | `Change` with several columns, SET and INCR; INCR computed from the snapshot | The snapshot is the DB row under `op` (I4); same single statement |
| After the fence, the snapshot is stored only if the write took effect | It is stored either way (7.3) | If it did not take effect, the row read equals the snapshot (`DbMatchesAbs`, I5) |
| The fence and the read always succeed | Retried with backoff while holding `op`; at shutdown abandoned with `op` still held (`errAbandoned`) | `op` stays held until the outcome is known or the process exits, which is what I5 requires; the restart fence settles the statement |
| A create after the fence reads the row and the dedup table in one step | Two reads, each retried until it succeeds; the dedup read is skipped for a gen generated by this create (7.5) | Nothing can change between them: `op` is held and the statement is settled. A gen never used before cannot be claimed except by this INSERT, which writes the row too |
| The superseded instance only sends statements; it never answers a client | It may still be settling an ambiguous write when the new instance starts | The owner check after the read (7.3): the read is trusted only if this instance was still the owner. Without it, a superseded instance could answer OK to an update the new instance's fence stopped. Not covered by the model; tested in M6 |
| "Superseded instance notices" is not a step | `unexpectedLocked`, `ErrNotOwner`, the owner check after a fence, the periodic check (7.8) | The model checks the effect of the epoch on every statement; when the instance stops is liveness |
| The dedup table never forgets a gen | Records are deleted after `GenLogRetention` (5.1) | Every possible duplicate INSERT is an SDK retry, and the SDK stops after `MaxRetryDuration`, far below the retention |
| `runSync` always releases `op` when the write ends | A panic crashes the process with `op` held | Nothing runs after the panic; the restart fence settles any statement |
| Not modeled: load errors and `LoadErrTTL`, backoff, `nowSec`, TTL and capacity policy, the protocol, schemas, SDK retry of delete | Present | They only return errors earlier or change timing |

**Not covered**: PG crash with `synchronous_commit = off` (excluded by decision, 13.1); a fence that never succeeds (liveness under an endless outage); more than one key; the variants of alternative-design.md 9; performance.

---

## 13. Decisions, holes, and open questions

### 13.1 Decisions

**Inherited from the memory-authoritative design** (its 13.1, confirmed by the business side; they do not depend on where authority lies): the type list of 5.3; no DB-generated primary keys; only this service writes the tables; no complex types; PG 14+ in docker without PgBouncer; gen optional on update and delete; SET and INCR; increment only on integer and float columns; Go SDK only; no TLS or authentication; delete without gen not retried by the SDK; Go 1.25.

**Decisions of 2026-11-14**, taken when this document was written:

| # | Question | Decision | Sections |
|---|---|---|---|
| 1 | Keep the per-entry `mu`? | **No.** Every change of an entry already holds `op`; the model has no `mu` | 4 (I3, I9), 5.2, 6 |
| 2 | INCR as a delta in SQL or as an absolute value? | **Absolute value**, computed from the snapshot with `schema.Add`. One SQL shape, Go-side NULL and range checks, matches the model. The cache still stores `RETURNING` | 7.4, 8.3 |
| 3 | `synchronous_commit` | **`on`**, set on the role, one connection pool, no PG-crash rule. If measurement shows `off` is needed, extend the model first (alternative-design.md 10) | 2, 8.4 |
| 4 | Share code with memory-authoritative? | **No.** An independent module; M1 copies what it needs | 3, 12.2 |
| 5 | External protocol | The same as memory-authoritative for now; reconsidered before M8 (13.4) | 7.9 |
| 6 | Tables with constraints on other rows | **Accepted** without an option: a rejection is synchronous and nothing of it can land later. Cascading foreign keys are still refused | 5.3 |
| 7 | Package of `Key`, `Row`, `ErrInvalid` | **`schema`**: `db` and `store` both use them, and `store` imports `db` (M1 plan, decision 1) | 5.2 |
| 8 | A create's dedup lookup after the fence fails | **Retried until it succeeds**, like the read, so the answer is definite; **skipped** for a gen this create generated (it cannot be claimed). `OUTCOME_UNKNOWN` then comes only from the client's deadline (M3 review) | 7.5, 12.3 |
| 9 | Create and Update read the caller's input in the background write | **Copy it first** (Create: the row with cloned `Vals`; Update: `ch.Cols`), then check its shape (`checkRow`, `checkChange`), before taking `op`. A write that outlives its caller (`ErrOutcomeUnknown`) must not read memory the caller may reuse (M2 plan decision 3, M3 review) | 7.4, 7.5 |
| 10 | A row after a fence that is neither the loaded one nor the loaded one with this write applied | **`unexpectedLocked`** (`ErrInternal`, snapshot dropped, alert) instead of a definite answer that may be wrong. Update and Delete compare gen and `ver`, not the values (M3 review) | 7.3-7.6 |

### 13.2 The holes of the memory-authoritative design, here

The eight holes are described in ../memory-authoritative/design.md 13.2. In this design (each line is a run of `suite.py claims`, alternative-design.md 11.4):

| Hole | Here | Fix kept |
|---|---|---|
| 1. A late INSERT resurrects a deleted row | Cannot occur: no statement outlives `op` | — |
| 2. An SDK retry of create resurrects a deleted row | Occurs without the fix (`NoResurrection`, 20 states) | Dedup table, create loads first (I12, 7.5) |
| 3. A statement of the old process lands after restart | Occurs without the fix (`StmtOnlyUnderOp`, 10 states); the cache becomes stale, the DB stays right | Startup fence (I13, 7.8) |
| 4. "Ambiguous, then not executed" taken as final | Cannot occur: after the fence the row is read once | — |
| 5. A retry rejected because of another row | Cannot occur: the ambiguous attempt is fenced first | — |
| 6. A retry answered "already exists" although it took effect | Occurs without the fix (`CreateFailureIsFinal`, 27 states) | Dedup lookup (7.5) |
| 7. The SDK reports "not executed" after an ambiguous attempt | Occurs without the fix (`CreateFailureIsFinal`, 11 states) | SDK rule (7.9) |
| 8. A second instance | Occurs without the fix (`DbMatchesAbs`, 10 states; also `CacheCoherent`, `UpdateAnswerSound`) | Owner epoch (I14, 7.8) |

### 13.3 Known and not fixed

- **Nothing has been measured.** Update latency, the single-key ceiling and the HOT ratio decide whether this design is acceptable (alternative-design.md 8). Run the measurement before relying on it in production.
- **A superseded instance may serve stale Gets for up to `EpochCheckInterval`.** Closing this needs leases with a clock assumption (multi-instance work). Its answers to writes stay sound: a definite answer comes from a statement that carried the epoch condition, and an answer decided after a fence is given only after an owner check (7.3); otherwise the answer is `OUTCOME_UNKNOWN`.
- **A DB that goes back in time** (asynchronous standby promoted, restore) can lose confirmed writes; the service detects a changed timeline and stops, but cannot restore what PG lost.
- **A hot key's updates are serialized** across the round trip (6.3).
- **Generated columns** are not exposed.

### 13.4 Open questions

- **Protocol (before M8).** The custom TCP protocol was chosen for microsecond updates; here updates cost a DB round trip, though cache hits are still microseconds. gRPC would remove the framing, multiplexing and fuzzing work, at the price of a dependency and some per-request overhead. Decide with a measurement of Get latency at the target read rate.

---

## 14. Key trade-offs

| Decision | Choice | Cost | Rationale |
|---|---|---|---|
| Authority | PostgreSQL; memory is a cache | One DB round trip per write; a hot-key ceiling | A crash loses nothing; a bug or a broken assumption makes a cache entry stale, not the data wrong |
| Ambiguous outcome | Fence the session, then read (path 1), holding `op` | A terminated connection and two queries in the rare ambiguous case; `op` held until settled | One invariant (I5) replaces gen/ver fencing, retry-until-sure, `unsure`, the three kinds of failure and the cross-row fence |
| Per-entry locking | `op` only, no `mu` | Updates of one key serialized | Every write waits for the DB anyway; one lock, one rule |
| What the cache holds | The DB's row (`RETURNING`) | None | Normalized values cannot differ between cache and DB |
| INCR | Absolute value computed from the snapshot | Relies on I4 under `op` | One SQL shape; NULL and range checked before sending; matches the model |
| Update's answer | Exact: OK applied, failure never applied | Ambiguous updates cost a fence | The client never has to guess, except on `OUTCOME_UNKNOWN` |
| Dedup table, startup fence, owner epoch | Kept | One table, one startup step, one condition per write | Holes 2, 3, 6, 8 occur without them (13.2) |
| Read path | Copy-on-write snapshot, atomic publication | An allocation per write | Lock-free reads that scale on hot keys |
| Lookup | Release first, then validate (`dead`) | The dead protocol | Contention on one key does not spread to its shard |
| Delete | Tombstone, physical removal by eviction | Tombstones use memory until `NegativeIdleTTL` | Holders of the pointer see it at once; negative cache; one place for removal |
| Eviction | Redis-style sampling; any entry with `op` free | Expiry is approximate | Bounded lock holds; no LRU list writes on the hit path |
| Constraints on other rows | Accepted | None | Rejections are synchronous and final after the fence |
| `synchronous_commit` | `on` | Some write latency | Nothing acknowledged is lost; the model covers it; no PG-crash rule |
| Code | Independent module, copied from memory-authoritative | Duplication | The two designs can be compared and changed independently |

The lock design principles behind sections 6 and 7 (latency decides lock shape, leaf locks, release first then validate, the dead protocol, logical versus physical delete) are in ../memory-authoritative/design.md section 14 and apply unchanged.

---

## 15. References

- [alternative-design.md](alternative-design.md): how this design was chosen, costs, measurement, full TLA+ results
- [../memory-authoritative/design.md](../memory-authoritative/design.md): the other design; 7.10 (protocol), 13.2 (the eight holes), 14 (lock design principles)
- Martin Kleppmann, *Designing Data-Intensive Applications*: chapter 8 (fencing tokens, unknown outcomes), chapter 9 (linearizability)
- Herlihy & Wing, *Linearizability: A Correctness Condition for Concurrent Objects*, 1990
- Porcupine: `github.com/anishathalye/porcupine`
- PostgreSQL documentation: `pg_terminate_backend`, `pg_stat_activity`, HOT updates, `synchronous_commit`
