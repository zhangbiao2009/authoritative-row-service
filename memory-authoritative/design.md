---
aliases: [In-Memory Authoritative Row Service Design, Row Data Service, write-behind row store]
tags: [design, concurrency, go, write-behind, fencing-token, linearizability]
created: 2026-11-11
---

# In-Memory Authoritative Row Service: Design Document

> [!info] Purpose of this document
> This document records the requirements, design, underlying principles, and trade-offs of this service, and is handed to Claude Code for implementation. Section 0 is the working agreement for the implementer; sections 14-15 cover design methodology and trade-offs, for later review.
>
> This design lives in the directory `memory-authoritative/`, together with its correctness properties and its TLA+ model (`tla/`); its implementation goes here too. The alternative that was considered, in which the DB stays authoritative, has its own directory: [db-authoritative/alternative-design.md](../db-authoritative/alternative-design.md).

---

## 0. Working agreement for the implementer (Claude Code)

1. Read the whole document first, especially **section 4 (invariants)** and **section 6 (lock design)**.
2. The questions in section 13 have been answered one by one by the user (13.1). 13.2 lists the eight design holes that were found; the fixes have been verified with TLA+ (12.3), confirmed by the user, and written into 5.1, 5.3, 7.4, 7.5, 7.6, 7.8, 7.10, 8.3, and 8.4. 13.3 lists what is known and deliberately not fixed.
3. Proceed according to the **milestones in section 12**. Every milestone must pass `go test -race` and the concurrency tests required for that stage.
4. Write the invariants from section 4, and the role of each lock (whether it is a leaf lock, what it protects), into code comments.
5. The following are **deliberate design choices**; do not remove them for the sake of "simplification":
   - DB writes are always conditional writes (gen/ver); **upsert is forbidden**
   - When a create/delete outcome is ambiguous, retry with the same parameters while holding op, until the outcome is definite
   - `mu`, `shard.mu`, and `qmu` are leaf locks; `op` is the only lock that may nest other locks
   - Enqueueing happens after `mu` is released, and the queue must not block
   - Tombstones (logical delete) and the dead protocol (physical delete) are kept separate
   - Poison rows go into quarantine and raise an alert; they must never be silently dropped (7.9)
   - Increment computes the new value inside `mu`, and flush writes the absolute value; **issuing `col = col + delta` is forbidden** (I12)
   - Requests on the same connection are processed concurrently and responses are returned out of order (7.10); do not change this to sequential one-by-one processing
   - create loads first; INSERTing the business row and writing the dedup record must be in the same statement; deleting a business row does not delete the dedup record (I14, 7.4)
   - At startup, first terminate the previous process's sessions and confirm they have exited, and only then load data (I15, 7.8)
   - After an attempt with an ambiguous outcome has occurred, a "not executed" kind of definite failure must not be treated as the final result (7.4, 8.4)
   - After an ambiguous attempt, a rejection caused by another row is final only once the sessions of the earlier attempts have been terminated; terminating them does not clear `unsure` (7.4, 13.2 hole 5)
   - Every write carries the owner-epoch condition, and an instance that finds another epoch stops at once without flushing (I16, 7.8)
   - The SDK never reports a "not executed" status as the final result of a call in which an earlier attempt was ambiguous (7.10)
6. If you find something in the design that does not make sense or is missing, stop and ask the user first; do not modify the design on your own.

---

## 1. Background and requirements

- This service is the **authoritative source** of the data, not a cache. The database only provides near-real-time persistence for it.
- The data is **a row (multiple columns)** in a database table, not plain key-value.
- Four operations:

| Operation | When it returns | Reason |
|---|---|---|
| Create | After the DB has **definitely succeeded** | Things the user creates must never disappear |
| Delete | After the DB has **definitely succeeded** | Things that were deleted must never reappear |
| Update | **Returns immediately** after memory is changed; flushed asynchronously afterwards | Very high frequency; must be as fast as possible |
| Get | On a memory hit, returns directly; on a miss, loads from the DB | — |

- An Update may modify only **one or a few columns** of a row; besides direct assignment, it also supports **increment** (adding a delta to the old value, see 7.3).
- The service manages **multiple tables of the business side**, and the table structures are **dynamic**: the service reads table structures from PG's system catalogs at runtime rather than hard-coding them. The only requirement is that each table must have the gen and ver columns (column names are configurable).
- Place as few restrictions on the business side as possible: primary keys may be integers, strings, UUIDs, or composite primary keys (see 5.3).
- **Tolerable**: when the service crashes and restarts, losing a small number of the most recent updates within the flush window. While the DB cannot be written the window grows beyond `FlushInterval`; it is capped by `MaxDirtyAge` (11.1).
- **Not tolerable**:
  - A confirmed create disappearing
  - Data whose delete has been confirmed reappearing
  - An old value overwriting a new value and staying in the DB long-term
  - Data of an old object being written into a new object re-created with the same id
- External protocol: **a custom TCP protocol with protobuf message bodies** (see 7.10).
- Language: Go 1.25 (`testing/synctest` requires 1.25; this machine has 1.25.14). Other languages are not considered for now.

---

## 2. Assumptions

- At any moment, a given key is owned by only **one service instance**. v1 is a **single-instance deployment** (confirmed); the deployment runs one instance at a time. If a second instance is started anyway, the owner epoch (I16, 7.8) turns that into a crash of the older instance instead of two writers: the older one can no longer change the DB and stops. Multi-instance routing by key will come later (see 11.3).
- These tables are **written only by this service**. If other programs of the business side bypass this service and write these tables directly, memory is no longer authoritative, and data will be overwritten or become inconsistent. **This is a hard prerequisite for adopting this service, and the business side has confirmed and accepted it** (together with the other conventions in 5.3 and 7.9).
- The **DB account** used by this service **is for this service only**: at startup it terminates all other sessions of that account (7.8). The database also needs a dedup table and an owner-epoch table managed by this service (5.1).
- **A transaction PostgreSQL has acknowledged is never lost, and the DB never goes back in time under a running service.** Concretely:
  - The service's sessions run with `synchronous_commit = on` (the default; set it at the role level so a changed server default cannot switch it off). With `off`, a PG crash can drop a create that was already confirmed.
  - A standby that can be promoted must be synchronous. Promoting an asynchronous standby, or restoring a backup, can drop confirmed creates and deletes; no design on top of PG can prevent that.
  - After any failover or restore, **restart the service**: its memory may be ahead of the DB it is now connected to. The service also checks for this itself and stops (7.8).
- **Tables with constraints that depend on other rows** (unique indexes other than the primary key, foreign keys, exclusion constraints, triggers) are refused unless enabled per table (5.3).
- **Memory cannot hold the full data set**; only the working set is kept:
  - If an entry has not been accessed for longer than `IdleTTL`, it must be cleared from memory (provided it is clean, see 7.7)
  - When the total number of entries exceeds `MaxEntries`, entries are forcibly evicted by approximate LRU
  - Cold data is loaded from the DB the first time it is accessed
- The database is **PostgreSQL 14 or later**, deployed in docker; the service **connects to PG directly, without PgBouncer**. The driver is `github.com/jackc/pgx/v5` (for the implications of PostgreSQL see 8.3-8.5).

---

## 3. Overall structure

```
Service
 ├── TCP server                 external protocol: length prefix + protobuf, multiplexed over a single connection (see 7.10)
 ├── tables map[tableName]*Store  loads schemas from PG system catalogs at startup, per the configured table list (see 5.3)
 │    └── Store (one table)
 │         ├── schema            immutable at runtime
 │         └── shards[N]         N is a power of 2 (e.g. 256), located by maphash(key) & (N-1)
 │              ├── mu  (RWMutex)   protects the map structure; leaf lock
 │              ├── m   map[Key]*entry   Key is the canonical encoding of the primary key
 │              ├── qmu (Mutex)     protects the flush queue; leaf lock
 │              └── queue []*entry  flush queue (non-blocking)
 ├── flusher                    background, periodic; writes dirty rows back to the DB in per-shard batches (worker pool shared by all tables)
 ├── evictor                    clears clean entries by idle TTL; evicts by approximate LRU when MaxEntries is exceeded
 └── DB interface               real implementation (PostgreSQL / pgx) + fake implementation with fault injection
```

Each key has a fixed **slot** (entry) in memory. Create, delete, and update in memory all just replace the immutable snapshot pointer in that slot, so "exists or not" is itself part of the value:

```
nil    → Row / absent   (load)
absent → Row            (create)
Row    → Row'           (update)
Row    → absent         (delete, tombstone)
```

Suggested directory layout (paths are relative to this design's directory, `memory-authoritative/`):

```
/api/rowsvc/v1            external protocol: .proto files and generated Go code (see 7.10)
/cmd/server               entry point: load config, take over the owner epoch and terminate the previous process's sessions (7.8), load schemas, start Store and TCP server
/internal/server          TCP server: framing, connection management, request dispatch, Status mapping
/internal/schema          load schemas from system catalogs, type conversion and validation, primary key encoding, increment arithmetic
/internal/store           entry, shard, Get/Create/Update/Delete, eviction
/internal/store/flush.go  flusher
/internal/db              DB interface, error classification, PostgreSQL (pgx) implementation
/internal/db/fakedb       fault-injecting fake DB (for tests)
/internal/store/*_test.go stress tests, Porcupine linearizability tests, failpoint tests
/client                   Go client SDK (see 7.10)
/deploy                   docker compose: PG for development and integration tests
```

---

## 4. Core invariants

All code must maintain the following invariants, and they must be written in code comments.

| ID | Invariant |
|---|---|
| I1 | Each key has **at most one** entry in the map. An entry that is not dead is always in the map; a dead entry remains in the map only for a very short time before it is removed from the map. |
| I2 | A published `*Row` is **never modified**. Any modification creates a new Row and then replaces the pointer. |
| I3 | Writing `snap` requires holding `mu`. Transitions involving nil/absent (load, create, delete) additionally require holding `op` at the same time. Update only performs Row→Row transitions. |
| I4 | While holding `op`, if `dirty == 0`, then this row in the DB is consistent with memory (or it exists in neither). |
| I5 | For a given key, at any moment there is at most one write SQL issued by this service that is still awaiting its result. For writes that execute late after a timeout, UPDATE/DELETE are blocked by gen/ver, INSERT is blocked by the dedup table (I14), writes from the previous process are blocked by the startup session cleanup (I15), and writes of an instance that has been superseded are blocked by the owner epoch (I16). |
| I6 | DB writes are always conditional writes: `UPDATE ... WHERE id AND gen AND ver <`, `DELETE ... WHERE id AND gen`, and INSERT relies on the unique constraints of the primary key and the dedup table. **Upsert is forbidden.** |
| I7 | Create/Delete modify memory only after the DB outcome has **definitely succeeded**, and the memory modification (the linearization point) happens before returning to the client. |
| I8 | Only `op` may acquire other locks while held. `mu`, `shard.mu`, and `qmu` are all leaf locks. |
| I9 | Writing `dead` requires holding both `op` and `mu`; reading `dead` requires holding either one. |
| I10 | An entry with dirty data (`dirty != 0`) must not be evicted. |
| I11 | The schema is immutable at runtime. `Row.Vals` is stored by the column ordinals in the schema; neither the slice itself nor any value in it may be modified (an extension of I2). |
| I12 | Increment computes the new value from the old value inside `mu`, and puts it into the new Row as an ordinary column value. What is written to the DB is always the absolute value in memory; **`col = col + delta` is never issued**. |
| I13 | Primary key columns cannot be modified by update: `Row.Key` and the primary key columns in `Vals` are always consistent. |
| I14 | A given (table, primary key, gen) can be inserted only once in its lifetime: INSERTing the business row and writing the dedup record are in the same statement, and deleting the business row does not delete the dedup record (5.1, 8.3). |
| I15 | Before the service starts loading data, all of the previous process's sessions on PG have exited, and SQL it issued can no longer land (7.8). |
| I16 | A write of this service changes the DB only while the owner-epoch row holds this instance's epoch: every INSERT, UPDATE and DELETE carries that condition (8.3). A starting instance takes the epoch over before its startup fence, and an instance that finds another epoch stops at once without flushing (7.8). |

---

## 5. Data structures

### 5.1 Table structure

Before a business-side table is onboarded, two columns need to be added (column names are configurable, defaulting to `row_gen` / `row_ver`; for brevity the SQL in this document writes them as gen / ver):

```sql
ALTER TABLE orders
  ADD COLUMN row_gen BIGINT NOT NULL DEFAULT 0,  -- incarnation number: a new value on each create, compared for "equality"
  ADD COLUMN row_ver BIGINT NOT NULL DEFAULT 0;  -- version number: +1 on each update, monotonically increasing
ALTER TABLE orders SET (fillfactor = 80);        -- leave room in the page for HOT updates (see 8.5)
```

- On PG 11 and later, adding a column with a constant default does not rewrite the whole table and can be done online.
- Existing rows all have gen 0, which is fine: gen only needs to differ from the other lives of the same key, and newly created rows use a non-zero random number.
- Do not index gen or ver; indexes on business columns should avoid frequently updated columns where possible, otherwise UPDATEs cannot be HOT.

In addition, the database needs a **dedup table** managed by this service (table name configurable), recording "which (table, primary key, gen) have been inserted", used to guarantee I14 (see 7.4, 13.2 holes 1 and 2):

```sql
CREATE TABLE row_service_gen_log (
  tbl        text        NOT NULL,                -- business table name
  pk         bytea       NOT NULL,                -- canonical encoding of the primary key (the Key from 5.3)
  gen        bigint      NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tbl, pk, gen)
);
CREATE INDEX ON row_service_gen_log (created_at);  -- for cleanup by retention period
```

- Each create writes one row, in the same statement as the business row (8.3); deleting the business row does **not** delete this row.
- A background task runs every `GenLogCleanInterval` and deletes records whose `created_at` is older than `GenLogRetention` (default 24 hours), a small batch at a time to avoid holding locks for long. This relies on one assumption: a duplicate INSERT will not arrive later than the retention period. Late statements within the process are bounded by the TCP timeout (on the order of minutes), statements from the old process are cleaned up on restart (7.8), and client retries are bounded by the client's own timeout, so 24 hours is ample.
- `pk` uses the canonical encoding from 5.3, so once that encoding goes live it can no longer be changed.
- This table has only INSERTs and time-based batch DELETEs, no UPDATEs, so write amplification is small.

And an **owner-epoch table** with exactly one row (table name configurable), used to guarantee I16 (see 7.8, 13.2 hole 8):

```sql
CREATE TABLE row_service_epoch (
  id    boolean PRIMARY KEY DEFAULT true CHECK (id),   -- exactly one row
  epoch bigint  NOT NULL
);
INSERT INTO row_service_epoch (epoch) VALUES (0);
```

- A starting instance adds 1 to `epoch` and remembers the result as its own epoch. Nothing else ever writes this table.
- Every write statement of the service contains `AND EXISTS (SELECT 1 FROM row_service_epoch WHERE epoch = <mine>)` (8.3). The row is read by every write and changed once per start, so it stays in shared buffers and costs one index probe.

Examples in the rest of this document consistently use a sample table `items(id, gen, ver, name, score)`.

### 5.2 Go structures

```go
package store

var (
	ErrNotFound       = errors.New("not found")
	ErrAlreadyExists  = errors.New("already exists")
	ErrOutcomeUnknown = errors.New("outcome unknown") // create/delete could not determine the outcome before the client timed out
	ErrBusy           = errors.New("busy")            // backpressure
	ErrInvalid        = errors.New("invalid argument") // old value is NULL on increment, result overflows, etc. (see 7.3)
	ErrInternal       = errors.New("internal")         // an invariant was violated: log it and raise an alert
)

// Key is the canonical encoding of the primary key (see 5.3) and can be used directly as a map key.
type Key string

// Row is immutable once published (I2, I11).
type Row struct {
	Key  Key
	Gen  int64 // incarnation number: distinguishes different "lives" of the same key
	Ver  int64 // version number: orders modifications within one life
	Vals []any // business column values, indexed by column ordinal in the schema; neither the slice nor its values may be modified
}

// Column bitmap: bit i represents column i (i < 63); bit 63 means "some column with ordinal ≥ 63 is dirty",
// and flush writes all columns with ordinal ≥ 63. This supports tables with any number of columns; with many columns it just writes a bit more.
func colBit(i int) uint64 {
	if i >= 63 {
		return 1 << 63
	}
	return 1 << uint(i)
}

// absent is a sentinel meaning definitely does not exist (tombstone or negative cache).
var absent = &Row{}

type entry struct {
	id Key

	// Lock-free readers read it directly; writes must hold mu (I3).
	// nil = not loaded; absent = definitely does not exist; other = current row
	snap atomic.Pointer[Row]

	// op: per-key lock held across DB IO. A channel of capacity 1, so it can be combined with ctx cancellation.
	// Serializes load / create / delete / flush for this key.
	// The only lock allowed to acquire other locks while held (I8).
	op chan struct{}

	// mu: leaf lock. While held: take no other locks, do not block, do no IO, do not log, do not call external code.
	mu         sync.Mutex
	dirty      uint64    // dirty columns bitmap (mu)
	queued     bool      // whether already in the flush queue (mu)
	poisoned   bool      // flush was definitely rejected by the DB; in quarantine (mu, see 7.9)
	dead       bool      // removed; writing requires both op+mu, reading requires either (I9)
	dirtySince time.Time // time when dirty went from 0 to non-zero (mu)

	// Last access time (coarse clock, in seconds), used for idle TTL and approximate LRU.
	lastAccess atomic.Int64

	// Accessed only while holding op
	lastLoadErr   error
	lastLoadErrAt time.Time
}

type shard struct {
	mu sync.RWMutex // leaf lock: protects only the map structure
	m  map[Key]*entry

	qmu   sync.Mutex // leaf lock: protects only the flush queue
	queue []*entry   // because of the queued flag, its length never exceeds the number of entries; must not be a blocking channel
}

type Store struct {
	schema     *schema.Schema  // immutable at runtime (I11): type validation, primary key encoding, increment arithmetic
	shards     []*shard
	db         db.DB
	cfg        Config
	nowSec     atomic.Int64    // coarse clock: updated once per second by a background ticker
	entryCount atomic.Int64
	dirtyCount atomic.Int64
	inflight   sync.WaitGroup  // in-flight create/delete
	bgCtx      context.Context // ctx for the service lifetime, cancelled on shutdown
}
```

> [!warning] Shallow copy pitfall
> `nr := *old` copies only the struct itself; the `Vals` slice still shares its underlying array with the old row. Before modifying, you must first do `nr.Vals = slices.Clone(old.Vals)` and then replace elements in it. Individual values (`[]byte`, `json.RawMessage`, etc.) are also immutable; to change one, replace it entirely with a new value.

### 5.3 Dynamic schema, multiple tables, and primary keys

**Loading the schema**

- The tables to be managed are listed in the configuration (a table-name allowlist); the service rejects access to tables not on the list.
- At startup, it reads from PG's system catalogs (`pg_attribute`, `pg_index`, `pg_constraint`, `pg_trigger`, or `information_schema`): column names, types (OID and `format_type`), whether NOT NULL, default values, whether a column is a generated column, which columns make up the primary key, as well as CHECK constraints, foreign keys, and triggers.
- It validates the following; if any is not satisfied, it refuses to load the table and gives a clear error message:
  - The gen and ver columns must exist, with type `BIGINT NOT NULL`
  - There must be a primary key, and the types of the primary key columns must be supported
  - The types of all columns must be in the supported type list
- After loading completes, the schema is **immutable** at runtime (I11).
- SQL is generated dynamically from the schema; table and column names are always escaped with `pgx.Identifier{...}.Sanitize()`. Generated SQL is cached by (table, column bitmap).

**Supported types (v1)**

| PG type | Go representation | Protocol representation (`Value` in 7.10) |
|---|---|---|
| bool | bool | `bool_v` |
| int2 / int4 / int8 | int64 (range checked on write) | `int_v` (sint64) |
| float4 / float8 | float64 | `float_v` (double) |
| numeric | pgtype.Numeric (or a decimal string) | `numeric_v` (decimal string) |
| text / varchar(n) / char(n) | string (length checked on write) | `text_v` |
| bytea | []byte | `bytes_v` |
| uuid | [16]byte | `uuid_v` (16 bytes) |
| date | time.Time | `date_v` (days since 1970-01-01) |
| timestamp / timestamptz | time.Time | `time_v` (Unix microseconds; PG's precision is microseconds) |
| json / jsonb | json.RawMessage (keeps the raw bytes, not parsed into a map) | `json_v` (raw bytes) |
| NULL of any type | nil | `is_null = true` |

The business side has confirmed there are no complex types such as arrays, enums, or composite types; v1 supports only the types in the table above, and if a column of any other type is encountered, the table is refused. All values are treated as immutable: for reference types such as `[]byte` and `json.RawMessage`, the API layer makes a copy upon receipt; after they are returned to the caller, the caller must not modify them.

**Primary keys: unified internal encoding, no restrictions on the business side**

- Internally, the primary key is uniformly encoded as a `Key` (a Go string, which can hold arbitrary bytes), used both as the map key and as the input to the shard hash (`hash/maphash`).
- Encoding rules:
  - int2 / int4 / int8: 8-byte big-endian integer
  - text / varchar: UTF-8 bytes
  - uuid: 16 bytes
  - Composite primary key: concatenate the encodings of the columns in order as "length prefix + content"
- The encoding is deterministic: the same primary key value always yields the same Key.
- This way integer, string, UUID, and composite primary keys are all supported. The cost is that using a string as a map key requires hashing by bytes, which is slightly slower than int64, but negligible compared with a DB round trip. If load testing later shows this to be a bottleneck, add a fast path for single-column integer primary keys.
- **The caller supplies the primary key on create**; DB-generated primary keys (serial / identity) are not supported, and the business side has confirmed they are not needed. Reason: when a create outcome is ambiguous, it must be retried with the same primary key and gen; if the primary key were generated by the DB, the retry would insert a new row.
- If a primary key column is defined as `GENERATED ALWAYS AS IDENTITY`, it cannot be assigned explicitly on INSERT, so the table is refused when the schema is loaded. `serial` and `GENERATED BY DEFAULT AS IDENTITY` have no effect: the caller-supplied value is written directly (only this service writes the table, so it does not matter that the sequence does not advance).

**Other objects on the table**

- **Default values**: create uses `INSERT ... RETURNING` to read the DB-filled default values back into memory, keeping memory and the DB consistent.
- **Generated columns** (`GENERATED ALWAYS AS`): cannot be written, and once the columns they depend on are modified, the value in memory becomes stale. v1 does not load generated columns and does not expose them.
- **Triggers that modify rows**: modifications made by a trigger on UPDATE (e.g. `updated_at = now()`) are not visible in memory, so memory and the DB would become inconsistent. The business side has confirmed: the onboarded tables have no triggers that modify row data; columns like `updated_at` are assigned by this service on update. If triggers are detected when the schema is loaded, log a warning and list them.
- **CHECK constraints**: can cause poison rows during flush (see 7.9). If detected when the schema is loaded, log a warning.
- **Constraints whose verdict depends on other rows**: unique indexes other than the primary key, exclusion constraints, foreign keys in either direction (this table referencing another, or another table referencing this one), and triggers that can raise an error. By default **the table is refused** when the schema is loaded, and the error lists the objects found. Setting `AllowCrossRowConstraints` for that table accepts it. The reasons for refusing by default:
  - Such a constraint can reject a statement now and accept the same statement later, once the other row has changed. The rule "a retry rejected by the data means the earlier attempt was rejected too" (7.4) does not hold for it (13.2 hole 5). With the option on, create and delete stay correct through the session fence of 7.4, at the price of terminating a connection in that rare case.
  - A flush that violates such a constraint is a poison row (7.9): the update was already acknowledged. A unique index on a column that is updated through this service makes that an everyday event.
  - **Always refused, with or without the option**: a foreign key on this table with `ON DELETE` / `ON UPDATE` `CASCADE`, `SET NULL` or `SET DEFAULT`. Deleting or changing the parent row would then modify this table behind the service's back, which breaks "only this service writes these tables" (section 2).

**Table structure changes**

- v1: table structure changes require **restarting the service**. Graceful shutdown flushes all dirty rows, and the schema is reloaded after restart.
- Adding a nullable column, or one with a default value, is a compatible change: even without an immediate restart, the old schema will not write this column incorrectly (INSERT gives it its default, UPDATE does not touch it); it just cannot be read for the time being. Dropping a column, renaming, or changing a type is an incompatible change: the service must be stopped first, then the table altered.
- In the background, periodically (e.g. every minute) compare the system catalogs with the loaded schema and raise an alert on any mismatch.

**Multiple tables**

- One `Store` per table, with its own schema and shards. The code skeleton below is a single `Store`.
- The flush worker pool and the evictor are shared by all tables and process shards in turn.
- Settings such as IdleTTL and MaxEntries can override the global defaults per table (see 11.4).

---

## 6. Lock design

### 6.1 The role of each lock

| Lock | What it protects | Scope | Hold time | Held across IO | Role |
|---|---|---|---|---|---|
| `shard.mu` | map structure (lookup, insert, delete) | many keys | very short | no | leaf lock |
| `shard.qmu` | flush queue | many keys | very short | no | leaf lock |
| `e.mu` | read-modify-write of the row, dirty, queued, dead | 1 key | very short | no | leaf lock |
| `e.op` | serialization of all DB operations for this key | 1 key | long | **yes** | root (the only lock that may nest) |
| `e.snap` (atomic) | lock-free readers reading a consistent whole row | 1 key | — | — | not a lock |

### 6.2 Lock order

There is only one rule: **only `op` may acquire other locks while held**; all other locks are leaf locks and acquire nothing while held.

The lock dependency graph has only the edges `op → mu`, `op → shard.mu`, and `op → qmu`, with no cycles, so deadlock is impossible.

### 6.3 Rationale for this design

- **shard.mu must be a leaf lock**: its scope is large. If it waits on anything while held, requests for every key on that shard queue up behind it.
- **update takes only mu and never waits on IO**: update and flush have different latency requirements and must not share a lock. Rule of thumb: the slowest of all users of a lock drags everyone waiting on that lock down to the same slowness.
- **Readers take no locks**: immutable snapshots plus atomic publication (copy-on-write). Readers write no shared memory among themselves, so they scale linearly even on hot keys. Even the read lock of an RWMutex becomes a bottleneck due to contention on the same cache line.
- **Lookup uses "release first, then validate" rather than crabbing**: after finding the entry under the shard lock, **immediately release** the shard lock, then lock the entry, and after acquiring it check dead. Reasons for not using crabbing (locking the entry while still holding the shard lock):
  - Crabbing on op: equivalent to holding the shard lock while waiting on someone else's DB IO, which stalls the entire shard
  - Crabbing on mu: contention on a hot key spreads to the entire shard (Go's RWMutex blocks new readers when a writer is queued)
  - Go has GC, so the pointer in hand cannot dangle during the gap, making after-the-fact validation safe

### 6.4 Prohibitions (code review checklist)

- While holding `mu` / `shard.mu` / `qmu`: take no other locks, do no channel send/receive, do no IO, do not log (`log.Logger` has internal locks and does IO), do not call external callbacks. Only pure in-memory computation is allowed (e.g. `schema.Add` used for increment in 7.3).
- While holding `shard.mu`, do not wait on `mu` or `op`.
- While holding `mu`, do not wait on `op`. If data needs to be loaded, release `mu` first, and after loading go through the whole path again from the start.
- Functions that require the caller to already hold a lock consistently use the `xxxLocked` suffix.
- Every time `op` or `mu` is acquired, check `dead` first.

---

## 7. Operation flows (code skeletons)

> The following are skeletons that illustrate the flows and how locks are used; the implementer fills in the details. In the skeletons, `Store` corresponds to one table, `id` is the encoded primary key (type `Key`), and business columns are stored in `Row.Vals`.

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

// touch records the access time. It reads the coarse-grained clock s.nowSec to avoid calling time.Now() on every hit;
// if the value has not changed it does not write, to avoid writing the same cache line on every hit.
func (s *Store) touch(e *entry) {
	now := s.nowSec.Load()
	if e.lastAccess.Load() != now {
		e.lastAccess.Store(now)
	}
}

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
		e.lastAccess.Store(s.nowSec.Load()) // a newly created entry must not be treated as idle from birth
		sh.m[id] = e
		s.entryCount.Add(1)
	}
	return e
}

// lockOpLive acquires op on an entry that is not dead.
func (s *Store) lockOpLive(ctx context.Context, id Key) (*entry, error) {
	for {
		e := s.getOrCreate(id)
		if err := e.lockOp(ctx); err != nil {
			return nil, err
		}
		if !e.dead { // holding op is enough to read dead (I9)
			return e, nil
		}
		e.unlockOp()
	}
}

func (s *Store) markDirtyLocked(e *entry, mask uint64) {
	if e.dirty == 0 {
		s.dirtyCount.Add(1)
		e.dirtySince = time.Now()
	}
	e.dirty |= mask
}

func (s *Store) clearDirtyLocked(e *entry) {
	if e.dirty != 0 {
		s.dirtyCount.Add(-1)
	}
	e.dirty = 0
}

// enqueue must be called after releasing mu (qmu is a leaf lock; the queue does not block).
func (s *Store) enqueue(e *entry) {
	sh := s.shardFor(e.id)
	sh.qmu.Lock()
	sh.queue = append(sh.queue, e)
	sh.qmu.Unlock()
}
```

### 7.2 Get and load

```go
func (s *Store) Get(ctx context.Context, id Key) (*Row, error) {
	for {
		e := s.getOrCreate(id)
		s.touch(e)
		if r := e.snap.Load(); r != nil { // linearization point
			if r == absent {
				return nil, ErrNotFound
			}
			return r, nil // shared immutable object; the caller only reads it
		}
		if err := s.load(ctx, e); err != nil {
			return nil, err
		}
		// After loading, go back to the top. If the entry was evicted, getOrCreate will get a new entry.
	}
}

// load is used by Get and Update. Concurrent misses all queue on op, which amounts to singleflight.
func (s *Store) load(ctx context.Context, e *entry) error {
	if err := e.lockOp(ctx); err != nil {
		return err // the client has given up; leave the queue
	}
	defer e.unlockOp()
	if e.dead {
		return nil // the caller will look up the map again
	}
	_, err := s.ensureLoadedLocked(ctx, e)
	return err
}

// ensureLoadedLocked requires the caller to hold op.
func (s *Store) ensureLoadedLocked(ctx context.Context, e *entry) (*Row, error) {
	if r := e.snap.Load(); r != nil {
		return r, nil // double check: someone else finished loading while we were queued
	}
	// If it just failed, return the error immediately, so queued requests do not time out one after another
	if e.lastLoadErr != nil && time.Since(e.lastLoadErrAt) < s.cfg.LoadErrTTL {
		return nil, e.lastLoadErr
	}
	r, err := s.db.Select(ctx, e.id)
	switch {
	case errors.Is(err, db.ErrNoRows):
		r = absent
	case err != nil:
		if ctx.Err() == nil { // client-side cancellations are not cached; only DB-side errors are
			e.lastLoadErr, e.lastLoadErrAt = err, time.Now()
		}
		return nil, err
	}
	e.lastLoadErr = nil
	e.mu.Lock()
	e.snap.Store(r)
	e.mu.Unlock()
	return r, nil
}
```

- Late execution of a read is harmless (it changes nothing), so SELECT does not need gen/ver.
- Querying a nonexistent key also leaves an absent entry (negative cache). Such entries and tombstones use the shorter `NegativeIdleTTL`, so that large numbers of random keys cannot blow up memory.

### 7.3 Update (memory only)

```go
// Change is built by the API layer from the request and the schema. Type conversion and validation are done at construction time (see 5.3, 7.9, 7.10).
type ColOp uint8

const (
	OpSet  ColOp = iota // assignment
	OpIncr              // increment: new value = old value + delta
)

type ColChange struct {
	Col int   // column index in the schema; must not be a primary-key column (I13), and must not repeat within one Change
	Op  ColOp
	Val any   // OpSet: the new value, already copied at construction and never modified afterwards; OpIncr: the delta (int64 or float64)
}

type Change struct {
	Cols []ColChange
	Mask uint64 // column bitmap of all Cols (bitwise OR of colBit)
	Gen  int64  // optional: the gen the client expects; 0 means no check
}

// Update returns the row after the update (immutable snapshot); the API layer extracts the new values of incremented columns from it and returns them to the client.
func (s *Store) Update(ctx context.Context, id Key, ch Change) (*Row, error) {
	if s.dirtyCount.Load() > s.cfg.MaxDirty {
		return nil, ErrBusy // backpressure (could be changed to a short wait; further updates to rows that are already dirty could be let through)
	}
	for {
		e := s.getOrCreate(id)
		s.touch(e)
		e.mu.Lock()
		old := e.snap.Load()
		if e.dead || old == nil {
			// Eviction is removing it, or it is not loaded. Release mu first, then take op (mu is a leaf lock).
			// The remover holds op until the entry is out of the map, so for a dead entry load returns only
			// then, without reading; either way, look up the map again.
			e.mu.Unlock()
			if err := s.load(ctx, e); err != nil {
				return nil, err
			}
			continue
		}
		if old == absent || (ch.Gen != 0 && old.Gen != ch.Gen) {
			e.mu.Unlock()
			return nil, ErrNotFound
		}
		nr := *old
		nr.Vals = slices.Clone(old.Vals) // copy only the slice itself; the individual values are immutable and can be shared
		for _, c := range ch.Cols {
			switch c.Op {
			case OpSet:
				nr.Vals[c.Col] = c.Val
			case OpIncr:
				// Pure computation (6.4): returns ErrInvalid if the old value is NULL or the result is out of the column type's range.
				// At this point snap.Store has not happened yet, so the whole Change has no effect.
				v, err := s.schema.Add(c.Col, old.Vals[c.Col], c.Val)
				if err != nil {
					e.mu.Unlock()
					return nil, err
				}
				nr.Vals[c.Col] = v
			}
		}
		nr.Ver = old.Ver + 1
		e.snap.Store(&nr) // linearization point
		s.markDirtyLocked(e, ch.Mask)
		need := !e.queued
		e.queued = true
		e.mu.Unlock()
		if need {
			s.enqueue(e) // enqueue after releasing mu
		}
		return &nr, nil
	}
}
```

- The read-modify-write is placed **entirely** inside `mu`, so two requests modifying different columns do not lose updates, and concurrent increments are not lost either.
- **Increment (OpIncr)**:
  - Supported on integer columns (int2/int4/int8) and floating-point columns (float4/float8). numeric columns do not need increment (confirmed).
  - If the old value is NULL, or the result is out of the column type's range (e.g. int4 overflow), `ErrInvalid` is returned and the whole Change has no effect. This is also rejecting early at update time, so the poison row does not only show up at flush time (7.9).
  - The response returns the new value after the increment (7.10), just like Redis's INCRBY.
  - **Flush writes absolute values** (I12): memory is authoritative and the DB is just its mirror. The flush writes `SET score = 15`, not `SET score = score + 5`. This way coalesced writes (no matter how many increments happen between two flushes, only one write), rewrites after failure, and late execution are all naturally correct; writing deltas would require tracking the accumulated delta separately, and the semantics would be hard to pin down when mixed with SET.
  - An increment is also an update: on a crash, increments within the flush window are lost just like ordinary updates. The business side has confirmed: increment is only used for less important data, and a small amount of loss is acceptable.
- Primary-key columns cannot be modified (I13); the same column cannot appear twice in one Change. Both are validated by the API layer, and violations return `INVALID_ARGUMENT`.
- An update that arrives while a delete is in progress is **allowed to succeed** (confirmed, see section 15). Its linearization point is before the delete, and when the delete succeeds its changes are discarded along with it.
- **`Change.Gen`: optional, not checked by default** (13.1 item 8). It distinguishes different lives of the same primary key. Scenario: client A reads 42 (gen=100); B deletes 42 and then recreates 42 (gen=200); A then sends an update intended for the old 42, but it would modify the new 42. If A includes Gen=100, this update returns NotFound.
  - Impact on the business side: if you do not care, leave it empty (0 means no check), which is exactly the same as not having this feature; the interface does not become more complex because of it. When primary keys are never reused (UUID, snowflake), this problem never arises.
  - When needed, gen is in the Get/Create response (7.10) and can be used directly. Delete supports the same optional gen (7.5).

### 7.4 Create (synchronous)

```go
// runSync runs fn in a background goroutine while holding op, and releases op only once the outcome is determined.
// If the client's ctx expires first, it returns ErrOutcomeUnknown to the client first, and the background keeps running.
func runSync[T any](ctx context.Context, s *Store, e *entry, fn func(*entry) (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	s.inflight.Add(1)
	go func() {
		defer s.inflight.Done()
		var r result
		// Release op first, then answer: a caller that has its answer always finds op free
		// (the order of the model's FinishOp). Deferred, so it happens on every path.
		defer func() {
			e.unlockOp()
			done <- r
		}()
		r.v, r.err = fn(e)
	}()
	select {
	case r := <-done:
		return r.v, r.err
	case <-ctx.Done():
		var zero T
		return zero, ErrOutcomeUnknown
	}
}

// On success Create returns the full row from the DB (including defaults filled in by the DB), which the API layer returns to the client as-is.
// Returning (nil, nil) means: the create for this gen took effect long ago, but the row was deleted afterwards.
func (s *Store) Create(ctx context.Context, in *Row) (*Row, error) {
	e, err := s.lockOpLive(ctx, in.Key)
	if err != nil {
		return nil, err // timed out while still queued: definitely not executed
	}
	return runSync(ctx, s, e, func(e *entry) (*Row, error) { return s.createLocked(e, in) })
}

func (s *Store) createLocked(e *entry, in *Row) (*Row, error) {
	// Load first, and use memory (authoritative) to decide whether it already exists. INSERT is sent only when it definitely does not exist:
	// then the row is also definitely absent from the DB, so the dedup record and the business row are necessarily written together (13.2 holes 1, 2).
	cur, err := s.ensureLoadedLocked(s.bgCtx, e)
	if err != nil {
		return nil, err
	}
	if cur != absent {
		if in.Gen != 0 && cur.Gen == in.Gen {
			return cur, nil // client retry; the earlier attempt already succeeded (idempotent)
		}
		if in.Gen != 0 {
			// Another life is here. If the dedup table holds in.Gen, this is a client retry of a create that took
			// effect earlier: that life was deleted since, and the key was created again (13.2 hole 6).
			// Answer success with no row, exactly like the "inserted, then deleted" case below.
			claimed, err := s.db.GenClaimed(s.bgCtx, e.id, in.Gen) // one SELECT on the dedup table's primary key
			if err != nil {
				return nil, err // not executed; the SDK does not take this as the final answer of a retry (7.10)
			}
			if claimed {
				return nil, nil
			}
		}
		return nil, ErrAlreadyExists
	}
	nr := *in
	if nr.Gen == 0 {
		nr.Gen = s.newGen() // non-zero 63-bit random number; it only needs to differ from other lives of the same id
	}
	nr.Ver = 0
	got, err := s.insertUntilSure(s.bgCtx, &nr)
	if err != nil {
		return nil, err // definite failure; memory unchanged
	}
	if got == nil {
		// This gen was inserted long ago and then deleted: the create took effect back then (its linearization point was at that time),
		// memory keeps the tombstone, and success is returned to the client (13.2 hole 2).
		return nil, nil
	}
	// got is the full row returned by INSERT ... RETURNING (or the row SELECTed during confirmation),
	// including the defaults filled in by the DB, so memory and the DB are exactly identical.
	e.mu.Lock()
	e.snap.Store(got) // linearization point: after the DB definitely succeeded, before returning (I7)
	s.clearDirtyLocked(e)
	e.mu.Unlock()
	s.touch(e)
	return got, nil
}

// insertUntilSure retries with the same gen until the outcome is determined.
// Each attempt writes the dedup record and the business row in the same statement (I14, see 8.3).
// On success it returns the full row from the DB; returning (nil, nil) means this gen was inserted and then deleted.
func (s *Store) insertUntilSure(ctx context.Context, r *Row) (*Row, error) {
	unsure := false             // some attempt had an ambiguous outcome and may have executed (13.2 hole 4). Never reset.
	var inflight []db.SessionID // PG sessions of ambiguous attempts that may still execute (13.2 hole 5). Emptied by a fence.
	for attempt := 0; ; attempt++ {
		got, err := s.db.InsertDedup(ctx, r)
		switch {
		case err == nil:
			return got, nil
		case errors.Is(err, db.ErrGenClaimed):
			// This gen is already in the dedup table: some earlier "ambiguous" attempt actually succeeded,
			// or this is a client retry. Either way, the create for this gen has already taken effect.
			unsure = true // from here on we must never return "failure"
			cur, serr := s.db.Select(ctx, r.Key)
			switch {
			case serr == nil && cur.Gen == r.Gen:
				return cur, nil
			case errors.Is(serr, db.ErrNoRows):
				return nil, nil // inserted, then deleted afterwards
			case serr == nil:
				// Memory says it does not exist, but the DB holds a different life: loading first guarantees this cannot happen, so an invariant has been broken
				return nil, fmt.Errorf("%w: gen %d claimed, row has gen %d", ErrInternal, r.Gen, cur.Gen)
			}
			err = serr // the SELECT itself failed; handle it with the rules below
		case errors.Is(err, db.ErrDuplicate):
			// The dedup record was written, but the business row conflicted on the primary key: as above, loading first guarantees this cannot happen
			return nil, fmt.Errorf("%w: gen %d claimed but row exists", ErrInternal, r.Gen)
		}
		switch {
		case db.IsAmbiguous(err):
			unsure = true // ambiguous outcome: keep retrying
			inflight = append(inflight, db.SessionOf(err)) // the session this attempt was sent on (8.3)
		case db.NotExecuted(err) && unsure:
			// This attempt was not executed at all, so it tells us nothing about the earlier ambiguous outcome: keep retrying (13.2 hole 4)
		case db.IsCrossRow(err) && len(inflight) > 0:
			// Rejected because of ANOTHER row (unique index, foreign key, ...; 8.4). That row can change, and then an
			// earlier attempt that is still in flight would succeed (13.2 hole 5). Terminate the sessions of those
			// attempts and confirm they are gone: after that, each of them has either executed already or never will.
			if ferr := s.db.FenceSessions(ctx, inflight); ferr != nil {
				break // could not confirm: back off and come round again, inflight is kept
			}
			inflight = nil // unsure stays true: an earlier attempt may have executed before the fence finished
			continue       // try again; with nothing in flight, the next verdict on the data is final
		default:
			// Definite failure, one of:
			//  - there was no earlier ambiguous attempt;
			//  - PG rejected it based on this row's own data (the earlier attempt had identical parameters, so it was rejected too);
			//  - PG rejected it because of another row, and nothing of this create is in flight any more. Had an earlier
			//    attempt succeeded, this one would have conflicted on the dedup table first and never reached that check.
			return nil, err
		}
		if werr := s.backoff(ctx, attempt); werr != nil {
			return nil, werr // the service is shutting down
		}
	}
}
```

- A Get arriving during creation that sees nil will queue on op and wait; if it sees absent, it returns not-found (linearized before the create). Either way, it never sees a row that has not yet been persisted.
- When the outcome is ambiguous, **you cannot just "confirm" with a SELECT and call it done**: SELECT seeing nothing does not mean that the late INSERT will not land afterwards. You must retry with the same gen until the outcome is determined.
- **The primary-key unique constraint alone is not enough** (13.2 holes 1, 2): after a row is deleted, a late INSERT, or a client retry carrying the same gen, would insert it back. So the INSERT and the dedup record are written in the same statement (8.3): the same (table, primary key, gen) can be inserted only once in its lifetime (I14). Deleting the business row does not delete the dedup record.
- **Why load first**: if we INSERT directly without memory having been loaded, and the DB actually already holds a different life, the dedup record gets written but the business row fails to insert, and this gen is "burned". A later retry sees "already inserted" and cannot tell whether it was "inserted and then deleted" or "never inserted at all". After loading first, INSERT happens only when memory definitely says it does not exist; then the row is also definitely absent from the DB (I4), and the two are necessarily written together.
- **Definite failures must be split into three kinds** (13.2 holes 4, 5; classification in 8.4). After an ambiguous attempt has occurred:
  - **Not executed** (`db.NotExecuted`): tells us nothing about the earlier attempt, so we must keep retrying.
  - **Rejected based on this row's own data** (NOT NULL, CHECK, type, length): can be returned, because the earlier attempt had exactly the same parameters and must have been rejected too. If the earlier attempt had actually succeeded, this attempt would first conflict on the dedup table and never reach those checks.
  - **Rejected because of another row** (`db.IsCrossRow`: a unique index other than the primary key, a foreign key, an exclusion constraint, a trigger): the same statement can be rejected now and accepted later, so the earlier attempt may still succeed after we have answered "failed". It can be returned only when no earlier attempt can still be in flight. `inflight` records the sessions of the ambiguous attempts; `FenceSessions` terminates them and confirms they are gone (8.3), which is the per-operation form of the startup fence (7.8). Only tables with `AllowCrossRowConstraints` reach this branch (5.3).
- **`unsure` and `inflight` are two different facts.** `unsure` means "an earlier attempt may have executed"; `inflight` means "an earlier attempt may still execute". A fence settles the second and not the first: an attempt can land a moment before its session is terminated. So the fence empties `inflight` and leaves `unsure` set, and a later "not executed" failure still does not end the loop. (The first version of this fix cleared both; TLC produced the counterexample in two seconds: ambiguous A, B rejected by another row, A lands, fence, C not executed, "failed" returned while the row exists.)
- **A retry that finds another life** (13.2 hole 6): when memory holds a row with a different gen, `createLocked` looks the request's gen up in the dedup table before answering `ErrAlreadyExists`. If it is there, the create took effect earlier and the answer is success with no row. The lookup happens while op is held. By then every earlier attempt with this gen has either put the gen into the dedup table or can no longer take effect (that is what "until sure" guarantees), so an `ErrAlreadyExists` answer cannot be overtaken by a late INSERT.
- When the client receives `ErrOutcomeUnknown` (or the connection drops after the request was sent), it retries the create with the same gen, which is idempotent: even if someone else deleted the row in the meantime, the dedup table blocks the re-insertion. The Go SDK generates the gen and retries automatically, so the business side never needs to touch gen (see 7.10).

### 7.5 Delete (synchronous)

```go
// gen is optional; 0 means no check. When non-zero only this one life is deleted, and a gen mismatch returns NotFound (see 7.3).
func (s *Store) Delete(ctx context.Context, id Key, gen int64) error {
	e, err := s.lockOpLive(ctx, id) // waits for any in-progress flush to finish
	if err != nil {
		return err
	}
	_, err = runSync(ctx, s, e, func(e *entry) (struct{}, error) {
		return struct{}{}, s.deleteLocked(e, gen)
	})
	return err
}

func (s *Store) deleteLocked(e *entry, gen int64) error {
	r, err := s.ensureLoadedLocked(s.bgCtx, e)
	if err != nil {
		return err
	}
	if r == absent || (gen != 0 && r.Gen != gen) {
		return ErrNotFound
	}
	// (1) Delete from the DB first. mu is not held here, so updates can keep writing to memory.
	if err := s.deleteUntilSure(s.bgCtx, e.id, r.Gen); err != nil {
		return err // definite failure: the row still exists, dirty is kept, and it is flushed as usual later
	}
	// (2) Linearization point: within the same mu critical section, swap in the tombstone and discard the dirty data
	e.mu.Lock()
	e.snap.Store(absent)
	s.clearDirtyLocked(e)
	e.mu.Unlock()
	s.touch(e) // the tombstone's NegativeIdleTTL is counted from the moment of deletion
	return nil // (3) return only after it has taken effect
}

// deleteUntilSure: DELETE FROM items WHERE id=? AND gen=?
// Affecting 0 or 1 rows both count as success (DELETE is idempotent). If the outcome is ambiguous, retry until it is determined.
func (s *Store) deleteUntilSure(ctx context.Context, id Key, gen int64) error {
	unsure := false             // some attempt had an ambiguous outcome and may have executed (13.2 hole 4). Never reset.
	var inflight []db.SessionID // sessions of ambiguous attempts that may still execute (13.2 hole 5)
	for attempt := 0; ; attempt++ {
		err := s.db.Delete(ctx, id, gen)
		switch {
		case err == nil:
			return nil
		case db.IsAmbiguous(err):
			unsure = true
			inflight = append(inflight, db.SessionOf(err))
		case db.NotExecuted(err) && unsure:
			// This attempt was not executed at all, so it tells us nothing about the earlier one: the earlier one may already have deleted it; keep retrying
		case db.IsCrossRow(err) && len(inflight) > 0:
			// A child row references this one. Once that child is gone, an earlier DELETE still in flight would
			// succeed: fence those sessions first, then try again (same rule as insertUntilSure; unsure stays true).
			if ferr := s.db.FenceSessions(ctx, inflight); ferr != nil {
				break
			}
			inflight = nil
			continue
		default:
			// Definite failure (same rules as insertUntilSure). For "rejected because of another row" with nothing in
			// flight: had an earlier attempt deleted the row, this one would have affected 0 rows and returned nil.
			return err
		}
		if werr := s.backoff(ctx, attempt); werr != nil {
			return werr // the service is shutting down
		}
	}
}
```

- **Why an update can never reach the row after delete returns**: Update's read-modify-write and step (2) are under the same `mu`, so one must come strictly before the other. If the Update is before (2), its changes are discarded by `dirty=0`; if the Update is after (2), it sees absent and returns NotFound.
- (2) must come **after** the DB succeeds (otherwise, if the DB delete fails, memory and the DB become inconsistent), and **before returning**.
- `deleteUntilSure` uses the same rules as `insertUntilSure` (7.4) to decide "definite failure": if the DELETE was actually executed but the response was lost, and the subsequent retry is rejected, returning failure would leave the row in memory while it is already gone from the DB (13.2 hole 4).
- The entry is not removed from the map; instead it is replaced with a tombstone. Reasons:
  1. Requests that already hold this entry pointer and are queued can see "deleted" immediately
  2. It acts as a negative cache (the client may keep sending updates after the delete)
  3. The delete path stays simple; physical delete is handled uniformly by eviction

### 7.6 Flush (write-behind)

Each flush worker is responsible for a fixed set of shards (e.g. shard i goes to worker i % W), and processes one round every `FlushInterval`:

```go
type flushItem struct {
	e     *entry
	row   *Row
	mask  uint64
	since time.Time
}

func (s *Store) flushShard(sh *shard) {
	sh.qmu.Lock()
	batch := sh.queue
	sh.queue = nil
	sh.qmu.Unlock()

	var items []flushItem
	var retry []*entry
	for _, e := range batch {
		if !e.tryLockOp() {
			retry = append(retry, e) // create/delete/load in progress; handle it next round; queued stays true
			continue
		}
		e.mu.Lock()
		r, mask := e.snap.Load(), e.dirty
		if e.dead || r == nil || r == absent || mask == 0 {
			e.queued = false
			e.mu.Unlock()
			e.unlockOp()
			continue
		}
		since := e.dirtySince
		s.clearDirtyLocked(e)
		e.queued = false
		e.mu.Unlock()
		items = append(items, flushItem{e, r, mask, since}) // op is held until this batch has been written
	}
	s.requeueAll(sh, retry)

	// Execute multiple statements in one transaction:
	// UPDATE <table> SET <columns in mask>, ver=? WHERE <primary key>=? AND gen=? AND ver < ?
	// Everything SET is the absolute value in memory, including incremented columns (I12).
	// A timeout or any failure is treated as failure (the conditional write is idempotent, so rewriting is safe).
	// If it is a "definite failure" (e.g. a constraint violation), the batch contains a poison row; retry row by row to find it (see 7.9).
	err := s.db.UpdateBatch(s.bgCtx, items)
	for _, it := range items {
		if err != nil {
			e := it.e
			e.mu.Lock()
			s.markDirtyLocked(e, it.mask) // merge the mask back; next time rewrite with the latest values
			if it.since.Before(e.dirtySince) {
				e.dirtySince = it.since // keep the earliest dirty time so monitoring stays accurate
			}
			need := !e.queued
			e.queued = true
			e.mu.Unlock()
			if need {
				s.enqueue(e)
			}
		}
		it.e.unlockOp()
	}
	// When the DB keeps failing, apply exponential backoff to this shard
}
```

Key points:

- **Coalesced writes**: between two flushes, no matter how many updates happen, the same row is written to the DB only once. The backlog is bounded by the "number of dirty rows", not the "number of updates".
- A new update arriving during a flush re-enqueues the row, but the next flush has to wait for op. So for the same key, only one UPDATE is executing at any moment (I5).
- During batch flush, op is taken with TryLock to avoid create/delete and the flush waiting on each other. Each shard has only one worker, so workers never contend for the same key.
- Affecting 0 rows is **not an error by itself**: it may be that an earlier "timed out" write actually succeeded, or that the row has been deleted. Record a `flush_noop_rows` metric. But 0 rows is also what a write returns when this instance has been superseded (I16), so **whenever an UPDATE or DELETE affects 0 rows, check the owner epoch before using the result** (`SELECT epoch FROM row_service_epoch`). If it is no longer this instance's epoch, stop the instance (7.8). The extra query runs only on this rare path.
- PostgreSQL's RowsAffected is the number of rows matched and updated (rows whose values did not change are counted too); it does not have MySQL's issue of "counting only rows actually changed".
- See 8.3 for the concrete SQL of batch writes.

### 7.7 Eviction (physical delete, the dead protocol)

**There is only one safety condition**: while holding op, `dirty == 0`, i.e. memory and the DB are exactly identical (I4). As long as this condition holds, if an evicted entry is accessed again later it is just a miss, and the state loaded back is exactly the same. Which keys will be accessed again later only affects the hit rate, not correctness. A dirty entry is not evicted even if it is idle; it is handled in the next round after the flush.

Eviction has two triggers:

| Trigger | Condition | Approach |
|---|---|---|
| Idle sweep | `now - lastAccess > IdleTTL` (absent entries or entries that failed to load use `NegativeIdleTTL`) | Each shard is sample-checked once every `EvictInterval` |
| Capacity eviction | `entryCount > MaxEntries` (high-water mark) | Sample, and evict the least recently accessed in the sample, until dropping to the low-water mark (e.g. 0.9 × MaxEntries) |

**Why sampling, rather than a full scan or an LRU list:**

- A full scan has to iterate the entire large map under RLock. Meanwhile getOrCreate's write lock can only wait, and Go's RWMutex, when a writer is queued, also blocks new readers, so the whole shard stalls. Sampling puts an upper bound on each lock hold time.
- An LRU list requires moving a list node on every hit; that is a write, needs a lock, and breaks "the hit path is read-only".
- This is exactly what Redis does: active expiration uses random sampling, and `allkeys-lru` approximates LRU with sampling.
- Cost: expired entries are not cleared precisely when the TTL is reached, but somewhat later. TTL is a memory policy, not a correctness requirement, so this is acceptable.

```go
const sampleK = 64

// idleSweepShard: Redis-style sampled expiration. If more than 25% of the sample is expired, immediately run another round.
func (s *Store) idleSweepShard(sh *shard) {
	now := s.nowSec.Load()
	expired := func(e *entry) bool { return s.idleExpired(e, now) }
	for round := 0; round < s.cfg.MaxSweepRounds; round++ {
		sampled := 0
		cands := make([]*entry, 0, sampleK)
		sh.mu.RLock()
		for _, e := range sh.m { // Go map iteration starts at a random point each time; take the first K as a random sample
			if expired(e) {
				cands = append(cands, e)
			}
			if sampled++; sampled == sampleK {
				break
			}
		}
		sh.mu.RUnlock() // release the shard lock before touching entry locks
		for _, e := range cands {
			s.tryEvict(sh, e, expired)
		}
		if len(cands)*4 <= sampled {
			return // expired ratio is at most 25%; end this round
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

// tryEvict runs the dead protocol. After acquiring the locks it re-checks with eligible: it may have been accessed again after sampling.
func (s *Store) tryEvict(sh *shard, e *entry, eligible func(*entry) bool) bool {
	if !e.tryLockOp() {
		return false // a DB operation is in progress
	}
	defer e.unlockOp()

	e.mu.Lock()
	ok := !e.dead && e.dirty == 0 && eligible(e) // eligible only does atomic reads; purely in-memory
	if ok {
		e.dead = true // holding both op and mu (I9)
	}
	e.mu.Unlock() // release mu first, keeping mu a leaf lock
	if !ok {
		return false
	}

	sh.mu.Lock() // still holding op
	if sh.m[e.id] == e { // defensive check
		delete(sh.m, e.id)
		s.entryCount.Add(-1)
	}
	sh.mu.Unlock()
	return true
}
```

Capacity eviction (`capacityEvictShard`) follows the same flow: take K samples under RLock, release the lock, sort by `lastAccess` from oldest to newest, and call `tryEvict` on each (eligible simply returns true), until this shard has evicted its quota. If capacity is already exceeded and all entries in the sample are dirty and cannot be evicted, the flush is falling behind; at that point backpressure (11.1) is relied on to limit growth.

**The three rules of the dead protocol:**

1. **The remover**: holds both op and mu, confirms `dirty == 0`, sets `dead = true`, then deletes it from the map.
2. **Everyone who takes a lock**: after acquiring op or mu, checks dead first. If dead, release the lock and getOrCreate again.
3. **Lock-free readers**: do not check dead. What they read is the latest value at the moment of removal; placing the linearization point at the removal moment keeps it linearizable.

- In the few microseconds between releasing mu and deleting from the map, an Update may get the same dead e. It releases mu and waits for op, which the remover holds until the map has dropped e, and then looks the key up again: it blocks instead of spinning. This is the cost of keeping mu a leaf lock, and is acceptable.
- `s.nowSec` is updated once per second by a background ticker. Idle TTL precision is at the second level, which is entirely sufficient.

### 7.8 Startup and graceful shutdown

**Startup**

1. Load the configuration and open **one** connection to PG (the pool is opened in step 4).
2. **Take over the owner epoch** (I16, 13.2 hole 8):

   ```sql
   UPDATE row_service_epoch SET epoch = epoch + 1 RETURNING epoch;   -- autocommit; the result is this instance's epoch
   SELECT clock_timestamp();                                         -- a separate statement, after the commit: the fence cutoff
   ```

   From the moment this commits, any statement that starts with an older epoch changes nothing.
3. **Clean up the sessions the previous process left on PG** (I15, 13.2 hole 3). On the same connection, execute:

   ```sql
   SELECT pg_terminate_backend(pid, 5000)      -- PG 14+: wait up to 5 seconds until it actually exits
   FROM pg_stat_activity
   WHERE usename = current_user
     AND backend_type = 'client backend'
     AND backend_start <= $1                   -- the cutoff from step 2
     AND pid <> pg_backend_pid();
   ```

   Then repeatedly query `pg_stat_activity` until no session with `backend_start <= cutoff` remains; if any remain after `StartupFenceTimeout`, refuse to start and report an error.
4. Open the connection pool, load the schema, and start the Store and background tasks (flush, eviction, dedup table cleanup, epoch check).
5. The TCP server starts accepting connections.

**Why the epoch and the fence are both needed, in this order.** A statement checks the epoch when it runs. A statement of the old instance that started before step 2 committed has already read the old epoch and passes the check; only terminating its session stops it, which is step 3. A statement that starts after step 2 committed sees the new epoch and changes nothing, whichever session it runs in; that covers everything the old instance sends after it reconnects, which step 3 cannot (the old instance's pool reconnects at once, so "no other session of this account" might never become true; that is why step 3 uses a cutoff). The cutoff is read after the bump has committed, so a session that starts after the cutoff can only run statements that see the new epoch.

**What happens to an instance that has been superseded.** It finds out in one of three ways: a create's result says it is not the owner (8.3), a write affects 0 rows and the check of 7.6 shows another epoch, or the periodic check (every `EpochCheckInterval`) shows another epoch. It then **stops at once**: it closes all client connections and exits, without flushing (a flush would change nothing). Its unflushed updates are lost, exactly as in a crash. The old instance therefore behaves like a process that crashed at the moment the new one took over, plus at most one `EpochCheckInterval` during which it still answers reads from its own memory and acknowledges updates that will be lost. During that interval, clients still connected to it are not served linearizably; routing clients to one instance remains the deployment's job (11.3). What the epoch guarantees is that the DB is written by one instance only, so none of the "not tolerable" clauses of section 1 can be violated by a second instance.

**The same check covers a DB that went back in time** (section 2): the periodic check also reads `pg_control_checkpoint()`'s `timeline_id` and compares it with the value read at startup. A promotion or a point-in-time recovery starts a new timeline; when the value changes, the instance stops in the same way. This is a detector, not a guarantee: restarting the service after a failover or restore remains an operations rule. (M7 must verify on the PG version in use that the service account may call this function and how soon after a promotion the value changes.)

**Why step 3 must complete before loading any data**: when the previous process crashed (or its graceful shutdown timed out), SQL it issued may still be in flight. ver is monotonic only within one process; after the new process loads from the DB, the ver it uses may collide with those SQL statements; if those statements land after the load, memory and the DB diverge permanently (13.2 hole 3). In PG, a SQL statement can only execute within the session it belongs to. Once the session is terminated, uncommitted statements are rolled back, and in-flight packets that arrive later have no session to execute them. So after step 3 completes, the previous process's SQL has either already finished executing (and step 4 will load it), or will never execute.

**Prerequisite**: the DB account used by this service is used only by this service (section 2); otherwise other programs' sessions would be terminated too. Step 3 alone does not protect against a second instance: the second instance terminates the first instance's sessions, but the first instance reconnects and keeps writing. Step 2 is what stops it (13.2 hole 8).

**Why "the new process can only bind the port after the old one exits" does not replace step 3**: it proves that the old *process* is gone, not that its SQL is gone. A PG backend notices that its client died only when it next talks to it, so a statement that is executing (or waiting on a lock) runs to completion and commits, and bytes already in the kernel send buffer or on the network are still delivered and executed. Verified on PG 16: after `kill -9` of a client whose UPDATE was running, its backend stayed `active` and the UPDATE committed 3 seconds later, after a new client had already read the old value; with step 2 run first, it never committed. (`client_connection_check_interval`, PG 14+, only narrows the window: a statement that finishes before the next check still commits.) Binding the same port is still useful: on a single host it makes "the old process is dead" checkable before the new one serves clients.

**Graceful shutdown**

1. The TCP server stops accepting new connections; new requests arriving on existing connections all return `UNAVAILABLE` (see 7.10).
2. Wait for in-progress create/delete to finish (`s.inflight.Wait()`; `bgCtx` has not been cancelled yet at this point).
3. Flush in a loop until all queues are empty and `dirtyCount == 0`, or the shutdown deadline is reached. If the deadline is reached first, log how many rows were still dirty and the age of the oldest: those updates are lost. **Restarting the service does not help while the DB cannot be written; it throws away the backlog** (11.1).
4. Close all connections, cancel `bgCtx`, and exit.

If undetermined create/delete operations are interrupted during shutdown, that is fine: on restart, step 3 guarantees those SQL statements will not land anymore, and afterwards all data is reloaded from the DB, so it is naturally consistent with the DB.

### 7.9 Poison rows: rows definitely rejected by the DB during flush

This is an inherent problem of write-behind: the update has already been acknowledged to the client as successful, but during flush the DB explicitly rejects it (violating a CHECK constraint, foreign key, NOT NULL, length limit, type error, etc.). Such a row fails no matter how many times it is retried. Moreover, when writing in batches within one transaction, if any one row fails the whole batch is rolled back, so one poison row drags down every row in the same batch.

How it is handled:

1. **Reject in memory up front as much as possible**: when the API layer builds a Change, it validates type, NOT NULL, varchar(n) length, and integer range against the schema; when the increment result is computed inside `mu`, NULL and overflow are also checked (7.3). Whatever can be rejected at update time should not be left to be discovered at flush time. CHECK constraints (arbitrary SQL expressions) and foreign keys cannot be fully validated in Go; the business side has confirmed: columns updated through this service should, as far as possible, carry no CHECK constraints or foreign keys.
2. **Isolation**: when a whole batch has a "definite failure", split the batch into row-by-row writes (one transaction per row, or using SAVEPOINT), find the few rows that definitely fail, and write the other rows normally.
3. **Quarantine**: mark the poison row as `poisoned` (protected by mu). It stays dirty, but no longer goes into ordinary batches; instead it is retried on its own with a longer backoff. Because a later update may correct the value, every new update also triggers a retry. At the same time, report the alert metric `poisoned_rows` and log the error returned by the DB.
4. **Never drop silently**: provide an admin interface so a human decides how to handle it: discard the in-memory changes and reload from the DB (which amounts to undoing updates that were already acknowledged), or correct the data and retry.
5. Poison rows are never evicted, because they are dirty (I10).

### 7.10 External API: custom TCP protocol + protobuf

Why this protocol was chosen (13.1 item 4): Update is a pure in-memory operation that takes the server only a few microseconds, so the protocol's own overhead is a large share of the cost. A custom protocol avoids HTTP/2 framing, header compression, and the gRPC middle layer. The costs are covered in section 15.

**Framing**

```
+-----------------------------+-------------------------------------+
| length: 4 bytes, big-endian | protobuf-encoded Request / Response |
+-----------------------------+-------------------------------------+
```

- length does not include its own 4 bytes. A value above `MaxFrameBytes` is treated as a protocol error and the connection is closed immediately.
- The client sends a `Request`, the server replies with a `Response`, and the two are paired by `request_id`. The first request after a connection is established must be `Hello`, which negotiates the protocol version.
- **Multiplexing on a single connection**: a connection can carry multiple in-flight requests at once, and **responses may be returned out of order**. Create/Delete have to wait for the DB (milliseconds) while Update takes only a few microseconds; if responses were returned in order, fast requests would be stuck behind slow ones (head-of-line blocking).
- **The server does not guarantee any execution order for requests that are in flight at the same time on the same connection**: they overlap in time, so by the definition of linearizability either order is valid (section 9). If ordering is required (for example, create then update), wait for the previous response before sending the next request. The Go SDK's synchronous interface naturally works this way.

**Message definitions (draft, `/api/rowsvc/v1/rowsvc.proto`)**

```protobuf
syntax = "proto3";
package rowsvc.v1;

message Request {
  uint64 request_id = 1;  // assigned by the client, unique within a connection
  uint32 timeout_ms = 2;  // the server uses this to set the ctx deadline; 0 means use DefaultRequestTimeout
  oneof body {
    HelloRequest    hello    = 10;
    DescribeRequest describe = 11;
    GetRequest      get      = 12;
    CreateRequest   create   = 13;
    UpdateRequest   update   = 14;
    DeleteRequest   delete   = 15;
    PingRequest     ping     = 16;
  }
}

message Response {
  uint64 request_id = 1;
  Status status     = 2;
  string error      = 3;  // error details, for logging and troubleshooting only; clients must not parse it
  oneof body {
    HelloResponse    hello    = 10;
    DescribeResponse describe = 11;
    GetResponse      get      = 12;
    CreateResponse   create   = 13;
    UpdateResponse   update   = 14;
    DeleteResponse   delete   = 15;
    PingResponse     ping     = 16;
  }
}

message HelloRequest  { uint32 protocol_version = 1; string client_name = 2; }
message HelloResponse { uint32 protocol_version = 1; uint32 max_frame_bytes = 2; uint32 max_inflight = 3; }
message PingRequest   {}
message PingResponse  {}

// Columns are identified by ordinal, so requests never carry column names. Ordinals come from Describe.
message DescribeRequest { string table = 1; }
message DescribeResponse {
  uint32 table_id            = 1;
  uint64 schema_version      = 2;  // schema fingerprint (hash of table name, column names, types, order); unchanged across restarts as long as the table structure is unchanged
  repeated Column columns    = 3;  // the index is the column ordinal; excludes gen, ver and generated columns
  repeated uint32 pk_columns = 4;  // ordinals of the primary key columns, in primary key order
}
message Column { string name = 1; string pg_type = 2; bool not_null = 3; bool has_default = 4; }

// Every table-level request below carries table_id and schema_version.
// On mismatch, SCHEMA_MISMATCH is returned and nothing is executed.

message GetRequest {
  uint32 table_id       = 1;
  uint64 schema_version = 2;
  repeated Value  key     = 3;  // values of the primary key columns, in pk_columns order
  repeated uint32 columns = 4;  // return only these columns; empty means return all columns
}
message GetResponse { Row row = 1; }

message CreateRequest {
  uint32 table_id       = 1;
  uint64 schema_version = 2;
  int64  gen            = 3;    // non-zero random number, generated automatically by the SDK; retries use the same gen to stay idempotent (7.4)
  repeated uint32 columns = 4;  // must include all primary key columns; columns not given take the DB default
  repeated Value  values  = 5;  // one-to-one with columns
}
message CreateResponse { Row row = 1; }  // the full row in the DB, including defaults filled in by the DB;
                                         // empty if, on a retry, this gen was already created long ago and has since been deleted (7.4)

message UpdateRequest {
  uint32 table_id       = 1;
  uint64 schema_version = 2;
  repeated Value key    = 3;
  int64  expected_gen   = 4;    // optional, 0 means no check (7.3)
  repeated ColumnChange changes = 5;
}
message ColumnChange {
  enum Op { SET = 0; INCR = 1; }
  uint32 column = 1;  // must not be a primary key column; must not repeat within one request
  Op     op     = 2;
  Value  value  = 3;  // SET: new value; INCR: delta (int_v for integer columns, float_v for floating-point columns)
}
message UpdateResponse {
  int64 gen = 1;
  int64 ver = 2;
  repeated Value incr_results = 3;  // new values after the increment, in the order the INCRs appear in changes
}

message DeleteRequest {
  uint32 table_id       = 1;
  uint64 schema_version = 2;
  repeated Value key    = 3;
  int64  expected_gen   = 4;    // optional, 0 means no check
}
message DeleteResponse {}

message Row {
  int64 gen = 1;
  int64 ver = 2;
  repeated Value values = 3;    // one-to-one with the request's columns; if the request did not specify columns, all columns in ordinal order
}

message Value {                 // mapping to column types: see 5.3
  oneof kind {
    bool   is_null   = 1;       // true means NULL
    bool   bool_v    = 2;
    sint64 int_v     = 3;       // int2 / int4 / int8
    double float_v   = 4;       // float4 / float8
    string numeric_v = 5;       // decimal string
    string text_v    = 6;
    bytes  bytes_v   = 7;       // bytea
    bytes  uuid_v    = 8;       // 16 bytes
    sint64 time_v    = 9;       // timestamp / timestamptz: Unix microseconds
    sint32 date_v    = 10;      // date: days since 1970-01-01
    bytes  json_v    = 11;      // raw bytes of json / jsonb
  }
}

enum Status {
  STATUS_OK                = 0;
  STATUS_NOT_FOUND         = 1;
  STATUS_ALREADY_EXISTS    = 2;
  STATUS_OUTCOME_UNKNOWN   = 3;
  STATUS_BUSY              = 4;
  STATUS_INVALID_ARGUMENT  = 5;
  STATUS_SCHEMA_MISMATCH   = 6;
  STATUS_DEADLINE_EXCEEDED = 7;
  STATUS_UNAVAILABLE       = 8;
  STATUS_INTERNAL          = 9;
}
```

**Status semantics** (whether a client may retry depends on the "Did the operation take effect" column):

| Status | Meaning | Did the operation take effect | SDK handling |
|---|---|---|---|
| `OK` | Success | Yes | — |
| `NOT_FOUND` | Does not exist, or expected_gen does not match | No | Returned to the caller |
| `ALREADY_EXISTS` | On create, the primary key already exists (another life), and the dedup table does not hold the request's gen (7.4) | No | Returned to the caller |
| `OUTCOME_UNKNOWN` | The create/delete could not determine its outcome before the timeout; the server keeps executing it in the background | **Ambiguous** | create, delete with gen: retry with the same gen; delete without gen: returned to the caller |
| `BUSY` | Backpressure (11.1) | **This request**: no | Retry after backoff |
| `INVALID_ARGUMENT` | Type mismatch, NOT NULL violation, too long, out of range, increment overflow or old value is NULL, modifying a primary key column, duplicate column; or, on create or delete, rejected by a DB constraint (for a constraint on another row: with no earlier attempt in flight, 7.4) | No | Returned to the caller |
| `SCHEMA_MISMATCH` | table_id / schema_version is stale | No | Describe again, then retry once |
| `DEADLINE_EXCEEDED` | Timed out before execution started (queued waiting for the op, waiting for load) | **This request**: no | Returned to the caller, unless an earlier attempt of the same call was ambiguous |
| `UNAVAILABLE` | The service is shutting down, or has been superseded (7.8); or the DB is unavailable (load failed, create/delete not executed) | **This request**: no | Retry after backoff |
| `INTERNAL` | Other errors (server bug) | Writes are treated as ambiguous | Returned to the caller |

**"This request: no" is a statement about one request, not about the call.** `BUSY`, `DEADLINE_EXCEEDED` and `UNAVAILABLE` say that this request did nothing. If an earlier request of the same SDK call (same gen) ended with `OUTCOME_UNKNOWN` or a dropped connection, that earlier request may still take effect, or already has. The SDK rule that follows from this is below ("after an ambiguous attempt").

**Returning gen and ver to the client**

- Get and Create responses carry gen and ver; the Update response carries gen and ver after the update.
- **gen is reliable**: it is written to the DB synchronously at create time and never changes afterwards. It can be used as the identifier of "this life" and passed as expected_gen in Update/Delete.
- **ver is for observation and troubleshooting only; clients must not depend on it**: updates lost in a crash make ver go backwards, so later the same ver may correspond to different contents.

**Server implementation notes**

- One read goroutine and one write goroutine per connection. After the read goroutine decodes a request, it starts a goroutine to handle each request; a semaphore limits the number of in-flight requests per connection to `MaxInflightPerConn`. When the semaphore is full, the read goroutine stops reading, and the pressure propagates back to the client through the TCP window (backpressure).
- Completed responses are put into the write goroutine's channel. The channel capacity equals `MaxInflightPerConn`, so request-handling goroutines never block there. The write goroutine uses a `bufio.Writer` and only Flushes when the channel is momentarily empty, coalescing multiple responses into a single system call.
- The request ctx = the connection ctx + `timeout_ms`. When the connection drops the ctx is cancelled: Get/Update requests queued waiting for load stop waiting; Create/Delete requests that have already entered runSync keep executing in the background until the outcome is determined (7.4).
- Store errors are mapped to Status: `ctx.Err()` -> `DEADLINE_EXCEEDED` (execution not yet started), `ErrOutcomeUnknown` -> `OUTCOME_UNKNOWN`, `ErrInvalid` -> `INVALID_ARGUMENT`, and so on.
- Start with `google.golang.org/protobuf`. If load testing shows encoding/decoding is a bottleneck, switch to code generated by `vtprotobuf` (faster encoding/decoding, and objects can be reused).
- Enable TCP keepalive; close connections idle longer than `IdleConnTimeout`. Clients can use Ping to keep a connection alive.
- The frame decoder must be fuzz tested (`go test -fuzz`): any malformed input may only cause the connection to be closed, never a panic.

**Go client SDK (`/client`)**

The SDK hides all protocol details; the business side sees an ordinary synchronous interface (the following is only illustrative; interface details will be settled during implementation):

```go
c, err := client.Dial(ctx, "10.0.0.1:7070")
items, err := c.Table(ctx, "items") // calls Describe internally and caches the column-name-to-ordinal mapping
row, err := items.Get(ctx, client.Key(int64(42)))
row, err = items.Create(ctx, client.Cols{"id": int64(42), "name": "a"}) // gen is generated by the SDK
res, err := items.Update(ctx, client.Key(int64(42)),
	client.Set("name", "b"),
	client.Incr("score", 5)) // res.Incr("score") is the value after the increment
err = items.Delete(ctx, client.Key(int64(42)))
```

- **Connections**: maintain a small number of connections, each multiplexed. Column-name-to-ordinal translation is done in the SDK; on `SCHEMA_MISMATCH` it calls Describe again and retries once.
- **Timeouts**: the SDK takes the time remaining on the caller's ctx, subtracts a small margin, and puts it in `timeout_ms`, so that the server times out first and returns an explicit Status (the same idea as "server-side timeout shorter than the Go-side timeout" in 8.4).
- **Principle: the SDK automatically retries only idempotent requests** (Get, Create, Delete with gen). For other requests with an ambiguous outcome, `ErrOutcomeUnknown` is handed to the caller.
- **After an ambiguous attempt, "not executed" is never the final answer** (13.2 hole 7). Once any attempt of a Create or Delete call has ended with `OUTCOME_UNKNOWN` or a dropped connection, the call can end in only three ways: a status that judges the data (`OK`, `ALREADY_EXISTS`, `NOT_FOUND`, `INVALID_ARGUMENT`), which is returned as usual; or `ErrOutcomeUnknown` when the caller's ctx expires. `DEADLINE_EXCEEDED`, `UNAVAILABLE` and `BUSY` received on a retry are retried after backoff, and if the ctx expires meanwhile the SDK returns `ErrOutcomeUnknown`, never the last status. The typical case: the first attempt is still running on the server and holds op; the retry queues behind it and times out there with `DEADLINE_EXCEEDED`; a moment later the first attempt succeeds.
- **Get**: read-only; retried directly when the connection drops.
- **Create**: automatically generates a non-zero random gen. On `OUTCOME_UNKNOWN`, or if the connection drops after the request was sent, it retries with the same gen until the outcome is determined or the caller's ctx expires (returning `ErrOutcomeUnknown` on expiry). The dedup table makes such retries idempotent: even if someone else deletes the row in the meantime, it will not be re-inserted (7.4). A retry answered `OK` with no row means: this create took effect earlier and that life has been deleted since (possibly with another life created after it); the SDK returns success. The gen must come from `crypto/rand` (or the OS-seeded `math/rand/v2` of Go 1.22+), never from a generator seeded with the time: SDKs started at the same moment, or cloned from one VM image, would produce identical sequences, a far likelier source of collisions than chance (correctness-properties.md A5).
- **Delete**: when the caller supplies a gen (`client.WithExpectedGen`), retries are exactly idempotent: on `OUTCOME_UNKNOWN` or a dropped connection, retry with the same gen; if the retry returns `NOT_FOUND`, the earlier attempt already deleted it (or someone else did), and it is treated as success. **Without a gen there is no automatic retry**; like Update, it returns `ErrOutcomeUnknown`: a retry in that case could delete the same primary key that someone else re-created during the retry (the client-level counterpart of example 3 in 8.2). Which life to delete is business semantics, left for the business side to decide (for example, Get first to obtain the gen, then delete).
- **Update**: if the connection drops after the request was sent, it is **not retried automatically**; `ErrOutcomeUnknown` is returned to the caller. Reason: an INCR retry could add twice; a SET retry could overwrite a newer value someone else wrote in the meantime. The business side has confirmed: increments are only used on less important data and do not need "exactly once".
- `BUSY`, `UNAVAILABLE`: retry after backoff. When the caller's ctx expires first: for Get, Update, and a Create or Delete that never had an ambiguous attempt, return the last status (nothing happened); otherwise return `ErrOutcomeUnknown` (the rule above).
- The business side normally does not need to deal with gen: only when it wants to distinguish different lives of the same primary key does it use `client.WithExpectedGen(row.Gen)`.

---

## 8. DB writes and fencing (gen/ver)

### 8.1 The root problem: a timeout does not mean it was not executed

A ctx timeout on the Go side only means we stopped waiting for the result; the SQL may still be stuck waiting on a row lock in the DB or in the network, and will still execute later. If at that point we release the op and send the next SQL over another connection, the order in which the DB actually executes them is no longer the order in which we sent them.

This is not a Go problem but a basic fact of distributed systems: besides success and failure, a request has a third outcome, called "**unknown**".

**Ordering within a process is guaranteed by locks; ordering across processes is guaranteed by version numbers carried in the data.** gen/ver serve as fencing tokens (see DDIA chapter 8): the storage side rejects stale writes, instead of relying on the order in which the client sends them.

Per-key serialization and gen/ver **must both be kept**: serialization handles the normal case, and gen/ver catch the rare but severe case of timeouts.

### 8.2 Three counterexamples (all of which gen/ver must block)

**Example 1: without ver, an old value overwrites a new one**

```
1. update score=10 -> in-memory Ver=5
2. flush A: UPDATE SET score=10 WHERE id=42      (stuck in the DB)
3. A times out on the Go side -> treated as failure: merge the mask back, re-enqueue, release the op
4. update score=20 -> in-memory Ver=6
5. flush B: UPDATE SET score=20 WHERE id=42      (executes immediately) DB: 20 ✓
6. A finally executes -> DB: 10 ✗ (memory is 20; after a restart the user's change is lost)
```

With `AND ver < 5` added, A affects 0 rows. Dropping A is safe because A's mask was merged back, so B necessarily includes all of A's columns, with newer values. This is exactly why the mask must be merged back on failure.

**Example 2: without gen, old data is written into a new object**

```
1. flush A: UPDATE SET score=10, ver=5 WHERE id=42 AND ver<5   (stuck, times out)
2. delete 42 succeeds
3. create 42 again -> INSERT gen=200, ver=0                       succeeds
4. A finally executes: new row ver=0 < 5 -> score=10 is written into the new object ✗
```

With `AND gen = 100` added, A affects 0 rows. gen distinguishes different lives of the same id; ver distinguishes ordering within one life.

**Example 3: a late DELETE deletes a newly created object (directly violating "what has been created must never disappear")**

```
1. DELETE A (connection 1) gets stuck and times out on the Go side
2. retry DELETE B (connection 2) succeeds -> returns "delete succeeded"
3. create 42 again -> INSERT gen=200 succeeds -> returns "create succeeded"
4. A finally executes -> the just-created object is deleted ✗
```

With `AND gen = 100` added, A affects 0 rows.

**What gen/ver cannot block**: gen/ver are conditions in the WHERE clause and only apply to "rows that already exist". The following three kinds of cases rely on three other mechanisms (the counterexamples found by TLA+ are in 13.2):

- **Late or duplicate INSERTs** (holes 1, 2): after a row is deleted, an INSERT has no old row to compare against and succeeds as long as the row does not exist. Blocked by the **dedup table** (I14, see 7.4, 8.3).
- **Late SQL from the previous process** (hole 3): ver is only monotonic within one process, so the ver used by a new process may duplicate one the previous process already sent. Blocked by the **startup fence (terminating the previous process's sessions)** (I15, see 7.8).
- **SQL from an instance that is still running after a newer one has started** (hole 8): it keeps sending new statements, with gens and vers that collide with the newer instance's. Blocked by the **owner epoch** carried in every write (I16, see 7.8). This is the fencing token in its original role: one token per owner, checked by the storage.

### 8.3 SQL templates (PostgreSQL)

The following uses the example table `items(id, gen, ver, name, score)`. The actual SQL is generated dynamically from the schema (see 5.3); column names follow the configuration.

```sql
-- load
SELECT id, gen, ver, name, score FROM items WHERE id = $1

-- create: the dedup record and the business row are written in the same statement; either both succeed or neither takes effect (I14).
-- ON CONFLICT DO NOTHING does not modify existing rows, so it is allowed (what is forbidden is DO UPDATE).
WITH owner AS (
  SELECT 1 FROM row_service_epoch WHERE epoch = $6               -- $6 is this instance's epoch (I16)
), claim AS (
  INSERT INTO row_service_gen_log (tbl, pk, gen)
  SELECT 'items', $1, $2 WHERE EXISTS (SELECT 1 FROM owner)      -- nothing is written by an instance that is not the owner
  ON CONFLICT DO NOTHING
  RETURNING 1                         -- returns 0 rows if this gen has already been inserted
), ins AS (
  INSERT INTO items (id, gen, ver, name, score)
  SELECT $3, $2, 0, $4, $5 WHERE EXISTS (SELECT 1 FROM claim)   -- insert the business row only if the dedup record was claimed
  ON CONFLICT (id) DO NOTHING         -- names the primary key: a conflict on any OTHER unique index raises 23505 (8.4)
  RETURNING id, gen, ver, name, score -- return the full row, including defaults filled in by the DB
)
SELECT EXISTS (SELECT 1 FROM owner) AS owner, EXISTS (SELECT 1 FROM claim) AS claimed, ins.*
FROM (SELECT 1) AS one LEFT JOIN ins ON TRUE   -- always returns exactly one row
-- $1 is the canonical encoding of the primary key, $3 is the primary key itself (multiple parameters for a composite key). Four possible results:
--   owner = false                              -> db.ErrNotOwner: nothing was written; the instance has been superseded and stops (7.8)
--   claimed = true, business columns not null  -> insert succeeded
--   claimed = false                            -> db.ErrGenClaimed (this gen was inserted before; 7.4 then SELECTs the business row to determine the current state)
--   claimed = true, business columns null      -> db.ErrDuplicate (business row conflicts on the primary key; loading first guarantees this cannot happen, so it is treated as a broken invariant)
-- Data-modifying CTEs and the main statement belong to the same statement and the same transaction: either both INSERTs take effect or neither does.
-- An error raised by the business INSERT (a unique index other than the primary key, a foreign key) rolls the dedup record back with it.

-- has this gen been inserted before? (createLocked, when memory holds another life; 7.4)
SELECT 1 FROM row_service_gen_log WHERE tbl = 'items' AND pk = $1 AND gen = $2

-- delete (idempotent; affecting 0 rows also counts as success, after the epoch check of 7.6)
DELETE FROM items WHERE id = $1 AND gen = $2
  AND EXISTS (SELECT 1 FROM row_service_epoch WHERE epoch = $3)

-- fence the sessions of earlier attempts (FenceSessions, 7.4): terminate, then confirm.
-- A session is identified by (pid, backend_start): the pid alone can be reused. Both are read once per connection,
-- in the pool's AfterConnect hook (SELECT pid, backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()),
-- and kept with the connection, so they are known even when the connection is already broken.
SELECT pg_terminate_backend(a.pid, 5000)
FROM pg_stat_activity a JOIN unnest($1::int[], $2::timestamptz[]) AS s(pid, backend_start)
  ON a.pid = s.pid AND a.backend_start = s.backend_start;
SELECT count(*)                       -- repeat until 0; an error or a timeout here means "not confirmed"
FROM pg_stat_activity a JOIN unnest($1::int[], $2::timestamptz[]) AS s(pid, backend_start)
  ON a.pid = s.pid AND a.backend_start = s.backend_start;
```

There are two ways to write a flush, both requiring only one network round trip:

1. **pgx.Batch (recommended to start with)**: one UPDATE per row, writing only the columns in the mask, placed in a `pgx.Batch` and sent all at once via pipelining within one transaction (`tx.SendBatch`):

   ```sql
   UPDATE items SET score = $1, ver = $2 WHERE id = $3 AND gen = $4 AND ver < $2
     AND EXISTS (SELECT 1 FROM row_service_epoch WHERE epoch = $5)   -- I16
   ```

2. **Single statement + unnest (for very large batches)**: writes whole rows at once.

   ```sql
   UPDATE items AS t
   SET name = v.name, score = v.score, ver = v.ver
   FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::bigint[])
        AS v(id, gen, ver, name, score)
   WHERE t.id = v.id AND t.gen = v.gen AND t.ver < v.ver
     AND EXISTS (SELECT 1 FROM row_service_epoch WHERE epoch = $6)   -- I16
   ```

   Because memory is authoritative and only this service writes the table, writing whole rows is also correct. In PG, an UPDATE rewrites the whole row anyway (see 8.5); writing only dirty columns saves only network transfer and SQL parsing, not disk writes.

   However, tables with large TOAST columns (long text, jsonb) can only use pgx.Batch: rewriting a large unchanged value makes PG rewrite its TOAST data, which is expensive. (unnest also flattens multidimensional arrays, so tables with array columns cannot use it either; the business side has no array columns, so this does not apply for now.)

- **Absolutely forbidden**: `INSERT ... ON CONFLICT DO UPDATE` (upsert): a late flush would "resurrect" a row that has already been deleted.
- Rows in the same batch are sorted by id before writing. Each shard has only one worker, and create/delete and flushes are serialized through the op, so different transactions never contend for the same row in the first place. Sorting by id is extra insurance against future row-lock deadlocks at the DB layer.

### 8.4 Error classification: `db.IsAmbiguous(err)` (pgx)

| Error | Classification | Notes |
|---|---|---|
| `pgconn.SafeToRetry(err) == true` | Definite failure: **not executed** | pgx guarantees the statement was not sent yet |
| `*pgconn.PgError`, SQLSTATE `23505` whose `ConstraintName` is the business table's primary key | Definite failure (duplicate key) | Mapped to `db.ErrDuplicate`. With `ON CONFLICT (id) DO NOTHING` this is reported through the result row, not as an error (8.3); a dedup table conflict is mapped to `db.ErrGenClaimed` the same way |
| `*pgconn.PgError`, SQLSTATE `23505` on any other unique index, `23503` (foreign key), `23P01` (exclusion), `P0001` and other errors raised by a trigger | Definite failure: **rejected by PG because of another row** (`db.IsCrossRow`) | The same statement may succeed later, once the other row has changed. After an ambiguous attempt it is final only when no earlier attempt can still be in flight (7.4). Reaches the client as `INVALID_ARGUMENT` |
| `*pgconn.PgError`, `57014` (cancelled, including when triggered by statement_timeout) | Definite failure: **not executed** | The statement has been rolled back |
| `*pgconn.PgError`, `55P03` (lock_timeout) | Definite failure: **not executed** | The statement has been rolled back |
| `*pgconn.PgError`, `40001` / `40P01` | Definite failure: **not executed** | The transaction has been rolled back and can be retried |
| `*pgconn.PgError`, `23502` (NOT NULL), `23514` (CHECK), `22xxx` | Definite failure: **rejected by PG based on this row's own data** | NOT NULL or CHECK violation; type or length error. A statement with the same parameters always gets the same result |
| Other `*pgconn.PgError` | Definite failure: **not executed** | The server explicitly returned an error. Conservatively treated as "not executed": once an ambiguous attempt has occurred, retries continue |
| ctx timeout or cancellation, connection interrupted, `io.EOF`, other network errors | **Ambiguous outcome** | The statement may have executed, or may execute later |
| Network error during COMMIT | **Ambiguous outcome** | The transaction may have been committed |
| Errors we are not sure about | **Ambiguous outcome** | All writes are conditional writes or idempotent, so retrying is always safe |

**Use server-side timeouts to turn most "ambiguous" outcomes into "definite failures".** A Go-side ctx timeout is the most common source of ambiguous outcomes. If the PG server times out first, the client receives an explicit error (`57014` or `55P03`), and the statement has definitely been rolled back:

```sql
SET LOCAL statement_timeout = '2s';  -- must be less than the Go-side ctx timeout (e.g. 3s)
SET LOCAL lock_timeout = '1s';
```

This can be set with `SET LOCAL` at the start of each transaction, or at the role level (`ALTER ROLE <service account> SET statement_timeout = '2s'`). The role level is the most reliable and also works through PgBouncer (see 8.5). In addition:

- Set `client_connection_check_interval` (only available in PG 14 and later; the minimum version this service requires happens to be 14), for example to `1s` at the role level: after the client disconnects, the server detects it sooner and aborts the running query, further reducing late execution.
- These measures can only **reduce** ambiguous outcomes, **not eliminate** them: for example, the COMMIT succeeded but the response was lost in the network. So gen/ver and "retry until determined" must still be kept.
- When the ctx is cancelled, pgx interrupts the wait on that connection (depending on configuration, it may also send a cancel request to the server), and the connection is then closed. The statement on the server may have finished, or may still be running. So a Go-side ctx timeout is always treated as an "ambiguous outcome".

### 8.5 Other effects of PostgreSQL on the design

1. **MVCC write amplification**: every UPDATE in PG writes a new version of the whole row, and the old version becomes a dead tuple awaiting reclamation by VACUUM. With a high update rate, this causes table bloat and autovacuum pressure. Countermeasures:
   - **Write coalescing is the primary tool**: DB write volume is roughly "number of dirty rows x number of flushes", independent of the number of updates. A larger `FlushInterval` means less write amplification, but also a larger window of loss on a crash; this is a trade-off.
   - **Make UPDATEs HOT** (heap-only tuple, no index update needed): do not index any updated column (including `ver`); set the table's `fillfactor` to 70-90 to leave room within pages. The HOT update ratio can be observed in `pg_stat_user_tables.n_tup_hot_upd`.
   - Lower the autovacuum trigger threshold for this table specifically (e.g. `ALTER TABLE items SET (autovacuum_vacuum_scale_factor = 0.01)`), so it runs more often with less work each time.
2. **RowsAffected semantics**: PG returns the number of rows matched and updated, so using `CommandTag.RowsAffected()` to tell 0 rows from 1 row is accurate.
3. **Transaction isolation level**: the default READ COMMITTED is sufficient. All writes are single-row conditional writes by primary key; correctness is guaranteed by gen/ver and does not depend on the isolation level.
4. **Connection pool and PgBouncer**: this service maintains its own connection pool with `pgxpool`, and it is single-instance. **v1 connects directly to PG in docker, not through PgBouncer (confirmed).** The following points are kept for reference: if in the future PgBouncer (transaction mode) must be used, the design does not need to change, but these points need attention:
   - **Prepared statements**: pgx uses named prepared statements by default, which fail in transaction mode. Fix: upgrade PgBouncer to 1.21 or later and enable `max_prepared_statements`; or have pgx use `QueryExecModeExec` / `QueryExecModeCacheDescribe` instead of named prepared statements.
   - **Session-level settings are unreliable**: in transaction mode, different transactions on the same client connection may land on different server connections, so neither `SET statement_timeout` nor pgxpool's `AfterConnect` hook is guaranteed to take effect. Set timeouts at the role level instead, or with `SET LOCAL` in each transaction.
   - A ctx timeout causes the connection to be closed and recreated. When the DB slows down, connection re-creation becomes frequent, so the pool needs headroom.
5. **gen generation**: generating a non-zero 63-bit random number on the Go side is enough; no PG sequence is needed, saving a round trip.

---

## 9. Linearizability and linearization points

**Definition**: every operation takes effect at **a single instant** somewhere within its "request to response" interval. The order given by these instants must match the result of a single-threaded system executing the operations one by one, and all clients see the same order. Operations that overlap in time may be ordered either way; but if B's request is sent only after A's response has come back, A must be ordered before B.

- The linearization point of a write = the moment its effect **becomes visible to other requests and can no longer be retracted**, and it must be an atomic instant.
- The linearization point of a read = the moment it observes the state.
- "Starting to execute" is not the same as "taking effect". For an update that arrives while a delete is executing, its linearization point is before the delete's linearization point, so the update is naturally ordered before the delete.

| Operation | Linearization point |
|---|---|
| Get (hit) | `snap.Load()` |
| Get (reads an entry that has been removed) | The moment of removal |
| Update | `snap.Store(&nr)` inside `mu` |
| Create | After the DB has definitely succeeded, `snap.Store(row)` inside `mu` |
| Delete | After the DB has definitely succeeded, `snap.Store(absent)` inside `mu` |

All linearization points are atomic operations on the same atomic pointer, so they naturally form a single global order.

---

## 10. Gap list

The gaps "after releasing a lock and before acquiring it again" are where concurrency bugs concentrate the most. Every gap must have a corresponding remedy:

| Gap | What can happen during it | Remedy |
|---|---|---|
| Release shard.mu -> acquire the entry's lock | The entry is evicted | Check dead |
| Waiting in line for op | Someone else has already loaded it | Double-check snap |
| Update releases mu -> load -> start over | Anything can happen | Go through the whole path again from the beginning |
| Flush takes a snapshot -> writes to DB | A new update comes in | Re-enqueue; the next flush must wait for op |
| DB timeout -> next SQL | The old SQL has a late execution | UPDATE/DELETE rely on gen/ver conditional writes, INSERT relies on the dedup table (I14); create/delete retry until the outcome is certain |
| create confirmed successful -> later deleted | A late INSERT, or a client retry carrying the same gen, arrives | The dedup table blocks the re-insertion (I14, 7.4) |
| Attempt A ambiguous -> retry B definite failure | A was actually executed | A "not executed" failure is not treated as the final result; keep retrying (7.4, 8.4) |
| Attempt A ambiguous -> retry B rejected because of another row | The other row changes, then A lands | Terminate A's session and confirm it is gone, then try again; `unsure` stays set (7.4, 8.4) |
| Client attempt ambiguous -> its retry answered "not executed" (queued behind the first attempt, DB down, backpressure) | The first attempt succeeds | The SDK never returns a "not executed" status as the final answer after an ambiguous attempt (7.10) |
| Client attempt ambiguous -> its retry finds another life of the key | The first attempt had succeeded and that life was deleted since | Look the gen up in the dedup table before answering `ALREADY_EXISTS` (7.4) |
| A new instance starts -> the old instance notices | The old instance keeps flushing, creating and deleting | Every write carries the owner epoch; the old instance's writes change nothing and it stops (I16, 7.8) |
| Process crash -> new process loads data | The previous process's SQL has a late execution | At startup, terminate the previous process's sessions, and load only after confirming they have exited (I15, 7.8) |
| tryEvict releases mu -> deletes from map | Update gets a dead e | Update checks dead, waits for op (held by the remover until the removal), then retries |
| DB DELETE executing (holding only op) | An update writes to memory | Step 2 clears dirty inside mu |
| Client sends request -> receives response | Connection drops; the request may already have taken effect | create retries with the same gen; delete with gen retries; update and delete without gen are not retried automatically and return ambiguous (7.10) |

If new code introduces a new gap, it must be added to this table with its remedy spelled out.

---

## 11. Operations

### 11.1 Backpressure

- Dirty rows cannot be evicted, so the lower bound on memory usage is the number of dirty rows. When the DB slows down, dirty rows pile up.
- When `dirtyCount > MaxDirty`, new updates return `ErrBusy` (or wait briefly). A further update to a row that is already dirty does not add to the backlog and can be let through.
- **`MaxDirty` bounds memory, not the loss window.** While the DB cannot be written, a workload that keeps updating the same rows never reaches `MaxDirty`, and every update is still acknowledged: after ten minutes of DB outage at 3000 updates/s, 1.8 million acknowledged updates exist only in memory, and a crash or a restart loses all of them. So there is a second limit, on age: when the oldest dirty row has been dirty for longer than `MaxDirtyAge`, **every** update returns `ErrBusy`, including updates to rows that are already dirty, until the flusher has caught up. Gets are not affected; create and delete depend on the DB anyway.
  - The age is `now - dirtySince` of the oldest dirty row, which the flusher already tracks for the metric of 11.2. It is refreshed once per flush round, so the limit is approximate: the real bound on what a crash can lose is about `MaxDirtyAge + FlushInterval`.
  - **Quarantined poison rows (7.9) are excluded** from this age. They stay dirty until a human acts, and they have their own alert; one poison row must not stop all updates.
  - The choice is between two failure modes during a DB outage: with the limit, updates fail after `MaxDirtyAge`; without it (`MaxDirtyAge = 0`), the service looks healthy while the amount of data a crash would lose keeps growing. Section 1 allows only "a small number of the most recent updates", so the limit is on by default.

### 11.2 Metrics

- Total number of entries, number of tombstones, number of negative-cache entries
- `dirtyCount`, **age of the oldest dirty row**: this is the maximum window of data that can be lost in a crash; an alert must be configured on it, well below `MaxDirtyAge`. Count of updates rejected by `MaxDirtyAge`
- Flush latency, batch size, failure count, `flush_noop_rows`
- Owner epoch: this instance's epoch; count of `ErrNotOwner` and of stops caused by a foreign epoch or a changed timeline (each one means a second instance was started or the DB was failed over under a running service: alert)
- Count of per-operation session fences (`FenceSessions`, 7.4) and of fences that could not be confirmed
- Load error count, op wait time
- Count of create/delete with ambiguous outcome, count of `ErrOutcomeUnknown`
- Eviction count, hit rate
- Poison row count `poisoned_rows` (7.9)
- Dedup table: row count, cleanup lag; count of `ErrGenClaimed` (number of duplicate INSERTs blocked); count of `ErrInternal` (should always be 0; a non-zero value must alert)
- Number of old sessions terminated at startup, time spent waiting for them to exit
- Protocol layer: connection count, in-flight requests per connection, latency percentiles per operation, count per Status, protocol error count (malformed frames, oversized frames)

In production, a background sampling audit can be run: randomly pick some clean rows, compare them against the DB, and alert on any inconsistency.

### 11.3 Single-instance deployment and multiple instances

**v1: single instance (confirmed)**

The whole service is deployed as a single instance, and the deployment process runs only one at any time. Without protection, two instances running at the same time would each flush independently and overwrite each other, and the damage would be silent: a flush that affects 0 rows is not an error (7.6), so an update acknowledged by one instance and overwritten by the other is lost without any crash (13.2 hole 8). The usual ways to get there are ordinary: a rolling deployment that starts the new instance before stopping the old one, or an old process still flushing during its graceful shutdown while the new one starts.

**Protection (v1): the owner epoch (I16, 7.8).** The instance that started last owns the DB; every write of an older instance changes nothing, and the older instance stops as soon as it notices. A second instance therefore costs what a crash of the first one costs: its unflushed updates. What the epoch does not do is route clients: for at most `EpochCheckInterval`, clients still connected to the old instance get answers from its memory. The deployment must still stop the old instance before the new one accepts connections (or switch clients over atomically) if linearizability across the switch matters.

**Later: multiple instances**

- Since memory is authoritative, it must be guaranteed that only one instance writes a given key: route by key (consistent hashing), plus a lease.
- During failover, the old instance may still be flushing. The owner epoch of v1 (one epoch for the whole service) becomes one epoch per key range, granted with the lease; the condition in the write statements stays the same.
- The new owner loads data from the DB. The window lost in a crash is the part the original owner had not yet flushed.

### 11.4 Configuration

| Config | Meaning | Reference value |
|---|---|---|
| `Tables` | List of tables to manage; IdleTTL, NegativeIdleTTL, MaxEntries, etc. can override the global defaults per table | - |
| `GenColumn` / `VerColumn` | Column names of gen and ver | `row_gen` / `row_ver` |
| `NumShards` | Number of shards per table, a power of 2 | 256 |
| `FlushInterval` | Flush interval; determines the maximum data lost in a crash, and also PG's write amplification | 200ms (configurable; tune after load testing) |
| `FlushBatchMax` | Maximum rows written per batch | 500 (configurable) |
| `IdleTTL` | How long a row must be idle before it is cleared from memory | TBD |
| `NegativeIdleTTL` | Idle TTL for tombstones and negative-cache entries | Shorter than IdleTTL, e.g. 1-5 minutes |
| `EvictInterval` | Interval between idle sampling runs | 1s |
| `MaxSweepRounds` | Maximum consecutive rounds per sampling run | 16 |
| `MaxEntries` | Upper limit (high-water mark) on the number of entries per table, determined by the memory budget | TBD |
| `MaxDirty` | Upper limit on dirty rows; beyond it, backpressure applies | TBD |
| `MaxDirtyAge` | Upper limit on the age of the oldest dirty row; beyond it, every update returns `ErrBusy` (11.1). Bounds what a crash can lose while the DB cannot be written. 0 disables it | 30s |
| `AllowCrossRowConstraints` | Per table: accept a table that has unique indexes other than the primary key, foreign keys, exclusion constraints or triggers (5.3) | false |
| `EpochTable` | Table name of the owner-epoch table (5.1) | `row_service_epoch` |
| `EpochCheckInterval` | Interval of the periodic owner-epoch and timeline check (7.8); bounds how long a superseded instance keeps answering | 1s |
| `OpFenceTimeout` | Maximum time one `FenceSessions` call waits for the sessions to exit before it reports "not confirmed" and the loop backs off (7.4) | 10s |
| `LoadErrTTL` | After a load failure, how long the same error is returned directly | 100ms |
| `DBTimeout` | Timeout for DB calls on the Go side | 3s |
| `StatementTimeout` / `LockTimeout` | PG server-side timeouts; must be less than `DBTimeout`; recommended to set at the role level | 2s / 1s |
| `QueryExecMode` | pgx execution mode; v1 connects directly to PG and uses the default (when going through PgBouncer, see 8.5) | Default |
| `SchemaCheckInterval` | Interval for the background comparison of the system catalog with the loaded schema | 1min |
| `ListenAddr` | TCP listen address | `:7070` |
| `MaxFrameBytes` | Maximum bytes per frame | 4MB |
| `MaxInflightPerConn` | Upper limit on concurrent in-flight requests per connection | 1024 |
| `MaxConns` | Maximum number of connections | 1000 |
| `DefaultRequestTimeout` | Timeout when a request carries no `timeout_ms`; should be greater than `DBTimeout`, so that most create/delete get a certain outcome before the timeout | 5s |
| `IdleConnTimeout` | How long a connection may be idle before it is closed | 5min |
| `GenLogTable` | Table name of the dedup table (5.1) | `row_service_gen_log` |
| `GenLogRetention` | Retention period of dedup records; must be much larger than how late a statement can be and the client's retry time | 24h |
| `GenLogCleanInterval` | Interval for cleaning up the dedup table | 10min |
| `StartupFenceTimeout` | Maximum time to wait at startup for the previous process's sessions to exit; beyond it, refuse to start (7.8) | 30s |

---

## 12. Test plan and milestones

### 12.1 Testing techniques (from least to most effort)

1. **`go test -race`**: always on for all tests. It only finds data races, not logic errors.
2. **Fault-injecting fake DB (`fakedb`)**:
   - Semantics match real SQL: conditional UPDATE/DELETE, primary key unique constraint
   - Can inject: random delays, random definite failures, connection errors (ambiguous outcome), failure before or after execution
   - **Can simulate "timed out, but executed later"**: immediately returns a ctx timeout to the caller, but the statement takes effect in the DB at some random later moment. This is the most important item; a real DB can hardly reproduce this reliably
   - Records all statements that actually took effect, in order, for after-the-fact checks
   - Simulates the dedup table (unique constraint), and "session terminated": after termination, statements in that session that have not yet executed never execute (7.8)
   - Distinguishes three kinds of definite failure: "not executed", "rejected based on this row's data" and "rejected because of another row" (8.4); the last one is controlled by a switch the test flips between attempts, and `FenceSessions` is simulated per session
   - Simulates the owner-epoch row: a second Store on the same fakedb takes the epoch over, after which every write of the first Store changes nothing
   - Can inject "definite failure" (e.g. simulating a constraint violation), to test poison row quarantine (7.9)
3. **High-contention stress test**: with very few keys (e.g. 4) and many goroutines (e.g. 64), randomly perform create/delete/get/update. Afterwards, shut down gracefully and flush everything, then compare memory with the DB: every row matches exactly (including gen, ver), and absent if and only if it does not exist in the DB.
4. **Porcupine linearizability checking** (`github.com/anishathalye/porcupine`):
   - Record each operation's request time, response time and return value; check partitioned by key
   - Sequential model: each key's state is `{exists, gen, cols}`; Update modifies columns by mask; INCR is computed in the model from the current value, and the new value returned in the response is checked; Update/Delete with gen return NotFound when gen does not match
   - Operations with unknown outcome (`ErrOutcomeUnknown`): set their return time to the end of the history; the model accepts both "happened" and "did not happen" for such operations
   - `ErrBusy` is treated as "definitely did not happen"
5. **Crash recovery test**: during a stress test, discard the whole Store and create a new Store with the same fakedb. Check: confirmed creates still exist (unless later confirmed deleted); data of confirmed deletes does not reappear (unless later re-created); only recent updates may be lost. **Second-instance variant**: do not discard the old Store; keep driving it with requests after the new one has started. Check the same properties against the new Store, and that the old Store stops within `EpochCheckInterval` without changing the DB.
6. **Failpoint tests**: plant test hooks at the key gaps (nil in production) to force specific interleavings:
   - After DB DELETE succeeds and before `Store(absent)`, insert an Update
   - After the flush takes its snapshot and before it writes to DB, insert an Update and a Delete
   - After tryEvict sets dead and before it deletes from map, insert an Update
   - Flush times out and executes late later; in between, insert a delete plus create
   - After idle sampling picks candidates and before tryEvict, insert a Get/Update (verifying the eligible double-check and the dead protocol)
7. **`testing/synctest`** (Go 1.25+): use virtual time to test time-related logic such as flush interval, timeouts, IdleTTL, NegativeIdleTTL, LoadErrTTL (the `nowSec` ticker also uses virtual time under synctest).
8. **Protocol layer tests**: fuzz testing of the frame decoder (`go test -fuzz`); randomly drop connections in end-to-end tests and check that the SDK behaves as specified in 7.10 (create never creates duplicates; update and delete without gen are not retried automatically).
9. **TLA+ model (M0, completed before implementation)**: write the core protocol (per-key state, flush, a DB that can execute late) as a small model and use TLC to exhaustively enumerate all interleavings. Already done; see 12.3.

### 12.2 Milestones

| Milestone | Content | Acceptance criteria |
|---|---|---|
| M0 | TLA+ model: first model the current design and reproduce the holes of 13.2; then add the fixes and verify | TLC exhausts the agreed scale and all invariants and properties hold; the fixes are written back into this document after the user confirms them |
| M1 | Project skeleton (Go 1.25), DB interface, schema abstraction (schema constructed directly in fakedb), primary key encoding, fakedb (no fault injection yet), shard/entry, Get + load | `-race` unit tests pass; primary key encoding covers integer, string, UUID, composite primary keys |
| M2 | Update (SET and INCR) + single-key flush + flush queue | Stress test passes; afterwards memory and DB match; N goroutines each increment the same column K times, the final value is exactly N×K and matches the DB; on increment overflow or an old value of NULL, the whole Change does not take effect |
| M3 | Create/Delete (including runSync, create loads first, dedup table, retry until certain on ambiguous outcome, distinguishing the three kinds of definite failure, the per-operation session fence, the dedup lookup when a retry finds another life, gen/ver conditional writes); optional gen for Update/Delete; fakedb gains fault injection | Stress test with fault injection passes; failpoint tests for the three counterexamples of section 8.2 and holes 1, 2, 4, 5, 6 of 13.2 pass (constructed step by step from the TLA+ counterexamples; expected to fail before the fix and pass after it) |
| M4 | Porcupine linearizability tests | No violations over long random runs |
| M5 | Eviction: idle TTL sampling cleanup, capacity-limit eviction, dead protocol | Failpoint tests plus stress test pass; in the stress test, IdleTTL is set very short so that eviction interleaves heavily with reads and writes |
| M6 | Batch flush, poison row quarantine (7.9), backpressure by count and by age (11.1), owner epoch, startup session cleanup and graceful shutdown (7.8), dedup table cleanup, metrics, crash recovery test | Crash recovery test passes, including "statements still in flight at crash time" (failpoint test for hole 3 of 13.2) and the second-instance variant (hole 8); when definite failures are injected, poison rows are quarantined and the other rows in the same batch are written normally; with the fake DB failing every flush, updates return `ErrBusy` once the oldest dirty row is older than `MaxDirtyAge`, and a poison row alone does not trigger it |
| M7 | PostgreSQL adaptation (pgx, pgxpool, server-side timeouts), loading schema from the system catalog (including the refusal rules of 5.3), type mapping and validation, `IsAmbiguous` / `NotExecuted` / `IsCrossRow` classification, dedup table and owner-epoch SQL (8.3), startup session cleanup and `FenceSessions` with `pg_terminate_backend` (7.4, 7.8), timeline check, schema drift detection | Integration tests against real PG (PG 14 or later in docker, started with testcontainers-go or docker compose) pass; check the ratio of HOT updates; integration tests verify: duplicate INSERTs are blocked by the dedup table; after an old session is terminated its statements no longer execute; a write carrying an old epoch affects 0 rows; a table with a unique index, a foreign key or a trigger is refused without the option; an INSERT rejected by a unique index is classified as `IsCrossRow` and leaves no dedup record |
| M8 | External protocol: .proto, TCP server (framing, multiplexing, backpressure, Status mapping), Go SDK (Describe cache, create/delete retry, timeout propagation) | End-to-end tests pass (SDK -> TCP -> Store -> PG); frame decoder fuzz tests pass; SDK behavior under random connection drops conforms to 7.10, including: a retry answered `DEADLINE_EXCEEDED`, `UNAVAILABLE` or `BUSY` after an ambiguous attempt never surfaces as that status (hole 7); load-test Update throughput and latency |

### 12.3 TLA+ model (M0)

**Files** (all under `tla/`):

| File | Content |
|---|---|
| `RowService.tla` | The model itself; comments indicate which section of this document each part corresponds to |
| `*.cfg` | The individual check scenarios (scale, which faults are enabled, whether the fixes are included) |
| `run.sh` | Run: `./run.sh <scenario name>`; requires Java 11+ and `tla2tools.jar` (TLC 2.19, by default placed in `~/.local/share/tla/`) |
| `live.cfg` | Liveness check under weak fairness (`FairSpec`) |
| `suite.py` | The acceptance suites: `python3 suite.py quick|regress|holes|ext|mutants|witnesses|sim|liveness|all` (planted bugs must be caught, reachability witnesses must be violated). **`quick` is what to run after changing the model: about 13 minutes instead of hours** (see "Checking a change quickly" below). Holes 5-8 and the `ext` environments are generated by it; they are not `.cfg` files |
| `trace.py` | Compresses a TLC counterexample into "what each step did and which variables changed": `./run.sh hole1 > out/hole1.out; python3 trace.py < out/hole1.out` |

**Modeling scope**: one key; allocation and eviction of the entry in the shard map (dead protocol); the entry's snap / dirty / queued / dead / op; Get, Update, Create, Delete, plus runSync continuing in the background after the client gives up; the flusher; a DB that can execute late (statements may still execute after a timeout, responses may be lost, statements may be definitely rejected); the SDK retrying create with the same gen, also while its first attempt is still running; a request timing out while queued for op; a cross-row constraint that starts or stops rejecting writes of the key; process crash and restart; a superseded instance that keeps sending statements. The critical sections of `mu`, `shard.mu` and `qmu` are each modeled as one atomic step, and every gap of section 10 is a separate step.

**Properties checked**: the full list, with the reasons, the environment model, the mutation catalogue and the reachability witnesses, is in [correctness-properties.md](correctness-properties.md). In short: an abstract sequential state is maintained, and each operation modifies it at its linearization point from section 9.

| Invariant | Meaning |
|---|---|
| `NoResurrection` | A deleted life never reappears in the DB or in memory |
| `MemMatchesAbs` | Memory matches the abstract state (all linearization points line up) |
| `NoViolation` | The value read by Get appeared within its request interval (read linearizability); the state loaded from the DB after a crash is explainable |
| `QuiescentConsistent` | When the entry is clean, nobody holds op, and no statement is in flight, the DB and memory match exactly (I4) |
| `DbMatchesAbsWhenIdle` | When the row is not in memory and everything has stopped, the DB equals the abstract state |
| `CreateDurable` | A confirmed create never disappears |
| `OneWriterPerKey` | For a given key, at most one write SQL is awaiting its result at any moment (I5) |
| `DirtyImpliesQueued`, `QueuedIsTracked` | Dirty data is always in the flush pipeline and is never forgotten |
| `LiveEntryInMap` | An entry that is not dead is always in the map (I1) |
| `CleanIsDurable` | When the entry is clean and nobody holds op, the DB equals memory, even with late statements in flight (I4) |
| `DbNotAhead` | The DB ver is never greater than the memory ver for the life memory holds |
| `DbValsBelongToGen` | Every column value in the DB belongs to the life that owns the row |
| `NoDirtyEviction` | An entry being evicted holds no dirty data (I10) |
| `FencedRestart` | Once up, no statement that a previous process sent before the fence is in flight (I15) |
| `CreateFailureIsFinal` | A create whose call ended in "failed" is never inserted, before or after the answer |
| `DbVerMonotonic` (action property) | Within one life the DB ver never decreases, and values never change without a larger ver |
| `EventuallyResponds`, `BackgroundSettles`, `EventuallyFlushed`, `NoLockLeak` (liveness, `FairSpec`) | Every request is answered; every background create/delete settles; dirty data is eventually flushed; op is never held forever |

TLC also checks for deadlock: apart from "all operations are done", every state must still be able to make progress.

**Results**:

| Scenario | Content | Result |
|---|---|---|
| `hole1` | The current design; INSERT may execute late | Violates `NoResurrection`; the counterexample matches hole 1 of 13.2 |
| `hole2` | The current design; client gives up waiting, SDK retries create | Violates `NoResurrection`; the counterexample matches hole 2 |
| `hole3` | The current design; a flush UPDATE is still in flight at crash time | Violates `DbNotAhead` the moment the old UPDATE lands (before the properties of correctness-properties.md were added: `QuiescentConsistent`); the counterexample matches hole 3 |
| `hole3_insert` | The current design; an INSERT is still in flight at crash time | Violates `QuiescentConsistent` (INSERT version of hole 3) |
| `hole4` | The current design; statement responses can be lost, and statements can be definitely rejected | Violates `DbMatchesAbsWhenIdle`; a new hole found by TLC (hole 4 of 13.2) |
| `hole1-4_fixed` | Same as above, with the corresponding fixes added | All pass |
| `base` | Hole 4 fixed, preconditions of holes 1-3 removed; 2 clients, 3 operations each, at most 2 creates, 2 updates, 3 entries (reallocated after eviction), one each of: ambiguous statement, rejected statement, client giving up waiting | **Pass**: 275 million distinct states, longest interleaving 71 steps, 19 minutes |
| `fixed` | Same scale as `base`, all faults enabled (late INSERT execution, SDK retrying create, 1 crash), plus all fixes | **Pass**: 1.67 billion distinct states, longest interleaving 74 steps, 2 hours (re-run with every property of `correctness-properties.md`: same 1,667,861,866 states, no error) |
| `base_2col` | Same design and faults as `base`, table has two columns, each update changes one of them: checks that only dirty columns are written and that the mask is merged back on failure. Scale: 2 clients, 3 operations each, 1 create, 3 updates, 2 entries | **Pass**: 39.66 million distinct states, longest interleaving 62 steps, 3 minutes |

| `base_big` | Random simulation (not exhaustive): same design as `base`, scale enlarged to 3 clients, two columns, 4 operations each, 2 of each kind of fault; randomly generated executions, each up to 300 steps | **No violation found**: 25 minutes, about 1.6 billion states checked |
| `fixed_big` | Random simulation: same as `fixed` (all faults + fixes), scale same as `base_big`, 2 crashes | **No violation found**: 25 minutes, about 1.5 billion states checked (re-run with the new properties: about 1.38 billion, no violation) |
| `live` | Liveness under weak fairness: all faults + fixes, 2 clients, 2 operations each, 1 crash | **Pass**: 9.63 million distinct states, all four liveness properties, 27 minutes |
| mutants | 15 planted bugs, one at a time, in the `fixed` environment (correctness-properties.md 7) | **All 15 caught** |
| witnesses | 17 "this situation never happens" claims in the `fixed` environment (correctness-properties.md 8) | **All 17 violated**: every fault scenario, including each hole's precondition, is reachable |
| liveness mutants | 2 planted bugs checked by liveness alone | **Both caught** |

Results after holes 5-8 were added to the model (2026-11-13; all from one `python3 suite.py quick`, 92 runs, 13 minutes):

| Scenario | Content | Result |
|---|---|---|
| `hole5` | A create or delete: an ambiguous attempt, a retry rejected because of another row, then that row changes | Violates `QuiescentConsistent`; 13 states; matches hole 5 of 13.2 |
| `hole6` | A create that succeeded after its client gave up; the row is deleted and created again; the SDK retries | Violates `CreateFailureIsFinal`; 27 states; matches hole 6 |
| `hole7` | An SDK retry that times out while queued behind its own first attempt | Violates `CreateFailureIsFinal`; 11 states; matches hole 7 |
| `hole8` | A superseded instance sends a statement after the new instance is up | Violates `NoViolation` (a state after restart that no operation of the new instance explains); 11 states; hole 8 |
| `hole5-8_fixed` | Same as above, with the corresponding fix on | All pass |
| `ext_crossrow`, `ext_sdk`, `ext_epoch` | Every earlier fault and every fix at the scale of `live`, plus the fault of hole 5 / holes 6-7 / hole 8 | **Pass**: 19.7 / 6.4 / 23.1 million distinct states |
| `ext_all` | The same with the faults of holes 5-8 all enabled | **Pass**: 118 million distinct states, longest interleaving 54 steps, 12 minutes |
| fix switches | Each fix of holes 5-8 switched off in its `ext` environment | **All 4 caught** |
| witnesses | 9 situations of holes 5-8 in the `ext` environments, in addition to the 17 above | **All 26 violated** |
| mutants | The 15 planted bugs, again | **All 15 caught** |
| `regress` | The model before holes 5-8 against the changed model with their switches off, in 12 scenarios | **Same in all 12**: same violation in the 5 that fail, same state counts in the 7 that pass (`base_2col`: 39,658,214 distinct states in both) |
| `sim` | Random simulation of `fixed_big`, now with the faults of holes 5-8, for 2 minutes | **No violation**: about 39 million states checked |
| `live` | Liveness, with the fixes of holes 5-8 on (their faults off) | **Pass**: 9.94 million distinct states, all four liveness properties, 34 minutes; both liveness mutants caught again |
| `fixed` | Full scale, with the fixes of holes 5-8 on | **Not re-run** (about 2 hours). With those switches off it is, by `regress`, the model of the 1.67-billion-state run above |

**Checking a change quickly.** The two-hour `fixed` run is not needed after every change to the model. The method used for holes 5-8, described in full in [correctness-properties.md](correctness-properties.md) 9.1:

1. **Every new fault and every new fix gets a switch**, and new variables stay out of the old variable groups. With the switches off the model must be the previous model.
2. **`regress` checks exactly that**: it runs the previous model (from git) and the changed one on the same scenarios and compares state counts. Equal counts mean the earlier long runs still describe the changed model with its switches off, so they need not be repeated.
3. **The new behavior is checked where it is cheap**: each hole in its minimal scenario (seconds), each new fault on top of all old ones at a small scale (`ext`, minutes, exhaustive), everything together at a large scale by random simulation (`sim`, time-boxed).
4. **Small is enough to catch a wrong fix**: all eight holes have counterexamples of at most 27 states, each found within 2 seconds. The first fix written for hole 5 was wrong, and `ext_crossrow` showed it in 2 seconds.
5. The full-scale exhaustive run stays as the last check before a release.

**Conclusion**: apart from the eight holes of 13.2, within the above scales no other interleaving that violates an invariant or deadlocks was found. The fixes have been written into the main body of the design:

| Switch in the model | In the design |
|---|---|
| `FixDedup` | The dedup table and "create loads first" (7.4, 8.3) |
| `FixFence` | The startup cleanup (7.8) |
| `FixUnsureRetry` | "Not executed" is not final after an ambiguous attempt (7.4, 7.5, 8.4) |
| `FixCrossRow` | The session fence before trusting a rejection caused by another row; `unsure` and `inflight` kept apart (7.4, 7.5, 8.3, 8.4) |
| `FixRetryAnswer` | The dedup lookup when a retry finds another life (7.4) |
| `FixSdkRetry` | The SDK rule after an ambiguous attempt (7.10) |
| `FixEpoch` | The owner epoch (I16; 5.1, 7.8, 8.3) |

When changing the design in the future, first change it in the model and run `python3 suite.py quick`, then change the code.

**What is not covered** (handled during implementation by the other techniques of section 12.1):

- This is bounded exhaustive checking: all interleavings within the above scales have been checked, but it is not a proof for arbitrary scale.
- Only one key is modeled. Different keys share only the shard's map and queue; those two locks are leaf locks and do not affect the correctness of a single key.
- Increment is not modeled separately: it is done inside the same `mu` critical section as assignment, and the concurrency structure is exactly the same.
- Not modeled: load failures and error caching, backpressure by count and by age (11.1), the poison row quarantine, interactions between multiple rows in one batch, the external protocol (7.10), schema.
- Not modeled for holes 5-8: what a superseded instance answers to its own clients before it notices; a session fence that fails or cannot be confirmed; SDK retry of delete (the rule of 7.10 is the same as for create, but no property checks delete's answer); a DB that loses acknowledged writes; values PG normalizes (13.3).
- The faults of holes 5-8 are checked exhaustively only at the scale of `live` (`ext_*`), and at a larger scale only by random simulation.
- Liveness is checked only at the smaller scale of `live.cfg`, without the faults of holes 5-8, and under the assumption that every fault is finite (correctness-properties.md A7).

---

## 13. Confirmed items and open questions

### 13.1 Confirmed

Confirmed from the outset:

- Memory cannot hold the full data set; only the working set is kept, cleared by idle TTL (see 7.7). IdleTTL and the memory budget are configuration items.
- The database is PostgreSQL (see 8.3-8.5).
- The table structure is dynamic and read from the system catalog at runtime; tables must have the two columns gen and ver (see 5.3).
- Primary keys are not restricted to integers: internally they are uniformly encoded as `Key`, supporting integer, string, UUID and composite primary keys (see 5.3).

Answers to the first round of questions (2026-11-11):

| # | Question | Conclusion | Related sections |
|---|---|---|---|
| 1 | Is the type list in 5.3 sufficient? | Yes. The business side has no complex types such as arrays, enums or composite types | 5.3, 8.3 |
| 2 | Do DB-generated primary keys need to be supported? | No. The caller provides the primary key on create | 5.3, 15 |
| 3 | Can the business side accept the conventions in 5.3 and 7.9? | Yes: only this service writes these tables; no triggers that modify rows; columns updated through this service should avoid CHECK constraints and foreign keys where possible; table structure changes require restarting the service | 2, 5.3, 7.9 |
| 4 | External API protocol? Do responses include gen/ver? | Custom TCP protocol, message bodies in protobuf. Responses include gen and ver (ver is only for observation) | 7.10 |
| 5 | What flush interval and batch size? | Configurable. Default 200ms, 500 rows per batch; tune after load testing | 11.4 |
| 6 | PG version? Will it go through PgBouncer? | PG 14 or later installed in docker; direct connection, not through PgBouncer | 2, 8.4, 8.5, 12.2 |
| 7 | Single instance or multiple instances? | Single instance for now. Protection against accidentally starting a second instance is not implemented for now | 2, 11.3 |
| 8 | Does Update need to carry gen? | The user was unsure, worried it would be unfriendly to the business side. The recommended option was adopted: **optional, not checked by default**. Business clients that do not use it are completely unaware of it; the gen used by create is generated automatically by the Go SDK, so business clients never touch it either | 7.3, 7.10 |
| 9 | Does Update have read-modify-write semantics such as increment? | Yes. Update supports SET and INCR | 7.3, 7.10 |
| 10 | Must an update that arrives while a delete is in progress fail? | No; it is allowed to succeed | 7.3, 15 |
| 11 | Which Go version? | Go 1.25 (installed: 1.25.14) | 1, 12.1 |

Answers to the second round of questions (2026-11-11):

| # | Question | Conclusion | Related sections |
|---|---|---|---|
| 12 | What language are the business side's clients written in? | Go. v1 provides only a Go SDK | 7.10 |
| 13 | What kind of data is increment used on? | Only on less important data; small losses are acceptable; "exactly once" is not needed | 7.3, 7.10 |
| 14 | Do numeric columns need increment? | No. Increment is supported only on integer and floating-point columns | 7.3 |
| 15 | Do TCP connections need TLS or authentication? | No. No encryption, no authentication | 7.10 |
| 16 | For delete without gen, should the SDK handle retries for the business side? | No. Which life to delete is business logic; the SDK only automatically retries idempotent requests | 7.10 |
| 17 | Should TLA+ be written before implementation? | Yes. As M0, completed before implementation | 12 |
| 18 | Are the fixes for the four holes of 13.2 adopted? | Adopted (confirmed after TLA+ verification passed): dedup table + create loads first, terminating old sessions at startup, distinguishing the two kinds of definite failure | 5.1, 7.4, 7.5, 7.8, 8.3, 8.4 |

Decisions after the design review (2026-11-13):

| # | Question | Conclusion | Related sections |
|---|---|---|---|
| 19 | The review found five gaps the model did not cover. Which are fixed? | All except value canonicalization. They are recorded as holes 5-8 of 13.2, together with the bound on the loss window (#20) and the PostgreSQL assumptions of section 2 | 2, 5.1, 5.3, 7.4-7.8, 7.10, 8.3, 8.4, 11 |
| 20 | While the DB cannot be written, is the loss window bounded? | Yes: `MaxDirtyAge`. The default of 30 s is a proposal that has not been confirmed separately; 0 switches the limit off | 1, 11.1, 11.4 |
| 21 | Values that PG stores differently from what the client sent (float4, numeric(p,s), char(n), timestamp(p), jsonb, NUL bytes) | Not fixed for now. Recorded in 13.3 | 13.3 |
| 22 | Is the single-instance assumption still left to deployment alone? | No. This replaces the second half of #7: the owner epoch is implemented in v1 | 2, 7.8, 11.3 |

### 13.2 Design holes found (reproduced with TLA+; fixes confirmed and written into the main body of the design)

Holes 1-4 violate the "intolerable" clauses of section 1. Holes 1-3 were found while re-examining the design; hole 4 was found by TLC while checking the rest. The M0 TLA+ model (`tla/`, see 12.3) reproduced all four against the current design, and none of them appears any more once the fixes are added. Holes 5-8 were found in the design review of 2026-11-13; they sat outside what the model covered at the time, and are described after the first four.

**Hole 1: a late INSERT resurrects a deleted row (within the same process).** I5 says late writes are blocked by gen/ver, but INSERT has no gen/ver condition and relies only on the primary key unique constraint:

```
1. create 42: INSERT A (gen=100) is stuck in the network, Go side times out
2. Retry INSERT B (gen=100) succeeds -> returns "created successfully"
3. delete 42: DELETE ... WHERE gen=100 succeeds -> returns "deleted successfully"
4. A finally reaches PG and executes: the row no longer exists, no conflict -> the gen=100 row is resurrected ✗
```

Memory holds a tombstone, but the DB has the row; after eviction and reload, the deleted data comes back.

**Hole 2: a client retry of create resurrects a deleted row.** The same class of problem as hole 1, except that the duplicate request comes from the client: the create actually succeeded but the response was lost; the SDK retries with the same gen; meanwhile someone else deletes it. When the retry arrives, memory holds a tombstone and the DB has no such row, so it is inserted again. The gen-based idempotency check in 7.4 only works while the row still exists.

**Hole 3: after a crash and restart, a late UPDATE from the old process overwrites the new value.** ver is only monotonically increasing within one process. The ver loaded from the DB after a restart may duplicate a ver that the old process already sent but that has not yet landed:

```
1. Old process: memory ver=6 (score=10), flush UPDATE A (ver=6) is stuck in the network; process crashes
2. New process: loads ver=5 from DB; update score=20 -> memory ver=6
3. A finally executes: DB becomes ver=6, score=10
4. New process flushes UPDATE ... WHERE ver < 6 -> affects 0 rows, treated as "already written before", clears dirty ✗
```

Memory has 20, the DB has 10, and this will never be corrected afterwards (violates I4). The premise in 7.8 that "after a restart, reloading from the DB naturally matches the DB" is that SQL sent by the old process will not land any more, and that does not hold. Late INSERTs and DELETEs from the old process have the same problem.

**Hole 4: "ambiguous" followed by "definite failure" is treated as a definite failure.** `insertUntilSure` returns failure when it hits a definite failure (7.4), but that only shows that **this attempt** was not executed; it does not show that the earlier ambiguous attempt was not executed either:

```
1. create 42: INSERT A executed successfully, but the response was lost -> ambiguous outcome, retry with the same gen
2. Retry INSERT B is definitely rejected by PG (connection failure, statement_timeout, etc.) -> returned as "definite failure", memory unchanged
3. Result: memory believes 42 does not exist, but the DB has the row ✗
```

`deleteUntilSure` has exactly the same problem: the DELETE was actually executed but it is returned as a "definite failure", the row stays in memory, subsequent flushes all affect 0 rows (treated as success), and after eviction the row disappears. Flush does not have this problem, because a failed flush already merges the mask back and rewrites in the next round.

**The fixes** (verified with TLA+, confirmed by the user. Below are the key points; details are in the sections in parentheses):

- Holes 1, 2 (I14; 5.1, 7.4, 8.3): add a dedup table managed by this service that records "which (table, primary key, gen) have been inserted". Inserting the business row and writing the dedup record are done in the same statement, so a given gen can be inserted only once in its lifetime. Deleting a business row does not delete the dedup record; dedup records are cleaned up only by retention period (24 hours by default). create is changed to load first: the INSERT is sent only when memory is certain the row does not exist, guaranteeing the two are written together.
- Hole 3 (I15; 7.8): when the new process starts, it first terminates all sessions the old process left on PG (`pg_terminate_backend`), and only after confirming they have all exited does it load data and serve requests. Once the sessions are gone, SQL sent by the old process can no longer land. The precondition is that the DB account used by this service is not used by any other program (section 2).
- Hole 4 (7.4, 7.5, 8.4): `insertUntilSure` / `deleteUntilSure` remember "whether an ambiguous attempt has occurred". Once one has occurred, the two kinds of definite failure must be distinguished:
  - **This attempt was not executed at all** (connection failure, `pgconn.SafeToRetry`, cancelled by statement_timeout / lock_timeout, serialization conflict): it says nothing about the earlier attempt's outcome; keep retrying with the same parameters until a certain outcome is obtained (insert succeeds, or after a conflict a SELECT sees a clear state).
  - **PG executed it and explicitly rejected it based on the data of this row** (violating NOT NULL or CHECK, type or length errors): the earlier attempt used exactly the same parameters, so it must have been rejected too, and failure can be returned. If the earlier attempt had actually succeeded, this attempt would first conflict on the primary key (or the dedup table) and never reach these checks. (As first written, this kind also included foreign keys. A rejection that depends on other rows does not follow this argument; that is hole 5 below.)

  When no ambiguous attempt has occurred, a definite failure is returned as usual.

**Holes 5-8** (design review of 2026-11-13). None of them was reachable in the model as it stood: the model had no rejection that depends on other rows, never retried while the first attempt was still running, checked no property on what create answers, and assumed a single instance. Each was added to the model as a fault with its own switch, reproduced, and then fixed (12.3).

**Hole 5: a retry rejected because of another row is treated as a definite failure.** The fix for hole 4 says "rejected by the data means the earlier attempt was rejected too". That holds for constraints on the row itself. A unique index, a foreign key, an exclusion constraint or a trigger looks at other rows, and those change:

```
table users(id PRIMARY KEY, email UNIQUE)
1. create 42 (email a@x, gen=100): INSERT A is stuck in the network, Go side times out -> ambiguous
2. Retry INSERT B: row 7 holds a@x -> unique violation; the statement is rolled back, dedup record included
   -> returned as "definite failure", memory keeps "42 does not exist"
3. Row 7 changes its email
4. A finally executes: nothing conflicts any more -> row 42 is inserted ✗
```

The client was told "failed", memory says the row does not exist, and the DB has it. `deleteUntilSure` has the same problem with a child row that references the row being deleted: once the child is gone, the late DELETE lands; the row stays in memory, every later flush affects 0 rows, and its updates are lost. There is also an everyday version that needs no fault: an ordinary "email already taken" was classified as a primary key conflict and reported as `INTERNAL` (a broken invariant).

**Hole 6: a create retry is answered "already exists" although it took effect.**

```
1. Client A: create 42 (gen=100) -> OUTCOME_UNKNOWN; the INSERT then succeeds in the background
2. Client B: delete 42; then create 42 again (gen=200)
3. Client A's SDK retries with gen=100: memory holds gen=200 -> ALREADY_EXISTS, which means "did not take effect"
```

A's create did take effect at step 1. It needs the key to be deleted and created again within the retry window, so it matters only where primary keys are reused.

**Hole 7: the SDK reports "this request was not executed" as the result of the call.**

```
1. create 42 (gen=100), the DB is slow -> OUTCOME_UNKNOWN; the first attempt keeps running in the background and holds op
2. The SDK retries with gen=100; the retry queues for op behind its own first attempt
3. The retry times out in the queue -> DEADLINE_EXCEEDED ("not executed") -> returned to the caller as a failure
4. The first attempt's INSERT succeeds ✗
```

The retry meets the same slow DB that made the first attempt ambiguous, so this is the common path of an ambiguous create, not a corner. The same happens with `UNAVAILABLE` and `BUSY`, and for delete with gen. 7.10 was contradictory here: the Create paragraph said "return `ErrOutcomeUnknown` on expiry", the status table said "`DEADLINE_EXCEEDED`: no effect, returned to the caller".

**Hole 8: a second instance.** v1 left "only one instance runs" to deployment. Nothing detected a violation:

```
DB: row 42 score=10, ver=5. Old instance A and new instance B have both loaded it.
1. B starts and terminates A's sessions (startup fence); A's pool reconnects
2. A client of B adds 1: B flushes -> DB score=11, ver=6
3. A client still connected to A adds 5: A's memory score=15, ver=6
4. A flushes UPDATE ... WHERE ver < 6 -> 0 rows, which is not an error -> A clears dirty ✗
```

The +5 was acknowledged and is lost without any crash; A answers 15 and B answers 11; A's next update (ver=7) overwrites B's value. The only trace is `flush_noop_rows`, which also grows in normal operation. A rolling deployment that starts the new instance first is enough to get here.

**The fixes for holes 5-8** (verified with TLA+, adopted on 2026-11-13; details in the sections in parentheses):

- Hole 5 (5.3, 7.4, 7.5, 8.3, 8.4):
  - Tables with constraints that depend on other rows are refused when the schema is loaded, unless enabled per table.
  - `23505` is classified by constraint name; "rejected because of another row" is a third kind of definite failure.
  - After an ambiguous attempt, such a rejection is final only when no earlier attempt can still be in flight: `insertUntilSure` / `deleteUntilSure` remember the sessions of their ambiguous attempts, terminate them and confirm they are gone (`FenceSessions`), then try again.
  - `unsure` stays set after the fence. The first version of this fix cleared it, and TLC found the counterexample: A ambiguous, B rejected because of another row, A lands, fence, C not executed, "failed" returned while the row exists.
- Hole 6 (7.4, 8.3): when a create with a gen finds another life in memory, it looks its gen up in the dedup table before answering. Found: success with no row. Not found: `ALREADY_EXISTS`.
- Hole 7 (7.10): one SDK rule. Once any attempt of a Create or Delete call was ambiguous, `DEADLINE_EXCEEDED`, `UNAVAILABLE` and `BUSY` are never the final answer: the SDK retries, and returns `ErrOutcomeUnknown` when the caller's ctx expires.
- Hole 8 (I16; 5.1, 7.6, 7.8, 8.3, 11.3): an owner-epoch row. A starting instance adds 1 to it before its startup fence; every write carries `AND EXISTS (... epoch = mine)`; an instance that sees another epoch stops without flushing. A second instance then costs what a crash of the first one costs.

### 13.3 Known and not fixed

**Values that PostgreSQL stores differently from what the client sent** (decision #21: not fixed for now). Memory keeps the client's value, the DB keeps the value normalized to the column type, so the two differ after a flush, and a Get returns a different value once the entry has been evicted and reloaded, although nobody wrote in between:

| Column type | Written | Get before reload | Get after eviction and reload |
|---|---|---|---|
| `real` (float4) | 0.1 | 0.1 | 0.10000000149011612 |
| `numeric(10,2)` | "1.005" | "1.005" | "1.01" |
| `timestamp(0)` | 12:00:00.6 | 12:00:00.6 | 12:00:01 |
| `char(5)` | "ab" | "ab" | "ab   " |
| `jsonb` | `{"b":1,"a":2}` | as written | `{"a": 2, "b": 1}` |

No data is lost, but I4 ("the DB is consistent with memory") holds only up to this normalization, and a job that compares the service with the DB reports false differences. One case is of a different kind: a `text` value containing a NUL byte or invalid UTF-8 is accepted in memory and rejected by PG, so the acknowledged update becomes a poison row (7.9). The fix belongs in the API layer's validation (7.9 item 1): reject or normalize on input, per type. Until then: check which of these column types the onboarded tables use.

**A superseded instance keeps answering for up to `EpochCheckInterval`** (7.8, 11.3): reads from its memory, and acknowledgements of updates that will be lost. Closing this window needs leases with a clock assumption, which is part of the multi-instance work.

**A DB that goes back in time** (promotion of an asynchronous standby, restore) can lose confirmed creates and deletes. The service detects a changed timeline and stops (7.8), but it cannot bring the lost transactions back; section 2 states the deployment requirement.

---

## 14. Design methodology (lock design principles)

### 14.1 Core idea

**Locks protect invariants; the shape of a lock is determined by the latency requirements of the operations.** Locks are derived from requirements: when the requirements change, the locks must change with them. In the original cache version of the design, one per-key lock was enough, because writes had to wait for the DB anyway; after switching to write-behind, update can no longer wait for IO, so that lock had to be split.

### 14.2 Eight reusable steps

1. List all operations and spell out their latency and consistency requirements.
2. List the invariants, and find a lock that guards each invariant. A lock for which you cannot say what it guards is either redundant or in the wrong place.
3. For each invariant, find the smallest scope of protection (spatial granularity).
4. Look at the slowest operation within each scope (temporal granularity). **A lock with a large scope must be held briefly; a lock that must span IO can only have its scope shrunk to one key.**
5. Split operations with different latency requirements onto different locks.
6. Make the read path lock-free wherever possible: immutable data plus atomic publication.
7. Fix the lock order (see 14.3), and revalidate the state every time after acquiring a lock.
8. For the parts that cross the process boundary (DB, network), do not rely on locks; use version numbers (fencing tokens) instead.

### 14.3 Lock granularity is two-dimensional

|  | Held briefly | Held long (spans IO) |
|---|---|---|
| **Large scope** (many keys) | ✓ shard.mu | ✗ Absolutely forbidden |
| **Small scope** (one key) | ✓ e.mu | ✓ e.op |

### 14.4 How to determine the lock order

1. **Draw the lock dependency graph**: check every code path: "while holding A, does it wait for B?" If so, draw an edge A->B. Only "holding one lock while waiting for another" counts as an edge; acquiring each once in sequence without overlap does not.
2. **If the graph has no cycle, there is no deadlock**. A topological sort of the graph gives the lock order. Proof: if there were a cycle, going once around it the rank would strictly increase all the way and end up back at itself, a contradiction.
3. **If some edge would create a cycle, remove it**:
   - Release the lock first, then acquire the other; the cost is that you must revalidate after acquiring (the dead protocol is essentially "the cost of releasing the lock")
   - Use TryLock: it never waits, so it can never appear in a cycle
   - Restructure so that the path does not need to hold both locks at once
4. **Rules of thumb for choosing edge directions**:
   - **Put leaf locks innermost**: while holding one, acquire no other lock, do not block, do no IO. A leaf lock can be acquired anywhere without causing deadlock (it has no outgoing edges, so it cannot be part of a cycle); its hold time is predictable; it composes with other code.
   - Locks that are held long and need to nest other locks go on the outside.
   - Multiple locks at the same level are locked after sorting by a stable key (e.g. in a transfer, always lock the account with the smaller id first).
   - In a tree structure, lock the parent before the child (provided the child lock is held only briefly).
5. **Lock order is not determined by coarseness of granularity, but by "who needs to hold whom while waiting for whom".** A coarse-grained lock is not necessarily outer. shard.mu has the largest scope, yet it is well suited to be the innermost leaf lock.
6. **A lock's leaf-lock status must be written in comments and actively maintained**. The most common way it gets broken is a "hidden lock": a function called inside the critical section takes a lock internally (e.g. logging).
7. **Go-specific pitfalls**: doing a blocking channel operation while holding a lock (a channel is effectively a lock too); `sync.Mutex` is not reentrant; recursively taking a read lock on an RWMutex (if a writer is queued in between, it deadlocks).
8. **Diagnostic tools**: `-race` cannot find deadlocks. For a partial deadlock, use `/debug/pprof/goroutine?debug=2` to see where every goroutine is stuck; during development, locks can be swapped for `github.com/sasha-s/go-deadlock`, which detects lock order inversions. The Linux kernel's lockdep follows the same idea.
9. **The other school**: do not prevent, but detect and then roll back (this is what databases do, because which rows a transaction will lock is not known in advance). In service code, which locks are needed is known when writing the code, so preventing via ordering is simpler and more reliable.

### 14.5 "Release first, then validate" vs. crabbing

- **Crabbing (hand-over-hand locking)**: lock parent -> lock child -> release parent. The hold times overlap, so the child cannot be removed in between, and no validation is needed. The cost is that the two locks become nested, and contention on the child lock spreads to the parent lock.
- **Release first, then validate**: lock parent -> release parent -> lock child -> check dead. There is a gap in between, remedied by validation afterwards. On the lookup path the two locks are not nested, which is what lets the eviction path safely acquire them in the reverse order.
- In languages without GC, the object may already have been freed during the gap (use-after-free), so there one must either use crabbing or keep the object alive with reference counting (Linux's lockref is exactly "reference count + dead mark"). Go has GC, so "release first, then validate" is safe.

### 14.6 The dead protocol is a general technique

The problem it solves: you find an object through a shared index and then lock the object, and there is a time window between "finding" and "locking". The general pattern is:

```
for {
    obj := lookUpIndex()
    lock(obj)
    if obj is still valid { break }
    unlock(obj)
}
```

The same technique appears in: Java ConcurrentHashMap's `putVal` (after locking the bin head, check that it is still the bin head), Go `sync.Map`'s `expunged` sentinel, Linux's `lockref_mark_dead()` and `I_FREEING`, and the Harris lock-free linked list's "mark first, then unlink".

### 14.7 Separating logical delete from physical delete

First set a mark (tombstone) so that everyone can see "deleted"; then do the actual reclamation later at a safe moment (at eviction, through the dead protocol). This turns the hardest thing to handle in concurrency, a "structural change" (inserting into or deleting from the map), into a simple "value change" (replacing a pointer). Structural changes remain in only two places, getOrCreate and eviction, and both are isolated in one place.

### 14.8 Locks are only effective within a process

All a lock can guarantee is the ordering within its own memory. Once you cross the process boundary, "unknown outcome" appears. Faced with "unknown", there are two paths:

1. Wait until the outcome is certain (KILL the connection, then confirm it has gone away): doable, but slow, cumbersome, and dependent on the specific database.
2. Have the storage side reject stale requests (fencing tokens, i.e. gen/ver, plus the dedup table): no matter how chaotic the network is, the result is correct. This design takes this path on the everyday path.

Both paths have their uses: every everyday read and write takes path 2, which is cheap; a process restart happens only once, so it can afford the cost of path 1, which is why at startup `pg_terminate_backend` is used to terminate the previous process's sessions (7.8). This is because ver is only monotonic within one process, and path 2 cannot block the cross-process case (hole 3 of 13.2).

Path 1 is also the only answer where no token can exist: a statement that another row rejected now and would accept later carries nothing the storage could compare (hole 5). There the sessions of the ambiguous attempts are terminated, per operation and only in that rare case (7.4). And path 2 has a second token besides gen/ver: the owner epoch (I16), which fences a whole instance instead of one row (hole 8).

### 14.9 How to make sure a concurrent program is written correctly

- The role of locks is to **reduce interleavings**: you do not need to consider interleavings between every line of code, only the ordering between critical sections and the gaps after a lock is released.
- Keep the gaps few, and list all of them (section 10).
- Reason with invariants, linearization points and state tables, rather than enumerating all interleavings.
- Use tools to find the cases you did not think of (section 12): TLA+ checks the design (exhaustive, but only for the model), Porcupine checks the implementation (real code, but only covering the interleavings the tests happen to hit). The two complement each other.

---

## 15. Key trade-offs

| Decision | Choice | What was given up / cost | Rationale |
|---|---|---|---|
| Persistence of Update | write-behind (change memory first, flush asynchronously) | Updates within the flush window are lost on a crash | Updates are very frequent and must be fast; this loss is tolerable |
| Persistence of Create/Delete | Synchronous write; return only when the outcome is certain | Higher latency | What is created must not disappear; what is deleted must not resurrect |
| Read path | copy-on-write, immutable snapshot plus atomic publication | Every update allocates memory, adding GC pressure | Reads are completely lock-free and scale linearly even on hot keys. If load testing shows allocation is the bottleneck (writes far outnumber reads, and rows are large), consider copy-on-read (RWMutex with in-place modification, copy on read) |
| Lock splitting | op (spans IO) and mu (leaf lock) separated | One more lock | update never waits for IO |
| Lookup approach | Release first, then validate (dead) | Requires the dead protocol | Keeps contention on hot keys from spreading to the whole shard; shard.mu stays a leaf lock |
| Whether mu is a leaf lock | Yes (eviction releases mu before acquiring shard.mu) | During the few microseconds of eviction, Update may wait for op | The rule simplifies to "only op may nest" |
| Delete approach | Tombstone + deferred physical delete | Tombstones take memory and need TTL cleanup | Those already holding the pointer see it immediately; negative caching; the delete path is simple |
| update arriving while delete is in progress | Allowed to succeed; discarded together after delete succeeds (confirmed) | Clients may find it surprising (update succeeded, then the data is deleted) | Linearizable; no extra state; update does not slow down. The other two options: return a retryable error (one more error to handle), or wait for delete to finish (update slows down) |
| Serialization and gen/ver | Keep both | Two more columns and WHERE conditions | Serialization handles the normal case, gen/ver covers timeouts; a late write causes a permanent error and would violate a hard requirement |
| When create/delete outcome is ambiguous | Hold op and retry until the outcome is certain; after an ambiguous attempt has occurred, keep retrying on "not executed" definite failures too | When the DB is unavailable for a long time, other DB operations on this key are blocked | Confirming via SELECT is unreliable (cannot block late execution); gen plus the dedup table makes INSERT retries idempotent; "this attempt was not executed" does not imply "the earlier attempt was not executed" |
| Duplicate INSERT | Dedup table (a given gen is inserted only once in its lifetime) + create loads first | One more table; each create writes one more row; cleanup by retention period | The primary key unique constraint alone cannot block a late INSERT or client retry that arrives "after the delete"; kept in PG, so it is not lost across a crash and restart |
| Late SQL from the previous process | Terminate its sessions at startup and load only after confirming they have exited | The DB account can only be used by this service; one more step at startup | ver is only monotonic within one process and cannot block across processes; once the sessions are gone, their SQL can no longer execute |
| What flush writes | By default only the dirty columns in mask (pgx.Batch); for very large batches, whole rows can be written (unnest) | Need to maintain a bitmap | Less network transfer and parsing. In PG, UPDATE always rewrites the whole row; reducing disk write amplification relies mainly on coalescing writes |
| Memory mode | Working set: idle TTL cleanup + capacity limit | First access to cold data must query the DB | The data volume does not fit in memory |
| Eviction algorithm | Redis-style random sampling (expiry + approximate LRU) | Expiry cleanup is imprecise and happens a bit late | Each lock hold time is bounded; the hit path writes only one coarse-grained timestamp, with no LRU list to maintain |
| Server-side timeouts | statement_timeout / lock_timeout smaller than the Go-side timeout | Two layers of timeouts must be configured carefully | Turns most "ambiguous outcomes" into "definite failures", reducing retries |
| Primary key type | Uniformly encoded as `Key` (string), supporting integer, string, UUID, composite primary keys | With string as the map key, hashing is slightly slower than int64 | Does not restrict the business side; the performance gap is negligible compared with a DB round trip; a fast path for single-column integer primary keys can be added if needed |
| DB-generated primary keys | Not supported; the caller must provide the primary key (confirmed not needed) | The business side must generate primary keys itself (UUID, snowflake, etc.) | When the outcome is ambiguous, a retry must be able to tell whether it is the same row; supporting it would require a unique index on gen |
| Table structure changes | v1 requires restarting the service; drift is detected in the background and alerted | Altering a table requires scheduled downtime or a rolling restart | Schema is immutable while running, so reasoning is simple; switching schema online would require pausing writes per table, flushing, and clearing, which is highly complex |
| Poison rows | Up-front validation + per-row isolation + quarantine + alerting; never silently dropped | Updates that were already confirmed may need manual handling | write-behind plus DB constraints inevitably produce "fails after being confirmed"; all that can be done is to minimize it and make it visible |
| PgBouncer | Direct connection to PG (confirmed) | - | The service has its own connection pool; one less layer means one less source of "ambiguous outcome", and no compatibility issues with prepared statements and session settings to handle |
| External protocol | Custom TCP protocol + protobuf; multiplexing on a single connection, responses returned out of order | Framing, multiplexing, timeouts and backpressure must all be implemented ourselves; an SDK must be written for each client language; no ready-made debugging tools like grpcurl | update is a pure in-memory operation, so protocol overhead is a large share; skips the HTTP/2 and gRPC middle layers |
| Column identification | Column ordinal + schema_version; ordinals obtained via Describe | Clients must Describe first and cache it | Requests do not need to carry column names; schema_version blocks requests that use stale ordinals |
| gen for Update/Delete | Optional, not checked by default | When omitted, in scenarios where primary keys are reused, an update may modify a new life | Zero cost for business clients that do not care; those that need it simply use the gen from the response |
| Increment | Compute the new value inside `mu`; flush writes the absolute value | Increments within the window are lost on a crash; cannot be safely retried when the connection drops | Coalesced writes, rewrites on failure, and late execution are all naturally correct, with no need to record deltas separately |
| SDK automatic retry | Only idempotent requests are retried automatically: Get, Create (same gen), Delete with gen | When update or delete without gen has an ambiguous outcome, the business side must handle it | An automatic retry must not let one call take effect twice; which life to delete is business semantics, and the SDK does not decide it for the business side |
| Answer of a call after an ambiguous attempt | Only a status that judges the data, or `ErrOutcomeUnknown`; never "not executed" | More calls end in "unknown" while the DB is slow | "This request did nothing" says nothing about the earlier request, which is often still running (hole 7) |
| Constraints that depend on other rows | Table refused by default; per-table option; with the option, terminate the sessions of ambiguous attempts before trusting such a rejection | Tables with a unique index or a foreign key need an explicit decision; a connection is terminated in a rare case | The same statement can be rejected now and accepted later, so no retry result settles it while an earlier attempt is in flight (hole 5); a flush that violates such a constraint is a poison row |
| A second instance | Owner epoch in every write; the older instance stops | One more table; one more condition in every write; one query per second; a superseded instance still answers for up to `EpochCheckInterval` | Leaving it to deployment made the most common operational accident silent and unbounded (hole 8); with the epoch it costs what a crash costs |
| Loss window while the DB cannot be written | `MaxDirtyAge`: beyond it every update gets `ErrBusy` | Updates become unavailable 30 s into a DB outage | `MaxDirty` bounds memory, not age; without an age limit the service acknowledges updates for as long as the outage lasts, and section 1 tolerates only a small loss |
| Values PG normalizes (float4, numeric scale, char(n), timestamp precision, jsonb) | Not handled for now (13.3) | A Get can return a different value after eviction and reload | Deferred by decision #21; the fix is local to input validation |
---

## 16. References

- Martin Kleppmann, *Designing Data-Intensive Applications*: chapter 8 on fencing tokens, chapter 9 on linearizability
- Herlihy & Wing, *Linearizability: A Correctness Condition for Concurrent Objects*, 1990
- Porcupine: `github.com/anishathalye/porcupine`
- Hillel Wayne, *Practical TLA+*; Leslie Lamport's TLA+ video course
- Go source: `sync/map.go` (`expunged` sentinel)
- Java source: `ConcurrentHashMap.putVal` (validation after locking)
- Linux kernel: `lib/lockref.c` (`lockref_mark_dead`), lockdep
- Timothy Harris, *A Pragmatic Implementation of Non-Blocking Linked-Lists*, 2001
- `github.com/sasha-s/go-deadlock`
