// Package fakedb is an in-memory db.DB for tests (design.md 12.1). Hook sees every statement and injects
// faults: a statement can execute now, later or never, and its caller can be told anything. It also has
// the dedup table (I14), sessions that FenceSessions terminates, and a cross-row-constraint switch.
package fakedb

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"rowsvc/internal/db"
	"rowsvc/internal/schema"
)

type Kind uint8

const (
	Select Kind = iota
	Insert
	Update
	Delete
	GenClaimed
	Fence
)

// Stmt is one statement, as the hook sees it.
type Stmt struct {
	Kind     Kind
	Key      schema.Key     // not set for Fence
	Gen, Ver int64          // Gen: Insert, Update, Delete, GenClaimed; Ver: Update
	Session  db.SessionID   // a new session per statement
	Sessions []db.SessionID // Fence: the sessions to terminate
	Exec     func()         // executes the statement; at most once (a second call does nothing)
}

// Ambiguous returns the error of a statement whose outcome the caller cannot know.
func Ambiguous(st *Stmt, err error) error { return &db.AmbiguousError{Session: st.Session, Err: err} }

// Stats counts events tests assert on to show they reached the hard cases (tla-mapping.md section 8).
type Stats struct {
	Late          int // Execs after their call had returned
	LateEffective int // of those, ones that changed a row or the dedup table
	GenClaimed    int // Inserts that found their gen in the dedup table
	CrossRow      int // Inserts and Deletes rejected by the cross-row switch
	Fenced        int // sessions fenced
}

type dedupKey struct {
	key schema.Key
	gen int64
}

type DB struct {
	// Hook, if set, runs for every statement before it executes, without the DB's lock held.
	//   - nil: the statement executes (Exec is called now unless the hook already called it) and its own
	//     result is returned.
	//   - an error: that error is returned. The statement executes only if and when the hook calls Exec:
	//     now (executed, response lost), later from any goroutine (late), or never.
	// Exec of a statement whose session was fenced does nothing. Set it before concurrent use.
	Hook func(ctx context.Context, st *Stmt) error

	// RowCheck, if set, runs on every row an Insert or Update writes; an error is a DbValsBelongToGen
	// violation. Set it before concurrent use.
	RowCheck func(r *schema.Row) error

	selects  atomic.Int64
	updates  atomic.Int64
	sessions atomic.Uint64

	mu        sync.Mutex
	rows      map[schema.Key]*schema.Row
	dedup     map[dedupKey]bool // row_service_gen_log (5.1): never deleted
	deleted   map[dedupKey]bool // lives whose row a Delete removed
	crossRow  map[schema.Key]bool
	fenced    map[db.SessionID]bool
	writing   map[schema.Key]int // Insert, Update and Delete calls in progress, per key
	stats     Stats
	violation error // the first broken assertion
}

func New() *DB {
	return &DB{
		rows:     make(map[schema.Key]*schema.Row),
		dedup:    make(map[dedupKey]bool),
		deleted:  make(map[dedupKey]bool),
		crossRow: make(map[schema.Key]bool),
		fenced:   make(map[db.SessionID]bool),
		writing:  make(map[schema.Key]int),
	}
}

// Put stores a copy of r, replacing any row with the same key.
func (d *DB) Put(r *schema.Row) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rows[r.Key] = clone(r)
}

// Row returns a copy of the stored row, or nil. Unlike Select it counts nothing and runs no hook.
func (d *DB) Row(key schema.Key) *schema.Row {
	d.mu.Lock()
	defer d.mu.Unlock()
	if r, ok := d.rows[key]; ok {
		return clone(r)
	}
	return nil
}

// Claimed reports whether (key, gen) is in the dedup table. Unlike GenClaimed it counts nothing and runs
// no hook.
func (d *DB) Claimed(key schema.Key, gen int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dedup[dedupKey{key, gen}]
}

// Deleted reports whether a Delete removed the row of (key, gen). It counts nothing and runs no hook.
func (d *DB) Deleted(key schema.Key, gen int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deleted[dedupKey{key, gen}]
}

// SetCrossRow turns the cross-row-constraint switch of key on or off. While it is on, an Insert that would
// write the row, or a Delete that would delete it, when it executes (on time or late), is rejected with
// db.ErrCrossRow and changes nothing. An Insert whose gen is claimed, or a Delete that finds no row, never
// reaches the constraint (8.3).
func (d *DB) SetCrossRow(key schema.Key, on bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.crossRow[key] = on
}

func (d *DB) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stats
}

// Selects returns the number of Select calls so far, including ones the hook failed.
func (d *DB) Selects() int { return int(d.selects.Load()) }

// Updates returns the number of Update calls so far, including ones the hook failed.
func (d *DB) Updates() int { return int(d.updates.Load()) }

// Violation returns the first broken assertion (tla-mapping.md section 5), or nil:
//   - OneWriterPerKey (I5): an Insert, Update or Delete of a key starts while another such call of that
//     key has not returned. A late exec, after its call returned, does not count.
//   - DbVerMonotonic: an applied write keeps the gen but does not increase ver.
//   - NoResurrection: a row is inserted for a (key, gen) whose row was deleted.
//   - DbValsBelongToGen: RowCheck rejects a row that was written.
func (d *DB) Violation() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.violation
}

func (d *DB) violateLocked(format string, args ...any) {
	if d.violation == nil {
		d.violation = fmt.Errorf(format, args...)
	}
}

// run sends st: it gives it a session and an Exec that runs execLocked once, with d.mu held, unless the
// session has been fenced; then it lets the hook decide. Without a hook, a statement whose ctx is done is
// not executed. execLocked reports whether it changed a row or the dedup table.
func (d *DB) run(ctx context.Context, st *Stmt, execLocked func() bool) error {
	st.Session = db.SessionID(d.sessions.Add(1))
	var returned atomic.Bool
	defer returned.Store(true)
	var once sync.Once
	st.Exec = func() {
		once.Do(func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			changed := !d.fenced[st.Session] && execLocked()
			if returned.Load() {
				d.stats.Late++
				if changed {
					d.stats.LateEffective++
				}
			}
		})
	}
	if d.Hook != nil {
		if err := d.Hook(ctx, st); err != nil {
			return err
		}
	} else if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", db.ErrNotExecuted, err)
	}
	st.Exec()
	return nil
}

// write registers a write call of key, for OneWriterPerKey; the returned func unregisters it.
func (d *DB) write(key schema.Key) (done func()) {
	d.mu.Lock()
	if d.writing[key]++; d.writing[key] > 1 {
		d.violateLocked("OneWriterPerKey (I5): two write calls of key %q in progress at once", key)
	}
	d.mu.Unlock()
	return func() {
		d.mu.Lock()
		d.writing[key]--
		d.mu.Unlock()
	}
}

func (d *DB) checkRowLocked(r *schema.Row) {
	if d.RowCheck != nil {
		if err := d.RowCheck(r); err != nil {
			d.violateLocked("DbValsBelongToGen: key %q gen %d: %v", r.Key, r.Gen, err)
		}
	}
}

// Select returns a copy of the stored row, so neither side can change the other's.
func (d *DB) Select(ctx context.Context, key schema.Key) (*schema.Row, error) {
	d.selects.Add(1)
	var r *schema.Row
	err := d.run(ctx, &Stmt{Kind: Select, Key: key}, func() bool {
		if cur, ok := d.rows[key]; ok {
			r = clone(cur)
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, db.ErrNoRows
	}
	return r, nil
}

// InsertDedup executes the create statement of design.md 8.3: the dedup record and the row, in one statement.
func (d *DB) InsertDedup(ctx context.Context, r *schema.Row) (*schema.Row, error) {
	defer d.write(r.Key)()
	var got *schema.Row
	var res error
	err := d.run(ctx, &Stmt{Kind: Insert, Key: r.Key, Gen: r.Gen}, func() bool {
		// In the order of the statement (8.3): the dedup claim first; the business row only if it was
		// claimed; a cross-row constraint is checked only by an INSERT that would write the row.
		dk := dedupKey{r.Key, r.Gen}
		if d.dedup[dk] {
			d.stats.GenClaimed++
			res = db.ErrGenClaimed
			return false
		}
		if _, ok := d.rows[r.Key]; ok {
			d.dedup[dk] = true
			res = db.ErrDuplicate // ON CONFLICT (id) DO NOTHING: the dedup record stays
			return true
		}
		if d.crossRow[r.Key] {
			d.stats.CrossRow++
			res = db.ErrCrossRow // the whole statement is rolled back, dedup record included
			return false
		}
		d.dedup[dk] = true
		if d.deleted[dk] {
			d.violateLocked("NoResurrection: key %q gen %d inserted again after its delete", r.Key, r.Gen)
		}
		d.checkRowLocked(r)
		d.rows[r.Key] = clone(r)
		got = clone(r)
		return true
	})
	if err != nil {
		return nil, err
	}
	return got, res
}

// GenClaimed reads the dedup table.
func (d *DB) GenClaimed(ctx context.Context, key schema.Key, gen int64) (bool, error) {
	var claimed bool
	err := d.run(ctx, &Stmt{Kind: GenClaimed, Key: key, Gen: gen}, func() bool {
		claimed = d.dedup[dedupKey{key, gen}]
		return false
	})
	return claimed, err
}

// Delete executes DELETE WHERE key AND gen. Zero rows is nil. The dedup record stays (I14).
func (d *DB) Delete(ctx context.Context, key schema.Key, gen int64) error {
	defer d.write(key)()
	var res error
	err := d.run(ctx, &Stmt{Kind: Delete, Key: key, Gen: gen}, func() bool {
		if cur, ok := d.rows[key]; ok && cur.Gen == gen {
			if d.crossRow[key] { // a referencing row: checked only when a row is deleted
				d.stats.CrossRow++
				res = db.ErrCrossRow
				return false
			}
			delete(d.rows, key)
			d.deleted[dedupKey{key, cur.Gen}] = true // the life of the row removed
			return true
		}
		return false
	})
	if err != nil {
		return err
	}
	return res
}

// FenceSessions terminates the sessions: nothing sent on them executes after it returns nil.
func (d *DB) FenceSessions(ctx context.Context, ids []db.SessionID) error {
	return d.run(ctx, &Stmt{Kind: Fence, Sessions: slices.Clone(ids)}, func() bool {
		for _, id := range ids {
			d.fenced[id] = true
		}
		d.stats.Fenced += len(ids)
		return false
	})
}

func (d *DB) Update(ctx context.Context, r *schema.Row, mask uint64) error {
	d.updates.Add(1)
	defer d.write(r.Key)()
	return d.run(ctx, &Stmt{Kind: Update, Key: r.Key, Gen: r.Gen, Ver: r.Ver}, func() bool { return d.applyLocked(r, mask) })
}

// applyLocked executes UPDATE SET <columns in mask>, ver = r.Ver WHERE key = r.Key AND gen = r.Gen AND
// ver < r.Ver. It never inserts.
func (d *DB) applyLocked(r *schema.Row, mask uint64) bool {
	cur, ok := d.rows[r.Key]
	if !ok || cur.Gen != r.Gen || cur.Ver >= r.Ver {
		return false // 0 rows
	}
	if cur.Gen == r.Gen && r.Ver <= cur.Ver {
		d.violateLocked("DbVerMonotonic: key %q gen %d: ver %d written over ver %d", r.Key, r.Gen, r.Ver, cur.Ver)
	}
	n := clone(cur)
	for i := range n.Vals {
		if mask&schema.ColBit(i) != 0 {
			n.Vals[i] = r.Vals[i]
		}
	}
	n.Ver = r.Ver
	d.checkRowLocked(n)
	d.rows[r.Key] = n
	return true
}

func clone(r *schema.Row) *schema.Row {
	c := *r
	c.Vals = slices.Clone(r.Vals)
	return &c
}
