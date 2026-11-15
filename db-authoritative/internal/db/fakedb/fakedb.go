// Package fakedb is an in-memory db.DB for tests (design.md 12.1). Hook sees every statement and injects
// faults: a statement can execute now, later or never, and its caller can be told anything. It also has
// the dedup table (I12), sessions that FenceSession terminates, and a rejection switch.
package fakedb

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"rowcache/internal/db"
	"rowcache/internal/schema"
)

type Kind uint8

const (
	Select Kind = iota
	Update
	Insert
	Delete
	GenClaimed
	Fence
)

// Stmt is one statement, as the hook sees it.
type Stmt struct {
	Kind    Kind
	Key     schema.Key   // not set for Fence
	Gen     int64        // Update, Insert, Delete, GenClaimed
	Session db.SessionID // the statement's own session (a new one per statement); Fence: the session to terminate
	Exec    func()       // executes the statement; at most once; does nothing once its session is fenced
}

// Ambiguous returns the error of a statement whose outcome the caller cannot know.
func Ambiguous(st *Stmt, err error) error { return &db.AmbiguousError{Session: st.Session, Err: err} }

type dedupKey struct {
	key schema.Key
	gen int64
}

type DB struct {
	// Hook, if set, runs for every statement before it executes, without the DB's lock held.
	//   - nil: the statement goes on like a plain DB call: if ctx is done it is not executed (unless the hook
	//     already called Exec), else it executes now and its own result is returned.
	//   - an error: that error is returned. The statement executes only if and when the hook calls Exec:
	//     now (executed, answer lost), later from any goroutine (late), or never.
	// Exec of a statement whose session was fenced does nothing. Set it before concurrent use.
	Hook func(ctx context.Context, st *Stmt) error

	// Normalize, if set, maps every value an Insert or Update writes to the value stored, as PG does for
	// float4, numeric scale or char(n). Set it before concurrent use.
	Normalize func(col int, v any) any

	// RowCheck, if set, runs on every row an Insert or Update writes; an error is a DbValsBelongToGen
	// violation. Set it before concurrent use.
	RowCheck func(r *schema.Row) error

	selects  atomic.Int64
	updates  atomic.Int64
	sessions atomic.Uint64

	mu        sync.Mutex
	rows      map[schema.Key]*schema.Row
	dedup     map[dedupKey]bool // the dedup table (5.1): never deleted
	deleted   map[dedupKey]bool // lives whose row a Delete removed
	rejecting map[schema.Key]bool
	fenced    map[db.SessionID]bool
	pending   map[db.SessionID]bool // answered ambiguous, Exec neither run nor fenced
	applied   []*schema.Row         // every row an Update wrote
	writing   map[schema.Key]int    // Insert, Update and Delete calls in progress, per key
	violation error                 // the first broken assertion
}

func New() *DB {
	return &DB{
		rows:      make(map[schema.Key]*schema.Row),
		dedup:     make(map[dedupKey]bool),
		deleted:   make(map[dedupKey]bool),
		rejecting: make(map[schema.Key]bool),
		fenced:    make(map[db.SessionID]bool),
		pending:   make(map[db.SessionID]bool),
		writing:   make(map[schema.Key]int),
	}
}

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

// SetRejecting turns the rejection switch of key on or off. While it is on, a write of that key that would
// change the row (an INSERT that would write it, an UPDATE or DELETE that matches it), when it executes (on
// time or late), is rejected with db.ErrRejected and changes nothing; an INSERT's dedup record is rolled back
// with it. An INSERT whose gen is claimed, or an UPDATE or DELETE that matches no row, is not rejected.
func (d *DB) SetRejecting(key schema.Key, on bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rejecting[key] = on
}

// Pending returns the number of write statements (Insert, Update, Delete) answered ambiguous whose Exec has neither run nor been fenced.
func (d *DB) Pending() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending)
}

// AppliedUpdates returns copies of every row an Update wrote, in order.
func (d *DB) AppliedUpdates() []*schema.Row {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]*schema.Row, len(d.applied))
	for i, r := range d.applied {
		out[i] = clone(r)
	}
	return out
}

// Selects returns the number of Select calls so far, including ones the hook failed.
func (d *DB) Selects() int { return int(d.selects.Load()) }

// Updates returns the number of Update calls so far, including ones the hook failed.
func (d *DB) Updates() int { return int(d.updates.Load()) }

// Violation returns the first broken assertion, or nil:
//   - OneWriterPerKey (I5): an Insert, Update or Delete of a key starts while another such call of that
//     key has not returned. A late exec, after its call returned, does not count as a call.
//   - StmtOnlyUnderOp (I5): a write statement executes late, after its call returned, while another write
//     call of its key is in progress.
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

// run sends st: it gives it a session (a Fence keeps the session it terminates) and an Exec that runs
// execLocked once, with d.mu held, unless the session has been fenced; then it lets the hook decide. When
// the hook returns nil, or there is none, the statement goes on like a plain DB call: not executed if ctx is
// done, else executed.
func (d *DB) run(ctx context.Context, st *Stmt, execLocked func()) error {
	if st.Kind != Fence {
		st.Session = db.SessionID(d.sessions.Add(1))
	}
	write := isWrite(st.Kind)
	returned := false // a write's call has returned (guarded by d.mu): its Exec is then a late one
	if write {
		defer d.startWrite(st.Key, &returned)()
	}
	var ran atomic.Bool
	var once sync.Once
	st.Exec = func() {
		once.Do(func() {
			ran.Store(true)
			d.mu.Lock()
			defer d.mu.Unlock()
			delete(d.pending, st.Session)
			if !d.fenced[st.Session] {
				if returned && d.writing[st.Key] > 0 {
					d.violateLocked("StmtOnlyUnderOp (I5): a late statement (kind %d) of key %q executes while another write call of the key is in progress", st.Kind, st.Key)
				}
				execLocked()
			}
		})
	}
	if d.Hook != nil {
		if err := d.Hook(ctx, st); err != nil {
			if write && db.IsAmbiguous(err) {
				d.mu.Lock()
				if !ran.Load() && !d.fenced[st.Session] {
					d.pending[st.Session] = true
				}
				d.mu.Unlock()
			}
			return err
		}
	}
	if err := ctx.Err(); err != nil && !ran.Load() {
		return fmt.Errorf("%w: %w", db.ErrNotExecuted, err)
	}
	st.Exec()
	return nil
}

func isWrite(k Kind) bool { return k == Insert || k == Update || k == Delete }

// startWrite registers a write call of key, for OneWriterPerKey; the returned func unregisters it and sets
// *returned, so that a later Exec of the statement is a late one.
func (d *DB) startWrite(key schema.Key, returned *bool) (done func()) {
	d.mu.Lock()
	if d.writing[key]++; d.writing[key] > 1 {
		d.violateLocked("OneWriterPerKey (I5): two write calls of key %q in progress at once", key)
	}
	d.mu.Unlock()
	return func() {
		d.mu.Lock()
		*returned = true
		if d.writing[key]--; d.writing[key] == 0 {
			delete(d.writing, key)
		}
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

func (d *DB) normalize(col int, v any) any {
	if d.Normalize != nil {
		return d.Normalize(col, v)
	}
	return v
}

// Select returns a copy of the stored row, so neither side can change the other's.
func (d *DB) Select(ctx context.Context, key schema.Key) (*schema.Row, error) {
	d.selects.Add(1)
	var r *schema.Row
	err := d.run(ctx, &Stmt{Kind: Select, Key: key}, func() {
		if cur, ok := d.rows[key]; ok {
			r = clone(cur)
		}
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
	st := &Stmt{Kind: Insert, Key: r.Key, Gen: r.Gen}
	var got *schema.Row
	var res error
	err := d.run(ctx, st, func() {
		// In the order of the statement (8.3): the dedup claim first; the business row only if it was
		// claimed; a constraint is checked only by an INSERT that would write the row.
		dk := dedupKey{r.Key, r.Gen}
		if d.dedup[dk] {
			res = db.ErrGenClaimed
			return
		}
		if _, ok := d.rows[r.Key]; ok {
			d.dedup[dk] = true
			res = db.ErrDuplicate // ON CONFLICT (id) DO NOTHING: the dedup record stays
			return
		}
		if d.rejecting[r.Key] {
			res = db.ErrRejected // the whole statement is rolled back, dedup record included
			return
		}
		d.dedup[dk] = true
		if d.deleted[dk] {
			d.violateLocked("NoResurrection: key %q gen %d inserted again after its delete", r.Key, r.Gen)
		}
		n := clone(r)
		n.Ver = 0
		for i, v := range n.Vals {
			n.Vals[i] = d.normalize(i, v)
		}
		d.checkRowLocked(n)
		d.rows[r.Key] = n
		got = clone(n)
	})
	if err != nil {
		return nil, err
	}
	return got, res
}

// GenClaimed reads the dedup table.
func (d *DB) GenClaimed(ctx context.Context, key schema.Key, gen int64) (bool, error) {
	var claimed bool
	err := d.run(ctx, &Stmt{Kind: GenClaimed, Key: key, Gen: gen}, func() {
		claimed = d.dedup[dedupKey{key, gen}]
	})
	if err != nil {
		return false, err
	}
	return claimed, nil
}

// Update applies r.Vals[c] for c in cols to the row with key r.Key and gen r.Gen, sets ver = ver + 1,
// and returns a copy of the stored row.
func (d *DB) Update(ctx context.Context, r *schema.Row, cols []int) (*schema.Row, error) {
	d.updates.Add(1)
	st := &Stmt{Kind: Update, Key: r.Key, Gen: r.Gen}
	var got *schema.Row
	var res error
	err := d.run(ctx, st, func() {
		cur, ok := d.rows[r.Key]
		if !ok || cur.Gen != r.Gen {
			res = db.ErrNoRows
			return
		}
		if d.rejecting[r.Key] {
			res = db.ErrRejected
			return
		}
		n := clone(cur)
		for _, c := range cols {
			n.Vals[c] = d.normalize(c, r.Vals[c])
		}
		n.Ver++
		d.checkRowLocked(n)
		d.rows[r.Key] = n
		d.applied = append(d.applied, n) // rows are replaced, never mutated
		got = clone(n)
	})
	if err != nil {
		return nil, err
	}
	return got, res
}

// Delete executes DELETE WHERE key AND gen. The dedup record stays (I12).
func (d *DB) Delete(ctx context.Context, key schema.Key, gen int64) error {
	st := &Stmt{Kind: Delete, Key: key, Gen: gen}
	var res error
	err := d.run(ctx, st, func() {
		cur, ok := d.rows[key]
		if !ok || cur.Gen != gen {
			res = db.ErrNoRows
			return
		}
		if d.rejecting[key] {
			res = db.ErrRejected
			return
		}
		delete(d.rows, key)
		d.deleted[dedupKey{key, cur.Gen}] = true // cur.Gen, not gen: so a Delete that ignores gen shows up
	})
	if err != nil {
		return err
	}
	return res
}

// FenceSession terminates the session: nothing sent on it executes after it returns nil, and a statement
// of it that was waiting is no longer pending. An error means not confirmed: the session is not fenced.
func (d *DB) FenceSession(ctx context.Context, id db.SessionID) error {
	return d.run(ctx, &Stmt{Kind: Fence, Session: id}, func() {
		d.fenced[id] = true
		delete(d.pending, id)
	})
}

func clone(r *schema.Row) *schema.Row {
	c := *r
	c.Vals = slices.Clone(r.Vals)
	return &c
}
