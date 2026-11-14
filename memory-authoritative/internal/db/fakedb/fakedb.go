// Package fakedb is an in-memory db.DB for tests (design.md 12.1). Hook sees every statement and injects
// faults: a statement can execute now, later or never, and its caller can be told anything.
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

type DB struct {
	// Hook, if set, runs for every statement before it executes, without the DB's lock held.
	//   - nil: the statement executes (Exec is called now unless the hook already called it) and its own
	//     result is returned.
	//   - an error: that error is returned. The statement executes only if and when the hook calls Exec:
	//     now (executed, response lost), later from any goroutine (late), or never.
	// Set it before concurrent use.
	Hook func(ctx context.Context, st *Stmt) error

	selects  atomic.Int64
	updates  atomic.Int64
	sessions atomic.Uint64

	mu        sync.Mutex
	rows      map[schema.Key]*schema.Row
	writing   map[schema.Key]int // Update calls in progress, per key
	violation error              // the first broken assertion
}

func New() *DB {
	return &DB{rows: make(map[schema.Key]*schema.Row), writing: make(map[schema.Key]int)}
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

// Selects returns the number of Select calls so far, including ones the hook failed.
func (d *DB) Selects() int { return int(d.selects.Load()) }

// Updates returns the number of Update calls so far, including ones the hook failed.
func (d *DB) Updates() int { return int(d.updates.Load()) }

// Violation returns the first broken assertion (tla-mapping.md section 5), or nil:
//   - OneWriterPerKey (I5): an Update of a key starts while another Update call of that key has not
//     returned. A late exec, after its call returned, does not count.
//   - DbVerMonotonic: an applied write keeps the gen but does not increase ver.
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

// run sends st: it gives it a session and an Exec that runs execLocked once, with d.mu held, and then
// lets the hook decide. Without a hook, a statement whose ctx is done is not executed.
func (d *DB) run(ctx context.Context, st *Stmt, execLocked func()) error {
	st.Session = db.SessionID(d.sessions.Add(1))
	var once sync.Once
	st.Exec = func() {
		once.Do(func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			execLocked()
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

func (d *DB) Update(ctx context.Context, r *schema.Row, mask uint64) error {
	d.updates.Add(1)
	d.mu.Lock()
	if d.writing[r.Key]++; d.writing[r.Key] > 1 {
		d.violateLocked("OneWriterPerKey (I5): two Update calls of key %q in progress at once", r.Key)
	}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.writing[r.Key]--
		d.mu.Unlock()
	}()
	return d.run(ctx, &Stmt{Kind: Update, Key: r.Key, Gen: r.Gen, Ver: r.Ver}, func() { d.applyLocked(r, mask) })
}

// applyLocked executes UPDATE SET <columns in mask>, ver = r.Ver WHERE key = r.Key AND gen = r.Gen AND
// ver < r.Ver.
func (d *DB) applyLocked(r *schema.Row, mask uint64) {
	cur, ok := d.rows[r.Key]
	if !ok || cur.Gen != r.Gen || cur.Ver >= r.Ver {
		return // 0 rows
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
	d.rows[r.Key] = n
}

func clone(r *schema.Row) *schema.Row {
	c := *r
	c.Vals = slices.Clone(r.Vals)
	return &c
}
