// Package fakedb is an in-memory db.DB for tests (design.md 12.1). UpdateHook injects failed and late
// writes; the rest of fault injection comes in M3.
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

type DB struct {
	// SelectHook, if set, runs at the start of every Select, without the DB's lock held. A non-nil
	// result is returned instead of reading. Set it before concurrent use.
	SelectHook func(ctx context.Context, key schema.Key) error

	// UpdateHook, if set, decides what happens to each Update. exec applies the statement to the stored
	// rows; call it at most once, at any time, from any goroutine. Calling it after returning is a late
	// statement: one that timed out for the caller and executed afterwards. Update returns what the hook
	// returns. It runs without the DB's lock held. Set it before concurrent use.
	UpdateHook func(ctx context.Context, key schema.Key, exec func()) error

	selects atomic.Int64
	updates atomic.Int64

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

// Select returns a copy of the stored row, so neither side can change the other's.
func (d *DB) Select(ctx context.Context, key schema.Key) (*schema.Row, error) {
	d.selects.Add(1)
	if d.SelectHook != nil {
		if err := d.SelectHook(ctx, key); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	r, ok := d.rows[key]
	if !ok {
		return nil, db.ErrNoRows
	}
	return clone(r), nil
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
	exec := func() { d.apply(r, mask) }
	if d.UpdateHook != nil {
		return d.UpdateHook(ctx, r.Key, exec)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	exec()
	return nil
}

// apply executes UPDATE SET <columns in mask>, ver = r.Ver WHERE key = r.Key AND gen = r.Gen AND ver < r.Ver.
func (d *DB) apply(r *schema.Row, mask uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
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
