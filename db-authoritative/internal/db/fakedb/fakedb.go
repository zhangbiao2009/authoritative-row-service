// Package fakedb is an in-memory db.DB for tests (design.md 12.1). Hook sees every statement, so a test
// can block or fail it. Later milestones add writes, sessions and faults.
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
)

// Stmt is one statement, as the hook sees it.
type Stmt struct {
	Kind Kind
	Key  schema.Key
	Gen  int64 // Update
}

type DB struct {
	// Hook, if set, runs for every statement before it executes, without the DB's lock held. An error is
	// returned instead, and the statement does not execute. Set it before concurrent use.
	Hook func(ctx context.Context, st *Stmt) error

	// Normalize, if set, maps every value an Update writes to the value stored, as PG does for
	// float4, numeric scale or char(n). Set it before concurrent use.
	Normalize func(col int, v any) any

	selects atomic.Int64
	updates atomic.Int64

	mu   sync.Mutex
	rows map[schema.Key]*schema.Row

	writing   map[schema.Key]int // Update calls in progress, per key
	violation error
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

// Select returns a copy of the stored row, so neither side can change the other's.
func (d *DB) Select(ctx context.Context, key schema.Key) (*schema.Row, error) {
	d.selects.Add(1)
	if d.Hook != nil {
		if err := d.Hook(ctx, &Stmt{Kind: Select, Key: key}); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", db.ErrNotExecuted, err)
	}
	if r := d.Row(key); r != nil {
		return r, nil
	}
	return nil, db.ErrNoRows
}

// Updates returns the number of Update calls so far, including ones the hook failed.
func (d *DB) Updates() int { return int(d.updates.Load()) }

// Violation returns the first broken assertion, or nil: OneWriterPerKey (I5).
func (d *DB) Violation() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.violation
}

// Update applies r.Vals[c] for c in cols to the row with key r.Key and gen r.Gen, sets ver = ver + 1,
// and returns a copy of the stored row.
func (d *DB) Update(ctx context.Context, r *schema.Row, cols []int) (*schema.Row, error) {
	d.updates.Add(1)
	d.mu.Lock()
	d.writing[r.Key]++
	if d.writing[r.Key] > 1 && d.violation == nil {
		d.violation = fmt.Errorf("OneWriterPerKey (I5): two Update calls of key %q in progress at once", r.Key)
	}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.writing[r.Key]--
		d.mu.Unlock()
	}()
	if d.Hook != nil {
		if err := d.Hook(ctx, &Stmt{Kind: Update, Key: r.Key, Gen: r.Gen}); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", db.ErrNotExecuted, err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	stored, ok := d.rows[r.Key]
	if !ok || stored.Gen != r.Gen {
		return nil, db.ErrNoRows
	}
	n := clone(stored)
	for _, c := range cols {
		v := r.Vals[c]
		if d.Normalize != nil {
			v = d.Normalize(c, v)
		}
		n.Vals[c] = v
	}
	n.Ver++
	d.rows[r.Key] = n
	return clone(n), nil
}

func clone(r *schema.Row) *schema.Row {
	c := *r
	c.Vals = slices.Clone(r.Vals)
	return &c
}
