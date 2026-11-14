// Package fakedb is an in-memory db.DB for tests (design.md 12.1). Fault injection comes in M3.
package fakedb

import (
	"context"
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

	selects atomic.Int64

	mu   sync.Mutex
	rows map[schema.Key]*schema.Row
}

func New() *DB {
	return &DB{rows: make(map[schema.Key]*schema.Row)}
}

// Put stores a copy of r, replacing any row with the same key.
func (d *DB) Put(r *schema.Row) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rows[r.Key] = clone(r)
}

// Selects returns the number of Select calls so far, including ones the hook failed.
func (d *DB) Selects() int { return int(d.selects.Load()) }

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

func clone(r *schema.Row) *schema.Row {
	c := *r
	c.Vals = slices.Clone(r.Vals)
	return &c
}
