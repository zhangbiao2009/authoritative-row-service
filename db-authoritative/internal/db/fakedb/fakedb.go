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

const Select Kind = iota

// Stmt is one statement, as the hook sees it.
type Stmt struct {
	Kind Kind
	Key  schema.Key
}

type DB struct {
	// Hook, if set, runs for every statement before it executes, without the DB's lock held. An error is
	// returned instead, and the statement does not execute. Set it before concurrent use.
	Hook func(ctx context.Context, st *Stmt) error

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
