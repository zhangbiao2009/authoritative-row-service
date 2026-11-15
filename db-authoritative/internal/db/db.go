// Package db is the interface between the Store and the database (design.md 5.2). Every error a call
// returns is classified by the implementation, and the Store trusts the classification (8.4).
package db

import (
	"context"
	"errors"

	"rowcache/internal/schema"
)

var (
	ErrNoRows      = errors.New("no rows")      // Select found nothing; Update matched no row
	ErrNotExecuted = errors.New("not executed") // definitely not executed, and never will be
)

// DB is one table's storage. Implementations: fakedb (tests) and PostgreSQL (M7). It is the only
// interface in the Store.
type DB interface {
	// Select returns the row, or ErrNoRows. A late read changes nothing, so it needs no gen condition
	// and no fence (7.2).
	Select(ctx context.Context, key schema.Key) (*schema.Row, error)
	// Update sets the columns cols of the row with key r.Key and gen r.Gen to r.Vals[c], and
	// ver = ver + 1, in one statement. Returns the row as stored (RETURNING). ErrNoRows: no row matched.
	Update(ctx context.Context, r *schema.Row, cols []int) (*schema.Row, error)
}
