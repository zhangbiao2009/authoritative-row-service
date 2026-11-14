// Package db is the interface between the Store and the database.
package db

import (
	"context"
	"errors"

	"rowsvc/internal/schema"
)

var ErrNoRows = errors.New("no rows")

// DB is one table's storage. Implementations: fakedb (tests) and PostgreSQL (M7).
type DB interface {
	// Select returns the row, or ErrNoRows. A read that executes late changes nothing, so it needs
	// no gen/ver condition (design.md 7.2).
	Select(ctx context.Context, key schema.Key) (*schema.Row, error)

	// Update writes the columns of r selected by mask (schema.ColBit), and ver = r.Ver, to the row with key
	// r.Key, only if that row exists with gen = r.Gen and ver < r.Ver (I6; design.md 8.3). It never inserts.
	// Zero rows affected is not an error (7.6): an earlier write that timed out may have landed, or the row
	// is gone. The values are absolute, never col = col + delta (I12).
	Update(ctx context.Context, r *schema.Row, mask uint64) error
}
