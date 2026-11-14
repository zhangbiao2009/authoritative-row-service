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
}
