// Package db is the interface between the Store and the database (design.md 5.2). Every error a call
// returns is classified by the implementation, and the Store trusts the classification (8.4).
package db

import (
	"context"
	"errors"

	"rowcache/internal/schema"
)

var (
	ErrNoRows      = errors.New("no rows")      // Select found nothing; Update / Delete matched no row
	ErrNotExecuted = errors.New("not executed") // definitely not executed, and never will be
	ErrRejected    = errors.New("rejected")     // a constraint rejected it; the statement was rolled back
	ErrGenClaimed  = errors.New("gen already claimed")
	ErrDuplicate   = errors.New("duplicate key")
)

// SessionID identifies the database connection a statement was sent on.
type SessionID uint64

// AmbiguousError is the outcome of a statement that may or may not have executed, and may still execute.
// Session is the connection it was sent on, which FenceSession terminates.
type AmbiguousError struct {
	Session SessionID
	Err     error
}

func (e *AmbiguousError) Error() string { return "outcome ambiguous: " + e.Err.Error() }
func (e *AmbiguousError) Unwrap() error { return e.Err }

func IsAmbiguous(err error) bool {
	var a *AmbiguousError
	return errors.As(err, &a)
}

// SessionOf returns the session of the *AmbiguousError in err, or 0 if there is none.
func SessionOf(err error) SessionID {
	var a *AmbiguousError
	if errors.As(err, &a) {
		return a.Session
	}
	return 0
}

// DB is one table's storage. Implementations: fakedb (tests) and PostgreSQL (M7). It is the only
// interface in the Store.
type DB interface {
	// Select returns the row, or ErrNoRows. A late read changes nothing, so it needs no gen condition
	// and no fence (7.2).
	Select(ctx context.Context, key schema.Key) (*schema.Row, error)
	// InsertDedup claims (r.Key, r.Gen) in the dedup table and inserts r with ver 0, in one statement
	// (8.3). Returns the row as stored. ErrGenClaimed: the gen is already claimed, nothing written.
	// ErrDuplicate: a row with the key exists; the dedup record is written. ErrRejected: a constraint
	// rejected the row; the whole statement is rolled back. (M6 adds ErrNotOwner.)
	InsertDedup(ctx context.Context, r *schema.Row) (*schema.Row, error)
	// GenClaimed reports whether (key, gen) is in the dedup table.
	GenClaimed(ctx context.Context, key schema.Key, gen int64) (bool, error)
	// Update sets the columns cols of the row with key r.Key and gen r.Gen to r.Vals[c], and
	// ver = ver + 1, in one statement. Returns the row as stored (RETURNING). ErrNoRows: no row matched.
	// ErrRejected: a constraint rejected it.
	Update(ctx context.Context, r *schema.Row, cols []int) (*schema.Row, error)
	// Delete deletes the row with key and gen. ErrNoRows: no row matched; the dedup record stays.
	// ErrRejected: a constraint rejected it.
	Delete(ctx context.Context, key schema.Key, gen int64) error
	// FenceSession terminates the session: nil means nothing sent on it executes any more.
	FenceSession(ctx context.Context, id SessionID) error
}
