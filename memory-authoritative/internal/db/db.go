// Package db is the interface between the Store and the database.
//
// Errors (design.md 8.4). Every error a call returns, a read's included, is classified by the DB
// implementation, and the Store trusts the classification. (A read matters too: after ErrGenClaimed, the
// error of the confirming Select decides whether the create retries or answers.)
//   - *AmbiguousError: the statement may have executed, or may still execute.
//   - ErrNotExecuted: definitely not executed.
//   - ErrCrossRow: rejected because of another row; the same statement may succeed later.
//   - ErrRejected: rejected by the row's own data; the same parameters always get the same result.
//
// Turning an error the implementation is not sure about into an *AmbiguousError is the implementation's
// job (the PostgreSQL adapter, M7).
package db

import (
	"context"
	"errors"

	"rowsvc/internal/schema"
)

var (
	ErrNoRows      = errors.New("no rows")
	ErrNotExecuted = errors.New("not executed")
	ErrCrossRow    = errors.New("rejected because of another row")
	ErrRejected    = errors.New("rejected by the row's own data")
	ErrGenClaimed  = errors.New("gen already claimed")
	ErrDuplicate   = errors.New("duplicate key")
)

// SessionID identifies the DB session a statement was sent on. The Store only keeps it and passes it to
// FenceSessions.
type SessionID uint64

// AmbiguousError reports a statement that may have executed, or may still execute, on Session (8.1).
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

func NotExecuted(err error) bool { return errors.Is(err, ErrNotExecuted) }
func IsCrossRow(err error) bool  { return errors.Is(err, ErrCrossRow) }

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

	// InsertDedup inserts r (r.Ver == 0) and its dedup record (table, r.Key, r.Gen) in one statement: both
	// take effect or neither (I14; design.md 5.1, 8.3). It returns the stored row, with any defaults the DB
	// filled in. ErrGenClaimed: the dedup record exists, nothing was written. ErrDuplicate: the dedup record
	// was new and is now written, but a row with r.Key exists.
	InsertDedup(ctx context.Context, r *schema.Row) (*schema.Row, error)

	// GenClaimed reports whether (key, gen) is in the dedup table: whether a create of that gen ever took
	// effect (7.4, hole 6).
	GenClaimed(ctx context.Context, key schema.Key, gen int64) (bool, error)

	// Delete deletes the row with key and gen (I6). Zero rows affected is not an error. The dedup record
	// stays (I14).
	Delete(ctx context.Context, key schema.Key, gen int64) error

	// FenceSessions terminates the sessions and returns nil once none of them can execute anything any
	// more (7.4, 8.3). An error means "not confirmed": some of them may still execute.
	FenceSessions(ctx context.Context, ids []SessionID) error
}
