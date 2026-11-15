package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"

	"rowcache/internal/db"
	"rowcache/internal/schema"
)

// Create returns the row as stored (with DB defaults). in.Gen 0 means "pick one"; a caller that may retry
// passes its own gen. (nil, nil): this gen was created earlier and that life has been deleted since
// (design.md 7.5, hole 2).
func (s *Store) Create(ctx context.Context, in *schema.Row) (*schema.Row, error) {
	// The create may go on after the caller has its answer (ErrOutcomeUnknown), and the caller may then reuse
	// its row. The values themselves are immutable (I11).
	in = &schema.Row{Key: in.Key, Gen: in.Gen, Vals: slices.Clone(in.Vals)} // Ver 0: a new life
	if err := s.checkRow(in); err != nil {
		return nil, err
	}
	e, err := s.lockOpLive(ctx, in.Key)
	if err != nil {
		return nil, err // timed out while queued: not executed
	}
	return runSync(ctx, s, e, func(e *entry) (*schema.Row, error) { return s.createLocked(e, in) })
}

// createLocked requires op (I5).
func (s *Store) createLocked(e *entry, in *schema.Row) (*schema.Row, error) {
	// TLA: B_1. Load first: INSERT is sent only when the row definitely does not exist, so the dedup record
	// and the business row are always written together (I12).
	cur, err := s.ensureLoadedLocked(s.bgCtx, e)
	if err != nil {
		return nil, err
	}
	if cur != absent {
		if in.Gen != 0 {
			if cur.Gen == in.Gen {
				return cur, nil // a retry whose earlier attempt succeeded
			}
			// Another life. If the dedup table holds this gen, this is a retry of a create that took effect
			// and whose life was deleted since (hole 6): success with no row.
			claimed, err := s.db.GenClaimed(s.bgCtx, e.id, in.Gen)
			if err != nil {
				return nil, fmt.Errorf("%w: dedup lookup: %v", db.ErrNotExecuted, err) // a read: nothing done
			}
			if claimed {
				return nil, nil
			}
		}
		return nil, ErrAlreadyExists
	}
	fresh := in.Gen == 0 // a gen generated here has never been used before
	if fresh {
		in.Gen = newGen()
	}
	got, err := s.db.InsertDedup(s.bgCtx, in)
	// TLA: B_2
	switch {
	case err == nil:
		e.snap.Store(got) // linearization point; the DB's row (I7)
		return got, nil
	case errors.Is(err, db.ErrGenClaimed):
		return nil, nil // inserted earlier, deleted since; the tombstone stays
	case errors.Is(err, db.ErrDuplicate):
		return nil, s.unexpectedLocked(e, err)
	// M6 adds the case db.ErrNotOwner here.
	case db.IsAmbiguous(err):
		now, ferr := s.fenceAndReadLocked(e, err)
		if ferr != nil {
			return nil, ferr
		}
		// TLA: B_3
		if now != absent && now.Gen == in.Gen {
			return now, nil
		}
		// The row is absent, so this INSERT did not execute (it writes the row and the dedup record together,
		// and op is held). The gen can still be claimed by an earlier create with the same gen whose life was
		// deleted since (an SDK retry, hole 2); a gen generated here cannot. The lookup is retried until it
		// succeeds, like the read: the outcome is settled in the DB, so the answer must be definite (7.5).
		if !fresh {
			for attempt := 0; ; attempt++ {
				claimed, cerr := s.db.GenClaimed(s.bgCtx, e.id, in.Gen)
				if cerr == nil {
					if claimed {
						return nil, nil
					}
					break
				}
				s.backoff(attempt) // M6: give up with errAbandoned at shutdown, as fenceAndReadLocked does
			}
		}
		return nil, fmt.Errorf("%w: create not applied after fence", db.ErrNotExecuted)
	default:
		return nil, err // ErrNotExecuted or ErrRejected
	}
}

// checkRow checks that in has a value per column and that its Key is the encoding of its primary key
// columns (I11). The types of the other values are not checked here (M7).
func (s *Store) checkRow(in *schema.Row) error {
	if len(in.Vals) != len(s.schema.Cols) {
		return fmt.Errorf("%w: table %s: %d values for %d columns", ErrInvalid, s.schema.Table, len(in.Vals), len(s.schema.Cols))
	}
	pk := make([]any, len(s.schema.PK))
	for i, c := range s.schema.PK {
		pk[i] = in.Vals[c]
	}
	k, err := s.schema.EncodeKey(pk)
	if err != nil {
		return err
	}
	if k != in.Key {
		return fmt.Errorf("%w: table %s: the key does not match the primary key columns", ErrInvalid, s.schema.Table)
	}
	return nil
}

// newGen returns a gen for a new life: non-zero, 63 bits. It only needs to differ from the other lives of
// the same key.
func newGen() int64 { return rand.Int64N(math.MaxInt64) + 1 }
