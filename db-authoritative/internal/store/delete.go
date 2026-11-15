package store

import (
	"context"
	"errors"
	"fmt"

	"rowcache/internal/db"
	"rowcache/internal/schema"
)

// Delete removes the row. gen is optional (0 = no check); non-zero: only that life is deleted, a mismatch is
// ErrNotFound. The dedup record stays (I12).
func (s *Store) Delete(ctx context.Context, key schema.Key, gen int64) error {
	e, err := s.lockOpLive(ctx, key)
	if err != nil {
		return err
	}
	_, err = runSync(ctx, s, e, func(e *entry) (struct{}, error) {
		return struct{}{}, s.deleteLocked(e, gen)
	})
	return err
}

// deleteLocked requires op (I5). Zero rows is not "already deleted": no earlier statement can be in flight,
// so it means a broken invariant (unexpectedLocked). The tombstone stays in the map (design.md 7.6).
func (s *Store) deleteLocked(e *entry, gen int64) error {
	// TLA: B_1
	r, err := s.ensureLoadedLocked(s.bgCtx, e)
	if err != nil {
		return err
	}
	if r == absent || (gen != 0 && r.Gen != gen) {
		return ErrNotFound
	}
	err = s.db.Delete(s.bgCtx, e.id, r.Gen)
	// TLA: B_2
	switch {
	case err == nil:
		e.snap.Store(absent) // linearization point (I7)
		return nil
	case errors.Is(err, db.ErrNoRows):
		return s.unexpectedLocked(e, err)
	case db.IsAmbiguous(err):
		now, ferr := s.fenceAndReadLocked(e, err)
		if ferr != nil {
			return ferr
		}
		// TLA: B_3. With op held the row is gone (deleted) or still the loaded one (I5); anything else is a
		// broken assumption, not an answer (7.3).
		switch {
		case now == absent:
			return nil
		case now.Gen == r.Gen && now.Ver == r.Ver:
			return fmt.Errorf("%w: delete not applied after fence", db.ErrNotExecuted)
		}
		return s.unexpectedLocked(e, fmt.Errorf("after the fence: row %+v, loaded gen %d ver %d", now, r.Gen, r.Ver))
	default:
		return err // ErrNotExecuted or ErrRejected
	}
}
