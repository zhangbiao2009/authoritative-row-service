package store

import (
	"context"

	"rowsvc/internal/db"
	"rowsvc/internal/schema"
)

// Delete deletes the row once the DB has definitely done so (design.md 7.5). gen 0 deletes whatever life
// is there; otherwise only that life, and ErrNotFound if the row has another gen. ErrOutcomeUnknown: ctx
// expired first; the delete goes on and settles in the background.
func (s *Store) Delete(ctx context.Context, key schema.Key, gen int64) error {
	// TLA: X_lock. Waits for a flush write of this key in progress (I5).
	e, err := s.lockOpLive(ctx, key)
	if err != nil {
		return err // TLA: X_qtimeout. Gave up while queued: definitely not executed
	}
	_, err = runSync(ctx, s, e, func() (struct{}, error) { return struct{}{}, s.deleteLocked(e, gen) })
	return err
}

// deleteLocked runs with op held.
func (s *Store) deleteLocked(e *entry, gen int64) error {
	// TLA: B_d1
	ctx, cancel := s.dbCtx()
	r, err := s.ensureLoadedLocked(ctx, e)
	cancel()
	if err != nil {
		return err
	}
	if r == absent || (gen != 0 && r.Gen != gen) {
		return ErrNotFound
	}
	// The DB first. mu is not held, so Updates keep changing memory meanwhile.
	if err := s.deleteUntilSure(e.id, r.Gen); err != nil {
		return err // definitely not deleted: the row stays, and its dirty columns are flushed as usual
	}
	// I7. In the same mu critical section: publish the tombstone and drop the dirty columns. An Update
	// before this point is discarded with the row; one after it sees absent.
	e.mu.Lock()
	e.snap.Store(absent) // linearization point
	s.clearDirtyLocked(e)
	e.mu.Unlock()
	s.touch(e) // the tombstone's NegativeIdleTTL counts from the delete
	return nil
}

// deleteUntilSure sends DELETE WHERE key AND gen until the outcome is definite (7.5), by the rules of
// insertUntilSure. Zero rows affected is success: DELETE is idempotent.
func (s *Store) deleteUntilSure(key schema.Key, gen int64) error {
	unsure := false             // an attempt was ambiguous and may have executed (13.2 hole 4). Never reset.
	var inflight []db.SessionID // sessions of ambiguous attempts that may still execute (hole 5). Emptied by a fence.
	for attempt := 0; ; attempt++ {
		// TLA: B_d2
		ctx, cancel := s.dbCtx()
		err := s.db.Delete(ctx, key, gen)
		cancel()
		switch {
		case err == nil:
			return nil
		case db.IsAmbiguous(err):
			unsure = true
			inflight = append(inflight, db.SessionOf(err))
		case db.NotExecuted(err) && unsure:
			// This attempt did not execute: the earlier one may have deleted the row (hole 4).
		case db.IsCrossRow(err) && len(inflight) > 0:
			// TLA: FenceAndSend. A child row references this one; once it is gone, an earlier DELETE still
			// in flight would succeed (hole 5). Terminate those sessions first, then try again.
			if s.fence(inflight) != nil {
				break // not confirmed: back off and come round again; inflight is kept
			}
			inflight = nil // unsure stays
			continue
		default:
			// Definite failure (the rules of insertUntilSure). Rejected because of another row with nothing
			// in flight: had an earlier attempt deleted the row, this one would have affected 0 rows.
			return err
		}
		backoff(attempt)
	}
}
