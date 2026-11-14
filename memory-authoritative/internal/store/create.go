package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"

	"rowsvc/internal/db"
	"rowsvc/internal/schema"
)

// Create inserts in, a row of this table, once the DB has definitely done so, and returns the stored row
// (design.md 7.4). in.Gen 0 means "pick one"; a caller that may retry passes its own gen and retries with
// it. (nil, nil): the create of this gen took effect earlier, and the row has been deleted since.
// ErrOutcomeUnknown: ctx expired first; the create goes on and settles in the background.
func (s *Store) Create(ctx context.Context, in *schema.Row) (*schema.Row, error) {
	// The create may go on after the caller has its answer (ErrOutcomeUnknown), and the caller may then reuse
	// its row. The values themselves are immutable (I11).
	in = &schema.Row{Key: in.Key, Gen: in.Gen, Vals: slices.Clone(in.Vals)} // Ver 0: a new life
	if err := s.checkRow(in); err != nil {
		return nil, err
	}
	// TLA: X_lock
	e, err := s.lockOpLive(ctx, in.Key)
	if err != nil {
		return nil, err // TLA: X_qtimeout. Gave up while queued: definitely not executed
	}
	return runSync(ctx, s, e, func() (*schema.Row, error) { return s.createLocked(e, in) })
}

// checkRow checks that in has a value per column and that its Key is the encoding of its primary key
// columns (I13). The types of the other values are not checked here (M7).
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

// runSync runs fn in a goroutine that holds e's op, which the caller has taken, and releases op only once
// fn has returned, that is once the outcome is definite. If ctx expires first, the caller gets
// ErrOutcomeUnknown and fn goes on.
func runSync[T any](ctx context.Context, s *Store, e *entry, fn func() (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	s.inflight.Add(1)
	go func() {
		defer s.inflight.Done()
		var r result
		// TLA: FinishOp. op is released on every path, and before the answer, so a caller that has its
		// answer finds op free.
		defer func() {
			e.unlockOp()
			done <- r
		}()
		r.v, r.err = fn()
	}()
	// TLA: X_wait
	select {
	case r := <-done:
		return r.v, r.err
	case <-ctx.Done():
		var zero T
		return zero, ErrOutcomeUnknown
	}
}

// createLocked runs with op held.
func (s *Store) createLocked(e *entry, in *schema.Row) (*schema.Row, error) {
	// TLA: B_c1. Load first and decide from memory, which is authoritative. INSERT is sent only when the
	// row definitely does not exist: then it is absent from the DB too (I4), so the dedup record and the row
	// are written together (13.2 holes 1, 2).
	ctx, cancel := s.dbCtx()
	cur, err := s.ensureLoadedLocked(ctx, e)
	cancel()
	if err != nil {
		return nil, err
	}
	if cur != absent {
		if in.Gen != 0 {
			if cur.Gen == in.Gen {
				return cur, nil // a retry of a create that succeeded
			}
			// Another life is here. If in.Gen is in the dedup table, this is a retry of a create that took
			// effect; that life has been deleted since and the key created again (13.2 hole 6). Every earlier
			// attempt of in.Gen has settled (op is held), so the answer cannot be overtaken by a late INSERT.
			ctx, cancel := s.dbCtx()
			claimed, err := s.db.GenClaimed(ctx, e.id, in.Gen)
			cancel()
			if err != nil {
				return nil, err
			}
			if claimed {
				return nil, nil
			}
		}
		return nil, ErrAlreadyExists
	}
	if in.Gen == 0 { // in is Create's own copy
		in.Gen = newGen()
	}
	got, err := s.insertUntilSure(in)
	if err != nil || got == nil {
		// Definitely failed, or inserted long ago and deleted since: memory keeps absent (13.2 hole 2).
		return nil, err
	}
	// I7: memory changes only now that the DB has definitely succeeded, and before the answer.
	e.mu.Lock()
	e.snap.Store(got) // linearization point
	s.clearDirtyLocked(e)
	e.mu.Unlock()
	return got, nil
}

// insertUntilSure sends r with the same gen until the outcome is definite (7.4). It returns the stored row;
// (nil, nil) means this gen was inserted earlier and deleted since.
func (s *Store) insertUntilSure(r *schema.Row) (*schema.Row, error) {
	unsure := false             // an attempt was ambiguous and may have executed (13.2 hole 4). Never reset.
	var inflight []db.SessionID // sessions of ambiguous attempts that may still execute (hole 5). Emptied by a fence.
	for attempt := 0; ; attempt++ {
		// TLA: B_c2
		ctx, cancel := s.dbCtx()
		got, err := s.db.InsertDedup(ctx, r)
		cancel()
		switch {
		case err == nil:
			return got, nil
		case errors.Is(err, db.ErrGenClaimed):
			// This gen is in the dedup table: an earlier attempt took effect, or this is a client retry.
			// Either way the create of this gen has happened; never answer "failed" from here on.
			unsure = true
			ctx, cancel := s.dbCtx()
			cur, serr := s.db.Select(ctx, r.Key)
			cancel()
			switch {
			case serr == nil && cur.Gen == r.Gen:
				return cur, nil
			case errors.Is(serr, db.ErrNoRows):
				return nil, nil // inserted, then deleted
			case serr == nil:
				// Memory said absent, the DB holds another life: loading first rules this out.
				return nil, fmt.Errorf("%w: key %q: gen %d claimed, row has gen %d", ErrInternal, r.Key, r.Gen, cur.Gen)
			}
			err = serr // the Select failed: the rules below decide
		case errors.Is(err, db.ErrDuplicate):
			// The dedup record was new but the row exists: loading first rules this out.
			return nil, fmt.Errorf("%w: key %q: gen %d claimed but a row exists", ErrInternal, r.Key, r.Gen)
		}
		switch {
		case db.IsAmbiguous(err):
			unsure = true
			inflight = append(inflight, db.SessionOf(err))
		case db.NotExecuted(err) && unsure:
			// This attempt did not execute: it says nothing about the earlier ambiguous one (hole 4).
		case db.IsCrossRow(err) && len(inflight) > 0:
			// TLA: FenceAndSend. Rejected because of another row, which can change; then an earlier attempt
			// still in flight would succeed (hole 5). Terminate those sessions first, then try again.
			if s.fence(inflight) != nil {
				break // not confirmed: back off and come round again; inflight is kept
			}
			inflight = nil // unsure stays: an earlier attempt may have landed before the fence
			continue
		default:
			// Definite failure: no earlier ambiguous attempt; or rejected by this row's own data (the
			// earlier attempts had the same parameters); or rejected because of another row with nothing of
			// this create in flight (had an earlier attempt succeeded, this one would have found its gen).
			return nil, err
		}
		backoff(attempt)
	}
}

// fence terminates the sessions and confirms they are gone (7.4, 8.3).
func (s *Store) fence(ids []db.SessionID) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.OpFenceTimeout)
	defer cancel()
	return s.db.FenceSessions(ctx, ids)
}

// newGen returns a gen for a new life: non-zero, 63 bits. It only needs to differ from the other lives of
// the same key (8.5).
func newGen() int64 { return rand.Int64N(math.MaxInt64) + 1 }
