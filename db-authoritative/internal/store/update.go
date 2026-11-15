package store

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"rowcache/internal/db"
	"rowcache/internal/schema"
)

type ColOp uint8

const (
	OpSet  ColOp = iota // assignment
	OpIncr              // new value = old value + delta
)

type ColChange struct {
	Col int // schema ordinal; not a primary key column (I11), not repeated within one Change
	Op  ColOp
	Val any // OpSet: the new value, never modified afterwards; OpIncr: the delta (int64 or float64)
}

type Change struct {
	Cols []ColChange
	Gen  int64 // optional: the gen the client expects; 0 = no check
}

// Update returns the row as the DB stored it (shared, immutable, I2). ErrNotFound if the row does not
// exist or is not the life ch.Gen names; ErrInvalid if ch is malformed or an increment fails, in which
// case nothing was sent.
func (s *Store) Update(ctx context.Context, key schema.Key, ch Change) (*schema.Row, error) {
	// The update may go on after the caller has its answer (ErrOutcomeUnknown), and the caller may then reuse
	// its slice. The values themselves are immutable (I2).
	ch.Cols = slices.Clone(ch.Cols)
	if err := s.checkChange(ch); err != nil {
		return nil, err
	}
	e, err := s.lockOpLive(ctx, key)
	if err != nil {
		return nil, err
	}
	return runSync(ctx, s, e, func(e *entry) (*schema.Row, error) { return s.updateLocked(e, ch) })
}

// updateLocked requires op (I5).
func (s *Store) updateLocked(e *entry, ch Change) (*schema.Row, error) {
	// TLA: B_1
	old, err := s.ensureLoadedLocked(s.bgCtx, e)
	if err != nil {
		return nil, err
	}
	if old == absent || (ch.Gen != 0 && old.Gen != ch.Gen) {
		return nil, ErrNotFound
	}
	nr, cols, err := s.applyChange(old, ch)
	if err != nil {
		return nil, err // ErrInvalid: nothing was sent
	}
	got, err := s.db.Update(s.bgCtx, nr, cols) // WHERE pk AND gen = old.Gen; ver = ver + 1 (I6)
	// TLA: B_2
	switch {
	case err == nil:
		e.snap.Store(got) // linearization point; the DB's row, not nr (I7)
		return got, nil
	case errors.Is(err, db.ErrNoRows):
		return nil, s.unexpectedLocked(e, err)
	case db.IsAmbiguous(err):
		now, ferr := s.fenceAndReadLocked(e, err)
		if ferr != nil {
			return nil, ferr
		}
		// TLA: B_3. With op held the row is the loaded one, or the loaded one with this update applied (I5);
		// anything else is a broken assumption, not an answer (7.3).
		switch {
		case now != absent && now.Gen == old.Gen && now.Ver == old.Ver+1:
			return now, nil
		case now != absent && now.Gen == old.Gen && now.Ver == old.Ver:
			return nil, fmt.Errorf("%w: update not applied after fence", db.ErrNotExecuted)
		}
		return nil, s.unexpectedLocked(e, fmt.Errorf("after the fence: row %+v, loaded gen %d ver %d", now, old.Gen, old.Ver))
	default:
		return nil, err // ErrNotExecuted or ErrRejected: nothing changed
	}
}

// applyChange builds the row to write: SET columns take the given value; INCR columns take old + delta,
// computed by schema.Add from the snapshot, which is the DB row (I4). Pure computation. ErrInvalid if an old
// value is NULL or a result is out of range; then nothing is sent. cols lists the changed ordinals in
// ch.Cols order.
func (s *Store) applyChange(old *schema.Row, ch Change) (*schema.Row, []int, error) {
	nr := *old
	nr.Vals = slices.Clone(old.Vals) // the values themselves are immutable and can be shared
	cols := make([]int, 0, len(ch.Cols))
	for _, c := range ch.Cols {
		switch c.Op {
		case OpSet:
			nr.Vals[c.Col] = c.Val
		case OpIncr:
			v, err := s.schema.Add(c.Col, old.Vals[c.Col], c.Val)
			if err != nil {
				return nil, nil, err
			}
			nr.Vals[c.Col] = v
		}
		cols = append(cols, c.Col)
	}
	return &nr, cols, nil
}

// checkChange checks the shape of ch: it must not panic applyChange or send an UPDATE that only bumps ver.
// The types of SET values are not checked here (M7).
func (s *Store) checkChange(ch Change) error {
	if len(ch.Cols) == 0 {
		return fmt.Errorf("%w: table %s: a change with no columns", ErrInvalid, s.schema.Table)
	}
	for i, c := range ch.Cols {
		var bad string
		switch {
		case c.Col < 0 || c.Col >= len(s.schema.Cols):
			bad = "no such column"
		case slices.Contains(s.schema.PK, c.Col):
			bad = "a primary key column cannot be updated" // I11
		case c.Op != OpSet && c.Op != OpIncr:
			bad = fmt.Sprintf("unknown op %d", c.Op)
		case slices.ContainsFunc(ch.Cols[:i], func(p ColChange) bool { return p.Col == c.Col }):
			bad = "changed twice in one change"
		}
		if bad != "" {
			return fmt.Errorf("%w: table %s: column %d: %s", ErrInvalid, s.schema.Table, c.Col, bad)
		}
	}
	return nil
}

// unexpectedLocked handles a result that cannot happen while op is held and the entry is loaded: an UPDATE
// that matched no row. Requires op. M6 adds the owner check before this (superseded instance, I14).
func (s *Store) unexpectedLocked(e *entry, what error) error {
	e.snap.Store(nil) // the next access reloads
	return fmt.Errorf("%w: %v", ErrInternal, what)
}
