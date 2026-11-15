package store

import (
	"context"
	"fmt"
	"slices"

	"rowsvc/internal/schema"
)

type ColOp uint8

const (
	OpSet  ColOp = iota
	OpIncr       // new value = old value + delta
)

type ColChange struct {
	Col int // column ordinal; not a primary key column (I13); at most once per Change
	Op  ColOp
	Val any // OpSet: the new value, never modified afterwards; OpIncr: the delta (int64 or float64)
}

// Change is one Update: the columns it sets or increments.
type Change struct {
	Cols []ColChange
	Gen  int64 // 0: no check; else ErrNotFound unless the row has this gen: an update for one life never changes another (7.3)
}

// Built once: they are answered exactly when the service is overloaded.
var (
	errBusyAge   = fmt.Errorf("%w: an unflushed update is older than MaxDirtyAge", ErrBusy)
	errBusyCount = fmt.Errorf("%w: MaxDirty rows are dirty", ErrBusy)
)

// Update applies ch to the row in memory and returns the new row, which is shared and immutable (I2). It
// does no DB IO: the flusher writes the row later. ErrNotFound if the row does not exist, or is not the
// life ch.Gen names; ErrInvalid if ch is malformed or an increment fails, in which case nothing changes.
func (s *Store) Update(ctx context.Context, key schema.Key, ch Change) (*schema.Row, error) {
	mask, err := s.changeMask(ch)
	if err != nil {
		return nil, err
	}
	if s.tooOld.Load() { // every update, those of dirty rows included: they would only lengthen what a crash loses
		s.m.busyAge.Add(1)
		return nil, errBusyAge
	}
	for {
		e := s.getOrCreate(key)
		s.touch(e)
		// TLA: U_mu. The whole read-modify-write is inside mu, so concurrent updates of other columns, and
		// concurrent increments, are not lost.
		e.mu.Lock()
		old := e.snap.Load()
		if e.dead || old == nil {
			// Being removed (7.7), or not loaded. mu is a leaf lock: release it before waiting for op (6.4).
			// The remover holds op until the entry is out of the map, so load returns only then, and for a
			// dead entry without reading; either way, look the key up again.
			e.mu.Unlock()
			if err := s.load(ctx, e); err != nil {
				return nil, err
			}
			continue
		}
		if old == absent || (ch.Gen != 0 && old.Gen != ch.Gen) {
			e.mu.Unlock()
			return nil, ErrNotFound
		}
		if e.dirty == 0 && s.dirtyCount.Load() >= int64(s.cfg.MaxDirty) { // a dirty row does not grow the backlog
			e.mu.Unlock()
			s.m.busyCount.Add(1)
			return nil, errBusyCount
		}
		nr := *old
		nr.Vals = slices.Clone(old.Vals) // the values themselves are immutable and can be shared (I11)
		for _, c := range ch.Cols {
			switch c.Op {
			case OpSet:
				nr.Vals[c.Col] = c.Val
			case OpIncr:
				// I12: the new value is computed from the old one inside mu, and is written to the DB as an
				// absolute value. Nothing is published yet, so on error the whole Change has no effect.
				v, err := s.schema.Add(c.Col, old.Vals[c.Col], c.Val)
				if err != nil {
					e.mu.Unlock()
					return nil, err
				}
				nr.Vals[c.Col] = v
			}
		}
		nr.Ver = old.Ver + 1
		e.snap.Store(&nr) // linearization point. Row → Row only, so mu alone is enough (I3)
		s.markDirtyLocked(e, mask)
		need := !e.queued
		e.queued = true
		e.mu.Unlock()
		// TLA: U_enq
		if need {
			s.enqueue(e)
		}
		return &nr, nil
	}
}

// changeMask checks the shape of ch and returns its dirty-column mask. The types of SET values are not
// checked here (M7).
func (s *Store) changeMask(ch Change) (uint64, error) {
	if len(ch.Cols) == 0 {
		return 0, fmt.Errorf("%w: table %s: a change with no columns", ErrInvalid, s.schema.Table)
	}
	var mask uint64
	for i, c := range ch.Cols {
		var bad string
		switch {
		case c.Col < 0 || c.Col >= len(s.schema.Cols):
			bad = "no such column"
		case slices.Contains(s.schema.PK, c.Col):
			bad = "a primary key column cannot be updated" // I13
		case c.Op != OpSet && c.Op != OpIncr:
			bad = fmt.Sprintf("unknown op %d", c.Op)
		case slices.ContainsFunc(ch.Cols[:i], func(p ColChange) bool { return p.Col == c.Col }):
			bad = "changed twice in one change"
		}
		if bad != "" {
			return 0, fmt.Errorf("%w: table %s: column %d: %s", ErrInvalid, s.schema.Table, c.Col, bad)
		}
		mask |= schema.ColBit(c.Col)
	}
	return mask, nil
}
