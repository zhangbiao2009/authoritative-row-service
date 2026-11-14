package store

import (
	"context"
	"errors"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/schema"
)

// Get returns the row, or ErrNotFound. The row is shared and immutable (I2): callers only read it.
func (s *Store) Get(ctx context.Context, key schema.Key) (*schema.Row, error) {
	for {
		e := s.getOrCreate(key)
		// TLA: G_read. Linearization point: the load of snap.
		if r := e.snap.Load(); r != nil {
			if r == absent {
				return nil, ErrNotFound
			}
			return r, nil
		}
		if err := s.load(ctx, e); err != nil {
			return nil, err
		}
	}
}

// load fills e.snap from the DB. Concurrent misses queue on op, so only the first one reads the DB.
func (s *Store) load(ctx context.Context, e *entry) error {
	// TLA: L_lock
	if err := e.lockOp(ctx); err != nil {
		return err // gave up while queued
	}
	defer e.unlockOp()
	// TLA: L_load
	_, err := s.ensureLoadedLocked(ctx, e)
	return err
}

// ensureLoadedLocked returns e's row, loading it from the DB first if needed. The caller holds op.
func (s *Store) ensureLoadedLocked(ctx context.Context, e *entry) (*schema.Row, error) {
	if r := e.snap.Load(); r != nil {
		return r, nil // loaded by whoever held op before us
	}
	// After a failure, return the same error for LoadErrTTL, so that queued callers do not each wait
	// for a DB timeout of their own.
	if e.lastLoadErr != nil && time.Since(e.lastLoadErrAt) < s.cfg.LoadErrTTL {
		return nil, e.lastLoadErr
	}
	r, err := s.db.Select(ctx, e.id)
	switch {
	case errors.Is(err, db.ErrNoRows):
		r = absent
	case err != nil:
		if ctx.Err() == nil { // the caller's own cancellation says nothing about the DB
			e.lastLoadErr, e.lastLoadErrAt = err, time.Now()
		}
		return nil, err
	}
	e.lastLoadErr = nil
	e.mu.Lock()
	e.snap.Store(r) // I3
	e.mu.Unlock()
	return r, nil
}
