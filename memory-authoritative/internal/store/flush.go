package store

import (
	"context"
	"time"
)

// RunFlusher flushes every shard once per FlushInterval until ctx is cancelled, then returns. The caller
// starts it. One goroutine serves all shards; a worker pool and batches come in M6.
func (s *Store) RunFlusher(ctx context.Context) {
	tick := time.NewTicker(s.cfg.FlushInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for _, sh := range s.shards {
				s.flushShard(ctx, sh)
			}
		}
	}
}

// flushShard writes the shard's queued entries back to the DB, one entry per DB call (design.md 7.6). It
// holds an entry's op from taking the snapshot until the write has returned, so the next write of that key
// cannot start before this one has finished (I5).
func (s *Store) flushShard(ctx context.Context, sh *shard) {
	// TLA: F_take
	sh.qmu.Lock()
	batch := sh.queue
	sh.queue = nil
	sh.qmu.Unlock()

	var retry []*entry
	for _, e := range batch {
		// TLA: F_scan
		if !e.tryLockOp() {
			retry = append(retry, e) // a load holds op: next round. queued stays true
			continue
		}
		e.mu.Lock()
		r, mask := e.snap.Load(), e.dirty
		if r == nil || r == absent || mask == 0 {
			e.queued = false
			e.mu.Unlock()
			e.unlockOp()
			continue
		}
		s.clearDirtyLocked(e)
		e.queued = false
		e.mu.Unlock()

		// TLA: F_send. A conditional write of absolute values (I6, I12).
		wctx, cancel := context.WithTimeout(ctx, s.cfg.DBTimeout)
		err := s.db.Update(wctx, r, mask)
		cancel()

		// TLA: F_done
		if err != nil {
			// The write may still land later; that is safe, because it is conditional. Merge the columns
			// back, so that the next round writes them with the newest values.
			e.mu.Lock()
			s.markDirtyLocked(e, mask)
			need := !e.queued
			e.queued = true
			e.mu.Unlock()
			if need {
				s.enqueue(e)
			}
		}
		e.unlockOp()
	}
	s.requeueAll(sh, retry)
}

// requeueAll puts back entries whose op was busy. They are still marked queued.
func (s *Store) requeueAll(sh *shard, es []*entry) {
	if len(es) == 0 {
		return
	}
	sh.qmu.Lock()
	sh.queue = append(sh.queue, es...)
	sh.qmu.Unlock()
}
