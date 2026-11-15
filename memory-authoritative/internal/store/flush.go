package store

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/schema"
)

const (
	flushWorkers    = 8               // RunFlusher's goroutines, at most; to be tuned in the M7 load test
	flushBackoffMax = 5 * time.Second // the longest a shard whose writes fail is skipped
)

// RunFlusher flushes every shard once per FlushInterval until ctx is cancelled, then returns once its
// goroutines have. The caller starts it. Shard i belongs to worker i % W (design.md 7.6), so a shard is
// never flushed by two goroutines at once. A shard whose write failed is skipped for FlushInterval,
// doubled per consecutive failure, at most flushBackoffMax.
func (s *Store) RunFlusher(ctx context.Context) {
	w := min(flushWorkers, len(s.shards))
	var wg sync.WaitGroup
	for first := range w {
		wg.Go(func() {
			var mine []*shard
			for i := first; i < len(s.shards); i += w {
				mine = append(mine, s.shards[i])
			}
			fails, next := make([]int, len(mine)), make([]time.Time, len(mine))
			tick := time.NewTicker(s.cfg.FlushInterval)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					for i, sh := range mine {
						if time.Now().Before(next[i]) {
							continue
						}
						if !s.flushShard(ctx, sh) {
							fails[i] = 0
							continue
						}
						fails[i]++
						next[i] = time.Now().Add(min(s.cfg.FlushInterval<<min(fails[i]-1, 20), flushBackoffMax))
					}
				}
			}
		})
	}
	wg.Wait()
}

// flushItem is one entry being written: the snapshot taken, its dirty columns, and since when they were
// dirty.
type flushItem struct {
	e     *entry
	row   *schema.Row
	mask  uint64
	since time.Time
}

// flushShard writes the shard's queued entries back to the DB in batches of at most FlushBatchMax rows,
// each one transaction (design.md 7.6). It holds an entry's op from taking the snapshot until its batch has
// returned, so the next write of that key cannot start before this one has finished (I5). After a write
// that failed it stops: the rest of the queue waits for the next round. It reports whether a write failed.
func (s *Store) flushShard(ctx context.Context, sh *shard) (failed bool) {
	// TLA: F_take
	sh.qmu.Lock()
	queue := sh.queue
	sh.queue = nil
	sh.qmu.Unlock()

	var retry []*entry
	var items []flushItem
	for i, e := range queue {
		// TLA: F_scan
		if !e.tryLockOp() {
			retry = append(retry, e) // a load, create or delete holds op: next round. queued stays true
			continue
		}
		e.mu.Lock()
		r, mask := e.snap.Load(), e.dirty
		if e.dead || r == nil || r == absent || mask == 0 {
			e.queued = false
			e.mu.Unlock()
			e.unlockOp()
			continue
		}
		// I4: dirty is cleared only here, with op held until the write returns, and merged back if the
		// write fails. So dirty == 0 with op free means the DB holds this row.
		items = append(items, flushItem{e, r, mask, e.dirtySince})
		s.clearDirtyLocked(e)
		e.queued = false
		e.mu.Unlock()
		if len(items) == s.cfg.FlushBatchMax {
			if !s.writeBatch(ctx, items) {
				failed = true
				retry = append(retry, queue[i+1:]...) // still marked queued
				break
			}
			items = nil
		}
	}
	if !failed && len(items) > 0 {
		failed = !s.writeBatch(ctx, items)
	}
	s.requeueAll(sh, retry)
	s.recordOldestDirty(sh)
	return failed
}

// writeBatch sends items as one UpdateBatch, merges them back if it failed, and releases their op. It
// reports whether the write succeeded.
func (s *Store) writeBatch(ctx context.Context, items []flushItem) bool {
	slices.SortFunc(items, func(a, b flushItem) int { return cmp.Compare(a.e.id, b.e.id) }) // 8.3
	ws := make([]db.Write, len(items))
	for i, it := range items {
		ws[i] = db.Write{Row: it.row, Mask: it.mask}
	}
	// TLA: F_send. One transaction of conditional writes of absolute values (I6, I12).
	wctx, cancel := context.WithTimeout(ctx, s.cfg.DBTimeout)
	err := s.db.UpdateBatch(wctx, ws)
	cancel()
	// TLA: F_done
	for _, it := range items {
		if err != nil {
			s.mergeBack(it)
		}
		it.e.unlockOp()
	}
	return err == nil
}

// mergeBack puts the columns of a failed write back, so that the next round writes them with the newest
// values. The write may still land later; that is safe, because it is conditional.
func (s *Store) mergeBack(it flushItem) {
	e := it.e
	e.mu.Lock()
	s.markDirtyLocked(e, it.mask)
	if it.since.Before(e.dirtySince) {
		e.dirtySince = it.since // the earliest: the age of 11.1 must not restart with every failed round
	}
	need := !e.queued
	e.queued = true
	e.mu.Unlock()
	if need {
		s.enqueue(e)
	}
}

// requeueAll puts back entries the round did not write. They are still marked queued.
func (s *Store) requeueAll(sh *shard, es []*entry) {
	if len(es) == 0 {
		return
	}
	sh.qmu.Lock()
	sh.queue = append(sh.queue, es...)
	sh.qmu.Unlock()
}

// recordOldestDirty stores the earliest dirtySince of the shard's dirty rows in sh.oldestDirty (11.1). It
// runs at the end of flushShard: every dirty entry is then in the queue, since only flushShard takes
// entries out and it has put back what it did not write.
func (s *Store) recordOldestDirty(sh *shard) {
	sh.qmu.Lock()
	queue := slices.Clone(sh.queue)
	sh.qmu.Unlock()
	var oldest int64
	for _, e := range queue {
		e.mu.Lock()
		if e.dirty != 0 && (oldest == 0 || e.dirtySince.UnixNano() < oldest) {
			oldest = e.dirtySince.UnixNano()
		}
		e.mu.Unlock()
	}
	sh.oldestDirty.Store(oldest)
}

// oldestDirty returns the earliest dirtySince over all shards, as their flushes last recorded it; the zero
// Time if no row was dirty.
func (s *Store) oldestDirty() time.Time {
	var oldest int64
	for _, sh := range s.shards {
		if o := sh.oldestDirty.Load(); o != 0 && (oldest == 0 || o < oldest) {
			oldest = o
		}
	}
	if oldest == 0 {
		return time.Time{}
	}
	return time.Unix(0, oldest)
}
