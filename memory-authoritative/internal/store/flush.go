package store

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"rowsvc/internal/db"
	"rowsvc/internal/schema"
)

const (
	flushWorkers    = 8               // RunFlusher's goroutines, at most; to be tuned in the M7 load test
	flushBackoffMax = 5 * time.Second // the longest a shard whose writes fail is skipped

	poisonRetryInterval = time.Minute // a quarantined row with no update since is tried again after this
)

// RunFlusher flushes every shard once per FlushInterval until ctx is cancelled, then returns once its
// goroutines have. The caller starts it. Shard i belongs to worker i % W (design.md 7.6), so a shard is
// never flushed by two goroutines at once.
func (s *Store) RunFlusher(ctx context.Context) {
	w := min(flushWorkers, len(s.shards))
	var wg sync.WaitGroup
	for first := range w {
		wg.Go(func() { s.flushWorker(ctx, first, w) })
	}
	wg.Wait()
}

// flushWorker flushes shards first, first+w, ... once per FlushInterval until ctx is cancelled. A shard
// whose write failed is skipped for FlushInterval, doubled per consecutive failure, at most
// flushBackoffMax.
func (s *Store) flushWorker(ctx context.Context, first, w int) {
	type slot struct {
		sh    *shard
		delay time.Duration // 0 after a success
		next  time.Time     // skipped until then
	}
	var mine []slot
	for i := first; i < len(s.shards); i += w {
		mine = append(mine, slot{sh: s.shards[i]})
	}
	var refreshed time.Time
	tick := time.NewTicker(s.cfg.FlushInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		for i := range mine {
			x := &mine[i]
			if !time.Now().Before(x.next) {
				if s.flushShard(ctx, x.sh) {
					x.delay = min(max(2*x.delay, s.cfg.FlushInterval), flushBackoffMax)
					x.next = time.Now().Add(x.delay)
				} else {
					x.delay = 0
				}
			}
			// Between shards, not only per round: while the DB hangs, a round takes one DBTimeout per
			// shard, far longer than MaxDirtyAge. At most once per FlushInterval.
			if time.Since(refreshed) >= s.cfg.FlushInterval {
				s.refreshTooOld()
				refreshed = time.Now()
			}
		}
	}
}

// flushItem is one entry being written: the snapshot taken, its dirty columns, and since when they were
// dirty.
type flushItem struct {
	e     *entry
	row   *schema.Row
	mask  uint64
	since int64 // dirtySince
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
	var items, solo []flushItem // solo: quarantined rows due for a retry, written alone (7.9)
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
		poisoned := e.poisoned
		if poisoned && r.Ver == e.poisonVer && time.Now().UnixNano() < e.poisonRetryAt {
			e.mu.Unlock() // quarantined, and not updated since the DB rejected it: its retry is not due
			e.unlockOp()
			retry = append(retry, e)
			continue
		}
		// I4: dirty is cleared only here, with op held until the write returns, and merged back if the
		// write fails. So dirty == 0 with op free means the DB holds this row.
		it := flushItem{e, r, mask, e.dirtySince}
		s.clearDirtyLocked(e)
		e.queued = false
		e.mu.Unlock()
		if poisoned {
			solo = append(solo, it)
			continue
		}
		if items = append(items, it); len(items) == s.cfg.FlushBatchMax {
			if failed = s.writeBatch(ctx, items); failed {
				retry = append(retry, queue[i+1:]...) // still marked queued
				break
			}
			items = items[:0] // writeBatch keeps no reference
		}
	}
	if !failed && len(items) > 0 {
		failed = s.writeBatch(ctx, items)
	}
	if failed {
		s.giveBack(solo)
	} else {
		failed = s.writeAlone(ctx, solo)
	}
	s.requeueAll(sh, retry)
	s.recordOldestDirty(sh)
	return failed
}

// writeBatch sends items as one UpdateBatch and releases their op. If the DB definitely rejected the
// batch, it writes the rows one by one to find the poison rows (7.9). It reports whether a write failed
// otherwise; every item not written is then merged back.
func (s *Store) writeBatch(ctx context.Context, items []flushItem) (failed bool) {
	slices.SortFunc(items, func(a, b flushItem) int { return cmp.Compare(a.e.id, b.e.id) }) // 8.3
	ws := make([]db.Write, len(items))
	for i, it := range items {
		ws[i] = db.Write{Row: it.row, Mask: it.mask}
	}
	// TLA: F_send. One transaction of conditional writes of absolute values (I6, I12).
	start := time.Now()
	wctx, cancel := context.WithTimeout(ctx, s.cfg.DBTimeout)
	err := s.db.UpdateBatch(wctx, ws)
	cancel()
	s.m.flushBatches.Add(1)
	s.m.flushRows.Add(int64(len(items)))
	s.m.flushTime.Add(int64(time.Since(start)))
	if err != nil {
		s.m.flushFailures.Add(1)
	}
	// TLA: F_done
	switch {
	case err == nil:
		for _, it := range items {
			it.e.unlockOp()
		}
		return false
	case rejected(err):
		return s.writeAlone(ctx, items) // the transaction was rolled back: nothing of it was written
	default:
		s.giveBack(items)
		return true
	}
}

// writeAlone writes each item in a transaction of its own and releases its op. A row the DB definitely
// rejects is quarantined (7.9). After any other failure the rest is merged back untried; it reports
// whether that happened.
func (s *Store) writeAlone(ctx context.Context, items []flushItem) (failed bool) {
	for i, it := range items {
		e := it.e
		// TLA: F_send
		wctx, cancel := context.WithTimeout(ctx, s.cfg.DBTimeout)
		err := s.db.Update(wctx, it.row, it.mask)
		cancel()
		// TLA: F_done
		switch {
		case err == nil:
			e.mu.Lock()
			s.unpoisonLocked(e)
			e.mu.Unlock()
		case rejected(err):
			s.mergeBack(it)
			e.poisonVer, e.poisonRetryAt = it.row.Ver, time.Now().Add(poisonRetryInterval).UnixNano() // op held
			e.mu.Lock()
			s.poisonLocked(e)
			e.mu.Unlock()
			slog.Warn("flush rejected by the DB; row quarantined", "table", s.schema.Table, "key", e.id, "err", err)
		default:
			s.giveBack(items[i:])
			return true
		}
		e.unlockOp()
	}
	return false
}

// rejected reports whether the DB definitely rejected a write because of the row's data or another row
// (8.4): the same write fails again, at least until something changes.
func rejected(err error) bool { return errors.Is(err, db.ErrRejected) || db.IsCrossRow(err) }

// giveBack merges back items that were not written and releases their op.
func (s *Store) giveBack(items []flushItem) {
	for _, it := range items {
		s.mergeBack(it)
		it.e.unlockOp()
	}
}

// mergeBack puts the columns of a failed write back, so that the next round writes them with the newest
// values. The write may still land later; that is safe, because it is conditional.
func (s *Store) mergeBack(it flushItem) {
	e := it.e
	e.mu.Lock()
	s.markDirtyLocked(e, it.mask)
	if it.since < e.dirtySince {
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

// recordOldestDirty stores the earliest dirtySince of the shard's dirty rows that are not quarantined in
// sh.oldestDirty (11.1): a poison row waits for a human, and must not stop every update. It
// runs at the end of flushShard: every dirty entry is then in the queue, since only flushShard takes
// entries out and it has put back what it did not write.
func (s *Store) recordOldestDirty(sh *shard) {
	sh.qmu.Lock()
	queue := slices.Clone(sh.queue)
	sh.qmu.Unlock()
	var oldest int64
	for _, e := range queue {
		e.mu.Lock()
		if e.dirty != 0 && !e.poisoned && (oldest == 0 || e.dirtySince < oldest) {
			oldest = e.dirtySince
		}
		e.mu.Unlock()
	}
	sh.oldestDirty.Store(oldest)
}

// refreshTooOld publishes whether the oldest dirty row that is not quarantined is older than MaxDirtyAge
// (11.1). The flusher calls it between shards, at most once per FlushInterval, so the limit is
// approximate: what a crash can lose is bounded by about MaxDirtyAge + max(FlushInterval, DBTimeout).
func (s *Store) refreshTooOld() {
	o := s.oldestDirty()
	if v := s.cfg.MaxDirtyAge > 0 && !o.IsZero() && time.Since(o) > s.cfg.MaxDirtyAge; v != s.tooOld.Load() {
		s.tooOld.Store(v) // only on a change: every Update reads it
	}
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
