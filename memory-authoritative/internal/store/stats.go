package store

import (
	"sync/atomic"
	"time"
)

// Stats is a snapshot of the Store's metrics (design.md 11.2). Counters only grow; a time is a total, so
// that an exporter can compute rates and means (FlushTime / FlushBatches is the mean batch latency).
type Stats struct {
	Entries, Absent, Dirty, Poisoned int64         // gauges; Absent: tombstones and negative-cache entries alike
	OldestDirtyAge                   time.Duration // of the rows not quarantined, as their shards' last flush found them; 0 if none

	BusyCount, BusyAge                     int64 // updates rejected by MaxDirty, by MaxDirtyAge
	FlushBatches, FlushRows, FlushFailures int64 // UpdateBatch statements, rows in them, batches that failed
	FlushTime                              time.Duration

	Hits, Misses, LoadErrors int64 // Gets answered from memory, Gets that loaded; failed loads
	OpWaits                  int64 // op acquisitions that had to wait
	OpWaitTime               time.Duration

	Ambiguous, OutcomeUnknown, GenClaimed int64 // create/delete attempts with an ambiguous outcome; ErrOutcomeUnknown answers; INSERTs that found their gen
	Internal                              int64 // ErrInternal answers: must stay 0, alert otherwise
	Fences, FencesUnconfirmed, Evictions  int64
}

// metrics are the Store's counters and gauges behind Stats. Get's hits and misses are kept per shard.
type metrics struct {
	poisoned  atomic.Int64 // entries with poisoned set; changed with that entry's mu held
	busyCount atomic.Int64
	busyAge   atomic.Int64

	flushBatches, flushRows, flushFailures, flushTime atomic.Int64 // flushTime in nanoseconds

	loadErrors, opWaits, opWaitTime atomic.Int64 // opWaitTime in nanoseconds

	ambiguous, outcomeUnknown, genClaimed, internal atomic.Int64
	fences, fencesUnconfirmed, evictions            atomic.Int64
}

// Stats returns the current metrics. It visits every entry to count Absent, so call it at scrape
// intervals, not per request.
func (s *Store) Stats() Stats {
	m := &s.m
	st := Stats{
		Entries:  s.entryCount.Load(),
		Dirty:    s.dirtyCount.Load(),
		Poisoned: m.poisoned.Load(),

		BusyCount:     m.busyCount.Load(),
		BusyAge:       m.busyAge.Load(),
		FlushBatches:  m.flushBatches.Load(),
		FlushRows:     m.flushRows.Load(),
		FlushFailures: m.flushFailures.Load(),
		FlushTime:     time.Duration(m.flushTime.Load()),

		LoadErrors: m.loadErrors.Load(),
		OpWaits:    m.opWaits.Load(),
		OpWaitTime: time.Duration(m.opWaitTime.Load()),

		Ambiguous:         m.ambiguous.Load(),
		OutcomeUnknown:    m.outcomeUnknown.Load(),
		GenClaimed:        m.genClaimed.Load(),
		Internal:          m.internal.Load(),
		Fences:            m.fences.Load(),
		FencesUnconfirmed: m.fencesUnconfirmed.Load(),
		Evictions:         m.evictions.Load(),
	}
	if o := s.oldestDirty(); !o.IsZero() {
		st.OldestDirtyAge = time.Since(o)
	}
	for _, sh := range s.shards {
		st.Hits += sh.hits.Load()
		st.Misses += sh.misses.Load()
		sh.mu.RLock()
		for _, e := range sh.m {
			if e.snap.Load() == absent {
				st.Absent++
			}
		}
		sh.mu.RUnlock()
	}
	return st
}
