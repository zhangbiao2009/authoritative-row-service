package store

import "sync/atomic"

// metrics are the Store's counters and gauges (design.md 11.2).
type metrics struct {
	poisoned atomic.Int64 // entries with poisoned set; changed with that entry's mu held
}
