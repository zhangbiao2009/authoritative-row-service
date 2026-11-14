package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anishathalye/porcupine"

	"rowsvc/internal/db/fakedb"
	"rowsvc/internal/schema"
)

// history records operations for Porcupine. Times come from a logical clock, not time.Now: under synctest
// virtual time stands still while goroutines run, which would make nearly every operation concurrent. A
// tick before each call and after each return keeps real-time order exact (M4 plan, decision 1).
type history struct {
	clock atomic.Int64
	mu    sync.Mutex
	ops   []porcupine.Operation
}

func (h *history) call() int64 { return h.clock.Add(1) }

// add records an answered operation. An unknown outcome returns at the end of the history: it may take
// effect at any point after its call (design.md 12.1 item 4).
func (h *history) add(client int, in opInput, call int64, out opOutput) {
	ret := h.clock.Add(1)
	if out.err == ErrOutcomeUnknown {
		ret = never
	}
	h.mu.Lock()
	h.ops = append(h.ops, porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
	h.mu.Unlock()
}

// answer turns a Store answer into an opOutput, copying the row's fields when it returns.
func answer(r *schema.Row, err error) opOutput {
	out := opOutput{err: err}
	if r != nil {
		out.row = rowState{loaded: true, exists: true, gen: r.Gen, ver: r.Ver, name: r.Vals[1].(string), score: r.Vals[2].(int64)}
	}
	return out
}

// verdict is nil only for Ok: a check that timed out (Unknown) proved nothing.
func verdict(res porcupine.CheckResult) error {
	if res == porcupine.Ok {
		return nil
	}
	return fmt.Errorf("linearizability check: %s", res)
}

// checkLinearizable fails t unless the history is linearizable per key, and then writes Porcupine's
// visualization to a temp file.
func checkLinearizable(t *testing.T, ops []porcupine.Operation, timeout time.Duration) {
	t.Helper()
	model := rowModel.ToModel()
	err := verdict(porcupine.CheckOperationsTimeout(model, ops, timeout))
	if err == nil {
		return
	}
	_, info := porcupine.CheckOperationsVerbose(model, ops, timeout) // again, only for the visualization
	f, ferr := os.CreateTemp("", "rowsvc-linearizability-*.html")
	if ferr == nil {
		ferr = porcupine.Visualize(model, info, f)
		f.Close()
	}
	if ferr != nil {
		t.Errorf("%v (no visualization: %v)", err, ferr)
		return
	}
	t.Errorf("%v; visualization: %s", err, f.Name())
}

// Acceptance (design.md 12.2 M4): random concurrent operations under injected DB faults are linearizable
// per key (correctness-properties.md C1, S1, S2). Each round runs on a fresh Store, in synctest, as a subtest;
// its history is checked after it, whatever the subtest found, and on real time, so the check's timeout
// works.
func TestLinearizable(t *testing.T) {
	const rounds = 12
	counters := map[string]int64{}
	total := 0
	for i := range rounds {
		h := &history{}
		t.Run(fmt.Sprint("round ", i), func(t *testing.T) {
			evictions, reloads := linearizableRound(t, h)
			counters["evictions"] += evictions
			counters["reloads after eviction"] += reloads
		})
		witnesses(counters, h.ops)
		total += len(h.ops)
		checkLinearizable(t, h.ops, 30*time.Second)
	}
	t.Logf("%d operations in %d rounds", total, rounds)
	nonVacuous(t, counters)
}

// One round: many short histories rather than a few long ones, because the cost of the check grows much
// faster than the length of a history; two workers per key keep the operations of a key concurrent.
const linKeys, linWorkers, linOps = 16, 32, 25

// witnesses counts, in a history, the cases that show it reached the hard parts.
func witnesses(counters map[string]int64, ops []porcupine.Operation) {
	for _, part := range rowModel.Partition(ops) {
		for j, a := range part {
			in, out := a.Input.(opInput), a.Output.(opOutput)
			switch {
			case in.kind == opInit:
			case out.err == ErrOutcomeUnknown:
				counters["unknown outcomes"]++
			case in.kind == opGet && out.err == nil:
				counters["Gets of a row"]++
			case in.kind == opGet:
				counters["Gets of no row"]++
			case in.kind == opUpdate && out.err == ErrNotFound && in.gen != 0:
				counters["Updates with a gen answered not found (absent or another life)"]++
			case in.kind == opCreate && out.err == nil:
				counters["creates"]++
			case in.kind == opDelete && out.err == nil:
				counters["deletes"]++
			}
			for _, b := range part[j+1:] { // overlapping intervals: the cases with a choice of order
				if a.Call < b.Return && b.Call < a.Return {
					counters["overlapping pairs"]++
				}
			}
		}
	}
}

// linearizableRound runs one round of TestLinearizable, recording its history in h. Eviction interleaves with
// every operation, so loads read the DB, and the check covers "a clean row in memory equals the DB" (I4).
func linearizableRound(t *testing.T, h *history) (evictions, reloads int64) {
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d, evictHard)
		ks := make([]schema.Key, linKeys)
		for i := range linKeys {
			ks[i] = key(t, s, int64(i+1))
			start := absentRow
			if i < linKeys/2 {
				row := testRow(ks[i], int64(i+1))
				d.Put(row)
				start = answer(row, nil).row
			}
			h.ops = append(h.ops, initOp(ks[i], start))
		}
		stop := startFlusher(s)
		f := startFaults(d, ks)
		ev := watchEvictions(s, f)
		stopEvictor := runEvictor(s)
		defer func() { evictions, reloads = ev.Load(), f.reloads.Load() }()
		var gens atomic.Int64
		gens.Store(1000)

		// record keeps an answer that took effect or tells something, and leaves out the ones that
		// definitely had no effect and say nothing about the row (M4 plan, decision 3).
		record := func(client int, in opInput, call int64, row *schema.Row, err error) {
			switch classify(err) {
			case unexpected:
				t.Errorf("%s: unexpected error %v", opNames[in.kind], err)
				return
			case noEffect:
				return
			}
			for _, sentinel := range []error{ErrNotFound, ErrAlreadyExists, ErrOutcomeUnknown} {
				if errors.Is(err, sentinel) {
					err = sentinel
				}
			}
			out := answer(row, err)
			out.deleted = in.kind == opCreate && row == nil && err == nil
			h.add(client, in, call, out)
		}

		var wg sync.WaitGroup
		for w := range linWorkers {
			wg.Go(func() {
				seen := make([]int64, linKeys) // the gen this worker last saw per key
				genOrZero := func(i int) int64 {
					if rand.IntN(2) == 0 {
						return 0
					}
					return seen[i]
				}
				for range linOps {
					think()
					i := rand.IntN(linKeys)
					k := ks[i]
					var in opInput
					var do func(context.Context) (*schema.Row, error)
					short := 0 // percent of calls with a 5ms ctx
					switch n := rand.IntN(10); {
					case n < 3:
						in = opInput{kind: opGet, key: k}
						do = func(ctx context.Context) (*schema.Row, error) { return s.Get(ctx, k) }
					case n < 6:
						c := incr(2, int64(rand.IntN(11)-5))
						if rand.IntN(2) == 0 {
							c = set(1, string(rune('a'+rand.IntN(5))))
						}
						in = opInput{kind: opUpdate, key: k, gen: genOrZero(i), cols: []ColChange{c}}
						do = func(ctx context.Context) (*schema.Row, error) {
							return s.Update(ctx, k, Change{Cols: in.cols, Gen: in.gen})
						}
					case n < 8:
						in = opInput{kind: opCreate, key: k, gen: gens.Add(1), name: "c", score: int64(rand.IntN(10))}
						nr := newRow(k, int64(i+1), in.gen)
						nr.Vals[1], nr.Vals[2] = in.name, in.score
						short = 20
						do = func(ctx context.Context) (*schema.Row, error) { return s.Create(ctx, nr) }
					default:
						in = opInput{kind: opDelete, key: k, gen: genOrZero(i)}
						short = 20
						do = func(ctx context.Context) (*schema.Row, error) { return nil, s.Delete(ctx, k, in.gen) }
					}
					ctx, cancel := opCtx(short)
					call := h.call()
					row, err := do(ctx)
					cancel()
					record(w, in, call, row, err)
					if row != nil {
						seen[i] = row.Gen
					}
				}
			})
		}
		wg.Wait()
		f.stop()
		s.inflight.Wait() // L2: the unknown outcomes settle
		stop()
		stopEvictor()
		drain(t, s)
		checkInvariants(t, s, d)
	})
	return evictions, reloads
}

// Review Focus 4: only Ok passes.
func TestCheckFailsOnTimeout(t *testing.T) {
	if verdict(porcupine.Ok) != nil || verdict(porcupine.Illegal) == nil || verdict(porcupine.Unknown) == nil {
		t.Fatalf("verdict: Ok %v, Illegal %v, Unknown %v; want only Ok to pass",
			verdict(porcupine.Ok), verdict(porcupine.Illegal), verdict(porcupine.Unknown))
	}
}

// Review Focus 5: what the history says is fixed when the call returns.
func TestRecorderCopiesAnswers(t *testing.T) {
	d := fakedb.New()
	s := newTestStore(t, d)
	k := key(t, s, 1)
	row := create(t, s, newRow(k, 1, 100))
	out := answer(row, nil)
	update(t, s, k, set(1, "b"))
	row.Vals[1] = "changed in place" // not allowed (I2), done here only to show the recorder kept a copy
	if want := r(100, 0, "a", 10); out.row != want {
		t.Fatalf("recorded %v, want %v", out.row, want)
	}
}
