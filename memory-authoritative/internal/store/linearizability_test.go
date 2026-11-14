package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anishathalye/porcupine"

	"rowsvc/internal/db"
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
		ret = math.MaxInt64
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
	res, info := porcupine.CheckOperationsVerbose(model, ops, timeout)
	err := verdict(res)
	if err == nil {
		return
	}
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
// per key (correctness-properties.md C1, S1, S2). Each round runs on a fresh Store, in synctest; its history
// is checked after it, on real time, so the check's timeout works. Several short histories rather than one
// long one: the cost of the check grows much faster than the length of a history.
func TestLinearizable(t *testing.T) {
	const rounds, keys, workers, ops = 6, 16, 32, 50
	var unknown, getRow, getNotFound, updateGenNotFound, created, deletedOK atomic.Int64
	overlaps, total := 0, 0 // overlapping pairs: operations on one key whose intervals overlap, the cases with a choice of order
	for range rounds {
		h := linearizableRound(t, keys, workers, ops, &unknown, &getRow, &getNotFound, &updateGenNotFound, &created, &deletedOK)
		for _, part := range rowModel.Partition(h.ops) {
			for i, a := range part {
				for _, b := range part[i+1:] {
					if a.Call < b.Return && b.Call < a.Return {
						overlaps++
					}
				}
			}
		}
		total += len(h.ops)
		checkLinearizable(t, h.ops, 30*time.Second)
	}
	counters := map[string]int64{
		"unknown outcomes": unknown.Load(), "Gets of a row": getRow.Load(), "Gets of no row": getNotFound.Load(),
		"Updates with a gen answered not found": updateGenNotFound.Load(), "creates": created.Load(),
		"deletes": deletedOK.Load(), "overlapping pairs": int64(overlaps),
	}
	for name, n := range counters {
		if n == 0 {
			t.Errorf("non-vacuity: no %s", name)
		}
	}
	t.Logf("%d operations in %d rounds; %v", total, rounds, counters)
}

// linearizableRound runs one round of TestLinearizable and returns its history.
func linearizableRound(t *testing.T, keys, workers, ops int, unknown, getRow, getNotFound, updateGenNotFound, created, deletedOK *atomic.Int64) *history {
	h := &history{}
	synctest.Test(t, func(t *testing.T) {
		d := fakedb.New()
		s := newTestStore(t, d)
		ks := make([]schema.Key, keys)
		for i := range keys {
			ks[i] = key(t, s, int64(i+1))
			start := absentRow
			if i < keys/2 { // preloaded: gen 101+i, ver 3, "a", 10
				d.Put(testRow(ks[i], int64(i+1)))
				start = r(int64(i+101), 3, "a", 10)
			}
			h.ops = append(h.ops, porcupine.Operation{Input: opInput{kind: opInit, key: ks[i], init: start}, Call: -2, Output: opOutput{}, Return: -1})
		}
		stop := startFlusher(s)
		f := startFaults(d, ks)
		var gens atomic.Int64
		gens.Store(1000)

		// record keeps an answer that took effect or tells something, and leaves out the ones that
		// definitely had no effect and say nothing about the row (decision 3).
		record := func(client int, in opInput, call int64, row *schema.Row, err error) {
			switch {
			case db.IsAmbiguous(err):
				t.Errorf("%s: a DB ambiguity leaked to the caller: %v", opNames[in.kind], err)
				return
			case err == nil, errors.Is(err, ErrNotFound), errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrOutcomeUnknown):
			case db.NotExecuted(err), db.IsCrossRow(err), errors.Is(err, db.ErrRejected),
				errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				return
			default:
				t.Errorf("%s: unexpected error %v", opNames[in.kind], err)
				return
			}
			for _, sentinel := range []error{ErrNotFound, ErrAlreadyExists, ErrOutcomeUnknown} {
				if errors.Is(err, sentinel) {
					err = sentinel
				}
			}
			out := answer(row, err)
			out.deleted = in.kind == opCreate && row == nil && err == nil
			switch {
			case err == ErrOutcomeUnknown:
				unknown.Add(1)
			case in.kind == opGet && err == nil:
				getRow.Add(1)
			case in.kind == opGet:
				getNotFound.Add(1)
			case in.kind == opUpdate && err == ErrNotFound && in.gen != 0:
				updateGenNotFound.Add(1)
			case in.kind == opCreate && err == nil:
				created.Add(1)
			case in.kind == opDelete && err == nil:
				deletedOK.Add(1)
			}
			h.add(client, in, call, out)
		}
		ctxFor := func(shortPercent int) (context.Context, context.CancelFunc) {
			if rand.IntN(100) < shortPercent {
				return context.WithTimeout(context.Background(), 5*time.Millisecond)
			}
			return context.WithTimeout(context.Background(), 5*time.Second) // L1 EventuallyResponds
		}

		var wg sync.WaitGroup
		for w := range workers {
			wg.Go(func() {
				seen := make([]int64, keys) // the gen this worker last saw per key
				genOrZero := func(i int) int64 {
					if rand.IntN(2) == 0 {
						return 0
					}
					return seen[i]
				}
				for range ops {
					i := rand.IntN(keys)
					k := ks[i]
					var in opInput
					var row *schema.Row
					var err error
					var call int64
					switch n := rand.IntN(10); {
					case n < 3:
						in = opInput{kind: opGet, key: k}
						ctx, cancel := ctxFor(0)
						call = h.call()
						row, err = s.Get(ctx, k)
						cancel()
					case n < 6:
						c := incr(2, int64(rand.IntN(11)-5))
						if rand.IntN(2) == 0 {
							c = set(1, string(rune('a'+rand.IntN(5))))
						}
						in = opInput{kind: opUpdate, key: k, gen: genOrZero(i), cols: []ColChange{c}}
						ctx, cancel := ctxFor(0)
						call = h.call()
						row, err = s.Update(ctx, k, Change{Cols: in.cols, Gen: in.gen})
						cancel()
					case n < 8:
						in = opInput{kind: opCreate, key: k, gen: gens.Add(1), name: "c", score: int64(rand.IntN(10))}
						nr := newRow(k, int64(i+1), in.gen)
						nr.Vals[1], nr.Vals[2] = in.name, in.score
						ctx, cancel := ctxFor(20)
						call = h.call()
						row, err = s.Create(ctx, nr)
						cancel()
					default:
						in = opInput{kind: opDelete, key: k, gen: genOrZero(i)}
						ctx, cancel := ctxFor(20)
						call = h.call()
						err = s.Delete(ctx, k, in.gen)
						cancel()
					}
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
		drain(t, s)
		checkInvariants(t, s, d)
	})

	return h
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
