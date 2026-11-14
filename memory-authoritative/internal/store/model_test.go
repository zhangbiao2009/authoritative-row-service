package store

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"rowsvc/internal/schema"
)

// The sequential specification Porcupine checks histories against (design.md 12.1 item 4): one row, the
// state a single-threaded service would have (correctness-properties.md 4.1, abs). Histories are checked
// per key.

// rowState is one key. Columns of the test schema: 0 id (primary key), 1 name, 2 score.
type rowState struct {
	loaded   bool // false only before the key's init operation
	exists   bool
	gen, ver int64
	name     string
	score    int64
}

type opKind uint8

const (
	opInit opKind = iota // sets the key's starting row; one per key, before everything else
	opGet
	opUpdate
	opCreate
	opDelete
)

type opInput struct {
	kind  opKind
	key   schema.Key
	init  rowState    // opInit
	cols  []ColChange // opUpdate: OpSet of column 1 (string), OpIncr of column 2 (int64)
	gen   int64       // opUpdate: Change.Gen; opCreate: the new life's gen; opDelete: the gen argument
	name  string      // opCreate
	score int64       // opCreate
}

type opOutput struct {
	err     error    // nil, ErrNotFound, ErrAlreadyExists or ErrOutcomeUnknown (the recorder keeps only these)
	row     rowState // the row answered, when err == nil and the operation answers one
	deleted bool     // Create answered (nil, nil); with one Create call per gen it is never right (M4 plan, decision 4)
}

// stepRow returns the states the row can be in after in answered out, starting from s; none if out is not
// a possible answer. ErrOutcomeUnknown gives both "took effect" and "did not".
func stepRow(s rowState, in opInput, out opOutput) []rowState {
	if in.kind == opInit {
		if s.loaded {
			return nil
		}
		return []rowState{in.init}
	}
	if !s.loaded {
		return nil
	}
	switch in.kind {
	case opGet:
		if s.exists && out.err == nil && out.row == s || !s.exists && out.err == ErrNotFound {
			return []rowState{s}
		}
	case opUpdate:
		if !s.exists || in.gen != 0 && in.gen != s.gen {
			if out.err == ErrNotFound {
				return []rowState{s}
			}
			return nil
		}
		n := s
		for _, c := range in.cols {
			switch {
			case c.Col == 1 && c.Op == OpSet:
				n.name = c.Val.(string)
			case c.Col == 2 && c.Op == OpIncr:
				n.score += c.Val.(int64)
			default:
				panic(fmt.Sprintf("model: unsupported column change %+v", c))
			}
		}
		n.ver++
		if out.err == nil && out.row == n {
			return []rowState{n}
		}
	case opCreate:
		born := rowState{loaded: true, exists: true, gen: in.gen, name: in.name, score: in.score} // ver 0
		switch {
		case out.err == nil && !out.deleted && !s.exists && out.row == born:
			return []rowState{born}
		case out.err == ErrAlreadyExists && s.exists:
			return []rowState{s}
		case out.err == ErrOutcomeUnknown && !s.exists:
			return []rowState{s, born}
		case out.err == ErrOutcomeUnknown:
			return []rowState{s} // an existing row: the create would answer "already exists"
		}
	case opDelete:
		match := s.exists && (in.gen == 0 || in.gen == s.gen)
		switch {
		case out.err == nil && match:
			return []rowState{absentRow}
		case out.err == ErrNotFound && !match:
			return []rowState{s}
		case out.err == ErrOutcomeUnknown && match:
			return []rowState{s, absentRow}
		case out.err == ErrOutcomeUnknown:
			return []rowState{s}
		}
	}
	return nil
}

func (s rowState) String() string {
	switch {
	case !s.loaded:
		return "?"
	case !s.exists:
		return "absent"
	}
	return fmt.Sprintf("gen %d ver %d %q %d", s.gen, s.ver, s.name, s.score)
}

var opNames = [...]string{opInit: "init", opGet: "Get", opUpdate: "Update", opCreate: "Create", opDelete: "Delete"}

var rowModel = porcupine.NondeterministicModel{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		var keys []schema.Key
		byKey := map[schema.Key][]porcupine.Operation{}
		for _, o := range h {
			k := o.Input.(opInput).key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], o)
		}
		parts := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, byKey[k])
		}
		return parts
	},
	Init: func() []any { return []any{rowState{}} },
	Step: func(state, input, output any) []any {
		var next []any
		for _, n := range stepRow(state.(rowState), input.(opInput), output.(opOutput)) {
			next = append(next, n)
		}
		return next
	},
	Equal: func(a, b any) bool { return a.(rowState) == b.(rowState) },
	DescribeOperation: func(input, output any) string {
		in, out := input.(opInput), output.(opOutput)
		desc := out.row.String()
		switch {
		case in.kind == opInit:
			desc = in.init.String()
		case out.err != nil:
			desc = out.err.Error()
		case out.deleted:
			desc = "created earlier, deleted since"
		case in.kind == opDelete:
			desc = "ok"
		}
		return fmt.Sprintf("%s(gen %d %v) -> %s", opNames[in.kind], in.gen, in.cols, desc)
	},
	DescribeState: func(state any) string { return state.(rowState).String() },
}

// Unit tests of the sequential model: hand-written histories with known answers.

func op(call, ret int64, in opInput, out opOutput) porcupine.Operation {
	return porcupine.Operation{Input: in, Call: call, Output: out, Return: ret}
}

func check(ops ...porcupine.Operation) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(rowModel.ToModel(), ops, 30*time.Second)
}

func r(gen, ver int64, name string, score int64) rowState {
	return rowState{loaded: true, exists: true, gen: gen, ver: ver, name: name, score: score}
}

var absentRow = rowState{loaded: true}

// initOp sets the key's starting row, before everything else.
func initOp(k schema.Key, s rowState) porcupine.Operation {
	return op(-2, -1, opInput{kind: opInit, key: k, init: s}, opOutput{})
}

func get(k string, call, ret int64, out opOutput) porcupine.Operation {
	return op(call, ret, opInput{kind: opGet, key: schema.Key(k)}, out)
}

func upd(k string, call, ret int64, gen int64, c ColChange, out opOutput) porcupine.Operation {
	return op(call, ret, opInput{kind: opUpdate, key: schema.Key(k), gen: gen, cols: []ColChange{c}}, out)
}

func crt(k string, call, ret int64, gen int64, out opOutput) porcupine.Operation {
	return op(call, ret, opInput{kind: opCreate, key: schema.Key(k), gen: gen, name: "a", score: 10}, out)
}

func dlt(k string, call, ret int64, gen int64, out opOutput) porcupine.Operation {
	return op(call, ret, opInput{kind: opDelete, key: schema.Key(k), gen: gen}, out)
}

func ok(s rowState) opOutput   { return opOutput{row: s} }
func fails(err error) opOutput { return opOutput{err: err} }

const never = math.MaxInt64 // the Return of an operation whose outcome is unknown

func wantResult(t *testing.T, want porcupine.CheckResult, ops ...porcupine.Operation) {
	t.Helper()
	if got := check(ops...); got != want {
		t.Errorf("check = %s, want %s", got, want)
	}
}

func TestModelAcceptsSequentialHistory(t *testing.T) {
	wantResult(t, porcupine.Ok,
		initOp("k", absentRow),
		crt("k", 1, 2, 7, ok(r(7, 0, "a", 10))),
		upd("k", 3, 4, 0, set(1, "b"), ok(r(7, 1, "b", 10))),
		upd("k", 5, 6, 0, incr(2, 5), ok(r(7, 2, "b", 15))),
		get("k", 7, 8, ok(r(7, 2, "b", 15))),
		dlt("k", 9, 10, 7, opOutput{}),
		get("k", 11, 12, fails(ErrNotFound)),
	)
}

func TestModelRejectsStaleRead(t *testing.T) {
	inc := upd("k", 1, 2, 0, incr(2, 1), ok(r(101, 4, "a", 11)))
	wantResult(t, porcupine.Illegal, initOp("k", r(101, 3, "a", 10)), inc, get("k", 3, 4, ok(r(101, 3, "a", 10))))
	wantResult(t, porcupine.Ok, initOp("k", r(101, 3, "a", 10)), inc, get("k", 1, 2, ok(r(101, 3, "a", 10))))
}

func TestModelRejectsLostIncrement(t *testing.T) {
	wantResult(t, porcupine.Illegal,
		initOp("k", r(101, 3, "a", 10)),
		upd("k", 1, 3, 0, incr(2, 1), ok(r(101, 4, "a", 11))),
		upd("k", 2, 4, 0, incr(2, 1), ok(r(101, 4, "a", 11))),
	)
}

func TestModelUnknownOutcomes(t *testing.T) {
	unknownCreate := crt("k", 1, never, 7, fails(ErrOutcomeUnknown))
	wantResult(t, porcupine.Ok, initOp("k", absentRow), unknownCreate, get("k", 3, 4, fails(ErrNotFound)))
	wantResult(t, porcupine.Ok, initOp("k", absentRow), unknownCreate, get("k", 3, 4, ok(r(7, 0, "a", 10))))
	wantResult(t, porcupine.Illegal, initOp("k", absentRow), unknownCreate, get("k", 3, 4, ok(r(8, 0, "a", 10))))
	unknownDelete := dlt("k", 1, never, 0, fails(ErrOutcomeUnknown))
	wantResult(t, porcupine.Ok, initOp("k", r(101, 3, "a", 10)), unknownDelete, get("k", 3, 4, fails(ErrNotFound)))
	wantResult(t, porcupine.Ok, initOp("k", r(101, 3, "a", 10)), unknownDelete, get("k", 3, 4, ok(r(101, 3, "a", 10))))
}

func TestModelGenMismatchIsNotFound(t *testing.T) {
	start := initOp("k", r(101, 3, "a", 10))
	wantResult(t, porcupine.Ok, start, upd("k", 1, 2, 99, set(1, "b"), fails(ErrNotFound)))
	wantResult(t, porcupine.Illegal, start, upd("k", 1, 2, 99, set(1, "b"), ok(r(101, 4, "b", 10))))
	wantResult(t, porcupine.Illegal, start, dlt("k", 1, 2, 99, opOutput{}))
	wantResult(t, porcupine.Ok, start, dlt("k", 1, 2, 99, fails(ErrNotFound)))
}

func TestModelRejectsCreateOverExisting(t *testing.T) {
	start := initOp("k", r(101, 3, "a", 10))
	wantResult(t, porcupine.Illegal, start, crt("k", 1, 2, 7, ok(r(7, 0, "a", 10))))
	wantResult(t, porcupine.Ok, start, crt("k", 1, 2, 7, fails(ErrAlreadyExists)))
	wantResult(t, porcupine.Illegal, start, crt("k", 1, 2, 7, opOutput{deleted: true}))
}

func TestModelPartitionsByKey(t *testing.T) {
	ops := []porcupine.Operation{
		initOp("a", absentRow), initOp("b", r(101, 3, "a", 10)),
		crt("a", 1, 4, 7, ok(r(7, 0, "a", 10))),
		get("b", 2, 3, ok(r(101, 3, "a", 10))),
		get("a", 5, 6, ok(r(7, 0, "a", 10))),
	}
	if n := len(rowModel.Partition(ops)); n != 2 {
		t.Fatalf("partitions = %d, want 2", n)
	}
	wantResult(t, porcupine.Ok, ops...)
}
