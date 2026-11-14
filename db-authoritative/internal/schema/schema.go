// Package schema describes a managed table: its columns, its primary key, and how a primary key is
// encoded into a Key (design.md 5.3).
package schema

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
)

// ErrInvalid wraps every error about a value supplied by a caller (design.md 5.2). It lives here
// rather than in store because store imports schema.
var ErrInvalid = errors.New("invalid argument")

// Key is the canonical encoding of a primary key. It is the map key and the shard hash input, and it
// is stored in the dedup table (5.1), so the encoding must never change once live.
type Key string

// Row is one row of a table. A published Row, including its Vals slice and every value in it, is never
// modified (I2): a change builds a new Row and swaps the pointer. Vals is indexed by the schema's column
// ordinals (I10).
type Row struct {
	Key  Key
	Gen  int64 // incarnation: distinguishes the lives of one key
	Ver  int64 // version within one life, incremented by the DB
	Vals []any
}

// Type is a column type. Go values: Int2, Int4, Int8 hold int64; Text holds string; UUID holds [16]byte;
// Float4, Float8 hold float64.
type Type uint8

const (
	Int2 Type = iota + 1
	Int4
	Int8
	Text
	UUID
	Float4
	Float8
)

type Column struct {
	Name string
	Type Type
}

// Schema is immutable once New returns (I10).
type Schema struct {
	Table string
	Cols  []Column
	PK    []int // ordinals into Cols, in primary key order
}

func New(table string, cols []Column, pk []int) (*Schema, error) {
	if len(pk) == 0 {
		return nil, fmt.Errorf("table %s: no primary key", table)
	}
	for i, c := range pk {
		if c < 0 || c >= len(cols) {
			return nil, fmt.Errorf("table %s: primary key column %d out of range", table, c)
		}
		if slices.Contains(pk[:i], c) {
			return nil, fmt.Errorf("table %s: column %s twice in the primary key", table, cols[c].Name)
		}
		switch cols[c].Type {
		case Int2, Int4, Int8, Text, UUID:
		default:
			return nil, fmt.Errorf("table %s: column %s: type %d cannot be a primary key", table, cols[c].Name, cols[c].Type)
		}
	}
	return &Schema{Table: table, Cols: slices.Clone(cols), PK: slices.Clone(pk)}, nil
}

// EncodeKey encodes primary key values, given in primary key order. With one column the key is that
// column's encoding; with several, each column's encoding is preceded by its length as a 4-byte
// big-endian uint32, so that bytes cannot shift from one column to the next.
func (s *Schema) EncodeKey(pk []any) (Key, error) {
	if len(pk) != len(s.PK) {
		return "", fmt.Errorf("%w: table %s: %d primary key values, want %d", ErrInvalid, s.Table, len(pk), len(s.PK))
	}
	if len(pk) == 1 {
		b, err := s.encodeCol(nil, s.PK[0], pk[0])
		return Key(b), err
	}
	var b []byte
	for i, v := range pk {
		b = append(b, 0, 0, 0, 0)
		start := len(b)
		var err error
		if b, err = s.encodeCol(b, s.PK[i], v); err != nil {
			return "", err
		}
		binary.BigEndian.PutUint32(b[start-4:], uint32(len(b)-start))
	}
	return Key(b), nil
}

// encodeCol appends the encoding of value v of column col to b: integers as 8-byte big-endian, text
// as its UTF-8 bytes, a UUID as its 16 bytes. New has already refused any other column type.
func (s *Schema) encodeCol(b []byte, col int, v any) ([]byte, error) {
	c := s.Cols[col]
	switch c.Type {
	case Int2, Int4, Int8:
		if n, ok := v.(int64); ok {
			if c.Type == Int2 && (n < math.MinInt16 || n > math.MaxInt16) ||
				c.Type == Int4 && (n < math.MinInt32 || n > math.MaxInt32) {
				return nil, fmt.Errorf("%w: table %s: column %s: %d out of range", ErrInvalid, s.Table, c.Name, n)
			}
			return binary.BigEndian.AppendUint64(b, uint64(n)), nil
		}
	case Text:
		if str, ok := v.(string); ok {
			return append(b, str...), nil
		}
	case UUID:
		if u, ok := v.([16]byte); ok {
			return append(b, u[:]...), nil
		}
	}
	return nil, fmt.Errorf("%w: table %s: column %s: unexpected Go type %T", ErrInvalid, s.Table, c.Name, v)
}

// Add returns old + delta for column col: an increment (design.md 7.4). Integer columns take an int64
// delta, float columns a float64 delta. A NULL old value, a column type without increment, a delta of the
// wrong Go type, or a result out of the column type's range is ErrInvalid. A Float4 result is rounded to
// float32 precision, the value PG stores. Pure computation: it is called with op held.
func (s *Schema) Add(col int, old, delta any) (any, error) {
	c := s.Cols[col]
	switch c.Type {
	case Int2, Int4, Int8:
		o, ok1 := old.(int64)
		d, ok2 := delta.(int64)
		if ok1 && ok2 {
			hi := int64(math.MaxInt64)
			switch c.Type {
			case Int2:
				hi = math.MaxInt16
			case Int4:
				hi = math.MaxInt32
			}
			if d > 0 && o > hi-d || d < 0 && o < -hi-1-d {
				return nil, fmt.Errorf("%w: table %s: column %s: %d + %d out of range", ErrInvalid, s.Table, c.Name, o, d)
			}
			return o + d, nil
		}
	case Float4, Float8:
		o, ok1 := old.(float64)
		d, ok2 := delta.(float64)
		if ok1 && ok2 {
			v := o + d
			if math.IsInf(v, 0) || math.IsNaN(v) || c.Type == Float4 && math.Abs(v) > math.MaxFloat32 {
				return nil, fmt.Errorf("%w: table %s: column %s: %g + %g out of range", ErrInvalid, s.Table, c.Name, o, d)
			}
			if c.Type == Float4 {
				v = float64(float32(v))
			}
			return v, nil
		}
	}
	return nil, fmt.Errorf("%w: table %s: column %s: cannot add %T to %#v", ErrInvalid, s.Table, c.Name, delta, old)
}
