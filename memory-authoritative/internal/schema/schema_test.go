package schema

import (
	"errors"
	"strings"
	"testing"
)

func oneCol(t *testing.T, typ Type) *Schema {
	t.Helper()
	s, err := New("t", []Column{{Name: "id", Type: typ}}, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestEncodeKeySingleColumn(t *testing.T) {
	cases := []struct {
		name string
		typ  Type
		val  any
		want Key
	}{
		{"int8", Int8, int64(1), Key("\x00\x00\x00\x00\x00\x00\x00\x01")},
		{"int8 negative", Int8, int64(-1), Key("\xff\xff\xff\xff\xff\xff\xff\xff")},
		{"int4", Int4, int64(258), Key("\x00\x00\x00\x00\x00\x00\x01\x02")},
		{"int2 min", Int2, int64(-32768), Key("\xff\xff\xff\xff\xff\xff\x80\x00")},
		{"text", Text, "abc", Key("abc")},
		{"text empty", Text, "", Key("")},
		{"uuid", UUID, [16]byte{0: 1, 15: 16}, Key("\x01" + strings.Repeat("\x00", 14) + "\x10")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := oneCol(t, c.typ).EncodeKey([]any{c.val})
			if err != nil || got != c.want {
				t.Fatalf("EncodeKey(%v) = %q, %v; want %q", c.val, got, err, c.want)
			}
		})
	}
}

func TestEncodeKeyComposite(t *testing.T) {
	// PK order (n, tenant) differs from column order: encoding follows PK order.
	s, err := New("t", []Column{{Name: "tenant", Type: Text}, {Name: "n", Type: Int8}, {Name: "v", Type: Text}}, []int{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.EncodeKey([]any{int64(7), "ab"})
	want := Key("\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x07" + "\x00\x00\x00\x02ab")
	if err != nil || got != want {
		t.Fatalf("EncodeKey = %q, %v; want %q", got, err, want)
	}

	s2, err := New("t", []Column{{Name: "a", Type: Text}, {Name: "b", Type: Text}}, []int{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	k1, _ := s2.EncodeKey([]any{"a", "bc"})
	k2, _ := s2.EncodeKey([]any{"ab", "c"})
	if k1 == k2 {
		t.Fatalf("(a, bc) and (ab, c) both encode to %q", k1)
	}
}

func TestEncodeKeyRejects(t *testing.T) {
	s, err := New("t", []Column{{Name: "id", Type: Int4}, {Name: "name", Type: Text}}, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	for name, pk := range map[string][]any{
		"too few":        {},
		"too many":       {int64(1), int64(2)},
		"go int":         {1},
		"string for int": {"1"},
		"nil":            {nil},
		"int4 overflow":  {int64(1) << 31},
		"int4 underflow": {-(int64(1) << 31) - 1},
	} {
		if k, err := s.EncodeKey(pk); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: EncodeKey(%v) = %q, %v; want ErrInvalid", name, pk, k, err)
		}
	}
	if k, err := oneCol(t, Int2).EncodeKey([]any{int64(32768)}); !errors.Is(err, ErrInvalid) {
		t.Errorf("int2 overflow: got %q, %v; want ErrInvalid", k, err)
	}
	if k, err := oneCol(t, UUID).EncodeKey([]any{[]byte("0123456789abcdef")}); !errors.Is(err, ErrInvalid) {
		t.Errorf("uuid as []byte: got %q, %v; want ErrInvalid", k, err)
	}
}

func TestNewRejects(t *testing.T) {
	cols := []Column{{Name: "id", Type: Int8}, {Name: "name", Type: Text}}
	for name, pk := range map[string][]int{
		"no primary key": nil,
		"out of range":   {2},
		"negative":       {-1},
		"duplicate":      {0, 0},
	} {
		if _, err := New("t", cols, pk); err == nil {
			t.Errorf("%s: New accepted pk %v", name, pk)
		}
	}
	// design.md 5.3: a table whose primary key type is not supported is refused when it is loaded.
	if _, err := New("t", []Column{{Name: "id"}}, []int{0}); err == nil {
		t.Error("New accepted a primary key column of type 0")
	}
}
