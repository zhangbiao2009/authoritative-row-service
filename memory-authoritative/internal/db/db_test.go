package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestClassify(t *testing.T) {
	amb := fmt.Errorf("insert: %w", &AmbiguousError{Session: 7, Err: context.DeadlineExceeded})
	if !IsAmbiguous(amb) || SessionOf(amb) != 7 || !errors.Is(amb, context.DeadlineExceeded) {
		t.Fatalf("ambiguous: IsAmbiguous %v, SessionOf %d", IsAmbiguous(amb), SessionOf(amb))
	}
	for _, err := range []error{ErrNotExecuted, ErrCrossRow, ErrRejected, errors.New("x")} {
		if IsAmbiguous(err) || SessionOf(err) != 0 {
			t.Errorf("%v classified as ambiguous", err)
		}
	}
	if !NotExecuted(fmt.Errorf("w: %w", ErrNotExecuted)) || NotExecuted(ErrCrossRow) {
		t.Error("NotExecuted")
	}
	if !IsCrossRow(fmt.Errorf("w: %w", ErrCrossRow)) || IsCrossRow(ErrRejected) {
		t.Error("IsCrossRow")
	}
}
