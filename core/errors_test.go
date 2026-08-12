package core

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrNonRetryable_WrappedThenDetected(t *testing.T) {
	err := fmt.Errorf("validation failed for field x: %w", ErrNonRetryable)

	if !errors.Is(err, ErrNonRetryable) {
		t.Error("expected wrapped error to be detectable as ErrNonRetryable via errors.Is")
	}
}
