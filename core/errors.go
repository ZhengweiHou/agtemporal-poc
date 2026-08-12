package core

import "errors"

// ErrNonRetryable 标记不可重试的业务错误。
// 用法：return fmt.Errorf("validation failed: %w", ErrNonRetryable)
var ErrNonRetryable = errors.New("non-retryable error")
