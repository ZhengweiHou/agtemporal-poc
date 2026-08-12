package core

import (
	"io"
	"log/slog"

	temporallog "go.temporal.io/sdk/log"
)

// SlogAdapter 将 *slog.Logger 适配为 Temporal SDK 的 log.Logger 接口。
type SlogAdapter struct {
	logger *slog.Logger
}

// NewSlogAdapter 创建一个 slog → Temporal log.Logger 桥接。
// 如果 logger 为 nil，日志输出到 io.Discard。
func NewSlogAdapter(logger *slog.Logger) temporallog.Logger {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &SlogAdapter{logger: logger}
}

func (a *SlogAdapter) Debug(msg string, keyvals ...interface{}) {
	a.logger.Debug(msg, keyvals...)
}

func (a *SlogAdapter) Info(msg string, keyvals ...interface{}) {
	a.logger.Info(msg, keyvals...)
}

func (a *SlogAdapter) Warn(msg string, keyvals ...interface{}) {
	a.logger.Warn(msg, keyvals...)
}

func (a *SlogAdapter) Error(msg string, keyvals ...interface{}) {
	a.logger.Error(msg, keyvals...)
}
