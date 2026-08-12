package core

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewSlogAdapter_LogOutput(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)

	adapter := NewSlogAdapter(logger)

	adapter.Debug("debug msg", "key", "val")
	adapter.Info("info msg", "count", 42)
	adapter.Warn("warn msg", "alert", true)
	adapter.Error("error msg", "code", 500)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 log lines, got %d:\n%s", len(lines), buf.String())
	}

	expected := []struct {
		level string
		msg   string
	}{
		{"DEBUG", "debug msg"},
		{"INFO", "info msg"},
		{"WARN", "warn msg"},
		{"ERROR", "error msg"},
	}

	for i, exp := range expected {
		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(lines[i]), &entry); err != nil {
			t.Fatalf("line %d: failed to parse JSON: %v", i, err)
		}
		if entry["msg"] != exp.msg {
			t.Errorf("line %d: expected msg '%s', got '%v'", i, exp.msg, entry["msg"])
		}
		if entry["level"] != exp.level {
			t.Errorf("line %d: expected level '%s', got '%v'", i, exp.level, entry["level"])
		}
	}
}
