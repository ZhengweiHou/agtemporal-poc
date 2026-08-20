package batch

import (
	"context"
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

// cursorReader 模拟 DB 游标 Reader：状态是"最后主键"（非 int），而非条数。
// 实现 RestartableReader——SaveState 返回 last_key，RestoreState 从 last_key 之后继续。
// 逐条读（对齐 Spring Batch ItemReader 逐条 read 语义），lastKey 精确跟踪最后读到的 key。
type cursorReader struct {
	items   []any // 有序数据（模拟按主键排序的查询结果）
	pos     int   // 当前游标位置（下标）
	lastKey string
	restored bool
}

func (r *cursorReader) Read(ctx context.Context) ([]any, error) {
	if r.pos >= len(r.items) {
		return nil, nil
	}
	item := r.items[r.pos]
	r.pos++
	r.lastKey = item.(string)
	return []any{item}, nil
}

// SaveState 返回游标状态（非 int，证明状态由 Reader 定义）。
func (r *cursorReader) SaveState() map[string]any {
	return map[string]any{"last_key": r.lastKey}
}

// RestoreState 从 last_key 之后继续（模拟 DB 游标恢复，按主键定位而非条数）。
func (r *cursorReader) RestoreState(state map[string]any) error {
	r.restored = true
	if state == nil {
		return nil
	}
	lastKey, _ := state["last_key"].(string)
	for i, item := range r.items {
		if item.(string) == lastKey {
			r.pos = i + 1
			return nil
		}
	}
	// lastKey 不在数据中（数据可能变了）→ 从头开始
	r.pos = 0
	return nil
}

// TestRunChunkLoop_RestartableReader 验证 RestartableReader 自定义状态恢复。
// 关键：状态是 last_key（string），而非 int 条数——证明计数与定位分离。
func TestRunChunkLoop_RestartableReader(t *testing.T) {
	items := []any{"a", "b", "c", "d", "e"} // 5 条，主键 a-e
	reader := &cursorReader{items: items}
	writer := &countingWriter{}

	// 预置断点：已处理 3 条，游标停在 "c"
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	}, withHeartbeatDetailsAndState(3, map[string]any{"last_key": "c"}))
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}

	if !reader.restored {
		t.Fatal("RestoreState should be called")
	}
	// 从 "c" 之后继续（"d","e"），只写 2 条——而非从头 5 条
	if writer.written() != 2 {
		t.Fatalf("written = %d, want 2 (resume from 'd','e')", writer.written())
	}
	if res.Processed != 5 {
		t.Fatalf("Processed = %d, want 5 (3 resumed + 2 new)", res.Processed)
	}
}

// TestRunChunkLoop_RestartableReaderSaveState 验证心跳时 SaveState 被调用、状态写入。
func TestRunChunkLoop_RestartableReaderSaveState(t *testing.T) {
	items := []any{"a", "b", "c"}
	reader := &cursorReader{items: items}
	writer := &countingWriter{}

	// chunkSize=1 → 每处理 1 条就心跳一次，SaveState 精确记录最后 key
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 1, nil)
	})
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	if res.Processed != 3 {
		t.Fatalf("Processed = %d, want 3", res.Processed)
	}
	// 心跳后 SaveState 记录 last_key = "c"（最后处理的）
	if reader.lastKey != "c" {
		t.Fatalf("lastKey = %q, want c (SaveState should track cursor)", reader.lastKey)
	}
}

// TestRunChunkLoop_RestartableRestoreError 验证 RestoreState 失败时引擎报错。
func TestRunChunkLoop_RestartableRestoreError(t *testing.T) {
	writer := &countingWriter{}
	badReader := &restoreErrorReader{}

	_, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, badReader, &stubProcessor{}, writer, nil, 50, nil)
	}, withHeartbeatDetailsAndState(1, map[string]any{"last_key": "x"}))
	if err == nil {
		t.Fatal("expected restore error, got nil")
	}
	if !strings.Contains(err.Error(), "restore failed") {
		t.Fatalf("expected restore error, got %v", err)
	}
}

type restoreErrorReader struct{}

func (r *restoreErrorReader) Read(ctx context.Context) ([]any, error) { return nil, nil }
func (r *restoreErrorReader) SaveState() map[string]any               { return nil }
func (r *restoreErrorReader) RestoreState(state map[string]any) error {
	return &restoreErr{}
}

type restoreErr struct{}

func (e *restoreErr) Error() string { return "restore failed" }

// withHeartbeatDetailsAndState 预置断点（含 ReaderState）。
func withHeartbeatDetailsAndState(processed int, state map[string]any) func(*testsuite.TestActivityEnvironment) {
	return func(env *testsuite.TestActivityEnvironment) {
		env.SetHeartbeatDetails(ChunkProgress{Processed: processed, ReaderState: state})
	}
}
