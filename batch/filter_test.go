package batch

import (
	"context"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

// filterProcessor 处理到 filterItem 时返回 nil（过滤）。
type filterProcessor struct {
	filterItem any
}

func (p *filterProcessor) Process(ctx context.Context, item any) (any, error) {
	if item == p.filterItem {
		return nil, nil // 过滤
	}
	return item, nil
}

// TestRunChunkLoop_Filter 验证过滤：Processor 返回 nil 不写 chunk，计 Filtered。
func TestRunChunkLoop_Filter(t *testing.T) {
	reader := &sliceReader{items: genItems(5)}
	writer := &countingWriter{}
	proc := &filterProcessor{filterItem: 2} // 过滤 item 2

	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, proc, writer, nil, 50, nil)
	})
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	if res.Processed != 4 {
		t.Fatalf("Processed = %d, want 4 (5 - 1 filtered)", res.Processed)
	}
	if res.Filtered != 1 {
		t.Fatalf("Filtered = %d, want 1", res.Filtered)
	}
	if writer.written() != 4 {
		t.Fatalf("written = %d, want 4 (filtered item not written)", writer.written())
	}
}

// TestRunChunkLoop_FilterResume 验证过滤 + 断点恢复（RestartableReader 的 offset 定位）。
// 关键：offset 是"已读条数"（含过滤记录），由 Reader 自己维护，过滤不影响定位正确性。
func TestRunChunkLoop_FilterResume(t *testing.T) {
	reader := &sliceReader{items: genItems(10)}
	writer := &countingWriter{}
	proc := &filterProcessor{filterItem: 2} // 过滤 item 2

	// 预置断点：已读 6 条（0..5，其中 2 过滤），processed=5, filtered=1, offset=6
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, proc, writer, nil, 50, nil)
	}, withFullHeartbeatDetails(5, 1, map[string]any{"offset": 6}))
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}

	// 恢复后从 offset=6 继续，处理 item 6,7,8,9（4 条），processed = 5 + 4 = 9
	if res.Processed != 9 {
		t.Fatalf("Processed = %d, want 9 (5 resumed + 4 new)", res.Processed)
	}
	if res.Filtered != 1 {
		t.Fatalf("Filtered = %d, want 1 (沿用 heartbeat)", res.Filtered)
	}
	if writer.written() != 4 {
		t.Fatalf("written = %d, want 4 (resume from offset 6, items 6-9)", writer.written())
	}
}

// withFullHeartbeatDetails 预置完整断点（Processed + Filtered + ReaderState）。
func withFullHeartbeatDetails(processed, filtered int, state map[string]any) func(*testsuite.TestActivityEnvironment) {
	return func(env *testsuite.TestActivityEnvironment) {
		env.SetHeartbeatDetails(ChunkProgress{Processed: processed, Filtered: filtered, ReaderState: state})
	}
}

// TestNewChunkPhase_Filter 验证 NewChunkPhase 构建的引擎支持过滤。
func TestNewChunkPhase_Filter(t *testing.T) {
	reader := &sliceReader{items: genItems(10)}
	writer := &countingWriter{}
	proc := &filterProcessor{filterItem: 2}

	ph := NewChunkPhase("engine", reader, proc, writer, WithActivityChunkSize(50))
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(ph.def.Fn)
	val, err := env.ExecuteActivity(ph.def.Fn, NewFlowCtx(nil))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	var out map[string]any
	if err := val.Get(&out); err != nil {
		t.Fatalf("get: %v", err)
	}
	if asIntAny(out["processed"]) != 9 || asIntAny(out["filtered"]) != 1 {
		t.Fatalf("processed=%v filtered=%v, want 9/1", out["processed"], out["filtered"])
	}
}
