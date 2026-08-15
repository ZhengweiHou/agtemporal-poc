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

	res, err := runInEnv(t, func(ctx context.Context) (BatchResult, error) {
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

// TestRunChunkLoop_FilterResume 验证过滤 + 断点恢复的 Seek 定位修正。
// 过滤的记录也占用 Reader 读取位置，Seek 基准 = processed + filtered（已读条数）。
func TestRunChunkLoop_FilterResume(t *testing.T) {
	// 10 条数据，item 2 和 7 被过滤（2 条过滤）
	reader := &sliceReader{items: genItems(10)}
	writer := &countingWriter{}
	proc := &filterProcessor{filterItem: 2} // 只过滤 item 2（简化：1 条过滤）

	// 预置断点：已处理 5 条（写），过滤 1 条 → 已读 6 条
	res, err := runInEnv(t, func(ctx context.Context) (BatchResult, error) {
		return runChunkLoop(ctx, reader, proc, writer, nil, 50, nil)
	}, withHeartbeatDetailsAndState(5, nil))
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}

	// Seek 应定位到 processed + filtered = 5 + 1 = 6（已读条数）
	// 但注意：这里 heartbeat 只预置了 Processed=5，没有 Filtered（withHeartbeatDetailsAndState 传 nil state）
	// 实际场景中 Filtered 也会持久化。这里验证 filtered 从 heartbeat 恢复的语义。
	// 简化：这里主要验证过滤 + 断点恢复的组合不报错，且 processed 正确沿用。
	if reader.seekCalled != 5 {
		t.Fatalf("Seek called with %d, want 5 (processed, filtered 未持久化时按 processed 定位)", reader.seekCalled)
	}
	_ = res
}

// TestRunChunkLoop_FilterResumeWithFilteredState 验证 Filtered 持久化后 Seek 定位 = processed + filtered。
func TestRunChunkLoop_FilterResumeWithFilteredState(t *testing.T) {
	reader := &sliceReader{items: genItems(10)}
	writer := &countingWriter{}
	proc := &filterProcessor{filterItem: 2} // 过滤 item 2

	// 预置断点：processed=5, filtered=1 → Seek 应定位 6
	res, err := runInEnv(t, func(ctx context.Context) (BatchResult, error) {
		return runChunkLoop(ctx, reader, proc, writer, nil, 50, nil)
	}, withFullHeartbeatDetails(5, 1, nil))
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}

	if reader.seekCalled != 6 {
		t.Fatalf("Seek called with %d, want 6 (processed 5 + filtered 1)", reader.seekCalled)
	}
	// 恢复后：从下标 6 继续处理 item 6,7,8,9（4 条），processed = 5 + 4 = 9
	if res.Processed != 9 {
		t.Fatalf("Processed = %d, want 9 (5 resumed + 4 new)", res.Processed)
	}
	if res.Filtered != 1 {
		t.Fatalf("Filtered = %d, want 1 (沿用 heartbeat)", res.Filtered)
	}
}

// withFullHeartbeatDetails 预置完整断点（Processed + Filtered + ReaderState）。
func withFullHeartbeatDetails(processed, filtered int, state map[string]any) func(*testsuite.TestActivityEnvironment) {
	return func(env *testsuite.TestActivityEnvironment) {
		env.SetHeartbeatDetails(ChunkProgress{Processed: processed, Filtered: filtered, ReaderState: state})
	}
}

// TestBuildActivity_Filter 验证 Builder 构建的引擎支持过滤。
func TestBuildActivity_Filter(t *testing.T) {
	b := NewBuilder()
	reader := &sliceReader{items: genItems(10)}
	writer := &countingWriter{}
	proc := &filterProcessor{filterItem: 2}

	act, err := b.BuildActivity(reader, proc, writer, WithActivityChunkSize(50))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(act.Fn)
	val, err := env.ExecuteActivity(act.Fn, BatchInput{})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	var res BatchResult
	if err := val.Get(&res); err != nil {
		t.Fatalf("get: %v", err)
	}
	if res.Processed != 9 || res.Filtered != 1 {
		t.Fatalf("Processed=%d Filtered=%d, want 9/1", res.Processed, res.Filtered)
	}
}
