package batch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
)

// genItems 生成 n 个元素（0..n-1）。
func genItems(n int) []any {
	items := make([]any, n)
	for i := range items {
		items[i] = i
	}
	return items
}

// sliceReader 实现 Reader + RestartableReader（嵌入 OffsetState 条数定位）。
type sliceReader struct {
	OffsetState          // 嵌入：自动获得 SaveState/RestoreState（条数定位）
	items      []any
	readErr    error
	restoreErr error // 测试用：RestoreState 返回错误
}

func (r *sliceReader) Read(ctx context.Context) ([]any, error) {
	if r.readErr != nil {
		return nil, r.readErr
	}
	if r.Offset >= len(r.items) {
		return nil, nil
	}
	item := r.items[r.Offset]
	r.Offset++
	return []any{item}, nil
}

// RestoreState 覆盖以支持错误注入（其余委托 OffsetState）。
func (r *sliceReader) RestoreState(state map[string]any) error {
	if r.restoreErr != nil {
		return r.restoreErr
	}
	return r.OffsetState.RestoreState(state)
}

// plainReader 只实现 Reader，不实现 RestartableReader（从头重跑场景）。
type plainReader struct {
	items []any
	err   error
}

func (r *plainReader) Read(ctx context.Context) ([]any, error) {
	if r.err != nil {
		return nil, r.err
	}
	if len(r.items) == 0 {
		return nil, nil
	}
	items := r.items
	r.items = nil
	return items, nil
}

// countingWriter 记录每次 Write 的 items，可注入错误。
type countingWriter struct {
	writes   [][]any
	writeErr error
}

func (w *countingWriter) Write(ctx context.Context, items []any) error {
	if w.writeErr != nil {
		return w.writeErr
	}
	w.writes = append(w.writes, append([]any(nil), items...))
	return nil
}

func (w *countingWriter) written() int {
	n := 0
	for _, chunk := range w.writes {
		n += len(chunk)
	}
	return n
}

// errProcessor 在第 failAt 次 Process 时返回错误。
type errProcessor struct {
	failAt int
	count  int
}

func (p *errProcessor) Process(ctx context.Context, item any) (any, error) {
	p.count++
	if p.failAt > 0 && p.count >= p.failAt {
		return nil, errors.New("process failed")
	}
	return item, nil
}

// recordTM 记录 WithTransaction 调用次数，回调内执行 fn。
type recordTM struct {
	txCount int
}

func (t *recordTM) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	t.txCount++
	return fn(ctx)
}

// runInEnv 在 TestActivityEnvironment 中执行包装闭包，返回 engineResult 与执行错误。
// SDK 的 activity 包函数要求真实 activity context（context.Background() 会 panic），
// 因此所有 runChunkLoop 测试均经 testsuite 执行。opts 可配置 env（SetHeartbeatDetails 等）。
func runInEnv(t *testing.T, fn func(ctx context.Context) (engineResult, error), opts ...func(*testsuite.TestActivityEnvironment)) (engineResult, error) {
	t.Helper()
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	for _, o := range opts {
		o(env)
	}
	env.RegisterActivity(fn)
	val, err := env.ExecuteActivity(fn)
	if err != nil {
		return engineResult{}, err
	}
	var res engineResult
	if err := val.Get(&res); err != nil {
		return engineResult{}, err
	}
	return res, nil
}

// withHeartbeatDetails 预置断点（模拟 Server 存储的 Heartbeat Details）。
func withHeartbeatDetails(processed int) func(*testsuite.TestActivityEnvironment) {
	return func(env *testsuite.TestActivityEnvironment) {
		env.SetHeartbeatDetails(ChunkProgress{Processed: processed})
	}
}

func TestRunChunkLoop_Normal(t *testing.T) {
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{}
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	})
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	if len(writer.writes) != 2 {
		t.Fatalf("writes = %d, want 2", len(writer.writes))
	}
	if len(writer.writes[0]) != 50 || len(writer.writes[1]) != 50 {
		t.Fatalf("chunk sizes = %d/%d, want 50/50", len(writer.writes[0]), len(writer.writes[1]))
	}
	if res.Processed != 100 {
		t.Fatalf("Processed = %d, want 100", res.Processed)
	}
}

func TestRunChunkLoop_Tail(t *testing.T) {
	reader := &sliceReader{items: genItems(105)}
	writer := &countingWriter{}
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	})
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	if len(writer.writes) != 3 {
		t.Fatalf("writes = %d, want 3", len(writer.writes))
	}
	if len(writer.writes[2]) != 5 {
		t.Fatalf("tail chunk = %d, want 5", len(writer.writes[2]))
	}
	if res.Processed != 105 {
		t.Fatalf("Processed = %d, want 105", res.Processed)
	}
}

func TestRunChunkLoop_Empty(t *testing.T) {
	reader := &sliceReader{items: nil}
	writer := &countingWriter{}
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	})
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	if len(writer.writes) != 0 {
		t.Fatalf("writes = %d, want 0", len(writer.writes))
	}
	if res.Processed != 0 {
		t.Fatalf("Processed = %d, want 0", res.Processed)
	}
}

func TestRunChunkLoop_ReadError(t *testing.T) {
	reader := &sliceReader{items: genItems(10), readErr: errors.New("read failed")}
	writer := &countingWriter{}
	_, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	})
	if err == nil || !strings.Contains(err.Error(), "read failed") {
		t.Fatalf("expected read error, got %v", err)
	}
	if len(writer.writes) != 0 {
		t.Fatalf("writes = %d, want 0 (failed before write)", len(writer.writes))
	}
}

func TestRunChunkLoop_ProcessError(t *testing.T) {
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{}
	proc := &errProcessor{failAt: 51}
	_, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, proc, writer, nil, 50, nil)
	})
	// activity 错误经序列化边界包装，errors.Is 不可穿透；断言错误文本。
	if err == nil || !strings.Contains(err.Error(), "process failed") {
		t.Fatalf("expected process error, got %v", err)
	}
	if writer.written() != 50 {
		t.Fatalf("written = %d, want 50 (committed chunks only)", writer.written())
	}
}

func TestRunChunkLoop_WriteError(t *testing.T) {
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{writeErr: errors.New("write failed")}
	_, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	})
	if err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("expected write error, got %v", err)
	}
}

func TestRunChunkLoop_WithTransaction(t *testing.T) {
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{}
	tm := &recordTM{}
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, tm, 50, nil)
	})
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	if tm.txCount != 2 {
		t.Fatalf("txCount = %d, want 2 (one per chunk)", tm.txCount)
	}
	if res.Processed != 100 {
		t.Fatalf("Processed = %d, want 100", res.Processed)
	}
}

func TestRunChunkLoop_NoTransaction(t *testing.T) {
	reader := &sliceReader{items: genItems(10)}
	writer := &countingWriter{}
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	})
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	if writer.written() != 10 {
		t.Fatalf("written = %d, want 10", writer.written())
	}
	if res.Processed != 10 {
		t.Fatalf("Processed = %d, want 10", res.Processed)
	}
}

func TestRunChunkLoop_Canceled(t *testing.T) {
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{}
	_, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		// 子上下文取消：继承 activity 的 values（心跳上下文），保证 HasHeartbeatDetails 不 panic。
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		return runChunkLoop(cctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	})
	if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if len(writer.writes) != 0 {
		t.Fatalf("writes = %d, want 0 (canceled before write)", len(writer.writes))
	}
}

func TestRunChunkLoop_ResumeRestartable(t *testing.T) {
	reader := &sliceReader{items: genItems(105)}
	writer := &countingWriter{}
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	}, withFullHeartbeatDetails(50, 0, map[string]any{"offset": 50}))
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	// 恢复后从 offset=50 继续，只写剩余 55 条（非从头 105 条）
	if writer.written() != 55 {
		t.Fatalf("written = %d, want 55 (resumed from offset 50)", writer.written())
	}
	if res.Processed != 105 {
		t.Fatalf("Processed = %d, want 105 (50 resumed + 55 new)", res.Processed)
	}
}

func TestRunChunkLoop_ResumeNonRestartable(t *testing.T) {
	reader := &plainReader{items: genItems(100)}
	writer := &countingWriter{}
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	}, withHeartbeatDetails(50))
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	// Q27：非 RestartableReader 从头重跑，processed 保持 0 重新累加——计数不虚高。
	if writer.written() != 100 {
		t.Fatalf("written = %d, want 100 (restart from head)", writer.written())
	}
	if res.Processed != 100 {
		t.Fatalf("Processed = %d, want 100 (total, not 50+100)", res.Processed)
	}
}

func TestRunChunkLoop_RestoreError(t *testing.T) {
	reader := &sliceReader{items: genItems(100), restoreErr: errors.New("restore failed")}
	writer := &countingWriter{}
	_, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	}, withHeartbeatDetails(50))
	if err == nil || !strings.Contains(err.Error(), "restore failed") {
		t.Fatalf("expected restore error, got %v", err)
	}
}

func TestRunChunkLoop_HeartbeatProgress(t *testing.T) {
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{}
	var heartbeats []ChunkProgress
	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, &stubProcessor{}, writer, nil, 50, nil)
	}, func(env *testsuite.TestActivityEnvironment) {
		env.SetOnActivityHeartbeatListener(func(ai *activity.Info, d converter.EncodedValues) {
			var p ChunkProgress
			_ = d.Get(&p)
			heartbeats = append(heartbeats, p)
		})
	})
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	if len(heartbeats) < 1 {
		t.Fatalf("heartbeats = %d, want at least 1", len(heartbeats))
	}
	for i := 1; i < len(heartbeats); i++ {
		if heartbeats[i].Processed <= heartbeats[i-1].Processed {
			t.Fatalf("heartbeat progresses not increasing: %v", heartbeats)
		}
	}
	if heartbeats[len(heartbeats)-1].Processed > 100 {
		t.Fatalf("heartbeat exceeds total: %v", heartbeats)
	}
	if res.Processed != 100 {
		t.Fatalf("Processed = %d, want 100", res.Processed)
	}
}
