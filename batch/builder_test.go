package batch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

// ═══ NewTaskletPhase（自定义执行单元）═══

func TestNewTaskletPhase_DefaultName(t *testing.T) {
	ph := NewTaskletPhase("step1-校验", func(ctx context.Context, fc *FlowCtx) (map[string]any, error) {
		return map[string]any{}, nil
	})
	if ph == nil {
		t.Fatal("NewTaskletPhase returned nil")
	}
	if ph.mode != PhaseActivity {
		t.Fatalf("mode = %v, want PhaseActivity", ph.mode)
	}
	// 注册名默认 = Phase name 派生
	if ph.def.Options.Name != "step1-校验" {
		t.Fatalf("regName = %q, want step1-校验", ph.def.Options.Name)
	}
	// 默认重试上限（DefaultConfig.MaxAttempts=3）
	if ph.def.Options.MaximumAttempts != 3 {
		t.Fatalf("MaximumAttempts = %d, want 3", ph.def.Options.MaximumAttempts)
	}
}

func TestNewTaskletPhase_WithActivityName(t *testing.T) {
	ph := NewTaskletPhase("step1", func(ctx context.Context, fc *FlowCtx) (map[string]any, error) {
		return map[string]any{}, nil
	}, WithActivityName("v2-step1"))
	if ph.def.Options.Name != "v2-step1" {
		t.Fatalf("regName = %q, want v2-step1", ph.def.Options.Name)
	}
}

func TestNewTaskletPhase_WithActivityMaxAttempts(t *testing.T) {
	ph := NewTaskletPhase("step1", func(ctx context.Context, fc *FlowCtx) (map[string]any, error) {
		return map[string]any{}, nil
	}, WithActivityMaxAttempts(5))
	if ph.def.Options.MaximumAttempts != 5 {
		t.Fatalf("MaximumAttempts = %d, want 5", ph.def.Options.MaximumAttempts)
	}
}

func TestNewTaskletPhase_NilFnPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil fn")
		}
	}()
	NewTaskletPhase("step1", nil)
}

// ═══ NewChunkPhase（引擎叶子）═══

func TestNewChunkPhase_NilArgsPanic(t *testing.T) {
	defer func() { _ = recover() }() // 期望 panic
	NewChunkPhase("engine", nil, &stubProcessor{}, &stubWriter{})
	t.Fatal("expected panic for nil reader")
}

func TestNewChunkPhase_ChunkSizeInvalidPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for chunk size <= 0")
		}
	}()
	NewChunkPhase("engine", &sliceReader{items: genItems(10)}, &stubProcessor{}, &stubWriter{}, WithActivityChunkSize(0))
}

func TestNewChunkPhase_TypeMismatchPanic(t *testing.T) {
	defer func() { _ = recover() }()
	NewChunkPhase("engine", struct{}{}, &stubProcessor{}, &stubWriter{})
	t.Fatal("expected panic for non-reader type")
}

func TestNewChunkPhase_DefaultName(t *testing.T) {
	ph := NewChunkPhase("step2a-分片处理", &sliceReader{items: genItems(10)}, &stubProcessor{}, &stubWriter{})
	if ph.def.Options.Name != "step2a-分片处理" {
		t.Fatalf("regName = %q, want step2a-分片处理", ph.def.Options.Name)
	}
}

func TestNewChunkPhase_EnginePath(t *testing.T) {
	ph := NewChunkPhase("engine", &sliceReader{items: genItems(100)}, &stubProcessor{}, &countingWriter{}, WithActivityChunkSize(50))
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(ph.def.Fn)
	val, err := env.ExecuteActivity(ph.def.Fn, NewFlowCtx(nil))
	if err != nil {
		t.Fatalf("engine path: %v", err)
	}
	var out map[string]any
	if err := val.Get(&out); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if asIntAny(out["processed"]) != 100 {
		t.Fatalf("processed = %v, want 100", out["processed"])
	}
}

// closeErrWriter 主流程成功但 Close 返回错误。
type closeErrWriter struct {
	*countingWriter
	closeErr error
}

func (w *closeErrWriter) Close() error { return w.closeErr }

func TestNewChunkPhase_CloseError(t *testing.T) {
	ph := NewChunkPhase("engine", &sliceReader{items: genItems(10)}, &stubProcessor{},
		&closeErrWriter{countingWriter: &countingWriter{}, closeErr: errors.New("flush failed")})
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(ph.def.Fn)
	_, err := env.ExecuteActivity(ph.def.Fn, NewFlowCtx(nil))
	if err == nil || !strings.Contains(err.Error(), "flush failed") {
		t.Fatalf("expected close error, got %v", err)
	}
}

// closeOKWriter Close 成功（返回 nil）。
type closeOKWriter struct {
	*countingWriter
}

func (w *closeOKWriter) Close() error { return nil }

func TestNewChunkPhase_CloseOK(t *testing.T) {
	ph := NewChunkPhase("engine", &sliceReader{items: genItems(10)}, &stubProcessor{},
		&closeOKWriter{countingWriter: &countingWriter{}})
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(ph.def.Fn)
	val, err := env.ExecuteActivity(ph.def.Fn, NewFlowCtx(nil))
	if err != nil {
		t.Fatalf("engine path: %v", err)
	}
	var out map[string]any
	if err := val.Get(&out); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if asIntAny(out["processed"]) != 10 {
		t.Fatalf("processed = %v, want 10", out["processed"])
	}
}

func TestNewChunkPhase_ResolveError(t *testing.T) {
	openErr := errors.New("open failed")
	ph := NewChunkPhase("engine", &stubReaderFactory{openErr: openErr}, &stubProcessor{}, &stubWriter{})
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(ph.def.Fn)
	_, err := env.ExecuteActivity(ph.def.Fn, NewFlowCtx(nil))
	// activity 错误经序列化边界包装，errors.Is 不可穿透——断言错误文本。
	if err == nil || !strings.Contains(err.Error(), "open failed") {
		t.Fatalf("expected open failed, got %v", err)
	}
}

func TestNewChunkPhase_Registerable(t *testing.T) {
	ph := NewChunkPhase("engine", &sliceReader{items: genItems(10)}, &stubProcessor{}, &countingWriter{})
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(ph.def.Fn) // SDK 接受 context.Context 签名则成功，否则 panic
	val, err := env.ExecuteActivity(ph.def.Fn, NewFlowCtx(nil))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	var out map[string]any
	if err := val.Get(&out); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if asIntAny(out["processed"]) != 10 {
		t.Fatalf("processed = %v, want 10", out["processed"])
	}
}

// ═══ 引擎入参 = FlowCtx 快照（getIn 消灭——执行单元自取）═══

func TestNewChunkPhase_InputFromFlowCtx(t *testing.T) {
	// Factory 从 fc.Input() 读坐标（替代 getIn——执行单元收 fc 快照）
	f := &stubReaderFactory{reads: []any{"a", "b", "c"}}
	ph := NewChunkPhase("engine", f, &stubProcessor{}, &countingWriter{})
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(ph.def.Fn)
	// 入参 = FlowCtx（含 input 字段——显式字段，无魔法 key）
	val, err := env.ExecuteActivity(ph.def.Fn, NewFlowCtx(map[string]any{"file_path": "x.txt", "start": 0}))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	var out map[string]any
	if err := val.Get(&out); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if asIntAny(out["processed"]) != 3 {
		t.Fatalf("processed = %v, want 3", out["processed"])
	}
}

// ═══ 解析 helper（保留——Factory 检测逻辑不变）═══

func TestResolveReader_Factory(t *testing.T) {
	f := &stubReaderFactory{reads: []any{"a", "b"}}
	r, err := resolveReader(f, context.Background(), BatchInput{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	rr, ok := r.(*stubReader)
	if !ok {
		t.Fatalf("expected *stubReader, got %T", r)
	}
	if len(rr.lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(rr.lines))
	}
}

func TestResolveReader_Shared(t *testing.T) {
	shared := &stubReader{lines: []any{"x"}}
	r, err := resolveReader(shared, context.Background(), BatchInput{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if r != shared {
		t.Fatalf("expected shared instance, got %T", r)
	}
}

func TestResolveProcessor_Factory(t *testing.T) {
	p, err := resolveProcessor(&stubProcessorFactory{}, context.Background(), BatchInput{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if _, ok := p.(*stubProcessor); !ok {
		t.Fatalf("expected *stubProcessor, got %T", p)
	}
}

func TestResolveWriter_Factory(t *testing.T) {
	w, err := resolveWriter(&stubWriterFactory{}, context.Background(), BatchInput{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if _, ok := w.(*stubWriter); !ok {
		t.Fatalf("expected *stubWriter, got %T", w)
	}
}
