package batch

import (
	"context"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

// resultWriter 实现 Writer + ResultProvider，累积写入值作为业务结果。
type resultWriter struct {
	sum int
}

func (w *resultWriter) Write(ctx context.Context, items []any) error {
	for _, it := range items {
		if v, ok := it.(int); ok {
			w.sum += v
		}
	}
	return nil
}

func (w *resultWriter) Result() map[string]any {
	return map[string]any{"sum": w.sum}
}

// TestNewChunkPhase_ResultProvider 验证 Writer 实现 ResultProvider 时，
// 引擎循环结束后其 Result 拼入返回 map（Output 扁平化——BatchResult 消灭后）。
func TestNewChunkPhase_ResultProvider(t *testing.T) {
	ph := NewChunkPhase("engine", &sliceReader{items: genItems(10)}, &stubProcessor{}, &resultWriter{}, WithActivityChunkSize(50))
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(ph.def.Fn)
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
	// ResultProvider 产物扁平进 map（sum 字段）
	if asIntAny(out["sum"]) != 45 {
		t.Fatalf("sum = %v, want 45 (0+1+...+9)", out["sum"])
	}
}

// TestNewChunkPhase_NoResultProvider 验证 Writer 未实现 ResultProvider 时，仅统计字段。
func TestNewChunkPhase_NoResultProvider(t *testing.T) {
	ph := NewChunkPhase("engine", &sliceReader{items: genItems(10)}, &stubProcessor{}, &countingWriter{}, WithActivityChunkSize(50))
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(ph.def.Fn)
	val, err := env.ExecuteActivity(ph.def.Fn, NewFlowCtx(nil))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	var out map[string]any
	if err := val.Get(&out); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if _, ok := out["sum"]; ok {
		t.Fatalf("sum should not exist (no ResultProvider), got %v", out)
	}
	if asIntAny(out["processed"]) != 10 {
		t.Fatalf("processed = %v, want 10", out["processed"])
	}
}

// TestEngineResult_ToMap 验证统计拼 map 的形态（processed/skipped/filtered + Output 扁平化）。
func TestEngineResult_ToMap(t *testing.T) {
	r := engineResult{Processed: 5, Skipped: 1, Filtered: 2, Output: map[string]any{"total_amount": 100}}
	m := r.toMap()
	if m["processed"] != 5 || m["skipped"] != 1 || m["filtered"] != 2 {
		t.Fatalf("stats mismatch: %v", m)
	}
	if m["total_amount"] != 100 {
		t.Fatalf("output not flattened: %v", m)
	}
}
