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

// TestBuildActivity_ResultProvider 验证 Writer 实现 ResultProvider 时，
// 引擎循环结束后其 Result 填入 BatchResult.Output。
func TestBuildActivity_ResultProvider(t *testing.T) {
	b := NewBuilder()
	reader := &sliceReader{items: genItems(10)} // 0..9
	writer := &resultWriter{}
	act, err := b.BuildActivity(reader, &stubProcessor{}, writer, WithActivityChunkSize(50))
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
		t.Fatalf("get result: %v", err)
	}
	if res.Processed != 10 {
		t.Fatalf("Processed = %d, want 10", res.Processed)
	}
	if res.Output == nil {
		t.Fatal("Output should not be nil (ResultProvider implemented)")
	}
	sum, ok := res.Output["sum"].(float64) // JSON 序列化后 int 变 float64
	if !ok || int(sum) != 45 {
		t.Fatalf("Output[sum] = %v, want 45 (0+1+...+9)", res.Output["sum"])
	}
}

// TestBuildActivity_NoResultProvider 验证 Writer 未实现 ResultProvider 时，Output 为 nil。
func TestBuildActivity_NoResultProvider(t *testing.T) {
	b := NewBuilder()
	reader := &sliceReader{items: genItems(10)}
	writer := &countingWriter{} // 不实现 ResultProvider
	act, err := b.BuildActivity(reader, &stubProcessor{}, writer, WithActivityChunkSize(50))
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
		t.Fatalf("get result: %v", err)
	}
	if res.Output != nil {
		t.Fatalf("Output should be nil (no ResultProvider), got %v", res.Output)
	}
}
