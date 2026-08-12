package batch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestNewBuilder(t *testing.T) {
	b := NewBuilder()
	if b == nil {
		t.Fatal("NewBuilder returned nil")
	}
}

func TestBuildActivity_NilArgs(t *testing.T) {
	b := NewBuilder()
	p := &stubProcessor{}
	w := &stubWriter{}

	if _, err := b.BuildActivity(nil, p, w); err == nil {
		t.Fatal("expected error for nil reader")
	}
	if _, err := b.BuildActivity(&stubReader{}, nil, w); err == nil {
		t.Fatal("expected error for nil processor")
	}
	if _, err := b.BuildActivity(&stubReader{}, p, nil); err == nil {
		t.Fatal("expected error for nil writer")
	}
}

func TestBuildActivity_ChunkSizeInvalid(t *testing.T) {
	b := NewBuilder()
	_, err := b.BuildActivity(&stubReader{}, &stubProcessor{}, &stubWriter{}, WithActivityChunkSize(0))
	if err == nil {
		t.Fatal("expected error for chunk size <= 0")
	}
}

func TestBuildActivity_TypeMismatch(t *testing.T) {
	b := NewBuilder()
	if _, err := b.BuildActivity(struct{}{}, &stubProcessor{}, &stubWriter{}); err == nil {
		t.Fatal("expected error for non-reader type")
	}
	if _, err := b.BuildActivity(&stubReader{}, struct{}{}, &stubWriter{}); err == nil {
		t.Fatal("expected error for non-processor type")
	}
	if _, err := b.BuildActivity(&stubReader{}, &stubProcessor{}, struct{}{}); err == nil {
		t.Fatal("expected error for non-writer type")
	}
}

func TestBuildActivity_AutoName(t *testing.T) {
	b := NewBuilder()
	act, err := b.BuildActivity(&stubReader{}, &stubProcessor{}, &stubWriter{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if act.Name != "chunk-activity-1" {
		t.Fatalf("Name = %q, want chunk-activity-1", act.Name)
	}
	if act.Fn == nil {
		t.Fatal("Fn is nil")
	}
}

func TestBuildActivity_AutoNameDistinct(t *testing.T) {
	b := NewBuilder()
	act1, err := b.BuildActivity(&stubReader{}, &stubProcessor{}, &stubWriter{})
	if err != nil {
		t.Fatalf("build 1: %v", err)
	}
	act2, err := b.BuildActivity(&stubReader{}, &stubProcessor{}, &stubWriter{})
	if err != nil {
		t.Fatalf("build 2: %v", err)
	}
	if act1.Name == act2.Name {
		t.Fatalf("auto names must be distinct, both = %q", act1.Name)
	}
	if act1.Name != "chunk-activity-1" || act2.Name != "chunk-activity-2" {
		t.Fatalf("names = %q / %q, want chunk-activity-1 / chunk-activity-2", act1.Name, act2.Name)
	}
}

func TestBuildActivity_WithActivityName(t *testing.T) {
	b := NewBuilder()
	act, err := b.BuildActivity(&stubReader{}, &stubProcessor{}, &stubWriter{}, WithActivityName("adjustment"))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if act.Name != "adjustment" {
		t.Fatalf("Name = %q, want adjustment", act.Name)
	}
}

func TestBuildActivity_WithActivityChunkSize(t *testing.T) {
	b := NewBuilder(WithChunkSize(100))
	act, err := b.BuildActivity(&stubReader{}, &stubProcessor{}, &stubWriter{}, WithActivityChunkSize(50))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if b.bc.ChunkSize != 100 {
		t.Fatalf("builder base ChunkSize mutated: got %d", b.bc.ChunkSize)
	}
	_ = act
}

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

func TestBuildActivity_ClosureResolveError(t *testing.T) {
	b := NewBuilder()
	openErr := errors.New("open failed")
	act, err := b.BuildActivity(&stubReaderFactory{openErr: openErr}, &stubProcessor{}, &stubWriter{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	res, err := act.Fn(context.Background(), BatchInput{})
	if !errors.Is(err, openErr) {
		t.Fatalf("expected openErr, got %v", err)
	}
	if res.Processed != 0 {
		t.Fatalf("expected empty result, got %d", res.Processed)
	}
}

func TestBuildActivity_EnginePath(t *testing.T) {
	b := NewBuilder()
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{}
	act, err := b.BuildActivity(reader, &stubProcessor{}, writer, WithActivityChunkSize(50))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(act.Fn)
	val, err := env.ExecuteActivity(act.Fn, BatchInput{})
	if err != nil {
		t.Fatalf("engine path: %v", err)
	}
	var res BatchResult
	if err := val.Get(&res); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if res.Processed != 100 {
		t.Fatalf("Processed = %d, want 100", res.Processed)
	}
}

// closeErrWriter 主流程成功但 Close 返回错误。
type closeErrWriter struct {
	*countingWriter
	closeErr error
}

func (w *closeErrWriter) Close() error { return w.closeErr }

func TestBuildActivity_CloseError(t *testing.T) {
	b := NewBuilder()
	reader := &sliceReader{items: genItems(10)}
	writer := &closeErrWriter{countingWriter: &countingWriter{}, closeErr: errors.New("flush failed")}
	act, err := b.BuildActivity(reader, &stubProcessor{}, writer)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(act.Fn)
	_, err = env.ExecuteActivity(act.Fn, BatchInput{})
	if err == nil || !strings.Contains(err.Error(), "flush failed") {
		t.Fatalf("expected close error, got %v", err)
	}
}

// closeOKWriter Close 成功（返回 nil）。
type closeOKWriter struct {
	*countingWriter
}

func (w *closeOKWriter) Close() error { return nil }

func TestBuildActivity_CloseOK(t *testing.T) {
	b := NewBuilder()
	reader := &sliceReader{items: genItems(10)}
	writer := &closeOKWriter{countingWriter: &countingWriter{}}
	act, err := b.BuildActivity(reader, &stubProcessor{}, writer)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(act.Fn)
	val, err := env.ExecuteActivity(act.Fn, BatchInput{})
	if err != nil {
		t.Fatalf("engine path: %v", err)
	}
	var res BatchResult
	if err := val.Get(&res); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if res.Processed != 10 {
		t.Fatalf("Processed = %d, want 10", res.Processed)
	}
}

func TestBuildWorkflow_AutoName(t *testing.T) {
	b := NewBuilder()
	wf := b.BuildWorkflow("adjustment")
	if wf.Name != "batch-workflow-1" {
		t.Fatalf("Name = %q, want batch-workflow-1", wf.Name)
	}
	if wf.Fn == nil {
		t.Fatal("Fn is nil")
	}
}

func TestBuildWorkflow_WithWorkflowName(t *testing.T) {
	b := NewBuilder()
	wf := b.BuildWorkflow("adjustment", WithWorkflowName("my-batch"))
	if wf.Name != "my-batch" {
		t.Fatalf("Name = %q, want my-batch", wf.Name)
	}
}

func TestBuildWorkflow_ExecuteActivity(t *testing.T) {
	b := NewBuilder(
		WithRetryInitialInterval(2*time.Second),
		WithMaxAttempts(5),
		WithHeartbeatTimeout(30*time.Second),
		WithStartToCloseTimeout(6*time.Hour),
	)

	var gotInput BatchInput
	captureAct := func(ctx context.Context, input BatchInput) (BatchResult, error) {
		gotInput = input
		return BatchResult{Processed: 42}, nil
	}
	const actName = "capture-activity"
	wf := b.BuildWorkflow(actName, WithWorkflowName("my-batch"))

	ts := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	ts.RegisterActivityWithOptions(captureAct, activity.RegisterOptions{Name: actName})
	ts.RegisterWorkflow(wf.Fn)
	ts.ExecuteWorkflow(wf.Fn, BatchInput{Params: map[string]string{"date": "x"}})

	if err := ts.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var result BatchResult
	if err := ts.GetWorkflowResult(&result); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if gotInput.Params["date"] != "x" {
		t.Fatalf("input not passed through: %v", gotInput.Params)
	}
	if result.Processed != 42 {
		t.Fatalf("result not passed through: %v", result)
	}
}

func TestDefaultActivityOpts_Copy(t *testing.T) {
	tm := &stubTM{}
	b := NewBuilder(WithChunkSize(50), WithTransactionManager(tm))
	ao := b.DefaultActivityOpts()
	if ao.ChunkSize != 50 {
		t.Fatalf("ChunkSize = %d, want 50", ao.ChunkSize)
	}
	if ao.TransactionManager != tm {
		t.Fatal("TransactionManager not copied")
	}
	ao.ChunkSize = 999
	if b.bc.ChunkSize != 50 {
		t.Fatalf("base ChunkSize mutated: got %d", b.bc.ChunkSize)
	}
}

func TestDefaultWorkflowOpts_Copy(t *testing.T) {
	b := NewBuilder(WithHeartbeatTimeout(30 * time.Second))
	wo := b.DefaultWorkflowOpts()
	if wo.HeartbeatTimeout != 30*time.Second {
		t.Fatalf("HeartbeatTimeout = %v, want 30s", wo.HeartbeatTimeout)
	}
	wo.HeartbeatTimeout = time.Hour
	if b.bc.HeartbeatTimeout != 30*time.Second {
		t.Fatalf("base HeartbeatTimeout mutated: got %v", b.bc.HeartbeatTimeout)
	}
}

func TestChunkActivity_Registerable(t *testing.T) {
	b := NewBuilder()
	reader := &sliceReader{items: genItems(10)}
	writer := &countingWriter{}
	act, err := b.BuildActivity(reader, &stubProcessor{}, writer)
	if err != nil {
		t.Fatalf("build activity: %v", err)
	}
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(act.Fn) // SDK 接受 context.Context 签名则成功，否则 panic
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
}
