package batch

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

// skipAllPolicy 跳过所有 Processor 错误。
type skipAllPolicy struct{}

func (p *skipAllPolicy) ShouldSkip(err error, item any, skipCount int) bool { return true }

// failItemProcessor 处理到特定 item 时失败一次（之后正常）。
type failItemProcessor struct {
	failItem any
	failed   bool
}

func (p *failItemProcessor) Process(ctx context.Context, item any) (any, error) {
	if !p.failed && item == p.failItem {
		p.failed = true
		return nil, errors.New("bad item")
	}
	return item, nil
}

// TestRunChunkLoop_Skip 验证 Skip：坏记录被跳过，其余正常处理。
func TestRunChunkLoop_Skip(t *testing.T) {
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{}
	proc := &failItemProcessor{failItem: 42}

	res, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, proc, writer, nil, 50, &skipAllPolicy{})
	})
	if err != nil {
		t.Fatalf("runChunkLoop: %v", err)
	}
	if res.Processed != 99 {
		t.Fatalf("Processed = %d, want 99 (100 - 1 skipped)", res.Processed)
	}
	if res.Skipped != 1 {
		t.Fatalf("Skipped = %d, want 1", res.Skipped)
	}
}

// TestRunChunkLoop_NoSkipPolicy 验证无 SkipPolicy 时，Processor 错误中断。
func TestRunChunkLoop_NoSkipPolicy(t *testing.T) {
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{}
	proc := &failItemProcessor{failItem: 42}

	_, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, proc, writer, nil, 50, nil) // nil SkipPolicy
	})
	if err == nil {
		t.Fatal("expected error (no skip policy), got nil")
	}
}

// TestRunChunkLoop_SkipPolicyReject 验证 SkipPolicy 拒绝跳过时，错误中断。
func TestRunChunkLoop_SkipPolicyReject(t *testing.T) {
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{}
	proc := &failItemProcessor{failItem: 42}

	// rejectPolicy 拒绝所有 skip
	rejectPolicy := &rejectAllPolicy{}
	_, err := runInEnv(t, func(ctx context.Context) (engineResult, error) {
		return runChunkLoop(ctx, reader, proc, writer, nil, 50, rejectPolicy)
	})
	if err == nil {
		t.Fatal("expected error (skip rejected), got nil")
	}
}

type rejectAllPolicy struct{}

func (p *rejectAllPolicy) ShouldSkip(err error, item any, skipCount int) bool { return false }

// TestNewChunkPhase_SkipPolicy 验证 NewChunkPhase 传递 SkipPolicy 到引擎。
func TestNewChunkPhase_SkipPolicy(t *testing.T) {
	reader := &sliceReader{items: genItems(100)}
	writer := &countingWriter{}
	proc := &failItemProcessor{failItem: 42}

	ph := NewChunkPhase("engine", reader, proc, writer,
		WithActivityChunkSize(50),
		WithActivitySkipPolicy(&skipAllPolicy{}),
	)
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
	if asIntAny(out["processed"]) != 99 || asIntAny(out["skipped"]) != 1 {
		t.Fatalf("processed=%v skipped=%v, want 99/1", out["processed"], out["skipped"])
	}
}
