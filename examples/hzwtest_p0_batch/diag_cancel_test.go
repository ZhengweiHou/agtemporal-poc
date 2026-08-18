// 批次 2 验证：#2 Parallel 失败取消传播。
// 场景：Parallel(A 失败, B sleep 30s) → A 失败后 B 是否被取消（快速失败 vs 等 30s）。
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// slowActivity sleep 10s 后写标记文件（验证取消传播：若被取消不写标记，副作用不发生）。
func slowActivity(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	mark := asStr(input.Params["file_path"]) + ".slow-done"
	select {
	case <-ctx.Done():
		return batch.BatchResult{}, ctx.Err()
	case <-time.After(10 * time.Second):
	}
	// 执行完成 → 写标记（副作用证据）
	if err := os.WriteFile(mark, []byte("slow done"), 0644); err != nil {
		return batch.BatchResult{}, err
	}
	return batch.BatchResult{Output: map[string]any{"slow_done": true}}, nil
}

// failActivity 立即失败（模拟分支 A）。
func failActivity(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	return batch.BatchResult{}, fmt.Errorf("simulated branch A failure")
}

// slowChildWf Child Workflow：内部调度 slowActivity（验证 Child 分支可被取消传播）。
func slowChildWf(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	ao := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}
	var result batch.BatchResult
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), "diag-cancel-slow", batch.BatchInput{Params: input}).Get(ctx, &result)
	if err != nil {
		return nil, err
	}
	return map[string]any{"slow_done": result.Output["slow_done"]}, nil
}

// TestDiagParallelCancel 验证 Parallel 失败后其他分支是否被取消（Child WF 分支）。
func TestDiagParallelCancel(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithMaxAttempts(1))
	failDef, err := b.BuildTasklet(failActivity, batch.WithActivityName("diag-cancel-fail"))
	require.NoError(t, err)
	slowDef, err := b.BuildTasklet(slowActivity, batch.WithActivityName("diag-cancel-slow"))
	require.NoError(t, err)
	_ = slowDef // slowActivity 由 slowChildWf 内部按注册名调度

	flow := batch.Parallel(
		batch.NewActivityPhase("fail", failDef, getInFilePath),
		batch.NewWorkflowPhase("slow", slowChildWf, getInFilePath), // Child WF 分支
	)
	job := batch.NewJob("diag-cancel", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := fmt.Sprintf("../testdata/test_diag_cancel_%d.txt", time.Now().UnixNano())
	require.NoError(t, os.WriteFile(filePath, []byte("x\n"), 0644))
	defer os.Remove(filePath)

	start := time.Now()
	run, err := job.Start(context.Background(), facade, map[string]any{"file_path": filePath})
	require.NoError(t, err)

	var result map[string]any
	err = run.Get(context.Background(), &result)
	elapsed := time.Since(start)
	t.Logf("主 WF 失败耗时: %v", elapsed)
	require.Error(t, err, "A 失败 → 主 WF 应失败")

	// 等待 slow 分支的 10s 完成窗口，检查副作用标记
	mark := filePath + ".slow-done"
	time.Sleep(12 * time.Second)
	_, markErr := os.Stat(mark)
	os.Remove(mark)

	if markErr != nil {
		t.Logf("✅ Child 分支被取消：无副作用标记（取消传播到 Child 内部 Activity）")
	} else {
		t.Logf("❌ Child 分支未取消：副作用发生（标记存在）")
		t.Fail()
	}
}
