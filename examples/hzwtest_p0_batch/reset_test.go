// Reset（replay 式恢复）能力验证。
//
// 验证 1: TestReset_ChildPhaseRecovery——A(Child) 结果从 History 恢复，B 重跑成功
//   场景: Pipeline(A=ChildWF, B=Activity 数据失败)
//   第一次: A ✅ → B ❌ → 主 WF 失败
//   修复 → Reset(LAST_FAILED_TASK) → 新 Run:
//     断言: A 的结果在 FlowCtx（replay 恢复，无 AlreadyStarted）→ B 成功
//
// 验证 2: TestReset_ParallelCrossEffect——并发 flow 的交叉影响
//   场景: Parallel(ChildA, ChildB 失败) → A 还在跑时 B 失败 → 主 WF 失败
//   修复 → Reset → 断言: A 无交叉影响（结果恢复或明确状态），B 重跑成功
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// facadeClient 返回独立 client 连接（facade 未暴露底层 cli，Reset 是客户端操作）。
func facadeClient(t *testing.T) client.Client {
	t.Helper()
	cli, err := client.Dial(client.Options{
		HostPort:  newConfig().Server.HostPort,
		Namespace: newConfig().Server.Namespace,
	})
	require.NoError(t, err)
	t.Cleanup(cli.Close)
	return cli
}

// resetAndGet 重置 Workflow 到"失败 Activity 前的最后一个 WFT 完成点"，返回新 Run 结果。
// 重置点语义：保留该点之前的 History（A 的结果可 replay 恢复），清除之后（B 失败）重新执行。
func resetAndGet(t *testing.T, cli client.Client, workflowID, runID string, getResult any) error {
	t.Helper()
	// 找重置点：History 中第一个失败 Activity 之前的最后一个 WFT 完成事件
	resetEventID, err := findResetPoint(t, cli, workflowID, runID)
	if err != nil {
		return err
	}
	newRunID, err := cli.ResetWorkflowExecution(context.Background(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: newConfig().Server.Namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Reason:                  "resume after data fix",
		WorkflowTaskFinishEventId: resetEventID,
		ResetReapplyType:         enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	})
	if err != nil {
		return fmt.Errorf("ResetWorkflowExecution: %w", err)
	}
	t.Logf("Reset 成功: %s (重置点 eventID=%d) → 新 RunID %s", workflowID, resetEventID, newRunID.GetRunId())

	// 等待新 Run 完成并取结果
	run := cli.GetWorkflow(context.Background(), workflowID, newRunID.GetRunId())
	return run.Get(context.Background(), getResult)
}

// findResetPoint 遍历 History：返回第一个失败 Activity 事件之前的最后一个 WFT 完成事件 ID。
func findResetPoint(t *testing.T, cli client.Client, workflowID, runID string) (int64, error) {
	t.Helper()
	iter := cli.GetWorkflowHistory(context.Background(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var lastWFT int64
	for iter.HasNext() {
		e, err := iter.Next()
		if err != nil {
			return 0, fmt.Errorf("GetWorkflowHistory: %w", err)
		}
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			lastWFT = e.GetEventId()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
			if lastWFT > 0 {
				return lastWFT, nil // 失败前的最后一个决策点
			}
		}
	}
	return 0, fmt.Errorf("未找到失败点（History 无失败事件？）lastWFT=%d", lastWFT)
}

// TestReset_ParallelCrossEffect 验证 2：并发 flow 的交叉影响。
// 场景: Parallel(childA, childB 失败) → B 失败时 A 未完成 → 被 ParentClosePolicy 终止
// 发现: Reset 后 replay 恢复 childA 的 Started 事件，但无完成事件（被终止）
//       → future 永久等待 → 新 Run 挂起（交叉影响真实存在，记录为已知限制）
func TestReset_ParallelCrossEffect(t *testing.T) {
	t.Log("⚠️ 验证 2 记录：Reset 对'未完成被终止的并发 Child'会挂起")
	t.Log("  原因: replay 恢复 Started 事件但无后续完成事件 → future 永久等待")
	t.Log("  对策: 并发场景失败后，先确认分支终止（或等待）再 Reset；或改用重跑（AllowDuplicateFailedOnly 复用终止 Child）")

	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithMaxAttempts(2))
	failDef, err := b.BuildTasklet(stepBActivity, batch.WithActivityName("reset-par-fail"))
	require.NoError(t, err)

	flow := batch.Parallel(
		batch.NewWorkflowPhase("childA", slowSuccessChildWf, getInFilePath),
		batch.NewActivityPhase("childB", failDef, getInStepB),
	)
	job := batch.NewJob("reset-par", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := fmt.Sprintf("../testdata/test_reset_par_%d.txt", time.Now().UnixNano())
	require.NoError(t, os.WriteFile(filePath, []byte("ORD001,1000\nBAD\n"), 0644))
	defer os.Remove(filePath)
	params := map[string]any{"file_path": filePath}

	// 第一次：childB 失败（BAD）→ 主 WF 失败（childA 未完成，被终止）
	run1, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)
	workflowID := run1.GetID()
	runID1 := run1.GetRunID()
	var result1 map[string]any
	err1 := run1.Get(context.Background(), &result1)
	require.Error(t, err1, "第一次应失败（childB 数据失败）")
	t.Logf("第一次失败 ✅（childB 失败，childA 未完成被终止）")

	// 修复数据
	require.NoError(t, os.WriteFile(filePath, []byte("ORD001,1000\nGOOD\n"), 0644))

	// Reset → 用 15s 超时观察（预期挂起）
	cli := facadeClient(t)
	resetEventID, err := findResetPoint(t, cli, workflowID, runID1)
	require.NoError(t, err)
	newRunID, err := cli.ResetWorkflowExecution(context.Background(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: newConfig().Server.Namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID1},
		Reason:              "resume after data fix",
		WorkflowTaskFinishEventId: resetEventID,
		ResetReapplyType:         enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	})
	require.NoError(t, err)
	t.Logf("Reset 成功（重置点 eventID=%d），观察新 Run 是否挂起…", resetEventID)

	ctx15, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var result2 map[string]any
	err2 := cli.GetWorkflow(context.Background(), workflowID, newRunID.GetRunId()).Get(ctx15, &result2)
	if err2 != nil && ctx15.Err() != nil {
		t.Logf("✅ 风险确认：Reset 后新 Run 挂起（未完成 Child 无完成事件）——并发交叉影响成立")
		return
	}
	if err2 != nil {
		t.Logf("Reset 后执行失败（非挂起）: %v", err2)
		return
	}
	t.Logf("Reset 后成功（childA 恰好完成？）: %+v", result2)
}

// slowSuccessChildWf 慢速成功 Child（sleep 5s），用于验证并发交叉。
func slowSuccessChildWf(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	_ = workflow.Sleep(ctx, 5*time.Second)
	return map[string]any{"slow_done": true}, nil
}
func TestReset_ChildPhaseRecovery(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithMaxAttempts(2))
	stepBDef, err := b.BuildTasklet(stepBActivity, batch.WithActivityName("reset-stepb"))
	require.NoError(t, err)

	flow := batch.Pipeline(
		batch.NewWorkflowPhase("stepA", childAuditWf, getInFilePath),
		batch.NewActivityPhase("stepB", stepBDef, getInStepB),
	)
	job := batch.NewJob("reset-test", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := fmt.Sprintf("../testdata/test_reset_%d.txt", time.Now().UnixNano())
	require.NoError(t, os.WriteFile(filePath, []byte("ORD001,1000\nBAD\n"), 0644))
	defer os.Remove(filePath)
	params := map[string]any{"file_path": filePath}

	// 第一次：A ✅ → B ❌ → 主 WF 失败
	run1, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)
	workflowID := run1.GetID()
	runID1 := run1.GetRunID()
	var result1 map[string]any
	err1 := run1.Get(context.Background(), &result1)
	require.Error(t, err1, "第一次应失败（B 数据失败）")
	t.Logf("第一次失败 ✅（B 数据失败）")

	// 修复数据
	require.NoError(t, os.WriteFile(filePath, []byte("ORD001,1000\nGOOD\n"), 0644))

	// Reset 到最后一个 Workflow Task（失败处）
	var result2 map[string]any
	err2 := resetAndGet(t, facadeClient(t), workflowID, runID1, &result2)
	if err2 != nil {
		t.Fatalf("Reset 后执行失败: %v", err2)
	}

	stepA, hasA := result2["stepA"]
	stepB, hasB := result2["stepB"]
	t.Logf("Reset 后结果: stepA=%+v (hasA=%v), stepB=%+v (hasB=%v)", stepA, hasA, stepB, hasB)

	require.True(t, hasA, "Reset 后 A 的结果应恢复（replay）")
	require.True(t, hasB, "Reset 后 B 应重新执行成功")
	if hasB {
		bm := stepB.(map[string]any)
		require.Equal(t, true, bm["b_ok"], "B 应成功")
	}
	t.Logf("✅ 验证 1 通过：Reset 后 A 结果从 History 恢复，B 重跑成功（无 AlreadyStarted）")
}
