// WorkflowIDConflictPolicy 验证——处理"运行中"冲突的三种策略。
//
// 与 WorkflowIDReusePolicy（处理"已完成"冲突）互补：
//   ConflictPolicy → 运行中（Running）冲突
//   ReusePolicy   → 已完成（Completed/Failed）冲突
package reuse_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const conflictTaskQueue = "reuse-conflict"

func ConflictWorkflow(ctx workflow.Context, name string) (string, error) {
	workflow.Sleep(ctx, 30*time.Second)
	return "done-" + name, nil
}

func startConflictWorker(t *testing.T, c client.Client) worker.Worker {
	w := worker.New(c, conflictTaskQueue, worker.Options{})
	w.RegisterWorkflow(ConflictWorkflow)
	require.NoError(t, w.Start())
	return w
}

func TestWorkflowIDConflictPolicy(t *testing.T) {
	c, _ := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	defer c.Close()
	w := startConflictWorker(t, c)
	defer w.Stop()

	// ═══ FAIL（默认）═══
	t.Run("FAIL默认报错", func(t *testing.T) {
		wfID := fmt.Sprintf("conflict-fail-%d", time.Now().UnixNano())
		opts := client.StartWorkflowOptions{
			ID:                                      wfID,
			TaskQueue:                               conflictTaskQueue,
			WorkflowIDConflictPolicy:                enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			WorkflowExecutionErrorWhenAlreadyStarted: true,
		}

		run1, err1 := c.ExecuteWorkflow(context.Background(), opts, ConflictWorkflow, "t")
		require.NoError(t, err1)
		t.Logf("第 1 次提交: err=%v runID=%s", err1, run1.GetRunID())
		time.Sleep(500 * time.Millisecond)

		_, err2 := c.ExecuteWorkflow(context.Background(), opts, ConflictWorkflow, "t")
		t.Logf("第 2 次提交（运行中）: err=%v", err2)
		if err2 != nil && temporal.IsWorkflowExecutionAlreadyStartedError(err2) {
			t.Log("✅ FAIL: 运行中冲突返回 AlreadyStarted 错误")
		} else {
			t.Logf("⚠️ 未报错 err=%v", err2)
		}
		c.TerminateWorkflow(context.Background(), wfID, "", "done")
	})

	// ═══ USE_EXISTING ═══
	t.Run("USE_EXISTING返回已有Run", func(t *testing.T) {
		wfID := fmt.Sprintf("conflict-use-%d", time.Now().UnixNano())
		opts := client.StartWorkflowOptions{
			ID:                       wfID,
			TaskQueue:                conflictTaskQueue,
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		}

		run1, err1 := c.ExecuteWorkflow(context.Background(), opts, ConflictWorkflow, "t")
		require.NoError(t, err1)
		t.Logf("第 1 次提交: err=%v runID=%s", err1, run1.GetRunID())
		time.Sleep(500 * time.Millisecond)

		run2, err2 := c.ExecuteWorkflow(context.Background(), opts, ConflictWorkflow, "t")
		t.Logf("第 2 次提交（运行中）: err=%v runID=%s", err2, run2.GetRunID())
		if err2 == nil && run2.GetRunID() == run1.GetRunID() {
			t.Log("✅ USE_EXISTING: 返回运行中的 Run 引用（幂等）")
		}
		c.TerminateWorkflow(context.Background(), wfID, "", "done")
	})

	// ═══ TERMINATE_EXISTING ═══
	t.Run("TERMINATE_EXISTING终止旧的", func(t *testing.T) {
		wfID := fmt.Sprintf("conflict-term-%d", time.Now().UnixNano())
		opts := client.StartWorkflowOptions{
			ID:                       wfID,
			TaskQueue:                conflictTaskQueue,
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
		}

		run1, err1 := c.ExecuteWorkflow(context.Background(), opts, ConflictWorkflow, "t")
		require.NoError(t, err1)
		t.Logf("第 1 次提交: err=%v runID=%s", err1, run1.GetRunID())
		time.Sleep(500 * time.Millisecond)

		run2, err2 := c.ExecuteWorkflow(context.Background(), opts, ConflictWorkflow, "t")
		t.Logf("第 2 次提交（运行中）: err=%v runID=%s", err2, run2.GetRunID())
		if err2 == nil && run2.GetRunID() != run1.GetRunID() {
			t.Logf("✅ TERMINATE_EXISTING: 旧 Run 终止，新 Run 启动（新 RunID %s）", run2.GetRunID())
		}
		c.TerminateWorkflow(context.Background(), wfID, "", "done")
	})
}
