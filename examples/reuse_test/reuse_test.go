// WorkflowIDReusePolicy 验证——实测五种复用策略的实际行为。
//
// 场景：相同 WorkflowID 提交，验证不同 ReusePolicy 下的行为。
//   - ALLOW_DUPLICATE           → 总是允许复用
//   - ALLOW_DUPLICATE_FAILED_ONLY → 仅失败后可复用
//   - REJECT_DUPLICATE          → 重复提交报错
//   - TERMINATE_IF_RUNNING      → 终止旧的，起新的
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

const reuseTaskQueue = "reuse-demo"

// ── 长跑 Workflow：Sleep 足够久，便于观察"运行中"状态 ──

func LongRunningWorkflow(ctx workflow.Context, name string) (string, error) {
	workflow.Sleep(ctx, 30*time.Second)
	return "done-" + name, nil
}

func startReuseWorker(t *testing.T, c client.Client) worker.Worker {
	w := worker.New(c, reuseTaskQueue, worker.Options{})
	w.RegisterWorkflow(LongRunningWorkflow)
	require.NoError(t, w.Start())
	return w
}

// 提交并返回 error（用于判断策略行为）
func start(t *testing.T, c client.Client, wfID string, policy enumspb.WorkflowIdReusePolicy, errorWhenStarted bool) (client.WorkflowRun, error) {
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:                                    wfID,
		TaskQueue:                             reuseTaskQueue,
		WorkflowIDReusePolicy:                 policy,
		WorkflowExecutionErrorWhenAlreadyStarted: errorWhenStarted,
		WorkflowExecutionTimeout:              1 * time.Minute,
	}, LongRunningWorkflow, "test")
	return run, err
}

func TestReusePolicyAllowDuplicate(t *testing.T) {
	c, _ := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	defer c.Close()
	w := startReuseWorker(t, c)
	defer w.Stop()

	wfID := fmt.Sprintf("reuse-allow-%d", time.Now().UnixNano())
	p := enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE

	run1, err1 := start(t, c, wfID, p, false)
	require.NoError(t, err1)
	t.Logf("第 1 次提交: err=%v runID=%s", err1, run1.GetRunID())

	// 运行中再次提交
	run2, err2 := start(t, c, wfID, p, false)
	t.Logf("第 2 次提交（运行中）: err=%v runID=%s", err2, run2.GetRunID())
	if err2 == nil {
		t.Log("✅ ALLOW_DUPLICATE: 运行中重复提交，返回已有 Run 引用（幂等）")
	}

	// 清理
	c.TerminateWorkflow(context.Background(), wfID, "", "test done")
}

func TestReusePolicyRejectDuplicate(t *testing.T) {
	c, _ := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	defer c.Close()
	w := startReuseWorker(t, c)
	defer w.Stop()

	wfID := fmt.Sprintf("reuse-reject-%d", time.Now().UnixNano())
	p := enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE

	t.Run("SDK默认吞错(幂等)", func(t *testing.T) {
		run1, err1 := start(t, c, wfID, p, false)
		require.NoError(t, err1)
		t.Logf("第 1 次提交: err=%v runID=%s", err1, run1.GetRunID())
		time.Sleep(500 * time.Millisecond)

		run2, err2 := start(t, c, wfID, p, false)
		t.Logf("第 2 次提交（errorWhenStarted=false）: err=%v runID=%s", err2, run2.GetRunID())
		if err2 == nil && run2.GetRunID() == run1.GetRunID() {
			t.Log("✅ SDK 默认吞掉 AlreadyStarted 错误，返回已有 Run（幂等）")
		}
	})

	t.Run("显式报错", func(t *testing.T) {
		wfID2 := fmt.Sprintf("reuse-reject2-%d", time.Now().UnixNano())
		run1, err1 := start(t, c, wfID2, p, true)
		require.NoError(t, err1)
		t.Logf("第 1 次提交: err=%v runID=%s", err1, run1.GetRunID())
		time.Sleep(500 * time.Millisecond)

		_, err2 := start(t, c, wfID2, p, true)
		t.Logf("第 2 次提交（errorWhenStarted=true）: err=%v", err2)
		if err2 != nil && temporal.IsWorkflowExecutionAlreadyStartedError(err2) {
			t.Log("✅ 显式设置 errorWhenStarted=true 后，REJECT_DUPLICATE 真正报 AlreadyStarted 错误")
		} else if err2 == nil {
			t.Log("⚠️ 仍返回 nil，需进一步查证")
		}
		c.TerminateWorkflow(context.Background(), wfID2, "", "test done")
	})

	c.TerminateWorkflow(context.Background(), wfID, "", "test done")
}

func TestReusePolicyTerminateIfRunning(t *testing.T) {
	c, _ := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	defer c.Close()
	w := startReuseWorker(t, c)
	defer w.Stop()

	wfID := fmt.Sprintf("reuse-terminate-%d", time.Now().UnixNano())
	p := enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING

	run1, err1 := start(t, c, wfID, p, false)
	require.NoError(t, err1)
	t.Logf("第 1 次提交: err=%v runID=%s", err1, run1.GetRunID())

	time.Sleep(500 * time.Millisecond) // 等第一个真正跑起来

	run2, err2 := start(t, c, wfID, p, false)
	t.Logf("第 2 次提交（运行中）: err=%v runID=%s", err2, run2.GetRunID())
	if err2 == nil && run2.GetRunID() != run1.GetRunID() {
		t.Logf("✅ TERMINATE_IF_RUNNING: 旧 Run 被终止，新 Run 启动（新 RunID %s）", run2.GetRunID())
	} else if err2 == nil {
		t.Log("⚠️ 新 RunID 与旧 RunID 相同，可能未终止")
	}

	c.TerminateWorkflow(context.Background(), wfID, "", "test done")
}
