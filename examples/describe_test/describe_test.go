// DescribeWorkflowExecution 验证——按 ID 查询执行状态，评估"续批"实现的可行性。
//
// 验证点：
//   1. 按 ID 查运行中状态（含 heartbeat 进度）
//   2. 按 ID 查失败状态
//   3. 评估：能否用 Describe 的 PendingActivities + heartbeat 实现"续批"
package describe_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const describeTaskQueue = "describe-demo"

// 分片 Activity：模拟长批处理，带 heartbeat 进度
func describeShardActivity(ctx context.Context, shardID int, total int) (int, error) {
	for i := 1; i <= total; i++ {
		// 每步 heartbeat 一次，报告进度
		activity.RecordHeartbeat(ctx, i)
		time.Sleep(200 * time.Millisecond)
	}
	return total, nil
}

// 串行分片 Workflow：跑 3 个分片，每个分片 10 步
func DescribeDemoWorkflow(ctx workflow.Context) (int, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	sum := 0
	for shardID := 0; shardID < 3; shardID++ {
		var out int
		if err := workflow.ExecuteActivity(ctx, describeShardActivity, shardID, 10).Get(ctx, &out); err != nil {
			return sum, err
		}
		sum += out
	}
	return sum, nil
}

func TestDescribeWorkflowExecution(t *testing.T) {
	c, err := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	require.NoError(t, err)
	defer c.Close()

	w := worker.New(c, describeTaskQueue, worker.Options{})
	w.RegisterWorkflow(DescribeDemoWorkflow)
	w.RegisterActivity(describeShardActivity)
	require.NoError(t, w.Start())
	defer w.Stop()

	wfID := fmt.Sprintf("describe-%d", time.Now().UnixNano())

	// ═══ 启动 Workflow（长跑，便于观察运行中状态）═══
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: describeTaskQueue,
	}, DescribeDemoWorkflow)
	require.NoError(t, err)

	// ═══ 验证 1：按 ID 查运行中状态（含 heartbeat 进度）═══
	time.Sleep(2 * time.Second) // 让第一个分片跑起来
	desc, err := c.DescribeWorkflowExecution(context.Background(), wfID, "")
	require.NoError(t, err)

	info := desc.GetWorkflowExecutionInfo()
	t.Log("══════════ 运行中查询 ══════════")
	t.Logf("  Status: %s", info.GetStatus())
	t.Logf("  WorkflowType: %s", info.GetType().GetName())
	t.Logf("  TaskQueue: %s", info.GetTaskQueue())

	// 查 heartbeat 进度（PendingActivities 含 heartbeat 细节）
	for i, pa := range desc.GetPendingActivities() {
		var progress int
		if hb := pa.GetHeartbeatDetails(); hb != nil {
			_ = converter.GetDefaultDataConverter().FromPayloads(hb, &progress)
		}
		t.Logf("  PendingActivity[%d]: type=%s heartbeat进度=%d", i, pa.GetActivityType().GetName(), progress)
	}

	// ═══ 等 Workflow 完成 ═══
	var result int
	require.NoError(t, run.Get(context.Background(), &result))
	t.Logf("  Workflow 完成，result=%d", result)

	// ═══ 验证 2：按 ID 查完成后的状态 ═══
	desc2, err := c.DescribeWorkflowExecution(context.Background(), wfID, "")
	require.NoError(t, err)
	info2 := desc2.GetWorkflowExecutionInfo()
	t.Log("══════════ 完成后查询 ══════════")
	t.Logf("  Status: %s", info2.GetStatus())
	t.Logf("  结束时间: %v", info2.GetCloseTime().AsTime())
	t.Logf("  PendingActivities 数: %d（应为 0）", len(desc2.GetPendingActivities()))

	// ═══ 验证 3：查不存在的 ID ═══
	_, err3 := c.DescribeWorkflowExecution(context.Background(), "not-exist-id", "")
	t.Logf("  查不存在 ID: err=%v", err3)
}

// ── 失败状态查询 ──

func FailWorkflow(ctx workflow.Context) (string, error) {
	return "", fmt.Errorf("模拟失败")
}

func TestDescribeFailedWorkflow(t *testing.T) {
	c, err := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	require.NoError(t, err)
	defer c.Close()

	w := worker.New(c, describeTaskQueue, worker.Options{})
	w.RegisterWorkflow(FailWorkflow)
	require.NoError(t, w.Start())
	defer w.Stop()

	wfID := fmt.Sprintf("describe-fail-%d", time.Now().UnixNano())
	run, _ := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: describeTaskQueue,
	}, FailWorkflow)
	var r string
	err = run.Get(context.Background(), &r)
	require.Error(t, err)

	desc, err := c.DescribeWorkflowExecution(context.Background(), wfID, "")
	require.NoError(t, err)
	info := desc.GetWorkflowExecutionInfo()
	t.Log("══════════ 失败后查询 ══════════")
	t.Logf("  Status: %s", info.GetStatus())
	t.Logf("  结束时间: %v", info.GetCloseTime().AsTime())
	_ = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
}
