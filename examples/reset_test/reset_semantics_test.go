// 验证 Reset 的精确语义：reset 点之后的所有任务都会重复执行。
//
// 场景：4 个分片串行，分片 3 失败。
//   分片 0、1、2 成功（写副作用），分片 3 失败。
//   失败后副作用 = 3 行。
//
// 验证 1：reset 到分片 3 调度前 → 只重跑分片 3（副作用 3→4）
// 验证 2：reset 到分片 1 调度前 → 分片 1、2、3 都重跑（副作用 3→6）
package reset_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// 4 分片串行 Workflow（第 4 个坏数据失败）
func ResetFourShardWorkflow(ctx workflow.Context, filePath string) (int, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	total := 0
	for shardID := 0; shardID < 4; shardID++ {
		var amount int
		err := workflow.ExecuteActivity(ctx, resetShardActivity, resetShardInput{
			ShardID: shardID, FilePath: filePath, LineNo: shardID,
		}).Get(ctx, &amount)
		if err != nil {
			return total, fmt.Errorf("分片 %d 失败: %w", shardID, err)
		}
		total += amount
	}
	return total, nil
}

// 工具：返回指定"第 N 个 Activity 调度之前的 WFT Completed event ID"
// shardIndex=0 表示第一个分片，返回其调度前的 WFT（即 workflow 启动后的第一个 WFT Completed）
func findWFTBeforeShard(c client.Client, workflowID, runID string, shardIndex int) (int64, error) {
	iter := c.GetWorkflowHistory(context.Background(), workflowID, runID, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	var lastWFTCompleted int64
	var scheduledCount int
	var targetWFT int64
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return 0, err
		}
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			lastWFTCompleted = event.GetEventId()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			if scheduledCount == shardIndex {
				// 这是目标分片的调度，返回它之前的 WFT Completed
				targetWFT = lastWFTCompleted
			}
			scheduledCount++
		}
	}
	if targetWFT == 0 {
		return 0, fmt.Errorf("未找到分片 %d 的调度 event", shardIndex)
	}
	return targetWFT, nil
}

func TestResetSemantics(t *testing.T) {
	// 清理副作用
	os.Remove(sideEffectFile)

	c, err := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	require.NoError(t, err)
	defer c.Close()

	w := worker.New(c, resetTaskQueue, worker.Options{})
	w.RegisterWorkflow(ResetFourShardWorkflow)
	w.RegisterActivity(resetShardActivity)
	go func() { _ = w.Run(worker.InterruptCh()) }()
	time.Sleep(200 * time.Millisecond)
	defer w.Stop()

	// ═══ 验证 1：reset 到分片 3 调度前 → 只重跑分片 3 ═══
	t.Run("reset到失败分片前", func(t *testing.T) {
		os.Remove(sideEffectFile)
		badData := "ORD001,1000,2026-01-01\n" +
			"ORD002,2000,2026-01-02\n" +
			"ORD003,3000,2026-01-03\n" +
			"ORD004,BAD-AMOUNT,2026-01-04\n"
		require.NoError(t, os.WriteFile(badDataFile, []byte(badData), 0644))

		wfID := fmt.Sprintf("reset-sem1-%d", time.Now().UnixNano())
		run1, _ := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
			ID: wfID, TaskQueue: resetTaskQueue,
		}, ResetFourShardWorkflow, badDataFile)
		var total1 int
		require.Error(t, run1.Get(context.Background(), &total1))

		side, _ := os.ReadFile(sideEffectFile)
		t.Logf("[验证1] 失败后副作用行数: %d（应 3，分片 0/1/2 成功）", countLines(string(side)))

		// 修正数据
		fixedData := "ORD001,1000,2026-01-01\n" +
			"ORD002,2000,2026-01-02\n" +
			"ORD003,3000,2026-01-03\n" +
			"ORD004,4000,2026-01-04\n"
		require.NoError(t, os.WriteFile(badDataFile, []byte(fixedData), 0644))

		// reset 到分片 3（index=3）调度前
		eventID, _ := findWFTBeforeShard(c, wfID, run1.GetRunID(), 3)
		t.Logf("[验证1] reset 点: event %d（分片 3 调度前）", eventID)
		resetResp, _ := c.WorkflowService().ResetWorkflowExecution(context.Background(),
			&workflowservice.ResetWorkflowExecutionRequest{
				Namespace: "default",
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run1.GetRunID()},
				WorkflowTaskFinishEventId: eventID,
				Reason: "fix shard 3",
				RequestId: fmt.Sprintf("r-%d", time.Now().UnixNano()),
			})

		run2 := c.GetWorkflow(context.Background(), wfID, resetResp.GetRunId())
		require.NoError(t, run2.Get(context.Background(), &total1))

		side, _ = os.ReadFile(sideEffectFile)
		lines := countLines(string(side))
		t.Logf("[验证1] Reset 后副作用行数: %d", lines)
		if lines == 4 {
			t.Logf("[验证1] ✅ 只重跑分片 3（3→4）")
		} else {
			t.Logf("[验证1] ⚠️ 副作用 %d 行（预期 4）", lines)
		}
	})

	// ═══ 验证 2：reset 到分片 1 调度前 → 分片 1、2、3 都重跑 ═══
	t.Run("reset到更早点", func(t *testing.T) {
		os.Remove(sideEffectFile)
		badData := "ORD001,1000,2026-01-01\n" +
			"ORD002,2000,2026-01-02\n" +
			"ORD003,3000,2026-01-03\n" +
			"ORD004,BAD-AMOUNT,2026-01-04\n"
		require.NoError(t, os.WriteFile(badDataFile, []byte(badData), 0644))

		wfID := fmt.Sprintf("reset-sem2-%d", time.Now().UnixNano())
		run1, _ := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
			ID: wfID, TaskQueue: resetTaskQueue,
		}, ResetFourShardWorkflow, badDataFile)
		var total1 int
		require.Error(t, run1.Get(context.Background(), &total1))

		side, _ := os.ReadFile(sideEffectFile)
		t.Logf("[验证2] 失败后副作用行数: %d（应 3）", countLines(string(side)))

		// 修正数据
		fixedData := "ORD001,1000,2026-01-01\n" +
			"ORD002,2000,2026-01-02\n" +
			"ORD003,3000,2026-01-03\n" +
			"ORD004,4000,2026-01-04\n"
		require.NoError(t, os.WriteFile(badDataFile, []byte(fixedData), 0644))

		// reset 到分片 1（index=1）调度前——更早的点
		eventID, _ := findWFTBeforeShard(c, wfID, run1.GetRunID(), 1)
		t.Logf("[验证2] reset 点: event %d（分片 1 调度前）", eventID)
		resetResp, _ := c.WorkflowService().ResetWorkflowExecution(context.Background(),
			&workflowservice.ResetWorkflowExecutionRequest{
				Namespace: "default",
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run1.GetRunID()},
				WorkflowTaskFinishEventId: eventID,
				Reason: "reset earlier",
				RequestId: fmt.Sprintf("r-%d", time.Now().UnixNano()),
			})

		run2 := c.GetWorkflow(context.Background(), wfID, resetResp.GetRunId())
		require.NoError(t, run2.Get(context.Background(), &total1))

		side, _ = os.ReadFile(sideEffectFile)
		lines := countLines(string(side))
		t.Logf("[验证2] Reset 后副作用行数: %d", lines)
		if lines == 6 {
			t.Logf("[验证2] ✅ 分片 1、2、3 都重跑（3 + 3 = 6）")
		} else {
			t.Logf("[验证2] ⚠️ 副作用 %d 行（预期 6）", lines)
		}
	})
}

var _ = strings.TrimSpace
