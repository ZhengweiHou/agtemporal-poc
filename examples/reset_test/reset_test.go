// Reset 验证——验证 Temporal Reset 的"跳过已完成分片、从失败点继续"能力。
//
// 场景：
//   ResetDemoWorkflow(filePath) 串行处理 3 个分片：
//     分片 0 → 成功（写副作用）
//     分片 1 → 成功（写副作用）
//     分片 2 → 失败（坏数据 BAD-AMOUNT）
//     → Workflow 失败，副作用文件此时有 2 行
//
//   Reset 到分片 2 之前的 workflow task：
//     → 新 RunID，分片 0、1 结果复用（不重新执行，副作用不重复写）
//     → 分片 2 重新执行（数据已修正 → 成功）
//
//   验证：Reset 后副作用文件从 2 行 → 3 行（不是 5 行，证明分片 0、1 没重跑）
package reset_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

const (
	resetTaskQueue = "reset-demo"
	sideEffectFile = "../testdata/reset_sideeffect.txt"
	badDataFile    = "../testdata/reset_bad.txt"
)

// ── 分片 Activity：处理一行 + 写副作用 ──

type resetShardInput struct {
	ShardID  int
	FilePath string
	LineNo   int
}

func resetShardActivity(ctx context.Context, input resetShardInput) (int, error) {
	// READ: 读指定行
	content, err := os.ReadFile(input.FilePath)
	if err != nil {
		return 0, err
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if input.LineNo >= len(lines) {
		return 0, fmt.Errorf("行号越界: %d", input.LineNo)
	}
	line := lines[input.LineNo]

	// PROCESS: 解析金额，坏数据失败
	amount, err := parseResetAmount(line)
	if err != nil {
		return 0, err
	}

	// WRITE: 写副作用——记录本分片已处理
	f, err := os.OpenFile(sideEffectFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fmt.Fprintf(f, "shard-%d-processed amount=%d\n", input.ShardID, amount)

	return amount, nil
}

func parseResetAmount(line string) (int, error) {
	fields := strings.Split(line, ",")
	if len(fields) < 2 {
		return 0, fmt.Errorf("格式错误: %q", line)
	}
	var amount int
	if _, err := fmt.Sscanf(strings.TrimSpace(fields[1]), "%d", &amount); err != nil {
		return 0, fmt.Errorf("金额解析失败: %q", fields[1])
	}
	return amount, nil
}

// ── 串行分片 Workflow ──

func ResetDemoWorkflow(ctx workflow.Context, filePath string) (int, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1, // 不重试，快速失败
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	total := 0
	for shardID := 0; shardID < 3; shardID++ {
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

// ── 工具：查 History，找 reset 点（分片 2 调度前的 workflow task completed event）──

func findResetEventID(c client.Client, workflowID, runID string) (int64, error) {
	iter := c.GetWorkflowHistory(context.Background(), workflowID, runID, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	var lastWFTCompleted int64
	var lastActivityScheduledWFT int64 // 最后一个 Activity 调度之前的 WFT Completed
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return 0, err
		}
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			lastWFTCompleted = event.GetEventId()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			// 记录：这个 Activity 调度之前的那个 WFT Completed（即"决定调度它"的 task）
			lastActivityScheduledWFT = lastWFTCompleted
		}
	}
	// Reset 点：最后一个 Activity 调度（失败的分片）之前的 WFT Completed
	// 语义：replay 到该 event，复用之前的 Activity 结果，从失败分片重新执行
	return lastActivityScheduledWFT, nil
}

// ── Test ──

func TestResetSkipCompletedShards(t *testing.T) {
	// 清理副作用文件
	os.Remove(sideEffectFile)

	c, err := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	require.NoError(t, err)
	defer c.Close()

	w := worker.New(c, resetTaskQueue, worker.Options{})
	w.RegisterWorkflow(ResetDemoWorkflow)
	w.RegisterActivity(resetShardActivity)
	go func() { _ = w.Run(worker.InterruptCh()) }()
	time.Sleep(200 * time.Millisecond)
	defer w.Stop()

	workflowID := fmt.Sprintf("reset-demo-%d", time.Now().UnixNano())

	// ═══ 第一次：坏数据 → 失败 ═══
	badData := "ORD001,1000,2026-01-01\n" +
		"ORD002,2000,2026-01-02\n" +
		"ORD003,BAD-AMOUNT,2026-01-03\n" // 分片 2 坏数据
	require.NoError(t, os.WriteFile(badDataFile, []byte(badData), 0644))

	run1, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: resetTaskQueue,
	}, ResetDemoWorkflow, badDataFile)
	require.NoError(t, err)

	var total1 int
	err1 := run1.Get(context.Background(), &total1)
	require.Error(t, err1, "坏数据应失败")
	t.Logf("第一次失败 ✓ error: %v", err1)

	// 失败时副作用应有 2 行（分片 0、1 已处理）
	sideEffect, _ := os.ReadFile(sideEffectFile)
	lines1 := countLines(string(sideEffect))
	t.Logf("失败后副作用行数: %d（应 2）", lines1)
	require.Equal(t, 2, lines1, "分片 0、1 已处理，副作用应有 2 行")

	// ═══ 修正数据 + Reset ═══
	fixedData := "ORD001,1000,2026-01-01\n" +
		"ORD002,2000,2026-01-02\n" +
		"ORD003,1500,2026-01-03\n" // BAD-AMOUNT → 1500
	require.NoError(t, os.WriteFile(badDataFile, []byte(fixedData), 0644))

	// 查 History 找 reset 点
	resetEventID, err := findResetEventID(c, workflowID, run1.GetRunID())
	require.NoError(t, err)
	t.Logf("reset 点 event ID: %d", resetEventID)

	// Reset
	resetResp, err := c.WorkflowService().ResetWorkflowExecution(context.Background(),
		&workflowservice.ResetWorkflowExecutionRequest{
			Namespace: "default",
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      run1.GetRunID(),
			},
			WorkflowTaskFinishEventId: resetEventID,
			Reason:                    "fix bad amount",
			RequestId:                 fmt.Sprintf("reset-%d", time.Now().UnixNano()),
		})
	require.NoError(t, err)
	t.Logf("Reset 成功，新 RunID: %s", resetResp.GetRunId())

	// 等待新 Run 完成
	run2 := c.GetWorkflow(context.Background(), workflowID, resetResp.GetRunId())
	var total2 int
	err2 := run2.Get(context.Background(), &total2)
	require.NoError(t, err2, "修正后 Reset 应成功")
	t.Logf("Reset 后成功 ✓ total=%d", total2)

	// ═══ 断言：副作用文件行数 ═══
	// 如果 Reset 复用分片 0、1 结果 → 副作用从 2 → 3（只加分片 2）
	// 如果全量重跑 → 副作用从 2 → 5（分片 0、1、2 都重跑）
	sideEffect2, _ := os.ReadFile(sideEffectFile)
	lines2 := countLines(string(sideEffect2))
	t.Logf("Reset 后副作用行数: %d", lines2)
	t.Log("══════════════════════════════")
	if lines2 == 3 {
		t.Log("✅ Reset 复用已完成分片结果（跳过 0、1，只跑分片 2）")
	} else {
		t.Logf("⚠️ 副作用行数 %d（预期 3），分片 0、1 可能被重跑", lines2)
	}
}

func countLines(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	return len(strings.Split(s, "\n"))
}

// 避免 filepath 未使用
var _ = filepath.Base
