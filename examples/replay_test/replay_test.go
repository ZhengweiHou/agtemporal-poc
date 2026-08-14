// Replay 验证——验证 Worker 崩溃后，Workflow 靠 History Replay 自动恢复。
//
// 场景：
//   ReplayDemoWorkflow 串行处理 5 个分片，每个分片间 Sleep 2 秒（留出崩溃窗口）。
//   每个分片 Activity 写一行副作用。
//
//   执行到分片 1 完成后（副作用 2 行），强制停掉 Worker（模拟崩溃）。
//   重启新 Worker（同 taskQueue）→ Workflow 靠 Replay 恢复，继续分片 2。
//
//   验证：副作用文件恰好 5 行（分片 0、1 不重复执行，从分片 2 继续）。
package replay_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	replayTaskQueue = "replay-demo"
	replaySideFile  = "../testdata/replay_sideeffect.txt"
)

// ── 分片 Activity：写副作用 ──

func replayShardActivity(ctx context.Context, shardID int) (int, error) {
	f, err := os.OpenFile(replaySideFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fmt.Fprintf(f, "shard-%d-processed\n", shardID)
	return shardID, nil
}

// ── 串行分片 Workflow（分片间 Sleep 留崩溃窗口）──

func ReplayDemoWorkflow(ctx workflow.Context, shardCount int) (int, error) {
	ao := workflow.ActivityOptions{StartToCloseTimeout: 1 * time.Minute}
	ctx = workflow.WithActivityOptions(ctx, ao)

	total := 0
	for shardID := 0; shardID < shardCount; shardID++ {
		var out int
		if err := workflow.ExecuteActivity(ctx, replayShardActivity, shardID).Get(ctx, &out); err != nil {
			return total, err
		}
		total += out
		// 分片间 Sleep，留出崩溃/重启窗口
		if shardID < shardCount-1 {
			workflow.Sleep(ctx, 2*time.Second)
		}
	}
	return total, nil
}

func countReplayLines(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	return len(strings.Split(s, "\n"))
}

// ── Test ──

func TestReplayRecovery(t *testing.T) {
	os.Remove(replaySideFile)

	c, err := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	require.NoError(t, err)
	defer c.Close()

	// ═══ Worker 1：启动，跑 Workflow ═══
	w1 := worker.New(c, replayTaskQueue, worker.Options{})
	w1.RegisterWorkflow(ReplayDemoWorkflow)
	w1.RegisterActivity(replayShardActivity)
	require.NoError(t, w1.Start())
	defer w1.Stop()

	workflowID := fmt.Sprintf("replay-demo-%d", time.Now().UnixNano())
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: replayTaskQueue,
	}, ReplayDemoWorkflow, 5)
	require.NoError(t, err)

	// 等副作用出现 2 行（分片 0、1 完成，此时 Workflow 在 Sleep 中）
	require.Eventually(t, func() bool {
		side, _ := os.ReadFile(replaySideFile)
		return countReplayLines(string(side)) >= 2
	}, 10*time.Second, 200*time.Millisecond)
	side, _ := os.ReadFile(replaySideFile)
	t.Logf("崩溃前副作用行数: %d（分片 0、1 完成）", countReplayLines(string(side)))

	// ═══ 崩溃：停 Worker 1 ═══
	w1.Stop()
	t.Log("⚠️ Worker 1 已停止（模拟崩溃）")
	time.Sleep(3 * time.Second) // 让 Sleep 超时，Workflow 卡在 pending

	// ═══ Worker 2：重启，同 taskQueue ═══
	w2 := worker.New(c, replayTaskQueue, worker.Options{})
	w2.RegisterWorkflow(ReplayDemoWorkflow)
	w2.RegisterActivity(replayShardActivity)
	require.NoError(t, w2.Start())
	defer w2.Stop()
	t.Log("✅ Worker 2 已启动（同 taskQueue）")

	// 等待 Workflow 完成
	var total int
	require.NoError(t, run.Get(context.Background(), &total))
	t.Logf("Workflow 完成，total=%d", total)

	// ═══ 断言：副作用恰好 5 行 ═══
	side, _ = os.ReadFile(replaySideFile)
	lines := countReplayLines(string(side))
	t.Log("══════════════════════════════")
	t.Logf("恢复后副作用行数: %d", lines)
	if lines == 5 {
		t.Log("✅ Replay 恢复成功：分片 0、1 未重跑，从分片 2 继续")
	} else {
		t.Logf("⚠️ 副作用 %d 行（预期 5），若 >5 说明有分片被重复执行", lines)
	}
	require.Equal(t, 5, lines, "Replay 应恢复执行位置，不重复已完成的 Activity")
}
