// 异常场景——坏数据导致失败 → 修正数据 → 重跑成功。
// 入参 map 化 + slog 打印（复用 mainworkflow_test.go 的 Activity/Workflow）。
package hzwtest_raw

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// TestMainWorkflowBadRaw 验证异常恢复：坏数据导致失败 → 修正数据 → 重跑成功。
//
// 场景：
//   1. 写坏数据（BAD-AMOUNT）→ 第一次运行失败
//   2. 修正数据（改成正常金额）→ 第二次运行成功
//   3. 两次用相同识别参数（filePath + date）→ 相同 WorkflowID → 第二次是新 RunID
func TestMainWorkflowBadRaw(t *testing.T) {
	c, _ := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	defer c.Close()

	w := startWorker(t, c)
	defer w.Stop()

	// 识别参数（两次运行相同）——date 加时间戳避免历史残留冲突
	filePath := "../testdata/test_orders_bad.txt"
	date := fmt.Sprintf("2026-08-12-%d", time.Now().UnixNano())
	workflowID := fmt.Sprintf("hzwtest-%s-%s", filepath.Base(filePath), date)
	mainInput := map[string]any{"file_path": filePath, "date": date}

	// ═══ 第一次：写坏数据 → 失败 ═══
	badData := "ORD001,1000,2026-01-01\n" +
		"ORD002,BAD-AMOUNT,2026-01-02\n" + // ← 金额非数字，parseAmount 失败
		"ORD003,1500,2026-01-03\n"
	require.NoError(t, os.WriteFile(filePath, []byte(badData), 0644))

	slog.Info("第一次运行（坏数据）", "workflow_id", workflowID)
	run1, _ := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, MainWorkflow, mainInput)

	var result1 map[string]any
	err1 := run1.Get(context.Background(), &result1)
	require.Error(t, err1, "坏数据应该导致失败")
	slog.Error("第一次失败", "workflow_id", workflowID, "err", err1)

	// ═══ 修正数据 ═══
	fixedData := "ORD001,1000,2026-01-01\n" +
		"ORD002,2000,2026-01-02\n" + // ← BAD-AMOUNT 改为 2000
		"ORD003,1500,2026-01-03\n"
	require.NoError(t, os.WriteFile(filePath, []byte(fixedData), 0644))
	slog.Info("数据已修正", "file_path", filePath)

	// ═══ 第二次：相同识别参数 → 重跑成功 ═══
	run2, _ := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, MainWorkflow, mainInput)

	var result2 map[string]any
	err2 := run2.Get(context.Background(), &result2)
	require.NoError(t, err2, "修正后应该成功")
	slog.Info("第二次成功", "report", result2["report"])
	slog.Info("RunID 对比", "run1", run1.GetRunID(), "run2", run2.GetRunID())
}
