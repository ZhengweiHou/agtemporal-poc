// hzwtest_p0_batch 最小闭环验证——单 Activity 批处理。
//
// 完全用 batch 提供的 BuildActivity + BuildWorkflow，不手写任何 Workflow：
//   BuildActivity(shardReaderFactory, amountProcessor, sumWriterFactory) → 引擎 Activity
//   BuildWorkflow(engineName) → 编排壳（薄壳：ExecuteActivity 透传）
//
// 验证点：
//   1. BuildWorkflow 改造后（返回 core.WorkflowDef）端到端可用
//   2. 最小闭环：提交 BuildWorkflow → 引擎 Activity 处理整个文件 → 返回 BatchResult
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// TestMinimalBatchLoop 验证最小闭环：BuildActivity + BuildWorkflow 组合。
// 不手写 Workflow，只用 batch 提供的构建能力。
func TestMinimalBatchLoop(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 1. BuildActivity 构建引擎 Activity ═══
	b := batch.NewBuilder(
		batch.WithChunkSize(3),
		batch.WithMaxAttempts(2),
		batch.WithHeartbeatTimeout(30*time.Second),
	)
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("minimal-engine"),
	)
	require.NoError(t, err)

	// ═══ 2. BuildWorkflow 构建编排壳（薄壳） ═══
	workflowDef := b.BuildWorkflow("minimal-engine", batch.WithWorkflowName("minimal-wf"))
	require.NotNil(t, workflowDef)

	// ═══ 3. 注册（引擎 ActivityDef + WorkflowDef 都实现 core 接口） ═══
	wm.RegisterActivity(engineDef)
	wm.RegisterWorkflow(workflowDef)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// ═══ 4. 提交：单 Activity 批处理整个文件 ═══
	filePath := "../testdata/test_orders.txt"
	workflowID := fmt.Sprintf("hzwtest-minimal-%d", time.Now().UnixNano())

	// 引擎 Activity 输入：读整个文件（start_line=0, line_count 足够大）
	engineInput := batch.BatchInput{Params: map[string]any{
		"file_path": filePath, "start_line": 0, "line_count": 999999,
	}}

	run, err := facade.StartWorkflow(context.Background(), workflowID, "minimal-wf", engineInput)
	require.NoError(t, err)

	var result batch.BatchResult
	require.NoError(t, run.Get(context.Background(), &result))

	slog.Info("最小闭环完成", "processed", result.Processed, "output", result.Output)
	t.Log("══════════ 最小闭环 ══════════")
	t.Logf("  WorkflowID: %s", workflowID)
	t.Logf("  Processed: %d", result.Processed)
	t.Logf("  Output: %+v", result.Output)

	require.Equal(t, 5, result.Processed, "应处理 5 行")
	require.NotNil(t, result.Output, "ResultProvider 应产出业务结果")
	require.Equal(t, 10000, asInt(result.Output["total_amount"]), "金额汇总应为 10000")
	require.Equal(t, 5, asInt(result.Output["count"]), "计数应为 5")
}
