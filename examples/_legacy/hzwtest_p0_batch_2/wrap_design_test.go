// hzwtest_p0_batch_2 包装设计完整 POC——验证"Activity 包装成 flow"的全部形态。
//
// 设计定案（分片语义讨论）：
//   - Activity 包装成叶子（NewActivityPhase）——统一契约（对标 Spring Step）
//   - 需要 flow 控制状态（循环/分支/进度）→ 包装成 Child WF（NewWorkflowPhase）
//   - 数据并行 → Shard（NewShardPhase，只包装单个执行单元 def）
//   - 子编排 → 嵌套（Pipeline/Parallel 递归）
//
// 本案例在一个 Job 里组合全部形态：
//   Pipeline(
//     step1-校验文件（叶子）,
//     分批导出（内部控制 → Child WF：内部循环分批）,
//     Parallel(
//       step2a-分片处理（Shard：数据并行）,
//       step2b-金额汇总（叶子）,
//     ),
//     step3-打印结果（叶子）,
//   )
package hzwtest_p0_batch_2

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// ═══════════════════════════════════════════════════════
// 需要内部控制的分批导出（Child WF：循环分批，状态控制）
// ═══════════════════════════════════════════════════════

// exportBatch 导出单批（自定义 Activity——由分批 Child WF 内部循环调度）。
func exportBatch(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	batchNo := asInt(input.Params["batch"])
	slog.Info("exportBatch", "batch", batchNo)
	return batch.BatchResult{Output: map[string]any{"batch": batchNo, "exported": 10}}, nil
}

// batchExportFlow 分批导出 Child WF：内部控制循环（每批调 exportBatch）。
// 例证"需要 flow 控制状态的 → 包装成 flow"：循环/进度控制需要独立执行上下文。
func batchExportFlow(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	totalBatches := asInt(input["total_batches"])
	exported := 0
	ao := workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Minute}
	for i := 0; i < totalBatches; i++ {
		var res batch.BatchResult
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, ao),
			"v2-wrap-export", // 字符串名（Child WF 内调度）
			batch.BatchInput{Params: map[string]any{"batch": i}},
		).Get(ctx, &res)
		if err != nil {
			return nil, err
		}
		exported += asInt(res.Output["exported"])
	}
	return map[string]any{"batches": totalBatches, "exported": exported}, nil
}

// getInExport 从 input 提取分批参数。
func getInExport(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	return map[string]any{"total_batches": input.(map[string]any)["total_batches"]}, nil
}

// ═══════════════════════════════════════════════════════
// 测试：包装设计完整 POC
// ═══════════════════════════════════════════════════════

func TestBatchCaseV2_WrapDesign(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 执行单元 ═══
	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	engineDef, err := b.BuildActivity(
		&engineReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("v2-wrap-engine"),
	)
	require.NoError(t, err)
	validateDef, err := b.BuildTasklet(step1ValidateFile, batch.WithActivityName("v2-wrap-validate"))
	require.NoError(t, err)
	sumDef, err := b.BuildTasklet(step2bSumAmounts, batch.WithActivityName("v2-wrap-sum"))
	require.NoError(t, err)
	reportDef, err := b.BuildTasklet(step3PrintReport, batch.WithActivityName("v2-wrap-report"))
	require.NoError(t, err)
	exportDef, err := b.BuildTasklet(exportBatch, batch.WithActivityName("v2-wrap-export"))
	require.NoError(t, err)

	// ═══ 编排：全部包装形态组合 ═══
	flow := batch.Pipeline(
		// ① 叶子：Activity → flow 叶子（统一契约）
		batch.NewActivityPhase("step1-校验文件", validateDef, getInFilePath),
		// ② 内部控制 → Child WF（循环分批，需要 flow 控制状态）
		batch.NewWorkflowPhase("分批导出", batchExportFlow, getInExport),
		// ③ 组合 + Shard + 叶子（Parallel 嵌套）
		batch.Parallel(
			batch.NewShardPhase("step2a-分片处理", &shardPartitioner{}, engineDef, getInShard),
			batch.NewActivityPhase("step2b-金额汇总", sumDef, getInFilePath),
		),
		// ④ 叶子
		batch.NewActivityPhase("step3-打印结果", reportDef, getInReport),
	)
	job := batch.NewJob("hzwtest2-wrap", flow)
	job.RegisterTo(wm)
	// ⚠️ 设计点：NewWorkflowPhase（手写 Child WF）是"逃逸舱"——内部字符串名调用的
	// Activity 不在编排树内，CollectDefs 收集不到 → 需手动注册（框架后续可提供 Job.WithActivity 补充）。
	wm.RegisterActivity(exportDef)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	params := map[string]any{
		"file_path":     "../testdata/test_orders.txt",
		"date":          "2026-08-18",
		"run_id":        time.Now().UnixNano(),
		"total_batches": 3, // 分批导出 Child WF 的内部控制参数
	}
	run, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))
	t.Log("══════════ 包装设计完整 POC ══════════")
	for k, v := range result {
		t.Logf("  %s: %+v", k, v)
	}

	// ═══ 断言：全部包装形态的结果 ═══
	// ① 叶子（校验）
	v, ok := result["step1-校验文件"].(map[string]any)
	require.True(t, ok, "叶子(校验) 应存在")
	require.Equal(t, float64(5), v["total_lines"])

	// ② 内部控制 → Child WF（分批导出）
	e, ok := result["分批导出"].(map[string]any)
	require.True(t, ok, "Child WF(分批导出) 应存在")
	require.Equal(t, float64(3), e["batches"], "3 批")
	require.Equal(t, float64(30), e["exported"], "每批 10 → 共 30")

	// ③ Shard（分片）
	s, ok := result["step2a-分片处理"].(map[string]any)
	require.True(t, ok, "Shard(分片) 应存在")
	require.Equal(t, float64(5), s["processed"])

	// ③ 叶子（汇总）
	m, ok := result["step2b-金额汇总"].(map[string]any)
	require.True(t, ok, "叶子(汇总) 应存在")
	require.Equal(t, float64(10000), m["total_amount"])

	// ④ 叶子（报告）
	_, ok = result["step3-打印结果"].(map[string]any)
	require.True(t, ok, "叶子(报告) 应存在")

	t.Log("  ✅ 包装设计完整 POC 通过：叶子 + Child WF(内部控制) + Shard + 嵌套 全部组合工作")
}
