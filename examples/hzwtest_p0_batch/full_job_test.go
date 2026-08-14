// hzwtest_p0_batch 完整批处理 Job 验证——所有能力协同。
//
// 对标 Spring Batch 完整 Job：多 Step + Skip + Partitioning + 数据流转。
//
// 场景：文件含 6 行（1 行坏数据），分 3 片并行处理，坏记录被 Skip。
//   validate → shard(Partitioner 拆分 + 并行引擎 + Skip) → report
//
// 验证点：
//   1. Phase 编排（Pipeline 串行 + Shard 分片复合）
//   2. Skip 坏记录（分片引擎内跳过 BAD-AMOUNT）
//   3. Partitioning（3 分片并行）
//   4. FlowCtx 数据流转（validate/shard 输出 → report）
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// TestFullBatchJob 验证完整批处理 Job。
func TestFullBatchJob(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 1. 构建引擎 Activity（含 SkipPolicy） ═══
	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("full-engine"),
		batch.WithActivitySkipPolicy(&skipBadAmount{}),
	)
	require.NoError(t, err)

	// ═══ 2. 编排完整 Job：validate → shard(分片+Skip) → report ═══
	flow := batch.Pipeline(
		batch.NewActivityPhase("validate", validateFile, getInFile),
		batch.NewShardPhase("shard", &filePartitioner{shardCount: 3, totalLines: 6}, "full-engine", getInFile),
		batch.NewActivityPhase("report", printReport, getInReportFromShard),
	)

	// ═══ 3. 注册 ═══
	wm.RegisterActivity(engineDef)
	for _, fn := range flow.CollectActivities() {
		wm.RegisterActivity(fn)
	}
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: batch.Compile(flow), Options: core.WorkflowDefOptions{Name: "full-batch-job"}})

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// ═══ 4. 数据文件：6 行（含 1 行坏数据） ═══
	dataFile := "../testdata/test_orders_full.txt"
	data := "ORD001,1000,2026-01-01\n" +
		"ORD002,2000,2026-01-02\n" +
		"ORD003,BAD-AMOUNT,2026-01-03\n" + // ← 坏记录，被 Skip
		"ORD004,3000,2026-01-04\n" +
		"ORD005,2500,2026-01-05\n" +
		"ORD006,1500,2026-01-06\n"
	require.NoError(t, os.WriteFile(dataFile, []byte(data), 0644))

	// ═══ 5. 提交 Job ═══
	workflowID := fmt.Sprintf("hzwtest-full-job-%d", time.Now().UnixNano())
	run, err := facade.StartWorkflow(context.Background(), workflowID, "full-batch-job",
		map[string]any{"file_path": dataFile})
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))

	t.Log("══════════ 完整批处理 Job ══════════")
	t.Logf("  FlowCtx: %+v", result)

	validate := result["validate"].(map[string]any)
	shard := result["shard"].(map[string]any)
	report := result["report"].(map[string]any)

	t.Logf("  validate: total=%v valid=%v", validate["total_lines"], validate["valid_count"])
	t.Logf("  shard: processed=%v skipped=%v amount=%v count=%v",
		shard["processed"], shard["skipped"], shard["total_amount"], shard["count"])
	t.Logf("  report: %v", report["report"])

	// 断言：6 行数据，1 行坏记录被 Skip
	require.Equal(t, float64(6), validate["total_lines"], "文件应 6 行")
	require.Equal(t, float64(5), shard["processed"], "处理 5 条正常记录")
	require.Equal(t, float64(1), shard["skipped"], "跳过 1 条坏记录")
	// 正常记录金额：1000+2000+3000+2500+1500=10000
	require.Equal(t, float64(10000), shard["total_amount"], "聚合金额 10000")
	require.Equal(t, float64(5), shard["count"], "计数 5")

	t.Logf("  ✅ 完整 Job 协同：validate → shard(分片+Skip) → report，所有能力协同工作")
	slog.Info("完整批处理 Job 验证通过", "workflowID", workflowID)
}
