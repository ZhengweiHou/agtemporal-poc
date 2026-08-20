// hzwtest_p0_batch Partitioning 验证——分片原语。
//
// 对标 Spring Batch Partitioner：Partitioner 拆分 → 每个分片独立引擎执行 → 聚合。
//
// 验证点：
//   1. Partitioner 接口（纯内存拆分坐标）
//   2. NewShardPhase 分片原语：拆分 → 并行引擎 → 聚合
//   3. 聚合规则：processed/skipped 求和，Output 数值字段求和
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

// filePartitioner 按行数拆分文件坐标（纯内存，不做 IO）。
// 这里用固定的 total_lines 计算分片（简化：直接按已知行数拆分）。
type filePartitioner struct {
	shardCount int
	totalLines int
}

func (p *filePartitioner) Partition(input map[string]any) ([]map[string]any, error) {
	filePath := asStr(input["file_path"])
	per := p.totalLines / p.shardCount
	if p.totalLines%p.shardCount != 0 {
		per++
	}
	var coords []map[string]any
	for i := 0; i < p.shardCount; i++ {
		start := i * per
		count := per
		if rem := p.totalLines - start; count > rem {
			count = rem
		}
		if count <= 0 {
			break
		}
		coords = append(coords, map[string]any{
			"shard_id": i, "start": start, "line_count": count, "file_path": filePath,
		})
	}
	return coords, nil
}

// TestShardPhase 验证分片原语：Partitioner 拆分 + 并行引擎 + 聚合。
func TestShardPhase(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 构建引擎 Activity ═══
	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("shard-engine"),
	)
	require.NoError(t, err)
	validateDef, err := b.BuildTasklet(validateFile, batch.WithActivityName("shard-validate"))
	require.NoError(t, err)
	reportDef, err := b.BuildTasklet(printReport, batch.WithActivityName("shard-report"))
	require.NoError(t, err)

	// ═══ 编排：validate → shard(分片原语) → report ═══
	flow := batch.Pipeline(
		batch.NewActivityPhase("validate", validateDef, getInFile),
		batch.NewShardPhase("shard", &filePartitioner{shardCount: 4, totalLines: 5}, engineDef, getInFile),
		batch.NewActivityPhase("report", reportDef, getInReportFromShard),
	)

	// ═══ 注册：一体化 ═══
	for _, def := range flow.CollectDefs() {
		wm.RegisterActivity(def)
	}
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: batch.Compile(flow), Options: core.WorkflowDefOptions{Name: "shard-phase-wf"}})
	for _, def := range flow.CollectWorkflowDefs() {
		wm.RegisterWorkflow(def) // 分片 ShardWF（内部生成的闭包，显式名注册）
	}

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := "../testdata/test_orders.txt"
	workflowID := fmt.Sprintf("hzwtest-shard-phase-%d", time.Now().UnixNano())
	run, err := facade.StartWorkflow(context.Background(), workflowID, "shard-phase-wf",
		map[string]any{"file_path": filePath})
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))

	t.Log("══════════ 分片原语 ══════════")
	t.Logf("  FlowCtx: %+v", result)

	shard, ok := result["shard"].(map[string]any)
	require.True(t, ok, "FlowCtx 应含 shard 聚合结果")
	t.Logf("  聚合结果: processed=%v skipped=%v amount=%v count=%v",
		shard["processed"], shard["skipped"], shard["total_amount"], shard["count"])

	require.Equal(t, float64(5), shard["processed"], "聚合 processed 应 5")
	require.Equal(t, float64(10000), shard["total_amount"], "聚合金额应 10000")
	require.Equal(t, float64(5), shard["count"], "聚合计数应 5")
	t.Logf("  ✅ 分片原语：拆分→并行引擎→聚合 processed=5 amount=10000 count=5")
}

// getInReportFromShard 从 validate + shard 聚合结果构建报告输入。
func getInReportFromShard(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	validate, _ := fc.Get("validate")
	shard, _ := fc.Get("shard")
	return map[string]any{
		"file_path":    input.(map[string]any)["file_path"],
		"total_lines":  validate.(map[string]any)["total_lines"],
		"processed":    shard.(map[string]any)["processed"],
		"total_amount": shard.(map[string]any)["total_amount"],
		"count":        shard.(map[string]any)["count"],
	}, nil
}

// 确保 slog 被使用（避免 import 未使用）。
var _ = slog.Info
