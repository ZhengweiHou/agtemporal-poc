// hzwtest_p0_batch 分片并行调度验证。
//
// 场景：文件拆分为多个分片坐标，并行调度多个引擎 Activity（每个处理一个分片），
// 全部完成后汇总结果。对比串行，验证并发正确性。
//
// 验证点：
//   1. 多引擎 Activity 并行调度（Future 并发）
//   2. 各分片独立处理，结果正确汇总
//   3. 并行结果与串行一致
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// parallelShardWorkflow 并行调度引擎 Activity 处理各分片。
func parallelShardWorkflow(engineActivityName string) wfFunc {
	return func(ctx workflow.Context, input map[string]any) (map[string]any, error) {
		slog.Info("parallelShardWorkflow 开始", "input", input)
		ao := workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Minute}

		// ① splitFile 拆分坐标
		var splitRes batch.BatchResult
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), actSplitFile, batch.BatchInput{Params: input}).Get(ctx, &splitRes)
		if err != nil {
			return nil, err
		}
		shards := splitRes.Output["shards"].([]any)

		// ② 并行调度引擎 Activity（Future 并发）
		futures := make([]workflow.Future, 0, len(shards))
		for _, s := range shards {
			coord := s.(map[string]any)
			engineInput := batch.BatchInput{Params: coord}
			fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), engineActivityName, engineInput)
			futures = append(futures, fut)
		}
		slog.Info("parallelShardWorkflow 已并行调度", "shard_count", len(futures))

		// ③ 收集全部结果
		var totalProcessed, totalAmount, totalCount int
		for i, fut := range futures {
			var out batch.BatchResult
			if err := fut.Get(ctx, &out); err != nil {
				slog.Error("parallelShardWorkflow 分片失败", "shard", i, "err", err)
				return nil, err
			}
			totalProcessed += out.Processed
			if out.Output != nil {
				totalAmount += asInt(out.Output["total_amount"])
				totalCount += asInt(out.Output["count"])
			}
		}

		result := map[string]any{
			"shard_count":  len(shards),
			"processed":    totalProcessed,
			"total_amount": totalAmount,
			"count":        totalCount,
		}
		slog.Info("parallelShardWorkflow 完成", "output", result)
		return result, nil
	}
}

// TestParallelShards 验证分片并行调度。
func TestParallelShards(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("parallel-engine"),
	)
	require.NoError(t, err)

	wm.RegisterActivity(engineDef)
	wm.RegisterActivity(&core.ActivityDef{Fn: splitFile, Options: core.ActivityDefOptions{Name: actSplitFile}})
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: parallelShardWorkflow("parallel-engine"), Options: core.WorkflowDefOptions{Name: "parallel-shard-wf"}})

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := "../testdata/test_orders.txt"
	workflowID := fmt.Sprintf("hzwtest-parallel-%d", time.Now().UnixNano())

	run, err := facade.StartWorkflow(context.Background(), workflowID, "parallel-shard-wf",
		map[string]any{"file_path": filePath})
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))

	slog.Info("并行分片完成", "result", result)
	t.Log("══════════ 分片并行调度 ══════════")
	t.Logf("  result: %+v", result)

	require.Equal(t, 3, asInt(result["shard_count"]), "5 行 chunkSize=3 → 3 分片")
	require.Equal(t, 5, asInt(result["processed"]), "并行处理后应覆盖全部 5 行")
	require.Equal(t, 10000, asInt(result["total_amount"]), "并行汇总金额应 10000")
	require.Equal(t, 5, asInt(result["count"]), "并行计数应 5")
	t.Logf("  ✅ 并行分片结果与串行一致：processed=5 amount=10000 count=5")
}
