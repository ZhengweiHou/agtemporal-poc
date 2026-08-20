// hzwtest_p0_batch 分片 Child Workflow 可寻址验证——B 落地后的核心价值。
//
// 验证点：
//   1. NewShardPhase 每个分片自动生成 Child Workflow（可推导 ID：{主ID}-shard-{n}）
//   2. 分片 Child 可寻址：DescribeWorkflowExecution 查询单个分片状态（Completed）
//   3. 分片结果正确（processed/amount 聚合）
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// TestShardPhaseChildAddressable 验证分片 Child Workflow 可寻址（Describe 单分片）。
func TestShardPhaseChildAddressable(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 构建引擎 + 编排（validate → shard → report） ═══
	b := batch.NewBuilder(batch.WithChunkSize(2), batch.WithMaxAttempts(2))
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("addr-engine"),
	)
	require.NoError(t, err)
	validateDef, err := b.BuildTasklet(validateFile, batch.WithActivityName("addr-validate"))
	require.NoError(t, err)
	reportDef, err := b.BuildTasklet(printReport, batch.WithActivityName("addr-report"))
	require.NoError(t, err)

	flow := batch.Pipeline(
		batch.NewActivityPhase("validate", validateDef, getInFile),
		batch.NewShardPhase("shard", &filePartitioner{shardCount: 3, totalLines: 5}, engineDef, getInFile),
		batch.NewActivityPhase("report", reportDef, getInReportFromShard),
	)
	job := batch.NewJob("addr-test", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// ═══ 启动（唯一识别参数避免幂等冲突） ═══
	filePath := fmt.Sprintf("../testdata/test_orders_addr_%d.txt", time.Now().UnixNano())
	fileData, err := os.ReadFile("../testdata/test_orders.txt")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filePath, fileData, 0644))
	defer os.Remove(filePath)

	run, err := job.Start(context.Background(), facade, map[string]any{"file_path": filePath})
	require.NoError(t, err)
	mainID := run.GetID()

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))

	// ═══ 验证：分片 Child 可寻址（Describe 每个分片） ═══
	shardAgg := result["shard"].(map[string]any)
	require.Equal(t, 5, asInt(shardAgg["processed"]), "3 分片共处理 5 行")

	// 分片 Child ID 可推导：{主ID}-shard-{n}
	raw := facade.GetRawClient()
	for i := 0; i < 3; i++ {
		childID := fmt.Sprintf("%s-shard-%d", mainID, i)
		desc, err := raw.DescribeWorkflowExecution(context.Background(), childID, "")
		require.NoError(t, err, "分片 %s 应可寻址", childID)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc.GetWorkflowExecutionInfo().GetStatus(),
			"分片 %s 应 Completed", childID)
		fmt.Printf("分片可寻址 ✅ childID=%s status=%v\n", childID, desc.GetWorkflowExecutionInfo().GetStatus())
	}
}
