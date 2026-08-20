// hzwtest_p0_batch 设计案例续跑场景——主 WF 失败 → 重跑 → 分片幂等级联。
//
// 对标设计文档 §3.2 异常恢复时序图：
//   第一次: 坏数据（BAD-AMOUNT）→ 分片 Child 失败 → 主 WF 失败 ✗
//   修正:   BAD-AMOUNT → 2000
//   第二次: 相同识别参数（file_path + date）→ 同 WorkflowID 新 RunID
//           → 已完成分片跳过（AlreadyStarted，skipped_shards>0）
//           → 失败分片重跑成功
//           → 主 WF 成功 ✓
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// failProcessor 坏数据返回错误（区别于 Skip——用于构造引擎失败场景）。
type failProcessor struct{}

func (p *failProcessor) Process(ctx context.Context, item any) (any, error) {
	line, _ := item.(string)
	fields := strings.Split(line, ",")
	if len(fields) < 2 {
		return nil, fmt.Errorf("格式错误: %q", line)
	}
	if strings.TrimSpace(fields[1]) == "BAD-AMOUNT" {
		return nil, fmt.Errorf("金额非法: %s", fields[1])
	}
	return map[string]any{"order_id": fields[0], "amount": fields[1]}, nil
}

// TestDesignCaseResumeAfterFailure 设计案例续跑：失败 → 重跑 → 分片幂等级联。
func TestDesignCaseResumeAfterFailure(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 构建（failProcessor 引擎——坏数据导致失败，区别于 Skip） ═══
	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	validateDef, err := b.BuildTasklet(step1ValidateFile, batch.WithActivityName("rs-validate"))
	require.NoError(t, err)
	sumDef, err := b.BuildTasklet(step2bSumAmounts, batch.WithActivityName("rs-sum"))
	require.NoError(t, err)
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &failProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("rs-engine"),
	)
	require.NoError(t, err)

	flow := batch.Pipeline(
		batch.NewActivityPhase("step1-校验文件", validateDef, getInFilePath),
		batch.Parallel(
			batch.NewShardPhase("step2a-分片处理", &designPartitioner{shardCount: 3}, engineDef, getInDesignValidate),
			batch.NewActivityPhase("step2b-金额汇总", sumDef, getInFilePath),
		),
	)
	job := batch.NewJob("hzwtest-rs", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// ═══ 数据文件：第 5 行坏数据（shard-2 命中，designPartitioner 3 片: [0,2) [2,4) [4,5)） ═══
	filePath := fmt.Sprintf("../testdata/test_rs_%d.txt", time.Now().UnixNano())
	data := "ORD001,1000,2026-01-01\n" +
		"ORD002,2000,2026-01-02\n" +
		"ORD003,3000,2026-01-03\n" +
		"ORD004,4000,2026-01-04\n" +
		"ORD005,BAD-AMOUNT,2026-01-05\n" // ← shard-2 失败
	require.NoError(t, os.WriteFile(filePath, []byte(data), 0644))
	defer os.Remove(filePath)

	// 识别参数：file_path + date（固定，两次跑相同 → 同 WorkflowID）
	params := map[string]any{"file_path": filePath, "date": "2026-08-13"}

	// ═══ 第一次：坏数据 → 分片失败 → 主 WF 失败 ═══
	run1, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)
	workflowID := run1.GetID()
	t.Logf("第一次启动 WorkflowID: %s", workflowID)
	var result1 map[string]any
	err1 := run1.Get(context.Background(), &result1)
	require.Error(t, err1, "第一次应失败（shard-2 坏数据）")
	t.Logf("第一次失败 ✅: %v", err1)

	// ═══ 修正数据（BAD-AMOUNT → 2000） ═══
	fixed := "ORD001,1000,2026-01-01\n" +
		"ORD002,2000,2026-01-02\n" +
		"ORD003,3000,2026-01-03\n" +
		"ORD004,4000,2026-01-04\n" +
		"ORD005,2000,2026-01-05\n"
	require.NoError(t, os.WriteFile(filePath, []byte(fixed), 0644))

	// ═══ 第二次：相同识别参数 → 同 WorkflowID 重跑（幂等级联） ═══
	run2, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)
	require.Equal(t, workflowID, run2.GetID(), "相同识别参数 → 相同 WorkflowID")

	var result2 map[string]any
	require.NoError(t, run2.Get(context.Background(), &result2), "第二次应成功")

	t.Logf("══════════ 续跑场景 ══════════")
	t.Logf("  FlowCtx: %+v", result2)

	// ═══ 断言：分片幂等级联 ═══
	s := result2["step2a-分片处理"].(map[string]any)
	t.Logf("  step2a: %+v", s)
	skipped, hasSkipped := s["skipped_shards"]
	require.True(t, hasSkipped, "聚合应含 skipped_shards 标记")
	require.Equal(t, float64(2), skipped, "已完成分片 shard-0/1 应被跳过（skipped_shards=2）")
	t.Logf("  ✅ 幂等级联：shard-0/1 跳过（skipped_shards=2），shard-2 重跑成功")

	m := result2["step2b-金额汇总"].(map[string]any)
	require.Equal(t, float64(12000), m["total_amount"], "修正后汇总 1000+2000+3000+4000+2000")
	t.Logf("  ✅ P2b 汇总：amount=%v count=%v", m["total_amount"], m["count"])

	slogInfo := result2["step2a-分片处理"].(map[string]any)
	require.True(t, asInt(slogInfo["processed"]) >= 1, "shard-2 重跑处理至少 1 行")
}
