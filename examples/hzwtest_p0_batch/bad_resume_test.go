// hzwtest_p0_batch 异常 + 断点恢复验证。
//
// 验证点：
//   1. 坏数据 → 引擎 Activity 失败 → 错误传播
//   2. PositionAware 断点恢复：Reader.Seek 被正确调用（batch 包单元测试已覆盖引擎逻辑，这里验证端到端）
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

// TestMainWorkflowP0BatchBadData 验证坏数据导致引擎失败。
func TestMainWorkflowP0BatchBadData(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(
		batch.WithChunkSize(3),
		batch.WithMaxAttempts(1), // 不重试，快速失败
	)
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName(actEngine),
	)
	require.NoError(t, err)

	wm.RegisterActivity(engineDef)
	wm.RegisterActivity(&core.ActivityDef{Fn: validateFile, Options: core.ActivityDefOptions{Name: actValidateFile}})
	wm.RegisterActivity(&core.ActivityDef{Fn: splitFile, Options: core.ActivityDefOptions{Name: actSplitFile}})
	wm.RegisterActivity(&core.ActivityDef{Fn: printReport, Options: core.ActivityDefOptions{Name: actPrintReport}})
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: shardProcessWorkflow(actEngine), Options: core.WorkflowDefOptions{Name: wfShardProcess}})
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: MainWorkflow(actEngine), Options: core.WorkflowDefOptions{Name: wfMain}})

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// 坏数据文件
	badFile := "../testdata/test_orders_bad.txt"
	badData := "ORD001,1000,2026-01-01\n" +
		"ORD002,BAD-AMOUNT,2026-01-02\n" + // ← 金额非数字
		"ORD003,1500,2026-01-03\n"
	require.NoError(t, os.WriteFile(badFile, []byte(badData), 0644))

	workflowID := fmt.Sprintf("hzwtest-batch-bad-%d", time.Now().UnixNano())
	run, err := facade.StartWorkflow(context.Background(), workflowID, wfMain,
		map[string]any{"file_path": badFile, "date": "2026-08-12"})
	require.NoError(t, err)

	var result map[string]any
	err = run.Get(context.Background(), &result)
	require.Error(t, err, "坏数据应导致引擎失败")
	slog.Info("坏数据失败传播验证通过", "err", err)
	t.Logf("✅ 坏数据导致引擎失败，错误传播: %v", err)
}

// TestShardReader_Seek 验证 PositionAware 断点恢复的 Reader 行为（单元级）。
// 引擎的断点恢复逻辑（HasHeartbeatDetails → Seek）在 batch 包 TestRunChunkLoop_ResumePositionAware 已覆盖，
// 这里验证 shardReader 的 Seek 语义正确。
func TestShardReader_Seek(t *testing.T) {
	filePath := "../testdata/test_orders.txt" // 5 行
	r, err := newShardReader(filePath, 0, 5)
	require.NoError(t, err)

	// 全量读
	items, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Len(t, items, 5, "应读满 5 行")

	// Seek 到 offset=2，再读应只剩 3 行
	require.NoError(t, r.Seek(2))
	items, err = r.Read(context.Background())
	require.NoError(t, err)
	require.Len(t, items, 3, "Seek(2) 后应剩 3 行")

	// Seek 越界报错
	require.Error(t, r.Seek(99))
}

// TestSumWriter_Result 验证 ResultProvider 产出业务结果。
func TestSumWriter_Result(t *testing.T) {
	w := &sumWriter{}
	require.NoError(t, w.Write(context.Background(), []any{1000, 2000, 1500}))
	result := w.Result()
	require.Equal(t, 4500, result["total_amount"])
	require.Equal(t, 3, result["count"])
}
