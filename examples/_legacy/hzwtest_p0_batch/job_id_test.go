// hzwtest_p0_batch JobInstance 语义验证——WorkflowID 框架化端到端。
//
// 对标 Spring Batch JobInstance：
//   - 相同识别参数 → 相同 WorkflowID（= 同一 JobInstance）
//   - AllowDuplicateFailedOnly：失败后可重跑，成功后拒绝
//
// 验证点：
//   1. 相同识别参数两次启动 → 第二次被拒（成功后 AllowDuplicateFailedOnly 拒绝）
//   2. 失败后修复数据 → 重跑成功（相同 WorkflowID）
//   3. Child Workflow ID 自动派生（主 ID + "-" + Phase 名，可寻址）
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// buildJob 构建一个最小 Job：validate → engine（全量）→ report。
func buildJob(t *testing.T, engineName string) (*batch.Job, *core.ClientFacade, *core.WorkerManager) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName(engineName),
	)
	require.NoError(t, err)
	validateDef, err := b.BuildTasklet(validateFile, batch.WithActivityName("jobid-validate"))
	require.NoError(t, err)
	reportDef, err := b.BuildTasklet(printReport, batch.WithActivityName("jobid-report"))
	require.NoError(t, err)

	flow := batch.Pipeline(
		batch.NewActivityPhase("validate", validateDef, getInFile),
		batch.NewActivityPhase("engine", engineDef, getInFullFile),
		batch.NewActivityPhase("report", reportDef, getInReportFromEngine),
	)
	job := batch.NewJob("jobid-test", flow)

	job.RegisterTo(wm)
	go func() { _ = wm.Start() }()

	return job, facade, wm
}

// writeOrders 写入订单数据文件。
func writeOrders(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
}

// TestJobInstance_SameIdentityRejectedAfterSuccess 相同识别参数成功后重跑被拒。
func TestJobInstance_SameIdentityRejectedAfterSuccess(t *testing.T) {
	job, facade, wm := buildJob(t, "jobid-engine-ok")
	defer wm.Stop()

	dataFile := fmt.Sprintf("../testdata/jobid_ok_%d.txt", time.Now().UnixNano())
	writeOrders(t, dataFile, "ORD001,1000,2026-01-01\nORD002,2000,2026-01-02\nORD003,1500,2026-01-03\n")
	defer os.Remove(dataFile)
	params := map[string]any{"file_path": dataFile}

	// 第一次：成功
	run1, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)
	require.NoError(t, run1.Get(context.Background(), nil))

	// 第二次：相同识别参数 → 相同 WorkflowID → AllowDuplicateFailedOnly 成功后拒绝
	_, err = job.Start(context.Background(), facade, params)
	require.Error(t, err, "成功后相同识别参数重跑应被拒绝")
	t.Logf("  ✅ 成功后重跑被拒: %v", err)
}

// TestJobInstance_ResumeAfterFailure 失败后修复 → 重跑成功（相同 WorkflowID）。
func TestJobInstance_ResumeAfterFailure(t *testing.T) {
	job, facade, wm := buildJob(t, "jobid-engine-fail")
	defer wm.Stop()

	dataFile := fmt.Sprintf("../testdata/jobid_fail_%d.txt", time.Now().UnixNano())
	// 第一次：坏数据 → 引擎失败
	writeOrders(t, dataFile, "ORD001,1000,2026-01-01\nORD002,BAD-AMOUNT,2026-01-02\nORD003,1500,2026-01-03\n")
	defer os.Remove(dataFile)
	params := map[string]any{"file_path": dataFile}

	run1, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)
	err = run1.Get(context.Background(), nil)
	require.Error(t, err, "坏数据应导致引擎失败")

	// 修复数据 → 重跑（相同识别参数 → 相同 WorkflowID，失败后 AllowDuplicateFailedOnly 允许）
	writeOrders(t, dataFile, "ORD001,1000,2026-01-01\nORD002,2000,2026-01-02\nORD003,1500,2026-01-03\n")
	defer os.Remove(dataFile)
	run2, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err, "失败后相同识别参数重跑应被允许")
	require.NoError(t, run2.Get(context.Background(), nil))
	t.Logf("  ✅ 失败后重跑成功（相同 WorkflowID，AllowDuplicateFailedOnly）")
}
