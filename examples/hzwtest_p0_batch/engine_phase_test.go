// hzwtest_p0_batch 引擎 Phase + Child Workflow Phase 验证。
//
// 对标 Spring Batch：chunk-oriented Step（引擎）和自定义 Step（Tasklet）统一纳入 Flow DSL。
//
// 验证点：
//   1. 引擎 Activity（BuildActivity 产物）通过 NewEnginePhase 纳入编排
//   2. Child Workflow 通过 NewWorkflowPhase 纳入编排
//   3. 引擎结果 BatchResult{Processed, Output} 转 map 存入 FlowCtx，下游消费
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

// getInFullFile 引擎 Phase 输入：读整个文件。
func getInFullFile(fc *batch.FlowCtx) (map[string]any, error) {
	v, _ := fc.Get("input")
	filePath := v.(map[string]any)["file_path"]
	return map[string]any{"file_path": filePath, "start_line": 0, "line_count": 999999}, nil
}

// getInReportFromEngine 合并 validate + engine 的输出。
func getInReportFromEngine(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	validate, _ := fc.Get("validate")
	engine, _ := fc.Get("engine")
	return map[string]any{
		"file_path":    input.(map[string]any)["file_path"],
		"total_lines":  validate.(map[string]any)["total_lines"],
		"processed":    engine.(map[string]any)["processed"],
		"total_amount": engine.(map[string]any)["total_amount"],
		"count":        engine.(map[string]any)["count"],
	}, nil
}

// TestEnginePhase 验证引擎 Activity 纳入 Phase 编排。
func TestEnginePhase(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 构建引擎 Activity ═══
	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("phase-engine"),
	)
	require.NoError(t, err)
	validateDef, err := b.BuildTasklet(validateFile, batch.WithActivityName("phase-validate"))
	require.NoError(t, err)
	reportDef, err := b.BuildTasklet(printReport, batch.WithActivityName("phase-report"))
	require.NoError(t, err)

	// ═══ 编排：validate → engine → report（引擎与自定义统一 NewActivityPhase 持 def）═══
	flow := batch.Pipeline(
		batch.NewActivityPhase("validate", validateDef, getInFile),
		batch.NewActivityPhase("engine", engineDef, getInFullFile),
		batch.NewActivityPhase("report", reportDef, getInReportFromEngine),
	)

	// ═══ 注册：一体化（CollectDefs 收集引擎 + 自定义 Activity）═══
	for _, def := range flow.CollectDefs() {
		wm.RegisterActivity(def)
	}
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: batch.Compile(flow), Options: core.WorkflowDefOptions{Name: "engine-phase-wf"}})

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := "../testdata/test_orders.txt"
	workflowID := fmt.Sprintf("hzwtest-engine-phase-%d", time.Now().UnixNano())
	run, err := facade.StartWorkflow(context.Background(), workflowID, "engine-phase-wf",
		map[string]any{"file_path": filePath})
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))

	t.Log("══════════ 引擎 Phase ══════════")
	t.Logf("  FlowCtx: %+v", result)

	engine, ok := result["engine"].(map[string]any)
	require.True(t, ok, "FlowCtx 应含 engine 输出")
	require.Equal(t, float64(5), engine["processed"], "引擎应处理 5 行")
	require.Equal(t, float64(10000), engine["total_amount"], "引擎汇总金额 10000")
	require.Equal(t, float64(5), engine["count"], "引擎计数 5")
	t.Logf("  ✅ 引擎 Activity 纳入 Phase：processed=%v amount=%v count=%v",
		engine["processed"], engine["total_amount"], engine["count"])
}

// ── Child Workflow Phase 验证 ──

// childAuditWorkflow 简单子 Workflow（模拟审计步骤）。
func childAuditWorkflow(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	slog.Info("childAuditWorkflow 执行", "input", input)
	// 模拟审计：返回审计结果
	totalLines := asInt(input["total_lines"])
	return map[string]any{"audited": true, "audit_lines": totalLines}, nil
}

// getInAudit 从 validate 输出提取审计输入。
func getInAudit(fc *batch.FlowCtx) (map[string]any, error) {
	validate, _ := fc.Get("validate")
	return map[string]any{"total_lines": validate.(map[string]any)["total_lines"]}, nil
}

// TestChildWorkflowPhase 验证 Child Workflow 纳入 Phase。
func TestChildWorkflowPhase(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 构建自定义 Activity（BuildTasklet）═══
	b := batch.NewBuilder(batch.WithMaxAttempts(2))
	validateDef, err := b.BuildTasklet(validateFile, batch.WithActivityName("childwf-validate"))
	require.NoError(t, err)

	// ═══ 编排：validate → audit(Child WF) ═══
	flow := batch.Pipeline(
		batch.NewActivityPhase("validate", validateDef, getInFile),
		batch.NewWorkflowPhase("audit", childAuditWorkflow, getInAudit),
	)

	// ═══ 注册：一体化（CollectDefs + Child Workflow + Workflow）═══
	for _, def := range flow.CollectDefs() {
		wm.RegisterActivity(def)
	}
	for _, fn := range flow.CollectWorkflows() {
		wm.RegisterWorkflow(fn)
	}
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: batch.Compile(flow), Options: core.WorkflowDefOptions{Name: "child-wf-phase"}})

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := "../testdata/test_orders.txt"
	workflowID := fmt.Sprintf("hzwtest-child-wf-%d", time.Now().UnixNano())
	run, err := facade.StartWorkflow(context.Background(), workflowID, "child-wf-phase",
		map[string]any{"file_path": filePath})
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))

	t.Log("══════════ Child Workflow Phase ══════════")
	t.Logf("  FlowCtx: %+v", result)

	audit, ok := result["audit"].(map[string]any)
	require.True(t, ok, "FlowCtx 应含 audit 输出")
	require.Equal(t, true, audit["audited"], "审计应完成")
	t.Logf("  ✅ Child Workflow 纳入 Phase：audited=%v audit_lines=%v", audit["audited"], audit["audit_lines"])
}
