// hzwtest_p0_batch Child Workflow ID 可寻址验证。
//
// 验证点：
//   1. PhaseWorkflow（Child Workflow）自动派生 ID：{主 WorkflowID}-{Phase 名}
//   2. 派生出的 Child ID 可查询（DescribeWorkflowExecution）
//   3. 幂等策略级联：AllowDuplicateFailedOnly（主重跑时已完成 Child 不重建）
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// childAuditWf 简单的 Child Workflow：审计校验结果，返回审计信息。
func childAuditWf(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	return map[string]any{"audited": true, "audit_lines": input["total_lines"]}, nil
}

// getInFromValidate 从 validate 输出提取输入（传给 Child Workflow）。
func getInFromValidate(fc *batch.FlowCtx) (map[string]any, error) {
	validate, _ := fc.Get("validate")
	v := validate.(map[string]any)
	return map[string]any{"total_lines": v["total_lines"]}, nil
}

// getInReportFromAudit 合并 validate + audit 构建报告输入。
func getInReportFromAudit(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	validate, _ := fc.Get("validate")
	audit, _ := fc.Get("audit")
	v := validate.(map[string]any)
	a := audit.(map[string]any)
	return map[string]any{
		"file_path":    input.(map[string]any)["file_path"],
		"total_lines":  v["total_lines"],
		"processed":    a["audit_lines"],
		"total_amount": 0,
		"count":        a["audit_lines"],
	}, nil
}

// TestChildWorkflowID_DerivedAndQueryable 验证 Child ID 自动派生 + 可查询。
func TestChildWorkflowID_DerivedAndQueryable(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// 编排：validate → child(审计) → report
	flow := batch.Pipeline(
		batch.NewActivityPhase("validate", validateFile, getInFile),
		batch.NewWorkflowPhase("audit", childAuditWf, getInFromValidate),
		batch.NewActivityPhase("report", printReport, getInReportFromAudit),
	)
	job := batch.NewJob("childid-test", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	dataFile := fmt.Sprintf("../testdata/childid_%d.txt", time.Now().UnixNano())
	writeOrders(t, dataFile, "ORD001,1000,2026-01-01\nORD002,2000,2026-01-02\n")
	params := map[string]any{"file_path": dataFile}

	run, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))
	t.Logf("  FlowCtx: %+v", result)

	// 验证 Child WorkflowID 自动派生：{主 ID}-audit
	mainID, err := job.DeriveWorkflowID(params)
	require.NoError(t, err)
	childID := mainID + "-audit"
	t.Logf("  主 ID: %s", mainID)
	t.Logf("  Child ID（派生）: %s", childID)

	// Child 可寻址：Describe 查询确认 Completed
	desc, err := facade.GetRawClient().DescribeWorkflowExecution(context.Background(), childID, "")
	require.NoError(t, err, "Child Workflow 应可通过派生 ID 查询")
	require.Equal(t, "Completed", desc.GetWorkflowExecutionInfo().GetStatus().String(), "Child 应已完成")
	t.Logf("  ✅ Child ID 可寻址: %s → %s", childID, desc.GetWorkflowExecutionInfo().GetStatus())

	// Child 输出正确
	childOut := result["audit"].(map[string]any)
	require.Equal(t, true, childOut["audited"])
	require.Equal(t, float64(2), childOut["audit_lines"])
	t.Logf("  ✅ Child 输出: %+v", childOut)
}
