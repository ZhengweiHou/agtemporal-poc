// 问题 P1 实测复现：Child Workflow Phase 重跑失败（无 AlreadyStarted 识别）。
//
// 场景：Job = Pipeline(A=ChildWF Phase, B=Activity 数据失败)
//   第一次: A ✅（Child 完成）→ B ❌（数据含坏标记）→ 主 WF 失败
//   修复后重跑: A 的 Child 已完成（同 ID + AllowDuplicateFailedOnly）
//     → fut.Get 返回 AlreadyStarted → PhaseWorkflow 无识别 → 主 WF 失败（bug 复现）
// 正确行为: 应跳过 A（幂等级联）继续 B（修复后成功）→ 主 WF 成功
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

// stepBActivity 读文件内容：含 BAD → 失败（模拟 B 数据失败）；否则成功。
func stepBActivity(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	filePath := asStr(input.Params["file_path"])
	data, err := os.ReadFile(filePath)
	if err != nil {
		return batch.BatchResult{}, err
	}
	if strings.Contains(string(data), "BAD") {
		return batch.BatchResult{}, fmt.Errorf("stepB: 数据含坏标记")
	}
	return batch.BatchResult{Output: map[string]any{"b_ok": true}}, nil
}

// getInStepB 从 A（Child Phase 输出）取数据传给 B。
func getInStepB(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	return map[string]any{"file_path": input.(map[string]any)["file_path"]}, nil
}

// TestRepro_ChildPhaseResume P1 回归测试：Child Phase 重跑跳过（AlreadyStarted 识别）。
// 修复后：A(Child) 上次 Run 已完成 → 重跑时跳过（skipped 标记）→ B 继续执行。
func TestRepro_ChildPhaseResume(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithMaxAttempts(2))
	stepBDef, err := b.BuildTasklet(stepBActivity, batch.WithActivityName("repro-stepb"))
	require.NoError(t, err)

	// Job = Pipeline(A=ChildWF, B=Activity)
	flow := batch.Pipeline(
		batch.NewWorkflowPhase("stepA", childAuditWf, getInFilePath),
		batch.NewActivityPhase("stepB", stepBDef, getInStepB),
	)
	job := batch.NewJob("repro-childphase", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// 数据：第一次含 BAD（B 失败），修复后正常
	filePath := fmt.Sprintf("../testdata/test_repro_childphase_%d.txt", time.Now().UnixNano())
	require.NoError(t, os.WriteFile(filePath, []byte("ORD001,1000\nBAD\n"), 0644))
	defer os.Remove(filePath)
	params := map[string]any{"file_path": filePath}

	// ═══ 第一次：A ✅ → B ❌ → 主 WF 失败 ═══
	run1, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)
	workflowID := run1.GetID()
	var result1 map[string]any
	err1 := run1.Get(context.Background(), &result1)
	require.Error(t, err1, "第一次应失败（B 数据失败）")
	t.Logf("第一次失败 ✅（A 的 Child 已完成）: %v", err1)

	// ═══ 修复数据 ═══
	require.NoError(t, os.WriteFile(filePath, []byte("ORD001,1000\nGOOD\n"), 0644))

	// ═══ 第二次：重跑 → A 的 Child 已完成 → 观察行为 ═══
	run2, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)
	require.Equal(t, workflowID, run2.GetID(), "相同识别参数 → 相同 WorkflowID")

	var result2 map[string]any
	err2 := run2.Get(context.Background(), &result2)
	t.Logf("第二次结果: err=%v result=%+v", err2, result2)

	require.NoError(t, err2, "P1 修复后重跑应成功（A 跳过 + B 继续）")
	stepA, hasA := result2["stepA"]
	require.True(t, hasA, "A 的标记应存在（skipped）")
	sa := stepA.(map[string]any)
	require.Equal(t, true, sa["skipped"], "A 应标记为 skipped（幂等跳过）")
	stepB, hasB := result2["stepB"]
	require.True(t, hasB, "B 应执行")
	require.Equal(t, true, stepB.(map[string]any)["b_ok"], "B 应成功")
	t.Logf("✅ P1 修复验证通过：重跑时 A（Child）被幂等跳过（skipped 标记），B 继续执行成功")
}
