// hzwtest_p0_batch 编排层验证——Phase + Pipeline/Parallel + FlowCtx。
//
// 对标 Spring Batch Flow DSL：split().add().next()
//   Parallel(countLines, sumAmounts) → report
//   即：并行「计数 ∥ 求和」→ 串行「报告」（报告消费前两者的输出）
//
// 验证点：
//   1. Pipeline 串行编排
//   2. Parallel 并行编排（两个 Activity 并发）
//   3. FlowCtx 数据传递（count/sum 的输出传给 report）
//   4. Compile 把 Phase 树编译成 Workflow
//   5. CollectActivities 收集叶子 Activity 注册
package hzwtest_p0_batch

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// ── 三个简单 Activity（用于编排验证） ──

// countLines 计数文件行数。
func countLines(ctx context.Context, input map[string]any) (map[string]any, error) {
	filePath := asStr(input["file_path"])
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	total := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		total++
	}
	slog.Info("countLines 完成", "total", total)
	return map[string]any{"total_lines": total}, nil
}

// sumAmounts 汇总文件金额。
func sumAmounts(ctx context.Context, input map[string]any) (map[string]any, error) {
	filePath := asStr(input["file_path"])
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sum := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if len(fields) >= 2 {
			if n, err := strconv.Atoi(strings.TrimSpace(fields[1])); err == nil {
				sum += n
			}
		}
	}
	slog.Info("sumAmounts 完成", "sum", sum)
	return map[string]any{"total_amount": sum}, nil
}

// buildReport 合并 count + sum 的输出，生成报告。
func buildReport(ctx context.Context, input map[string]any) (map[string]any, error) {
	msg := fmt.Sprintf("total=%v amount=%v", input["total_lines"], input["total_amount"])
	slog.Info("buildReport 完成", "report", msg)
	return map[string]any{"report": msg}, nil
}

// ── getIn 函数（FlowCtx 输入提取） ──

// getInFile 从初始 input 提取 file_path。
func getInFile(fc *batch.FlowCtx) (map[string]any, error) {
	v, _ := fc.Get("input")
	filePath := v.(map[string]any)["file_path"]
	return map[string]any{"file_path": filePath}, nil
}

// getInReport 合并 count + sum 的输出。
func getInReport(fc *batch.FlowCtx) (map[string]any, error) {
	count, _ := fc.Get("count")
	sum, _ := fc.Get("sum")
	return map[string]any{
		"total_lines":  count.(map[string]any)["total_lines"],
		"total_amount": sum.(map[string]any)["total_amount"],
	}, nil
}

// TestPhaseOrchestration 验证 Phase + Pipeline/Parallel + FlowCtx。
func TestPhaseOrchestration(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 编排定义（声明式，对标 Spring Batch split().add().next()） ═══
	flow := batch.Pipeline(
		batch.Parallel(
			batch.NewActivityPhase("count", countLines, getInFile),
			batch.NewActivityPhase("sum", sumAmounts, getInFile),
		),
		batch.NewActivityPhase("report", buildReport, getInReport),
	)

	// ═══ 编译 + 注册 ═══
	wf := batch.Compile(flow)
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: wf, Options: core.WorkflowDefOptions{Name: "phase-orchestration"}})
	for _, fn := range flow.CollectActivities() {
		wm.RegisterActivity(fn)
	}

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// ═══ 提交 ═══
	filePath := "../testdata/test_orders.txt"
	workflowID := fmt.Sprintf("hzwtest-phase-%d", time.Now().UnixNano())
	run, err := facade.StartWorkflow(context.Background(), workflowID, "phase-orchestration",
		map[string]any{"file_path": filePath})
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))

	t.Log("══════════ Phase 编排 ══════════")
	t.Logf("  FlowCtx 全部输出: %+v", result)

	// FlowCtx 应含 count/sum/report 三个 Phase 的输出
	countOut, ok := result["count"].(map[string]any)
	require.True(t, ok, "FlowCtx 应含 count 输出")
	sumOut, ok := result["sum"].(map[string]any)
	require.True(t, ok, "FlowCtx 应含 sum 输出")
	reportOut, ok := result["report"].(map[string]any)
	require.True(t, ok, "FlowCtx 应含 report 输出")

	require.Equal(t, float64(5), countOut["total_lines"], "count 应得 5 行")
	require.Equal(t, float64(10000), sumOut["total_amount"], "sum 应得 10000")
	t.Logf("  ✅ 数据流转成功: count=%v sum=%v report=%v", countOut["total_lines"], sumOut["total_amount"], reportOut["report"])
}
