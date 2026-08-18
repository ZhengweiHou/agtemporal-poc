// hzwtest_p0_batch 设计文档标准案例验证——对标 hzwtest_案例流程设计.md。
//
// 流程（设计文档 §1）：
//
//	MainWorkflow(filePath, date)
//	  ├─ P1: step1-校验文件  (Activity)   → {exists, valid_count, error_count, total_lines}
//	  ├─ P2: Parallel(
//	  │   ├─ step2a-分片处理 (Child WF)   → {shard_count, processed}  （NewShardPhase，B 落地）
//	  │   └─ step2b-金额汇总 (Activity)   → {total_amount, count}
//	  └─ P3: step3-打印结果 (Activity)    → {report}（汇集 P1+P2 全部）
//
// 识别参数: filePath + date → WorkflowID = hash（设计文档 §1.1）
//
// 验证点（设计文档 §5）：
//  1. Pipeline + Parallel 组合（Activity ∥ 分片 Child WF）
//  2. 分片 Child WF（可寻址 + 幂等级联）
//  3. FlowCtx 跨 Phase k-v 传递（P1 输出供 P2a/P2b 输入，P3 汇集）
//  4. WorkflowID 识别参数推导 + 幂等
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

// step1ValidateFile 校验文件（P1，设计文档 step1）。
func step1ValidateFile(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	filePath := asStr(input.Params["file_path"])
	f, err := os.Open(filePath)
	if err != nil {
		return batch.BatchResult{Output: map[string]any{"exists": false}}, nil
	}
	defer f.Close()
	var total, valid int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		total++
		if len(sc.Text()) > 0 {
			valid++
		}
	}
	return batch.BatchResult{Output: map[string]any{
		"exists": true, "valid_count": valid, "error_count": total - valid, "total_lines": total,
	}}, nil
}

// step2bSumAmounts 金额汇总（P2b，设计文档 step2b）。
func step2bSumAmounts(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	filePath := asStr(input.Params["file_path"])
	f, err := os.Open(filePath)
	if err != nil {
		return batch.BatchResult{}, err
	}
	defer f.Close()
	sum, count := 0, 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if len(fields) >= 2 {
			if n, err := strconv.Atoi(strings.TrimSpace(fields[1])); err == nil {
				sum += n
				count++
			}
		}
	}
	return batch.BatchResult{Output: map[string]any{"total_amount": sum, "count": count}}, nil
}

// step3PrintReport 打印结果（P3，汇集 P1+P2 全部，设计文档 step3）。
func step3PrintReport(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	msg := fmt.Sprintf("file=%v date=%v total=%v valid=%v shards=%v processed=%v amount=%v count=%v",
		input.Params["file_path"], input.Params["date"], input.Params["total_lines"],
		input.Params["valid_count"], input.Params["shard_count"], input.Params["processed"],
		input.Params["total_amount"], input.Params["count"])
	slog.Info("step3-打印结果", "report", msg)
	return batch.BatchResult{Output: map[string]any{"report": msg}}, nil
}

// anySkip 任何 Processor 错误都跳过（坏记录容错，TestDesignCase 用）。
type anySkip struct{}

func (anySkip) ShouldSkip(err error, item any, skipCount int) bool { return true }

// designPartitioner 基于输入 total_lines 拆分坐标（确定性纯内存——分片坐标由 P1 提供，Workflow 内不 IO）。
type designPartitioner struct {
	shardCount int
}

func (p *designPartitioner) Partition(in map[string]any) ([]map[string]any, error) {
	total := asInt(in["total_lines"])
	per := total / p.shardCount
	if total%p.shardCount != 0 {
		per++
	}
	var coords []map[string]any
	for i := 0; i < p.shardCount; i++ {
		start := i * per
		count := per
		if rem := total - start; count > rem {
			count = rem
		}
		if count <= 0 {
			break
		}
		coords = append(coords, map[string]any{
			"shard_id": i, "start": start, "line_count": count, "file_path": in["file_path"],
		})
	}
	return coords, nil
}

// getInFromValidate 从 P1 输出取 total_lines，拼 step2a 输入（file_path + total_lines）。
// shardCount 是分片 flow 定义参数（designPartitioner 内部），不入参（设计文档 §1.1）。
func getInDesignValidate(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	validate, _ := fc.Get("step1-校验文件")
	return map[string]any{
		"file_path":   input.(map[string]any)["file_path"],
		"total_lines": validate.(map[string]any)["total_lines"],
	}, nil
}

// getInFilePath 提取 file_path。
func getInFilePath(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	return map[string]any{"file_path": input.(map[string]any)["file_path"]}, nil
}

// getInReportAll 汇集 P1+P2a+P2b 全部输出（P3 输入，设计文档 §3.3）。
func getInReportAll(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	v, _ := fc.Get("step1-校验文件")
	s, _ := fc.Get("step2a-分片处理")
	m, _ := fc.Get("step2b-金额汇总")
	return map[string]any{
		"file_path":    input.(map[string]any)["file_path"],
		"date":         input.(map[string]any)["date"],
		"total_lines":  v.(map[string]any)["total_lines"],
		"valid_count":  v.(map[string]any)["valid_count"],
		"shard_count":  s.(map[string]any)["shard_count"],
		"processed":    s.(map[string]any)["processed"],
		"total_amount": m.(map[string]any)["total_amount"],
		"count":        m.(map[string]any)["count"],
	}, nil
}

// TestDesignCase 设计文档标准案例端到端验证。
func TestDesignCase(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 构建执行单元（BuildTasklet / BuildActivity 统一） ═══
	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	validateDef, err := b.BuildTasklet(step1ValidateFile, batch.WithActivityName("dc-validate"))
	require.NoError(t, err)
	sumDef, err := b.BuildTasklet(step2bSumAmounts, batch.WithActivityName("dc-sum"))
	require.NoError(t, err)
	reportDef, err := b.BuildTasklet(step3PrintReport, batch.WithActivityName("dc-report"))
	require.NoError(t, err)
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("dc-engine"),
		batch.WithActivitySkipPolicy(anySkip{}),
	)
	require.NoError(t, err)

	// ═══ 编排（设计文档 §1）：Pipeline(P1, Parallel(P2a, P2b), P3) ═══
	flow := batch.Pipeline(
		batch.NewActivityPhase("step1-校验文件", validateDef, getInFilePath),
		batch.Parallel(
			batch.NewShardPhase("step2a-分片处理", &designPartitioner{shardCount: 3}, engineDef, getInDesignValidate),
			batch.NewActivityPhase("step2b-金额汇总", sumDef, getInFilePath),
		),
		batch.NewActivityPhase("step3-打印结果", reportDef, getInReportAll),
	)

	// ═══ NewJob + 识别参数（file_path + date，设计文档 §1.1） ═══
	job := batch.NewJob("hzwtest-design", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// ═══ 启动：唯一文件（幂等防冲突） ═══
	filePath := fmt.Sprintf("../testdata/test_design_%d.txt", time.Now().UnixNano())
	data := "ORD001,1000,2026-01-01\n" +
		"ORD002,2000,2026-01-02\n" +
		"ORD003,BAD-AMOUNT,2026-01-03\n" + // 坏记录（引擎 Skip 或失败）
		"ORD004,3000,2026-01-04\n" +
		"ORD005,2500,2026-01-05\n"
	require.NoError(t, os.WriteFile(filePath, []byte(data), 0644))
	defer os.Remove(filePath)

	params := map[string]any{"file_path": filePath, "date": "2026-08-12"}
	workflowID, err := job.DeriveWorkflowID(params)
	require.NoError(t, err)
	t.Logf("识别参数推导 WorkflowID: %s", workflowID)

	run, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))

	// ═══ 断言（设计文档 §3.3 数据转换表） ═══
	t.Logf("══════════ 设计案例 ══════════")
	t.Logf("  FlowCtx: %+v", result)
	fmt.Printf("  FlowCtx: %+v", result)

	v := result["step1-校验文件"].(map[string]any)
	require.Equal(t, float64(5), v["total_lines"], "P1 校验 5 行")

	s := result["step2a-分片处理"].(map[string]any)
	require.Equal(t, float64(4), s["processed"], "P2a 分片处理 4 条正常（BAD 跳过）")
	require.Equal(t, float64(1), s["skipped"], "P2a 跳过 1 条坏记录")

	m := result["step2b-金额汇总"].(map[string]any)
	require.Equal(t, float64(8500), m["total_amount"], "P2b 金额汇总 1000+2000+3000+2500")
	require.Equal(t, float64(4), m["count"], "P2b 计数 4（坏记录不计）")

	r := result["step3-打印结果"].(map[string]any)
	require.NotNil(t, r["report"], "P3 报告存在")

	t.Logf("  ✅ 设计案例通过：Pipeline + Parallel(分片∥汇总) + FlowCtx + 识别参数")
	slog.Info("设计案例验证通过", "workflowID", workflowID)
}
