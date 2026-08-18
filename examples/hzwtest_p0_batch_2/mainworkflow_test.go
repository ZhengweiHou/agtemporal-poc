// hzwtest_p0_batch_2 —— 独立完整实现 hzwtest_案例流程设计（对标设计文档 §1）。
//
// 单文件自包含：不依赖其他测试包的符号。
//
// 案例结构（设计文档 §1）：
//   MainWorkflow(filePath, date)
//     ├─ P1: step1-校验文件       ← Activity（自定义，BuildTasklet）
//     ├─ P2: Parallel
//     │   ├─ step2a-分片处理      ← Child Workflow（NewShardPhase 内部生成）
//     │   │     内部: 按 total_lines 拆坐标（Partitioner 确定性）→ 引擎 Activity ×N
//     │   └─ step2b-金额汇总      ← Activity（自定义，BuildTasklet）
//     └─ P3: step3-打印结果       ← Activity（自定义，BuildTasklet）
//
// 识别参数: filePath + date（实例区分）；shardCount 是分片 flow 定义参数（不入参）。
// run_id: 每次运行变化的 input 变量（参与 WorkflowID）——防残留 Run 复用。
package hzwtest_p0_batch_2

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
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// shardCount 是分片 flow 的定义参数（设计文档 §1.1：不入参、不走 FlowCtx）。
const shardCount = 4

const taskQueue = "hzwtest-p0-batch-2"

// ── map 取值 helper ──

func asInt(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	case string:
		n, _ := strconv.Atoi(x)
		return n
	}
	return 0
}

func asStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// ═══════════════════════════════════════════════════════
// 引擎组件：Reader / Processor / Writer（batch 接口实现）
// ═══════════════════════════════════════════════════════

// engineReaderFactory 从输入读分片坐标，创建 engineReader。
type engineReaderFactory struct{}

func (f *engineReaderFactory) NewReader(ctx context.Context, input batch.BatchInput) (batch.Reader, error) {
	filePath := asStr(input.Params["file_path"])
	start := asInt(input.Params["start"])
	lineCount := asInt(input.Params["line_count"])
	return newEngineReader(filePath, start, lineCount)
}

// engineReader 读文件的 [start, start+lineCount) 行。实现 Reader + RestartableReader（嵌入 OffsetState）。
type engineReader struct {
	batch.OffsetState
	lines []any
}

func newEngineReader(filePath string, start, lineCount int) (*engineReader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var all []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		all = append(all, sc.Text())
	}
	end := start + lineCount
	if end > len(all) {
		end = len(all)
	}
	if start > len(all) {
		start = len(all)
	}
	seg := all[start:end]

	lines := make([]any, len(seg))
	for i, l := range seg {
		lines[i] = l
	}
	return &engineReader{lines: lines}, nil
}

func (r *engineReader) Read(ctx context.Context) ([]any, error) {
	if r.Offset >= len(r.lines) {
		return nil, nil // EOF
	}
	item := r.lines[r.Offset]
	r.Offset++
	return []any{item}, nil
}

// amountProcessor 解析 "order_id,amount,date" 的金额（BAD-AMOUNT → 错误，模拟数据异常）。
type amountProcessor struct{}

func (p *amountProcessor) Process(ctx context.Context, item any) (any, error) {
	line, _ := item.(string)
	fields := strings.Split(line, ",")
	if len(fields) < 2 {
		return nil, fmt.Errorf("格式错误: %q", line)
	}
	amount, err := strconv.Atoi(strings.TrimSpace(fields[1]))
	if err != nil {
		return nil, fmt.Errorf("金额解析失败: %q", fields[1])
	}
	return amount, nil
}

// sumWriterFactory 创建 sumWriter。
type sumWriterFactory struct{}

func (f *sumWriterFactory) NewWriter(ctx context.Context, input batch.BatchInput) (batch.Writer, error) {
	return &sumWriter{}, nil
}

// sumWriter 汇总金额。实现 batch.Writer + batch.ResultProvider。
type sumWriter struct {
	totalAmount int
	count       int
}

func (w *sumWriter) Write(ctx context.Context, items []any) error {
	for _, it := range items {
		if amount, ok := it.(int); ok {
			w.totalAmount += amount
			w.count++
		}
	}
	return nil
}

func (w *sumWriter) Result() map[string]any {
	return map[string]any{"total_amount": w.totalAmount, "count": w.count}
}

// ═══════════════════════════════════════════════════════
// P1: step1-校验文件（自定义 Activity）
// ═══════════════════════════════════════════════════════

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

// ═══════════════════════════════════════════════════════
// P2b: step2b-金额汇总（自定义 Activity）
// ═══════════════════════════════════════════════════════

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

// ═══════════════════════════════════════════════════════
// P3: step3-打印结果（自定义 Activity）
// ═══════════════════════════════════════════════════════

func step3PrintReport(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	msg := fmt.Sprintf("file=%v date=%v total=%v valid=%v errors=%v shards=%v processed=%v amount=%v count=%v",
		input.Params["file_path"], input.Params["date"],
		input.Params["total_lines"], input.Params["valid_count"], input.Params["error_count"],
		input.Params["shard_count"], input.Params["processed"],
		input.Params["total_amount"], input.Params["count"])
	slog.Info("step3PrintReport", "report", msg)
	return batch.BatchResult{Output: map[string]any{"report": msg}}, nil
}

// ═══════════════════════════════════════════════════════
// P2a: 分片坐标 Partitioner（设计文档 §2 的 splitFile 拆分逻辑，
// 由 Partitioner 承担——确定性纯内存，坐标基于 P1 提供的 total_lines）
// ═══════════════════════════════════════════════════════

type shardPartitioner struct{}

func (p *shardPartitioner) Partition(in map[string]any) ([]map[string]any, error) {
	total := asInt(in["total_lines"])
	per := total / shardCount
	if total%shardCount != 0 {
		per++
	}
	var coords []map[string]any
	for i := 0; i < shardCount; i++ {
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

// ═══════════════════════════════════════════════════════
// P1 包装成 flow：step1ValidateFlow（Child WF）——内部调度 step1ValidateFile Activity
// ═══════════════════════════════════════════════════════

// step1ValidateFlow 把"校验"包装成 flow（Child WF）：
// 例证"Activity 包装成 flow"——单个 Activity 包进 flow 后获得 Child WF 的能力
// （可寻址/独立执行/未来内部可扩展为多 Activity 编排而不改编排层）。
func step1ValidateFlow(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	ao := workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Minute}
	var res batch.BatchResult
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, ao),
		"v2-step1-validate", // 字符串名（Child WF 内调度）
		batch.BatchInput{Params: input},
	).Get(ctx, &res)
	if err != nil {
		return nil, err
	}
	return res.Output, nil
}

// ═══════════════════════════════════════════════════════
// getIn：FlowCtx → Phase 输入
// ═══════════════════════════════════════════════════════

func getInFilePath(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	return map[string]any{"file_path": asStr(input.(map[string]any)["file_path"])}, nil
}

func getInShard(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	validate, _ := fc.Get("step1-校验文件")
	return map[string]any{
		"file_path":   asStr(input.(map[string]any)["file_path"]),
		"total_lines": validate.(map[string]any)["total_lines"],
	}, nil
}

func getInReport(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	validate, _ := fc.Get("step1-校验文件")
	shard, _ := fc.Get("step2a-分片处理")
	sum, _ := fc.Get("step2b-金额汇总")
	v := validate.(map[string]any)
	s := shard.(map[string]any)
	m := sum.(map[string]any)
	return map[string]any{
		"file_path":    asStr(input.(map[string]any)["file_path"]),
		"date":         asStr(input.(map[string]any)["date"]),
		"total_lines":  v["total_lines"],
		"valid_count":  v["valid_count"],
		"error_count":  v["error_count"],
		"shard_count":  s["shard_count"],
		"processed":    s["processed"],
		"total_amount": m["total_amount"],
		"count":        m["count"],
	}, nil
}

// ═══════════════════════════════════════════════════════
// 测试：完整案例（NewJob 一体化 + 编排 Phase + 分片=Child WF）
// ═══════════════════════════════════════════════════════

func newConfig() *core.Config {
	cfg := core.NewConfig()
	// cfg.Server.HostPort = "172.17.0.1:7233"
	cfg.Server.HostPort = "127.0.0.1:7233"
	cfg.Worker.TaskQueue = taskQueue
	return cfg
}

// TestBatchCaseV2 完整案例：Pipeline(step1, Parallel(step2a∥step2b), step3)。
func TestBatchCaseV2(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 执行单元：引擎 BuildActivity + 自定义 BuildTasklet ═══
	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	engineDef, err := b.BuildActivity(
		&engineReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("v2-engine"),
	)
	require.NoError(t, err)
	validateDef, err := b.BuildTasklet(step1ValidateFile, batch.WithActivityName("v2-step1-validate"))
	require.NoError(t, err)
	sumDef, err := b.BuildTasklet(step2bSumAmounts, batch.WithActivityName("v2-step2b-sum"))
	require.NoError(t, err)
	reportDef, err := b.BuildTasklet(step3PrintReport, batch.WithActivityName("v2-step3-report"))
	require.NoError(t, err)

	// ═══ 编排（设计文档 §1）：P1 包装成 flow → Parallel(P2a∥P2b) → P3 ═══
	flow := batch.Pipeline(
		batch.NewWorkflowPhase("step1-校验文件", step1ValidateFlow, getInFilePath), // P1: Activity 包装成 flow（Child WF）
		batch.Parallel( // P2
			batch.NewShardPhase("step2a-分片处理", &shardPartitioner{}, engineDef, getInShard), // P2a: 分片 Child WF
			batch.NewActivityPhase("step2b-金额汇总", sumDef, getInFilePath),                    // P2b
		),
		batch.NewActivityPhase("step3-打印结果", reportDef, getInReport), // P3
	)

	// ═══ NewJob 一体化（识别参数 file_path+date+run_id → WorkflowID）═══
	job := batch.NewJob("hzwtest2", flow)
	job.RegisterTo(wm)
	// ⚠️ 设计点：step1ValidateFlow（Child WF）内部字符串名调用 step1ValidateFile——
	// validateDef 不在编排树内（NewWorkflowPhase 持 fn 不持 def）→ 手动注册。
	wm.RegisterActivity(validateDef)

	go func() {
		if err := wm.Start(); err != nil {
			slog.Error("Worker 启动失败", "err", err)
		}
	}()
	defer wm.Stop()

	// ═══ 数据：固定文件（5 行，金额 1000+2000+1500+3000+2500 = 10000）═══
	// filePath 固定（只读）；run_id 变化保证 flowId 每次不同（防残留复用）。
	filePath := "../testdata/test_orders.txt"

	params := map[string]any{
		"file_path": filePath,
		"date":      "2026-08-18",
		"run_id":    time.Now().UnixNano(), // 变化变量 → flowId 每次不同（防残留复用）
	}
	run, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))
	t.Log("══════════ hzwtest_p0_batch_2（设计文档完整案例）══════════")
	t.Logf("  WorkflowID: %s", run.GetID())
	for k, v := range result {
		t.Logf("  %s: %+v", k, v)
	}

	// ═══ 断言（设计文档 §3.3 数据转换表）═══
	v, ok := result["step1-校验文件"].(map[string]any)
	require.True(t, ok, "P1 step1-校验文件 应存在")
	require.Equal(t, true, v["exists"], "文件存在")
	require.Equal(t, float64(5), v["total_lines"], "P1 校验 5 行")
	require.Equal(t, float64(5), v["valid_count"], "P1 有效 5 行")

	s, ok := result["step2a-分片处理"].(map[string]any)
	require.True(t, ok, "P2a step2a-分片处理 应存在")
	require.Equal(t, float64(5), s["processed"], "P2a 分片处理 5 条")
	require.Equal(t, float64(10000), s["total_amount"], "P2a 引擎金额 10000")

	m, ok := result["step2b-金额汇总"].(map[string]any)
	require.True(t, ok, "P2b step2b-金额汇总 应存在")
	require.Equal(t, float64(10000), m["total_amount"], "P2b 汇总金额 10000")
	require.Equal(t, float64(5), m["count"], "P2b 计数 5")

	r, ok := result["step3-打印结果"].(map[string]any)
	require.True(t, ok, "P3 step3-打印结果 应存在")
	require.Contains(t, asStr(r["report"]), "amount=10000", "P3 报告金额")

	t.Log("  ✅ 案例通过：Pipeline + Parallel(分片∥汇总) + FlowCtx + 识别参数")
}

// TestBatchCaseV2BadData 坏数据 → 引擎失败传播（设计文档 §3.2 异常时序）。
func TestBatchCaseV2BadData(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(1))
	engineDef, err := b.BuildActivity(
		&engineReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("v2-engine-bad"),
	)
	require.NoError(t, err)
	validateDef, err := b.BuildTasklet(step1ValidateFile, batch.WithActivityName("v2-step1-validate-bad"))
	require.NoError(t, err)

	flow := batch.Pipeline(
		batch.NewActivityPhase("step1-校验文件", validateDef, getInFilePath),
		batch.NewShardPhase("step2a-分片处理", &shardPartitioner{}, engineDef, getInShard),
	)
	job := batch.NewJob("hzwtest2-bad", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	badFile := fmt.Sprintf("../testdata/v2_bad_%d.txt", time.Now().UnixNano())
	badData := "ORD001,1000,2026-01-01\n" +
		"ORD002,BAD-AMOUNT,2026-01-02\n" + // ← 金额非数字
		"ORD003,1500,2026-01-03\n"
	require.NoError(t, os.WriteFile(badFile, []byte(badData), 0644))
	defer os.Remove(badFile)

	run, err := job.Start(context.Background(), facade, map[string]any{
		"file_path": badFile,
		"date":      "2026-08-18",
		"run_id":    time.Now().UnixNano(),
	})
	require.NoError(t, err)

	var result map[string]any
	err = run.Get(context.Background(), &result)
	require.Error(t, err, "坏数据应导致引擎失败")
	t.Logf("✅ 坏数据失败传播: %v", err)
}
