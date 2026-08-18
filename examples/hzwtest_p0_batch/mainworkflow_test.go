// hzwtest_p0_batch —— 当前 POC 框架设计的核心案例（NewJob 一体化 + 编排 Phase）。
//
// 对比手写 Workflow 时代（hzwtest_raw / 早期 shardProcessWorkflow+MainWorkflow）：
//   - 执行单元统一：引擎 BuildActivity + 自定义 BuildTasklet（同一 BatchInput/BatchResult 签名）
//   - 编排声明式：Pipeline(校验 → 分片 → 报告)，分片=Child WF（NewShardPhase 内部生成）
//   - Job 一体化：NewJob（识别参数推导 ID + Compile + RegisterTo）
//
// 验证点：
//  1. batch 引擎（BuildActivity）在真实 Temporal 上跑通 R/P/W + heartbeat + 断点恢复
//  2. 分片=Child Workflow（可推导 ID + 聚合）
//  3. FlowCtx k-v 跨 Phase 传递（validate → shard → report）
//  4. NewJob 识别参数 → WorkflowID 推导
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
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

const (
	shardCount   = 4
	actSplitFile = "p0batch-split-file" // parallel_test 手写 Workflow 用
)

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

// shardReaderFactory 从 BatchInput.Params 读分片坐标，创建 shardReader。
// 实现 batch.ReaderFactory——引擎每次执行时创建独立 Reader（对齐 Step Scope）。
type shardReaderFactory struct{}

func (f *shardReaderFactory) NewReader(ctx context.Context, input batch.BatchInput) (batch.Reader, error) {
	filePath := asStr(input.Params["file_path"])
	startLine := asInt(input.Params["start"])
	lineCount := asInt(input.Params["line_count"])
	return newShardReader(filePath, startLine, lineCount)
}

// shardReader 读文件的 [startLine, startLine+lineCount) 行。实现 Reader + RestartableReader（嵌入 OffsetState）。
type shardReader struct {
	batch.OffsetState // 嵌入：条数定位（SaveState/RestoreState）
	lines             []any
}

func newShardReader(filePath string, startLine, lineCount int) (*shardReader, error) {
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

	// 截取分片范围
	end := startLine + lineCount
	if end > len(all) {
		end = len(all)
	}
	if startLine > len(all) {
		startLine = len(all)
	}
	seg := all[startLine:end]

	lines := make([]any, len(seg))
	for i, l := range seg {
		lines[i] = l
	}
	return &shardReader{lines: lines}, nil
}

func (r *shardReader) Read(ctx context.Context) ([]any, error) {
	if r.Offset >= len(r.lines) {
		return nil, nil // EOF
	}
	item := r.lines[r.Offset]
	r.Offset++
	return []any{item}, nil
}

// amountProcessor 解析 "order_id,amount,date" 行的金额。实现 batch.Processor。
// 金额非数字（BAD-AMOUNT）返回错误——模拟数据异常。
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

// sumWriterFactory 创建 sumWriter。实现 batch.WriterFactory。
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

// Result 实现 ResultProvider——引擎循环结束后填入 BatchResult.Output。
func (w *sumWriter) Result() map[string]any {
	return map[string]any{"total_amount": w.totalAmount, "count": w.count}
}

// ═══════════════════════════════════════════════════════
// 自定义 Activity（BuildTasklet，统一 BatchInput/BatchResult 签名）
// ═══════════════════════════════════════════════════════

// validateFile 校验文件：逐行计数。
func validateFile(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	slog.Info("validateFile 开始", "input", input.Params)
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

// splitFile 拆分文件为分片坐标（供 parallel_test 手写 Workflow 使用）。
func splitFile(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	filePath := asStr(input.Params["file_path"])
	f, err := os.Open(filePath)
	if err != nil {
		return batch.BatchResult{}, err
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	total := len(lines)
	per := total / shardCount
	if total%shardCount != 0 {
		per++
	}

	var shards []map[string]any
	for i := 0; i < shardCount; i++ {
		start := i * per
		count := per
		if rem := total - start; count > rem {
			count = rem
		}
		if count <= 0 {
			break
		}
		shards = append(shards, map[string]any{
			"shard_id": i, "start": start, "line_count": count, "file_path": filePath,
		})
	}
	return batch.BatchResult{Output: map[string]any{"shards": shards}}, nil
}

// printReport 打印结果。
func printReport(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	msg := fmt.Sprintf("file=%v total=%v processed=%v amount=%v count=%v",
		input.Params["file_path"], input.Params["total_lines"],
		input.Params["processed"], input.Params["total_amount"], input.Params["count"])
	slog.Info("printReport", "report", msg)
	return batch.BatchResult{Output: map[string]any{"report": msg}}, nil
}

// ═══════════════════════════════════════════════════════
// getIn：FlowCtx → Phase 输入（k-v 传递）
// ═══════════════════════════════════════════════════════

// wfFunc 是手写 Workflow 的统一函数签名（parallel_test 手写 Workflow 用）。
type wfFunc = func(workflow.Context, map[string]any) (map[string]any, error)

// getInShard 从 step1 输出取 total_lines，拼 step2a 分片输入
// （Partitioner 确定性拆坐标，Workflow 内不 IO；设计文档 §2 的 splitFile 拆分逻辑由 Partitioner 承担）。
func getInShard(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	validate, _ := fc.Get("step1-校验文件")
	return map[string]any{
		"file_path":   asStr(input.(map[string]any)["file_path"]),
		"total_lines": validate.(map[string]any)["total_lines"],
	}, nil
}

// getInReportMain 汇集 P1 + P2a + P2b 全部结果，拼 step3 报告输入（设计文档 §3.3 P3）。
func getInReportMain(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	validate, _ := fc.Get("step1-校验文件")
	shard, _ := fc.Get("step2a-分片处理")
	sum, _ := fc.Get("step2b-金额汇总")
	v := validate.(map[string]any)
	s := shard.(map[string]any)
	m := sum.(map[string]any)
	return map[string]any{
		"file_path":    asStr(input.(map[string]any)["file_path"]),
		"total_lines":  v["total_lines"],
		"valid_count":  v["valid_count"],
		"error_count":  v["error_count"],
		"processed":    s["processed"],
		"shard_count":  s["shard_count"],
		"total_amount": m["total_amount"],
		"count":        m["count"],
	}, nil
}

// ═══════════════════════════════════════════════════════
// 测试：核心案例（NewJob 一体化）
// ═══════════════════════════════════════════════════════

const taskQueue = "hzwtest-p0-batch"

func newConfig() *core.Config {
	cfg := core.NewConfig()
	// cfg.Server.HostPort = "172.17.0.1:7233"
	cfg.Server.HostPort = "127.0.0.1:7233"
	cfg.Worker.TaskQueue = taskQueue
	return cfg
}

// TestMainWorkflowP0Batch 核心案例：NewJob 一体化 + 编排 Phase + 分片=Child WF。
func TestMainWorkflowP0Batch(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 执行单元：引擎 BuildActivity + 自定义 BuildTasklet ═══
	b := batch.NewBuilder(
		batch.WithChunkSize(3),
		batch.WithMaxAttempts(2),
		batch.WithHeartbeatTimeout(30*time.Second),
	)
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("p0batch-engine"),
	)
	require.NoError(t, err)
	validateDef, err := b.BuildTasklet(validateFile, batch.WithActivityName("p0batch-validate"))
	require.NoError(t, err)
	sumDef, err := b.BuildTasklet(step2bSumAmounts, batch.WithActivityName("p0batch-sum"))
	require.NoError(t, err)
	reportDef, err := b.BuildTasklet(printReport, batch.WithActivityName("p0batch-report"))
	require.NoError(t, err)

	// ═══ 编排（对标 hzwtest_案例流程设计 §1）═══
	// Pipeline(step1-校验文件, Parallel(step2a-分片处理∥step2b-金额汇总), step3-打印结果)
	flow := batch.Pipeline(
		batch.NewActivityPhase("step1-校验文件", validateDef, getInFilePath), // P1
		batch.Parallel( // P2：分片 ∥ 金额汇总
			batch.NewShardPhase("step2a-分片处理", &designPartitioner{shardCount: shardCount}, engineDef, getInShard),
			batch.NewActivityPhase("step2b-金额汇总", sumDef, getInFilePath),
		),
		batch.NewActivityPhase("step3-打印结果", reportDef, getInReportMain), // P3
	)

	// ═══ NewJob 一体化（识别参数 file_path+date → WorkflowID 推导）═══
	job := batch.NewJob("hzwtest-batch", flow)
	job.RegisterTo(wm)

	go func() {
		if err := wm.Start(); err != nil {
			slog.Error("Worker 启动失败", "err", err)
		}
	}()
	defer wm.Stop()

	// ═══ 启动（唯一 file_path 防幂等冲突）═══
	filePath := fmt.Sprintf("../testdata/test_orders_%d.txt", time.Now().UnixNano())
	data, err := os.ReadFile("../testdata/test_orders.txt")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filePath, data, 0644))
	defer os.Remove(filePath)

	params := map[string]any{"file_path": filePath, "date": "2026-08-12"}
	run, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))
	t.Log("══════════ hzwtest_p0_batch（NewJob 一体化）══════════")
	t.Logf("  WorkflowID: %s", run.GetID())
	for k, v := range result {
		t.Logf("  %s: %+v", k, v)
	}

	// 断言（对标设计文档 §3.3 数据转换表）
	v, ok := result["step1-校验文件"].(map[string]any)
	require.True(t, ok, "step1 结果应存在")
	require.Equal(t, true, v["exists"], "文件应存在")
	require.Equal(t, float64(5), v["total_lines"], "step1 校验 5 行")

	s, ok := result["step2a-分片处理"].(map[string]any)
	require.True(t, ok, "step2a 聚合应存在")
	require.Greater(t, asInt(s["processed"]), 0, "分片应处理数据")

	m, ok := result["step2b-金额汇总"].(map[string]any)
	require.True(t, ok, "step2b 汇总应存在")
	require.Equal(t, float64(10000), m["total_amount"], "step2b 汇总 1000+2000+3000+2500+1500")

	r, ok := result["step3-打印结果"].(map[string]any)
	require.True(t, ok, "step3 报告应存在")
	require.Contains(t, asStr(r["report"]), "processed=", "报告应含处理统计")
}
