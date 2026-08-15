// hzwtest_p0_batch —— 用 batch 引擎（BuildActivity）+ core 封装重构 hzwtest 案例。
//
// 对比 hzwtest_raw（手写 step2aEngine 的 RPW 循环）：
//
//	本案例用 batch.Builder.BuildActivity 构建引擎 Activity（读→处理→写 + chunk + heartbeat + 断点恢复）。
//
// 验证点：
//  1. batch 引擎（BuildActivity 产出的 core.ActivityDef）在真实 Temporal 上跑通 R/P/W
//  2. ResultProvider 产出业务聚合结果（Output）
//  3. PositionAware 断点恢复（heartbeat + Seek）
//  4. 分片场景：多个引擎 Activity 处理不同分片
//  5. core 封装（ClientFacade + WorkerManager + Def 接口）
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
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

const shardCount = 4

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
	startLine := asInt(input.Params["start_line"])
	lineCount := asInt(input.Params["line_count"])
	return newShardReader(filePath, startLine, lineCount)
}

// shardReader 读文件的 [startLine, startLine+lineCount) 行。实现 Reader + PositionAware。
type shardReader struct {
	lines []any
	pos   int
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
	if r.pos >= len(r.lines) {
		return nil, nil // EOF
	}
	items := r.lines[r.pos:]
	r.pos = len(r.lines)
	return items, nil
}

// Seek 断点恢复：跳到第 offset 条（offset = 已提交条数）。
func (r *shardReader) Seek(offset int) error {
	if offset < 0 || offset > len(r.lines) {
		return fmt.Errorf("seek offset %d out of range", offset)
	}
	r.pos = offset
	return nil
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
// 自定义 Activity（非引擎，core.ActivityDef 注册）
// ═══════════════════════════════════════════════════════

// validateFile 校验文件：逐行计数。
func validateFile(ctx context.Context, input map[string]any) (map[string]any, error) {
	slog.Info("validateFile 开始", "input", input)
	filePath := asStr(input["file_path"])
	f, err := os.Open(filePath)
	if err != nil {
		return map[string]any{"exists": false}, nil
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
	return map[string]any{
		"exists": true, "valid_count": valid, "error_count": total - valid, "total_lines": total,
	}, nil
}

// splitFile 拆分文件为分片坐标。
func splitFile(ctx context.Context, input map[string]any) (map[string]any, error) {
	filePath := asStr(input["file_path"])
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
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
			"shard_id": i, "start_line": start, "line_count": count, "file_path": filePath,
		})
	}
	return map[string]any{"shards": shards}, nil
}

// printReport 打印结果。
func printReport(ctx context.Context, input map[string]any) (map[string]any, error) {
	msg := fmt.Sprintf("file=%v total=%v shards=%v processed=%v amount=%v count=%v",
		input["file_path"], input["total_lines"], input["shard_count"],
		input["processed"], input["total_amount"], input["count"])
	slog.Info("printReport", "report", msg)
	return map[string]any{"report": msg}, nil
}

// ═══════════════════════════════════════════════════════
// Workflow：分片调度（Child WF）—— splitFile → 循环引擎 Activity
// ═══════════════════════════════════════════════════════

// wfFunc 是案例 Workflow 的统一函数签名。
type wfFunc = func(workflow.Context, map[string]any) (map[string]any, error)

// shardProcessWorkflow 是分片内聚单元：拆分 + 逐分片调度引擎 Activity。
// engineActivityName 是 BuildActivity 产出的注册名，用于 ExecuteActivity 字符串调用。
func shardProcessWorkflow(engineActivityName string) wfFunc {
	return func(ctx workflow.Context, input map[string]any) (map[string]any, error) {
		slog.Info("shardProcessWorkflow 开始", "input", input)
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Minute,
			HeartbeatTimeout:    30 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
		}

		// ① splitFile（自定义 Activity）
		var splitRes map[string]any
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), actSplitFile, input).Get(ctx, &splitRes)
		if err != nil {
			slog.Error("shardProcessWorkflow splitFile 失败", "err", err)
			return nil, err
		}

		// ② 逐分片调度引擎 Activity（batch 构建）
		shards := splitRes["shards"].([]any)
		var totalProcessed, totalAmount, totalCount int
		for _, s := range shards {
			coord := s.(map[string]any)
			engineInput := batch.BatchInput{Params: coord}
			var out batch.BatchResult
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), engineActivityName, engineInput).Get(ctx, &out)
			if err != nil {
				slog.Error("shardProcessWorkflow 引擎失败", "coord", coord, "err", err)
				return nil, err
			}
			totalProcessed += out.Processed
			if out.Output != nil {
				totalAmount += asInt(out.Output["total_amount"])
				totalCount += asInt(out.Output["count"])
			}
		}

		result := map[string]any{
			"shard_count":  len(shards),
			"processed":    totalProcessed,
			"total_amount": totalAmount,
			"count":        totalCount,
		}
		slog.Info("shardProcessWorkflow 完成", "output", result)
		return result, nil
	}
}

// ═══════════════════════════════════════════════════════
// 编排 Workflow
// ═══════════════════════════════════════════════════════

func MainWorkflow(shardEngineName string) wfFunc {
	return func(ctx workflow.Context, input map[string]any) (map[string]any, error) {
		filePath := asStr(input["file_path"])
		date := asStr(input["date"])
		slog.Info("MainWorkflow 开始", "file_path", filePath, "date", date)
		ao := workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Minute}

		// P1: 校验
		validateInput := map[string]any{"file_path": filePath}
		var valRes map[string]any
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), actValidateFile, validateInput).Get(ctx, &valRes)
		if err != nil {
			return nil, err
		}
		if exists, _ := valRes["exists"].(bool); !exists {
			return map[string]any{"report": "file not found"}, nil
		}

		// P2: 分片（Child WF）
		shardInput := map[string]any{"file_path": filePath}
		var shardRes map[string]any
		err = workflow.ExecuteChildWorkflow(ctx, wfShardProcess, shardInput).Get(ctx, &shardRes)
		if err != nil {
			slog.Error("MainWorkflow 分片失败", "err", err)
			return nil, err
		}

		// P3: 报告
		reportInput := map[string]any{
			"file_path":    filePath,
			"total_lines":  valRes["total_lines"],
			"shard_count":  shardRes["shard_count"],
			"processed":    shardRes["processed"],
			"total_amount": shardRes["total_amount"],
			"count":        shardRes["count"],
		}
		var report map[string]any
		err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), actPrintReport, reportInput).Get(ctx, &report)
		if err != nil {
			return nil, err
		}

		slog.Info("MainWorkflow 完成", "report", report)
		return map[string]any{"report": report}, nil
	}
}

// ═══════════════════════════════════════════════════════
// 测试：core 封装 + batch 引擎
// ═══════════════════════════════════════════════════════

const (
	taskQueue       = "hzwtest-p0-batch"
	actValidateFile = "p0batch-validate-file"
	actSplitFile    = "p0batch-split-file"
	actPrintReport  = "p0batch-print-report"
	actEngine       = "p0batch-shard-engine" // BuildActivity 产出的引擎 Activity 名
	wfShardProcess  = "p0batch-shard-process"
	wfMain          = "p0batch-main"
)

func newConfig() *core.Config {
	cfg := core.NewConfig()
	cfg.Server.HostPort = "172.17.0.1:7233"
	cfg.Worker.TaskQueue = taskQueue
	return cfg
}

func TestMainWorkflowP0Batch(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ batch.Builder 构建引擎 Activity ═══
	b := batch.NewBuilder(
		batch.WithChunkSize(3),
		batch.WithMaxAttempts(2),
		batch.WithHeartbeatTimeout(30*time.Second),
	)
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName(actEngine),
	)
	require.NoError(t, err)
	slog.Info("引擎 Activity 已构建", "name", engineDef.Options.Name)

	// ═══ 注册：引擎 Activity（batch 产出，实现 core 接口）+ 自定义 Activity + Workflow ═══
	wm.RegisterActivity(engineDef) // 引擎 ActivityDef 实现 core.ActivityRegistrable
	wm.RegisterActivity(&core.ActivityDef{Fn: validateFile, Options: core.ActivityDefOptions{Name: actValidateFile}})
	wm.RegisterActivity(&core.ActivityDef{Fn: splitFile, Options: core.ActivityDefOptions{Name: actSplitFile}})
	wm.RegisterActivity(&core.ActivityDef{Fn: printReport, Options: core.ActivityDefOptions{Name: actPrintReport}})

	// Workflow 用字符串名（引擎名作为闭包捕获参数，避免用函数引用）
	shardWf := shardProcessWorkflow(actEngine)
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: shardWf, Options: core.WorkflowDefOptions{Name: wfShardProcess}})
	mainWf := MainWorkflow(actEngine)
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: mainWf, Options: core.WorkflowDefOptions{Name: wfMain}})

	go func() {
		if err := wm.Start(); err != nil {
			slog.Error("Worker 启动失败", "err", err)
		}
	}()
	slog.Info("Worker 已启动", "task_queue", taskQueue)
	defer wm.Stop()

	// ═══ 提交 Workflow ═══
	filePath := "../testdata/test_orders.txt"
	date := "2026-08-12"
	workflowID := fmt.Sprintf("hzwtest-batch-%s-%s", filepathBase(filePath), date)

	mainInput := map[string]any{"file_path": filePath, "date": date}
	run, err := facade.StartWorkflow(context.Background(), workflowID, wfMain, mainInput)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))
	t.Log("══════════ hzwtest_p0_batch ══════════")
	t.Logf("  WorkflowID: %s", workflowID)
	for k, v := range result {
		t.Logf("  %s: %+v", k, v)
	}
}

func filepathBase(p string) string {
	idx := strings.LastIndexAny(p, "/\\")
	if idx >= 0 {
		return p[idx+1:]
	}
	return p
}
