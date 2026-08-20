// hzwtest_p0_batch_3 —— 重构后完整案例（对标设计文档 hzwtest_案例流程设计 §1 + 重构设计 §3）。
//
// 单文件自包含：不依赖其他测试包的符号。
//
// 案例结构（设计文档 §1）：
//
//	MainWorkflow(filePath, date)
//	  ├─ P1: step1-校验文件       ← NewFlowPhase(NewTaskletPhase)（静态子树 → Child WF，零 SDK）
//	  ├─ P2: Parallel
//	  │   ├─ step2a-分片处理      ← NewShardPhase(PartitionerFn, handler=NewChunkPhase)
//	  │   │     内部: 按 total_lines 拆坐标（Partitioner 确定性纯内存）→ 分片执行形态 = handler 类型
//	  │   │           （T16：Chunk 是 Activity 类 → 每分片 ExecuteActivity；坐标注入 fc.Input()）
//	  │   └─ step2b-金额汇总      ← NewTaskletPhase（收 fc 快照——fc.Str/Int 自取）
//	  └─ P3: step3-打印结果       ← NewTaskletPhase
//
// 重构验证点：
//
//	① getIn 消灭——执行单元收 *FlowCtx 快照，fc.Str/Int/Input 自取
//	② Builder 消灭——NewTaskletPhase/NewChunkPhase 直接构建（配置在构造 opts）
//	③ BatchResult 消灭——执行单元统一返回 map[string]any
//	④ PartitionerFn → []Partition（T15 方案 B——分区带名字）
//	⑤ 注册名默认 = Phase name 派生（无 WithActivityName 显式指定）
//	⑥ 分片执行形态 = handler 类型（T16）
//	⑦ FlowCtx input 显式字段 + 路径访问（fc.Str("input.file_path") / fc.Int("step1-校验文件.total_lines")）
//
// 识别参数: filePath + date + run_id（防残留 Run 复用）。
package hzwtest_p0_batch_3

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

// shardCount 是分片 flow 的定义参数（设计文档 §1.1：不入参、不走 FlowCtx）。
const shardCount = 4

const taskQueue = "hzwtest-p0-batch-3"

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

// engineReaderFactory 从 fc.Input() 读分片坐标（坐标由 Shard 注入），创建 engineReader。
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

// sumWriter 汇总金额。实现 batch.Writer + batch.ResultProvider（业务聚合结果拼入返回 map）。
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
// P1: step1-校验文件（Tasklet——收 fc 快照自取，消灭 getIn）
// ═══════════════════════════════════════════════════════

func step1ValidateFile(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	filePath, _ := fc.Str("input.file_path") // 路径访问——作业入参
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

// ═══════════════════════════════════════════════════════
// P2b: step2b-金额汇总（Tasklet）
// ═══════════════════════════════════════════════════════

func step2bSumAmounts(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	filePath, _ := fc.Str("input.file_path")
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
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
	return map[string]any{"total_amount": sum, "count": count}, nil
}

// ═══════════════════════════════════════════════════════
// P3: step3-打印结果（Tasklet——路径访问各 Phase 输出）
// ═══════════════════════════════════════════════════════

func step3PrintReport(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	msg := fmt.Sprintf("file=%v date=%v total=%v valid=%v errors=%v processed=%v amount=%v count=%v",
		mustStr(fc, "input.file_path"), mustStr(fc, "input.date"),
		mustInt(fc, "step1-校验文件.total_lines"), mustInt(fc, "step1-校验文件.valid_count"),
		mustInt(fc, "step1-校验文件.error_count"),
		mustInt(fc, "step2a-分片处理.processed"),
		mustInt(fc, "step2b-金额汇总.total_amount"), mustInt(fc, "step2b-金额汇总.count"))
	slog.Info("step3PrintReport", "report", msg)
	return map[string]any{"report": msg}, nil
}

func mustStr(fc *batch.FlowCtx, path string) string {
	s, _ := fc.Str(path)
	return s
}

func mustInt(fc *batch.FlowCtx, path string) int {
	n, _ := fc.Int(path)
	return n
}

// ═══════════════════════════════════════════════════════
// P2a: 分片坐标 Partitioner（PartitionerFn——T15 方案 B：输出带名字的分区列表）
// 坐标基于 P1 输出的 total_lines（路径访问）+ input.file_path；确定性纯内存。
// ═══════════════════════════════════════════════════════

func splitOrders(fc *batch.FlowCtx) ([]batch.Partition, error) {
	total, _ := fc.Int("step1-校验文件.total_lines") // 路径访问——上游 Phase 输出
	filePath, _ := fc.Str("input.file_path")
	per := total / shardCount
	if total%shardCount != 0 {
		per++
	}
	var parts []batch.Partition
	for i := 0; i < shardCount; i++ {
		start := i * per
		count := per
		if rem := total - start; count > rem {
			count = rem
		}
		if count <= 0 {
			break
		}
		parts = append(parts, batch.Partition{
			Name: fmt.Sprintf("shard-%d", i), // 分区名（Child ID 派生基础——T15）
			Data: map[string]any{
				"shard_id": i, "start": start, "line_count": count, "file_path": filePath,
			},
		})
	}
	return parts, nil
}

// ═══════════════════════════════════════════════════════
// 测试：完整案例（重构后 API——NewJob + 编排 Phase + 分片 handler=Chunk）
// ═══════════════════════════════════════════════════════

func newConfig() *core.Config {
	cfg := core.NewConfig()
	cfg.Server.HostPort = "172.17.0.1:7233"
	// cfg.Server.HostPort = "127.0.0.1:7233"
	cfg.Worker.TaskQueue = taskQueue
	return cfg
}

// TestBatchCaseV3 完整案例：Pipeline(P1-flow, Parallel(step2a∥step2b), P3)。
func TestBatchCaseV3(t *testing.T) {

	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// ═══ 执行单元：引擎 NewChunkPhase + 自定义 NewTaskletPhase（配置在构造 opts——Builder 消灭）═══
	engine := batch.NewChunkPhase("step2a-分片处理",
		&engineReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityChunkSize(3), batch.WithActivityMaxAttempts(2),
	)
	validate := batch.NewTaskletPhase("校验", step1ValidateFile, batch.WithActivityMaxAttempts(2))
	sum := batch.NewTaskletPhase("step2b-金额汇总", step2bSumAmounts, batch.WithActivityMaxAttempts(2))
	report := batch.NewTaskletPhase("step3-打印结果", step3PrintReport, batch.WithActivityMaxAttempts(2))

	// ═══ Phase 声明（对标设计文档 §1——P1 包装成 flow；P2a 分片 partitioner=纯函数 + handler=engine）═══
	phaseP1 := batch.NewFlowPhase("step1-校验文件", validate) // 静态子树 → Child WF（零 SDK）
	phaseP2a := batch.NewShardPhase("step2a-分片处理", batch.NewPartitionerPhase("拆坐标", splitOrders), engine)
	phaseP2b := sum
	phaseP3 := report

	// ═══ 编排（设计文档 §1）：P1 → Parallel(P2a∥P2b) → P3 ═══
	flow := batch.Pipeline(
		phaseP1,
		batch.Parallel(phaseP2a, phaseP2b),
		phaseP3,
	)

	// ═══ NewJob 一体化（识别参数 file_path+date+run_id → WorkflowID）═══
	job := batch.NewJob("hzwtest3", flow)
	job.RegisterTo(wm) // 一体化注册：Workflow + Activity（子树内 def 自动收集）+ Child WF

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
		"date":      "2026-08-20",
		"run_id":    time.Now().UnixNano(), // 变化变量 → flowId 每次不同（防残留复用）
	}

	run, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))
	t.Log("══════════ hzwtest_p0_batch_3（重构后完整案例）══════════")
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

	t.Log("  ✅ 案例通过：Pipeline + Parallel(分片∥汇总) + FlowCtx 快照 + 分区名 + 识别参数")
}

// TestBatchCaseV3BadData 坏数据 → 引擎失败传播（设计文档 §3.2 异常时序）。
func TestBatchCaseV3BadData(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	engine := batch.NewChunkPhase("step2a-分片处理",
		&engineReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityChunkSize(3), batch.WithActivityMaxAttempts(1),
	)
	validate := batch.NewTaskletPhase("校验", step1ValidateFile, batch.WithActivityMaxAttempts(1))

	flow := batch.Pipeline(
		batch.NewFlowPhase("step1-校验文件", validate),
		batch.NewShardPhase("step2a-分片处理", batch.NewPartitionerPhase("拆坐标", splitOrders), engine),
	)
	job := batch.NewJob("hzwtest3-bad", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	badFile := fmt.Sprintf("../testdata/v3_bad_%d.txt", time.Now().UnixNano())
	badData := "ORD001,1000,2026-01-01\n" +
		"ORD002,BAD-AMOUNT,2026-01-02\n" + // ← 金额非数字
		"ORD003,1500,2026-01-03\n"
	require.NoError(t, os.WriteFile(badFile, []byte(badData), 0644))
	defer os.Remove(badFile) // 清理必须在测试函数（非 helper）

	run, err := job.Start(context.Background(), facade, map[string]any{
		"file_path": badFile,
		"date":      "2026-08-20",
		"run_id":    time.Now().UnixNano(),
	})
	require.NoError(t, err)

	var result map[string]any
	err = run.Get(context.Background(), &result)
	require.Error(t, err, "坏数据应导致引擎失败")
	t.Logf("✅ 坏数据失败传播: %v", err)
}
