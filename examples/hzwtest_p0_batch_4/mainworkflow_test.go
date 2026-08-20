// hzwtest_p0_batch_4 —— Shard 定义场景全覆盖验证（partitioner/handler 统一 *Phase——形态 A）。
//
// 单文件自包含：不依赖其他测试包的符号。
//
// 覆盖矩阵（partition 形态 × handler 形态 + 边界）：
//
//	场景 1: partition = IO Tasklet（读文件算行数拆坐标——Activity 域 IO） + handler = Chunk（每分片 Activity）
//	场景 2: partition = Flow（PartitionFlow 独立 Child——replay 保留分区结果） + handler = Chunk
//	场景 3: partition = 纯函数（NewPartitionerPhase） + handler = Flow（每分片 Child WF——分区名 → ID 派生）+ 空名派生
//	场景 4: partition = 纯函数 + handler = Workflow（手写逃逸舱——内部 ExecuteActivity）
//	场景 5: partition = 纯函数 + handler = Pipeline（组合——主 WF 内展开）
//	场景 6: partition = 空分区（边界——空聚合结构）
//
// 识别参数: file_path + run_id（防残留 Run 复用）；taskQueue 独立。
package hzwtest_p0_batch_4

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

const taskQueue = "hzwtest-p0-batch-4"

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
// 引擎组件（Reader/Processor/Writer——分片 handler=Chunk 时用）
// ═══════════════════════════════════════════════════════

type engineReaderFactory struct{}

func (f *engineReaderFactory) NewReader(ctx context.Context, input batch.BatchInput) (batch.Reader, error) {
	filePath := asStr(input.Params["file_path"])
	start := asInt(input.Params["start"])
	lineCount := asInt(input.Params["line_count"])
	return newEngineReader(filePath, start, lineCount)
}

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
		return nil, nil
	}
	item := r.lines[r.Offset]
	r.Offset++
	return []any{item}, nil
}

type amountProcessor struct{}

func (p *amountProcessor) Process(ctx context.Context, item any) (any, error) {
	line, _ := item.(string)
	fields := strings.Split(line, ",")
	amount, err := strconv.Atoi(strings.TrimSpace(fields[1]))
	if err != nil {
		return nil, fmt.Errorf("金额解析失败: %q", fields[1])
	}
	return amount, nil
}

type sumWriterFactory struct{}

func (f *sumWriterFactory) NewWriter(ctx context.Context, input batch.BatchInput) (batch.Writer, error) {
	return &sumWriter{}, nil
}

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
// 坐标拆分（Partitioner——三种形态）
// ═══════════════════════════════════════════════════════

const shardCount = 4

// splitOrdersPure 纯函数拆分（纯内存——坐标基于 fc.Input() 的 total_lines，模拟上游提供）。
// 分区名留空——验证 runShard 自动派生 {Phase name}-{i}。
func splitOrdersPure(fc *batch.FlowCtx) ([]batch.Partition, error) {
	total, _ := fc.Int("input.total_lines")
	filePath, _ := fc.Str("input.file_path")
	return splitByCount(total, filePath), nil
}

// splitOrdersFromFile IO 拆分（Tasklet 签名——Activity 域读文件算行数，不经上游 Phase）。
// 返回输出契约：{"partitions": [{"name":..., "data":{...}}, ...]}。
func splitOrdersFromFile(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	filePath, _ := fc.Str("input.file_path")
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
	parts := splitByCount(total, filePath)
	return partitionListToMapHelper(parts), nil
}

func splitByCount(total int, filePath string) []batch.Partition {
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
			Name: fmt.Sprintf("shard-%d", i),
			Data: map[string]any{"shard_id": i, "start": start, "line_count": count, "file_path": filePath},
		})
	}
	return parts
}

// partitionListToMapHelper 契约封装（与框架 batch.partitionListToMap 一致——业务手写 IO 拆分用）。
func partitionListToMapHelper(parts []batch.Partition) map[string]any {
	list := make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		list = append(list, map[string]any{"name": p.Name, "data": p.Data})
	}
	return map[string]any{"partitions": list}
}

// splitOrdersEmpty 空分区（边界场景 6）。
func splitOrdersEmpty(fc *batch.FlowCtx) ([]batch.Partition, error) {
	return nil, nil
}

// ═══════════════════════════════════════════════════════
// 分片执行单元（handler——四种形态）
// ═══════════════════════════════════════════════════════

// engine 构建（handler=Chunk——每分片 Activity）。
func newChunkHandler(name string) *batch.Phase {
	return batch.NewChunkPhase(name,
		&engineReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityChunkSize(3), batch.WithActivityMaxAttempts(2))
}

// shardProcessorTasklet 单分片处理（handler=Flow 的子树叶子——收 fc 读坐标，处理返回统计）。
func shardProcessorTasklet(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	filePath, _ := fc.Str("input.file_path")
	start, _ := fc.Int("input.start")
	lineCount, _ := fc.Int("input.line_count")
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	processed, totalAmount := 0, 0
	line := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line < start {
			line++
			continue
		}
		if line >= start+lineCount {
			break
		}
		line++
		fields := strings.Split(sc.Text(), ",")
		if n, err := strconv.Atoi(strings.TrimSpace(fields[1])); err == nil {
			totalAmount += n
			processed++
		}
	}
	return map[string]any{"processed": processed, "total_amount": totalAmount}, nil
}

// shardProcessorWorkflow 手写分片 Child WF（handler=Workflow——逃逸舱）。
// 内部 ExecuteActivity 调普通函数（反射名可靠——手写 workflow 惯例）。
// ⚠️ 必须设置 ActivityOptions（StartToCloseTimeout）——否则 Activity 永不调度（SDK 硬约束）。
func shardProcessorWorkflow(ctx workflow.Context, fc *batch.FlowCtx) (map[string]any, error) {
	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
	var out map[string]any
	err := workflow.ExecuteActivity(actCtx, sumFileActivity, fc).Get(ctx, &out)
	return out, err
}

// sumFileActivity 普通 Activity 函数（手写 workflow 内部调用——需显式注册）。
func sumFileActivity(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	return shardProcessorTasklet(ctx, fc)
}

// ═══════════════════════════════════════════════════════
// 测试脚手架
// ═══════════════════════════════════════════════════════

func newConfig() *core.Config {
	cfg := core.NewConfig()
	cfg.Server.HostPort = "172.17.0.1:7233"
	// cfg.Server.HostPort = "127.0.0.1:7233"
	cfg.Worker.TaskQueue = taskQueue
	return cfg
}

// startJob 启动并等待完成，返回 result（file_path 唯一 + run_id 防残留）。
func startJob(t *testing.T, jobName string, flow *batch.Phase, extraActs ...interface{}) map[string]any {
	t.Helper()
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	job := batch.NewJob(jobName, flow)
	job.RegisterTo(wm)
	for _, act := range extraActs {
		wm.RegisterActivity(act) // 手写 workflow 内部 Activity（树外）
	}

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := fmt.Sprintf("../testdata/v4_%s_%d.txt", jobName, time.Now().UnixNano())
	data := "ORD001,1000,2026-01-01\nORD002,2000,2026-01-02\nORD003,1500,2026-01-03\nORD004,3000,2026-01-04\nORD005,2500,2026-01-05\n"
	require.NoError(t, os.WriteFile(filePath, []byte(data), 0644))
	defer os.Remove(filePath) // 清理必须在测试函数（非 helper）

	run, err := job.Start(context.Background(), facade, map[string]any{
		"file_path":   filePath,
		"total_lines": 5, // 纯函数 partitioner 的坐标来源（模拟上游 Phase 输出）
		"run_id":      time.Now().UnixNano(),
	})
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))
	t.Logf("  WorkflowID: %s", run.GetID())
	for k, v := range result {
		t.Logf("  %s: %+v", k, v)
	}
	return result
}

// assertShardResult 统一断言分片聚合（processed=5, total_amount=10000）。
func assertShardResult(t *testing.T, result map[string]any, key string) {
	t.Helper()
	s, ok := result[key].(map[string]any)
	require.True(t, ok, "%s 应存在", key)
	require.Equal(t, float64(5), s["processed"], "%s processed 5 条", key)
	require.Equal(t, float64(10000), s["total_amount"], "%s 金额 10000", key)
	require.Equal(t, float64(0), s["skipped_shards"], "%s 无跳过分片", key)
}

// ═══════════════════════════════════════════════════════
// 场景 1: partition = IO Tasklet（Activity 域读文件拆坐标） + handler = Chunk
// ═══════════════════════════════════════════════════════

func TestShard_PartitionerIO_HandlerChunk(t *testing.T) {
	flow := batch.Pipeline(
		batch.NewShardPhase("分片",
			batch.NewTaskletPhase("拆坐标", splitOrdersFromFile, batch.WithActivityMaxAttempts(2)), // IO 拆分——读文件算行数
			newChunkHandler("分片引擎"),
		),
	)
	result := startJob(t, "io-chunk", flow)
	assertShardResult(t, result, "分片")
	t.Log("  ✅ 场景 1：partition=IO Tasklet + handler=Chunk（坐标 IO 能力）")
}

// ═══════════════════════════════════════════════════════
// 场景 2: partition = Flow（PartitionFlow 独立 Child） + handler = Chunk
// ═══════════════════════════════════════════════════════

func TestShard_PartitionerFlow_HandlerChunk(t *testing.T) {
	flow := batch.Pipeline(
		batch.NewShardPhase("分片",
			batch.NewFlowPhase("拆坐标", batch.NewTaskletPhase("拆坐标IO", splitOrdersFromFile, batch.WithActivityMaxAttempts(2))), // PartitionFlow
			newChunkHandler("分片引擎"),
		),
	)
	result := startJob(t, "flow-chunk", flow)
	assertShardResult(t, result, "分片")
	t.Log("  ✅ 场景 2：partition=Flow（PartitionFlow 独立 Child）+ handler=Chunk")
}

// ═══════════════════════════════════════════════════════
// 场景 3: partition = 纯函数（空分区名→自动派生） + handler = Flow（每分片 Child WF）
// ═══════════════════════════════════════════════════════

func TestShard_PurePartitioner_HandlerFlow(t *testing.T) {
	flow := batch.Pipeline(
		batch.NewShardPhase("分片",
			batch.NewPartitionerPhase("拆坐标", splitOrdersPure), // 纯函数——坐标来自 input.total_lines
			batch.NewFlowPhase("分片处理", batch.NewTaskletPhase("分片内处理", shardProcessorTasklet, batch.WithActivityMaxAttempts(2))),
		),
	)
	result := startJob(t, "pure-flow", flow)
	assertShardResult(t, result, "分片")
	t.Log("  ✅ 场景 3：partition=纯函数（空名派生）+ handler=Flow（每分片 Child WF——分区名→ID）")
}

// ═══════════════════════════════════════════════════════
// 场景 4: partition = 纯函数 + handler = Workflow（手写逃逸舱）
// ═══════════════════════════════════════════════════════

func TestShard_PurePartitioner_HandlerWorkflow(t *testing.T) {
	flow := batch.Pipeline(
		batch.NewShardPhase("分片",
			batch.NewPartitionerPhase("拆坐标", splitOrdersPure),
			batch.NewWorkflowPhase("分片处理", shardProcessorWorkflow, batch.WithWorkflowMaxAttempts(2)),
		),
	)
	result := startJob(t, "pure-wf", flow, sumFileActivity) // 手写 workflow 内部 Activity 需显式注册
	assertShardResult(t, result, "分片")
	t.Log("  ✅ 场景 4：partition=纯函数 + handler=Workflow（手写逃逸舱——内部 ExecuteActivity）")
}

// ═══════════════════════════════════════════════════════
// 场景 5: partition = 纯函数 + handler = Pipeline（组合——主 WF 内展开）
// ═══════════════════════════════════════════════════════

func TestShard_PurePartitioner_HandlerComposite(t *testing.T) {
	flow := batch.Pipeline(
		batch.NewShardPhase("分片",
			batch.NewPartitionerPhase("拆坐标", splitOrdersPure),
			batch.Pipeline( // 组合 handler：每分片在主 WF 内跑两步
				batch.NewTaskletPhase("分片内-step1", shardProcessorTasklet, batch.WithActivityMaxAttempts(2)),
				batch.NewTaskletPhase("分片内-step2", shardProcessorTasklet, batch.WithActivityMaxAttempts(2)),
			),
		),
	)
	result := startJob(t, "pure-comp", flow)
	// 组合 handler：processed 是两个子 Phase 之和（每步 5）→ 10；total_amount 同理 ×2 → 20000
	s, ok := result["分片"].(map[string]any)
	require.True(t, ok, "分片 应存在")
	require.Equal(t, float64(10), s["processed"], "组合 handler processed = 两步之和 10")
	require.Equal(t, float64(20000), s["total_amount"], "组合 handler 金额 = 两步之和 20000")
	require.Equal(t, float64(0), s["skipped_shards"], "无跳过分片")
	t.Log("  ✅ 场景 5：partition=纯函数 + handler=Pipeline（组合——主 WF 内展开，子输出合并聚合）")
}

// ═══════════════════════════════════════════════════════
// 场景 6: 空分区（边界——空聚合结构统一）
// ═══════════════════════════════════════════════════════

func TestShard_EmptyPartitions(t *testing.T) {
	flow := batch.Pipeline(
		batch.NewShardPhase("分片",
			batch.NewPartitionerPhase("拆坐标", splitOrdersEmpty),
			newChunkHandler("分片引擎"),
		),
	)
	result := startJob(t, "empty", flow)
	s, ok := result["分片"].(map[string]any)
	require.True(t, ok, "分片 应存在")
	require.Equal(t, float64(0), s["processed"], "空分区 processed 0")
	require.Equal(t, float64(0), s["skipped_shards"], "空分区 skipped_shards 0")
	t.Log("  ✅ 场景 6：空分区——空聚合结构统一")
}

// ═══════════════════════════════════════════════════════
// 场景 8: 全 flow——partition = Flow（PartitionFlow）+ handler = Flow（每分片 Child WF）
// 主 WF 只面对 Child：PartitionFlow（拆坐标）+ 分片 Childs（ID 派生自分区名）
// ═══════════════════════════════════════════════════════

func TestShard_PartitionerFlow_HandlerFlow(t *testing.T) {
	flow := batch.Pipeline(
		batch.NewShardPhase("分片",
			batch.NewFlowPhase("拆坐标", // PartitionFlow：独立 Child WF（内部 IO 拆坐标——replay 保留分区结果）
				batch.NewTaskletPhase("拆坐标IO", splitOrdersFromFile, batch.WithActivityMaxAttempts(2)),
			),
			batch.NewFlowPhase("分片处理", // handler=Flow：每分片一个 Child WF（ID 派生 {主ID}-shard-{分区名}——跨 Run 幂等）
				batch.NewTaskletPhase("分片内处理", shardProcessorTasklet, batch.WithActivityMaxAttempts(2)),
			),
		),
	)
	result := startJob(t, "flow-flow", flow)
	assertShardResult(t, result, "分片")
	t.Log("  ✅ 场景 8：全 flow——partition=Flow（PartitionFlow）+ handler=Flow（每分片 Child WF）")
}

// ═══════════════════════════════════════════════════════
// 场景 7: 主流程编排（全形态组合——partition IO + handler Chunk + 并行 + P3 报告）
// ═══════════════════════════════════════════════════════

func TestShard_FullOrchestration(t *testing.T) {
	report := batch.NewTaskletPhase("step3-打印结果", func(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
		processed, _ := fc.Int("分片.processed")
		amount, _ := fc.Int("分片.total_amount")
		msg := fmt.Sprintf("processed=%d amount=%d", processed, amount)
		slog.Info("report", "msg", msg)
		return map[string]any{"report": msg}, nil
	}, batch.WithActivityMaxAttempts(2))

	flow := batch.Pipeline(
		batch.NewShardPhase("分片",
			batch.NewTaskletPhase("拆坐标", splitOrdersFromFile, batch.WithActivityMaxAttempts(2)),
			newChunkHandler("分片引擎"),
		),
		report,
	)
	result := startJob(t, "full-arch", flow)
	assertShardResult(t, result, "分片")
	r, ok := result["step3-打印结果"].(map[string]any)
	require.True(t, ok, "step3 应存在")
	require.Contains(t, asStr(r["report"]), "amount=10000", "报告金额")
	t.Log("  ✅ 场景 7：全形态组合（partition IO → 分片 → P3 报告——路径访问聚合结果）")
}
