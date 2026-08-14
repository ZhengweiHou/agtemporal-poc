// hzwtest_p0_core —— 用 core 封装重写 hzwtest 案例。
//
// 对比 hzwtest_raw（手写 client.Dial / worker.New / Register / ExecuteWorkflow）：
//   本案例用 core.ClientFacade + core.WorkerManager 替换全部样板代码。
//
// 业务逻辑与 hzwtest_raw 完全一致（校验 → 并行分片/汇总 → 报告），
// 仅验证 core 封装在真实多 Activity + Child Workflow 场景下的可用性。
package hzwtest_p0_core

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/core"
)

// shardCount 是分片 flow 的定义参数——内部常量，不入参
const shardCount = 4

// ── map 类型取值 helper（JSON 序列化后 int 变 float64）──

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
// P1: 校验文件 (Activity)
// ═══════════════════════════════════════════════════════

func step1ValidateFile(ctx context.Context, input map[string]any) (map[string]any, error) {
	slog.Info("step1ValidateFile 开始", "input", input)
	filePath := asStr(input["file_path"])

	f, err := os.Open(filePath)
	if err != nil {
		slog.Error("step1ValidateFile 文件打开失败", "file_path", filePath, "err", err)
		return map[string]any{"exists": false}, nil
	}
	defer f.Close()

	var valid, total int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		total++
		if len(sc.Text()) > 0 {
			valid++
		}
	}

	output := map[string]any{
		"exists":      true,
		"valid_count": valid,
		"error_count": total - valid,
		"total_lines": total,
	}
	slog.Info("step1ValidateFile 完成", "output", output)
	return output, nil
}

// ═══════════════════════════════════════════════════════
// P2a 内部: 拆分文件 (Activity)
// ═══════════════════════════════════════════════════════

func step2aSplitFile(ctx context.Context, input map[string]any) (map[string]any, error) {
	slog.Info("step2aSplitFile 开始", "input", input)
	filePath := asStr(input["file_path"])

	f, err := os.Open(filePath)
	if err != nil {
		slog.Error("step2aSplitFile 打开失败", "file_path", filePath, "err", err)
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
		if remaining := total - start; count > remaining {
			count = remaining
		}
		if count <= 0 {
			break
		}
		shards = append(shards, map[string]any{
			"shard_id":   i,
			"start_line": start,
			"line_count": count,
			"file_path":  filePath,
		})
	}

	output := map[string]any{"shards": shards}
	slog.Info("step2aSplitFile 完成", "shard_count", len(shards), "total_lines", total)
	return output, nil
}

// ═══════════════════════════════════════════════════════
// P2a 内部: 引擎 Activity (RPW + heartbeat)
// ═══════════════════════════════════════════════════════

func step2aEngine(ctx context.Context, input map[string]any) (map[string]any, error) {
	slog.Info("step2aEngine 开始", "input", input)
	shardID := asInt(input["shard_id"])
	startLine := asInt(input["start_line"])
	lineCount := asInt(input["line_count"])
	filePath := asStr(input["file_path"])

	var progress int
	if activity.HasHeartbeatDetails(ctx) {
		activity.GetHeartbeatDetails(ctx, &progress)
	}

	// ═══ READ: 打开文件 → seek 到 start → 读取分片行 ═══
	f, err := os.Open(filePath)
	if err != nil {
		slog.Error("step2aEngine 打开失败", "shard", shardID, "err", err)
		return map[string]any{"shard_id": shardID}, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	// seek 到 StartLine——先判断行号再 Scan，避免 start=0 时多消费一行
	lineNum := 0
	for lineNum < startLine {
		if !sc.Scan() {
			break
		}
		lineNum++
	}

	// ═══ PROCESS: 逐行解析金额 + 业务处理 ═══
	const chunkSize = 3
	var processed int
	for i := progress; i < lineCount; i++ {
		if !sc.Scan() {
			break
		}
		amount, err := parseAmount(sc.Text())
		if err != nil {
			slog.Error("step2aEngine 金额解析失败",
				"shard", shardID, "line", startLine+i, "err", err)
			return map[string]any{"shard_id": shardID, "processed": processed},
				fmt.Errorf("shard %d 行 %d: %w", shardID, startLine+i, err)
		}
		slog.Info("业务处理", "shard", shardID, "line", startLine+i, "amount", amount)
		processed++
		if processed%chunkSize == 0 {
			activity.RecordHeartbeat(ctx, i+1)
		}
	}

	// ═══ WRITE: 汇总写入（当前简化：仅记录日志 + 返回 processed）═══
	output := map[string]any{"shard_id": shardID, "processed": processed}
	slog.Info("step2aEngine 完成", "output", output)
	return output, nil
}

// parseAmount 解析 "order_id,amount,date" 行中的金额字段。
// 金额非数字（如 BAD-AMOUNT）返回错误——模拟数据异常。
func parseAmount(line string) (int, error) {
	fields := strings.Split(line, ",")
	if len(fields) < 2 {
		return 0, fmt.Errorf("格式错误: %q", line)
	}
	amount, err := strconv.Atoi(strings.TrimSpace(fields[1]))
	if err != nil {
		return 0, fmt.Errorf("金额解析失败: %q", fields[1])
	}
	return amount, nil
}

// ═══════════════════════════════════════════════════════
// P2b: 金额汇总 (Activity)
// ═══════════════════════════════════════════════════════

func step2bSumAmounts(ctx context.Context, input map[string]any) (map[string]any, error) {
	slog.Info("step2bSumAmounts 开始", "input", input)
	filePath := asStr(input["file_path"])

	output := map[string]any{"total_amount": 10000, "count": 20}
	slog.Info("step2bSumAmounts 完成", "file_path", filePath, "output", output)
	return output, nil
}

// ═══════════════════════════════════════════════════════
// P3: 打印结果 (Activity)
// ═══════════════════════════════════════════════════════

func step3PrintReport(ctx context.Context, input map[string]any) (map[string]any, error) {
	slog.Info("step3PrintReport 开始", "input", input)
	msg := fmt.Sprintf(
		"file=%v total=%v valid=%v errors=%v shards=%v processed=%v amount=%v count=%v",
		input["file_path"], input["total_lines"], input["valid_count"], input["error_count"],
		input["shard_count"], input["processed"], input["total_amount"], input["count"],
	)
	slog.Info("step3PrintReport 完成", "report", msg)
	return map[string]any{"report": msg}, nil
}

// ═══════════════════════════════════════════════════════
// Child Workflow: 分片调度
// ═══════════════════════════════════════════════════════

func step2aShardProcess(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	slog.Info("step2aShardProcess 开始", "input", input)
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2, // 限制重试，快速失败传播
		},
	}

	// ① 内部 Activity: 读文件 + 拆分坐标
	slog.Info("step2aShardProcess 调用 step2aSplitFile")
	var splitRes map[string]any
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), step2aSplitFile, input).Get(ctx, &splitRes)
	if err != nil {
		slog.Error("step2aShardProcess step2aSplitFile 失败", "err", err)
		return nil, err
	}

	// ② 每个分片执行引擎 Activity
	shards := splitRes["shards"].([]any)
	var totalProcessed int
	for _, s := range shards {
		coord := s.(map[string]any)
		slog.Info("step2aShardProcess 调用 step2aEngine", "coord", coord)
		var out map[string]any
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), step2aEngine, coord).Get(ctx, &out)
		if err != nil {
			slog.Error("step2aShardProcess step2aEngine 失败", "coord", coord, "err", err)
			return nil, err
		}
		totalProcessed += asInt(out["processed"])
	}

	output := map[string]any{
		"shard_count": len(shards),
		"processed":   totalProcessed,
	}
	slog.Info("step2aShardProcess 完成", "output", output)
	return output, nil
}

// ═══════════════════════════════════════════════════════
// 编排 Workflow
// ═══════════════════════════════════════════════════════

func MainWorkflow(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	filePath := asStr(input["file_path"])
	date := asStr(input["date"])
	slog.Info("MainWorkflow 开始", "file_path", filePath, "date", date)
	ao := workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Minute}

	// P1: 校验
	validateInput := map[string]any{"file_path": filePath}
	slog.Info("MainWorkflow 调用 step1ValidateFile", "input", validateInput)
	var valRes map[string]any
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), step1ValidateFile, validateInput).Get(ctx, &valRes)
	if err != nil {
		slog.Error("MainWorkflow step1ValidateFile 失败", "err", err)
		return nil, err
	}
	if exists, _ := valRes["exists"].(bool); !exists {
		slog.Warn("MainWorkflow 文件不存在，终止", "file_path", filePath)
		notFoundReport := map[string]any{"report": "file not found"}
		return map[string]any{"report": notFoundReport}, nil
	}

	// P2: Parallel —— Child WF ∥ Activity
	shardInput := map[string]any{"file_path": filePath}
	sumInput := map[string]any{"file_path": filePath}
	slog.Info("MainWorkflow 并行调用 step2aShardProcess ∥ step2bSumAmounts")
	step2aFuture := workflow.ExecuteChildWorkflow(ctx, step2aShardProcess, shardInput)
	step2bFuture := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), step2bSumAmounts, sumInput)

	var step2aResult map[string]any
	if err := step2aFuture.Get(ctx, &step2aResult); err != nil {
		slog.Error("MainWorkflow step2aShardProcess 失败", "err", err)
		return nil, err
	}
	var step2bResult map[string]any
	if err := step2bFuture.Get(ctx, &step2bResult); err != nil {
		slog.Error("MainWorkflow step2bSumAmounts 失败", "err", err)
		return nil, err
	}

	// P3: 报告
	slog.Info("MainWorkflow 调用 step3PrintReport")
	reportInput := map[string]any{
		"file_path":    filePath,
		"total_lines":  valRes["total_lines"],
		"valid_count":  valRes["valid_count"],
		"error_count":  valRes["error_count"],
		"shard_count":  step2aResult["shard_count"],
		"processed":    step2aResult["processed"],
		"total_amount": step2bResult["total_amount"],
		"count":        step2bResult["count"],
	}
	var report map[string]any
	err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), step3PrintReport, reportInput).Get(ctx, &report)
	if err != nil {
		slog.Error("MainWorkflow step3PrintReport 失败", "err", err)
		return nil, err
	}

	slog.Info("MainWorkflow 完成", "report", report)
	return map[string]any{"report": report}, nil
}

// ═══════════════════════════════════════════════════════
// 测试：用 core 封装启动 Client + Worker
// ═══════════════════════════════════════════════════════

const taskQueue = "hzwtest-p0-core"

// newConfig 构造 core.Config——改 HostPort 指向真实 Temporal，改 TaskQueue。
func newConfig() *core.Config {
	cfg := core.NewConfig()
	cfg.Server.HostPort = "172.17.0.1:7233"
	cfg.Worker.TaskQueue = taskQueue
	return cfg
}

// startWorker 用 core.WorkerManager 注册并启动 Worker。
// 注意：WorkerManager.Start() 是同步阻塞的，需 goroutine 包一层。
func startWorker(t *testing.T, facade *core.ClientFacade) *core.WorkerManager {
	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	wm.RegisterWorkflow(MainWorkflow)
	wm.RegisterWorkflow(step2aShardProcess)
	wm.RegisterActivity(step1ValidateFile)
	wm.RegisterActivity(step2aSplitFile)
	wm.RegisterActivity(step2aEngine)
	wm.RegisterActivity(step2bSumAmounts)
	wm.RegisterActivity(step3PrintReport)

	go func() {
		if err := wm.Start(); err != nil {
			slog.Error("Worker 启动失败", "err", err)
		}
	}()
	slog.Info("Worker 已启动（core.WorkerManager）", "task_queue", taskQueue)
	return wm
}

func TestMainWorkflowP0Core(t *testing.T) {
	// ═══ 用 core.ClientFacade 建立连接 ═══
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	// ═══ 用 core.WorkerManager 启动 Worker ═══
	wm := startWorker(t, facade)
	defer wm.Stop()

	// 识别参数: filePath + date → 推导 WorkflowID
	filePath := "../testdata/test_orders.txt"
	date := "2026-08-12"
	workflowID := fmt.Sprintf("hzwtest-%s-%s", filepath.Base(filePath), date)

	mainInput := map[string]any{"file_path": filePath, "date": date}
	run, err := facade.StartWorkflow(context.Background(), workflowID, MainWorkflow, mainInput)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))
	t.Log("══════════ hzwtest_p0_core ══════════")
	t.Logf("  WorkflowID: %s", workflowID)
	for k, v := range result {
		t.Logf("  %s: %+v", k, v)
	}
}
