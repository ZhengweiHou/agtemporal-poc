// 基线——直接 Temporal SDK。
// 结构：校验 → 并行(分片 RPW ∥ 汇总) → 报告
// 分片处理为内聚单元：自己读文件 + 拆分 + 调度引擎 Activity
package hzwtest_raw

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// ── 坐标 ──

type shardCoord struct {
	ShardID   int
	StartLine int
	LineCount int
	FilePath  string
}

// shardCount 是分片 flow 的定义参数——内部常量，不入参
const shardCount = 4

// ═══════════════════════════════════════════════════════
// P1: 校验文件 (Activity)
// ═══════════════════════════════════════════════════════

type validateOutput struct {
	Exists     bool
	ValidCount int
	ErrorCount int
	TotalLines int
}

func step1ValidateFile(ctx context.Context, filePath string) (validateOutput, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return validateOutput{Exists: false}, nil
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
	return validateOutput{Exists: true, ValidCount: valid, ErrorCount: total - valid, TotalLines: total}, nil
}

// ═══════════════════════════════════════════════════════
// P2a 内部: 拆分文件 (Activity)
// ═══════════════════════════════════════════════════════

type splitInput struct {
	FilePath string
}

type splitOutput struct {
	Shards []shardCoord
}

func step2aSplitFile(ctx context.Context, input splitInput) (splitOutput, error) {
	f, err := os.Open(input.FilePath)
	if err != nil {
		return splitOutput{}, err
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

	var shards []shardCoord
	for i := 0; i < shardCount; i++ {
		start := i * per
		count := per
		if remaining := total - start; count > remaining {
			count = remaining
		}
		if count <= 0 {
			break
		}
		shards = append(shards, shardCoord{
			ShardID: i, StartLine: start, LineCount: count, FilePath: input.FilePath,
		})
	}
	return splitOutput{Shards: shards}, nil
}

// ═══════════════════════════════════════════════════════
// P2a 内部: 引擎 Activity (RPW + heartbeat)
// ═══════════════════════════════════════════════════════

type engineOutput struct {
	ShardID   int
	Processed int
}

func step2aEngine(ctx context.Context, coord shardCoord) (engineOutput, error) {
	var progress int
	if activity.HasHeartbeatDetails(ctx) {
		activity.GetHeartbeatDetails(ctx, &progress)
	}

	f, err := os.Open(coord.FilePath)
	if err != nil {
		return engineOutput{ShardID: coord.ShardID}, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	lineNum := 0
	for sc.Scan() && lineNum < coord.StartLine {
		lineNum++
	}

	const chunkSize = 3
	var processed int
	for i := progress; i < coord.LineCount; i++ {
		if !sc.Scan() {
			break
		}
		processed++
		if processed%chunkSize == 0 {
			activity.RecordHeartbeat(ctx, i+1)
		}
	}
	return engineOutput{ShardID: coord.ShardID, Processed: processed}, nil
}

// ═══════════════════════════════════════════════════════
// P2b: 金额汇总 (Activity)
// ═══════════════════════════════════════════════════════

func step2bSumAmounts(ctx context.Context, filePath string) (map[string]any, error) {
	_ = filePath
	return map[string]any{"total_amount": 10000, "count": 20}, nil
}

// ═══════════════════════════════════════════════════════
// P3: 打印结果 (Activity)
// ═══════════════════════════════════════════════════════

type reportInput struct {
	FilePath   string
	TotalLines int
	ValidCount int
	ErrorCount int
	Shards     int
	Processed  int
	Amount     int
	Count      int
}

type reportOutput struct {
	Report string
}

func step3PrintReport(ctx context.Context, input reportInput) (reportOutput, error) {
	msg := fmt.Sprintf(
		"file=%s total=%d valid=%d errors=%d shards=%d processed=%d amount=%d count=%d",
		input.FilePath, input.TotalLines, input.ValidCount, input.ErrorCount,
		input.Shards, input.Processed, input.Amount, input.Count,
	)
	return reportOutput{Report: msg}, nil
}

// ═══════════════════════════════════════════════════════
// Child Workflow: 分片调度
// ═══════════════════════════════════════════════════════

func step2aShardProcess(ctx workflow.Context, input splitInput) (map[string]any, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
	}

	// ① 内部 Activity: 读文件 + 拆分坐标
	var splitRes splitOutput
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), step2aSplitFile, input).Get(ctx, &splitRes); err != nil {
		return nil, err
	}

	// ② 每个分片执行引擎 Activity
	var totalProcessed int
	for _, coord := range splitRes.Shards {
		var out engineOutput
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), step2aEngine, coord).Get(ctx, &out); err != nil {
			return nil, err
		}
		totalProcessed += out.Processed
	}

	return map[string]any{
		"shard_count": len(splitRes.Shards),
		"processed":   totalProcessed,
	}, nil
}

// ═══════════════════════════════════════════════════════
// 编排 Workflow
// ═══════════════════════════════════════════════════════

func MainWorkflow(ctx workflow.Context, filePath string, date string) (map[string]any, error) {
	ao := workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Minute}

	// P1: 校验
	var valRes validateOutput
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), step1ValidateFile, filePath).Get(ctx, &valRes); err != nil {
		return nil, err
	}
	if !valRes.Exists {
		return map[string]any{"report": reportOutput{Report: "file not found"}}, nil
	}

	// P2: Parallel —— Child WF ∥ Activity
	step2aFuture := workflow.ExecuteChildWorkflow(ctx, step2aShardProcess, splitInput{
		FilePath: filePath,
	})
	step2bFuture := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), step2bSumAmounts, filePath)

	var step2aResult map[string]any
	if err := step2aFuture.Get(ctx, &step2aResult); err != nil {
		return nil, err
	}
	var step2bResult map[string]any
	if err := step2bFuture.Get(ctx, &step2bResult); err != nil {
		return nil, err
	}

	// P3: 报告
	var report reportOutput
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), step3PrintReport, reportInput{
		FilePath:   filePath,
		TotalLines: valRes.TotalLines,
		ValidCount: valRes.ValidCount,
		ErrorCount: valRes.ErrorCount,
		Shards:     int(step2aResult["shard_count"].(float64)),
		Processed:  int(step2aResult["processed"].(float64)),
		Amount:     int(step2bResult["total_amount"].(float64)),
		Count:      int(step2bResult["count"].(float64)),
	}).Get(ctx, &report); err != nil {
		return nil, err
	}

	return map[string]any{"report": report}, nil
}

// ═══════════════════════════════════════════════════════
// Test
// ═══════════════════════════════════════════════════════

const taskQueue = "hzwtest-raw"

func TestMainWorkflowRaw(t *testing.T) {
	// c, _ := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	c, _ := client.Dial(client.Options{HostPort: "127.0.0.1:7233", Namespace: "default"})
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(MainWorkflow)
	w.RegisterWorkflow(step2aShardProcess)
	w.RegisterActivity(step1ValidateFile)
	w.RegisterActivity(step2aSplitFile)
	w.RegisterActivity(step2aEngine)
	w.RegisterActivity(step2bSumAmounts)
	w.RegisterActivity(step3PrintReport)
	go func() { _ = w.Run(worker.InterruptCh()) }()
	time.Sleep(200 * time.Millisecond)
	defer w.Stop()

	// 识别参数: filePath + date → 推导 WorkflowID
	filePath := "../testdata/test_orders.txt"
	date := "2026-08-12"
	workflowID := fmt.Sprintf("hzwtest-%s-%s", filepath.Base(filePath), date)

	run, _ := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, MainWorkflow, filePath, date)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))
	t.Log("══════════ hzwtest_raw ══════════")
	t.Logf("  WorkflowID: %s", workflowID)
	for k, v := range result {
		t.Logf("  %s: %+v", k, v)
	}
}
