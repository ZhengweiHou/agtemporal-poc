// hzwtest_p0_batch 过滤端到端验证。
//
// 对标 Spring Batch ItemProcessor 返回 null 过滤：金额为 0 的记录被过滤（不写、不计数），
// 而非报错中断或 Skip。
//
// 验证点：
//   1. Processor 返回 nil → 过滤（不写 chunk、不计 Processed、计 Filtered）
//   2. 过滤与 Skip 的区别：过滤是"合法跳过"，Skip 是"错误跳过"
package hzwtest_p0_batch

import (
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

// filterAmountProcessor 金额为 0 的记录过滤（返回 nil），金额非数字报错。
type filterAmountProcessor struct{}

func (p *filterAmountProcessor) Process(ctx context.Context, item any) (any, error) {
	line, _ := item.(string)
	fields := strings.Split(line, ",")
	if len(fields) < 2 {
		return nil, fmt.Errorf("格式错误: %q", line)
	}
	amount, err := strconv.Atoi(strings.TrimSpace(fields[1]))
	if err != nil {
		return nil, fmt.Errorf("金额解析失败: %q", fields[1])
	}
	if amount == 0 {
		return nil, nil // 过滤：金额为 0 的记录不处理
	}
	return amount, nil
}

// TestFilterRecord 验证过滤：金额为 0 的记录被过滤，批继续。
func TestFilterRecord(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &filterAmountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("filter-engine"),
	)
	require.NoError(t, err)

	wm.RegisterActivity(engineDef)
	workflowDef := b.BuildWorkflow("filter-engine", batch.WithWorkflowName("filter-wf"))
	wm.RegisterWorkflow(workflowDef)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// 数据：5 行，其中 1 行金额为 0（过滤）
	dataFile := "../testdata/test_orders_filter.txt"
	data := "ORD001,1000,2026-01-01\n" +
		"ORD002,0,2026-01-02\n" + // ← 金额 0，过滤
		"ORD003,1500,2026-01-03\n" +
		"ORD004,3000,2026-01-04\n" +
		"ORD005,2500,2026-01-05\n"
	require.NoError(t, os.WriteFile(dataFile, []byte(data), 0644))

	workflowID := fmt.Sprintf("hzwtest-filter-%d", time.Now().UnixNano())
	run, err := facade.StartWorkflow(context.Background(), workflowID, "filter-wf",
		batch.BatchInput{Params: map[string]any{
			"file_path": dataFile, "start_line": 0, "line_count": 999999,
		}})
	require.NoError(t, err)

	var result batch.BatchResult
	require.NoError(t, run.Get(context.Background(), &result))

	slog.Info("过滤完成", "processed", result.Processed, "filtered", result.Filtered, "output", result.Output)
	t.Log("══════════ 过滤端到端 ══════════")
	t.Logf("  Processed: %d (应 4)", result.Processed)
	t.Logf("  Filtered: %d (应 1)", result.Filtered)
	t.Logf("  Output: %+v", result.Output)

	require.Equal(t, 4, result.Processed, "过滤 1 条，处理 4 条")
	require.Equal(t, 1, result.Filtered, "过滤 1 条金额为 0 的记录")
	// 汇总不含过滤记录：1000+1500+3000+2500=8000
	require.Equal(t, 8000, asInt(result.Output["total_amount"]), "汇总不含过滤记录")
	require.Equal(t, 4, asInt(result.Output["count"]), "计数 4")
	t.Logf("  ✅ 过滤生效：金额为 0 的记录被过滤，processed=4 filtered=1 amount=8000")
}
