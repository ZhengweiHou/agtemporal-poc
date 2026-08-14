// hzwtest_p0_batch Skip 端到端验证。
//
// 场景：文件含 1 行坏数据（BAD-AMOUNT），SkipPolicy 跳过它，其余正常处理。
//   对标 Spring Batch：坏记录 skip，批不中断，继续处理。
//
// 验证点：
//   1. 坏数据（金额解析失败）被 Skip，批不中断
//   2. 其余记录正常处理，processed=4 skipped=1
//   3. 汇总只含正常记录（不含坏记录）
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// skipBadAmount 跳过"金额解析失败"错误（业务数据问题），其他错误不跳过。
type skipBadAmount struct{}

func (p *skipBadAmount) ShouldSkip(err error, item any, skipCount int) bool {
	return strings.Contains(err.Error(), "金额解析失败")
}

// TestSkipBadRecord 验证坏记录被 Skip，批继续。
func TestSkipBadRecord(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("skip-engine"),
		batch.WithActivitySkipPolicy(&skipBadAmount{}),
	)
	require.NoError(t, err)

	wm.RegisterActivity(engineDef)
	workflowDef := b.BuildWorkflow("skip-engine", batch.WithWorkflowName("skip-wf"))
	wm.RegisterWorkflow(workflowDef)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// 坏数据文件：1 行 BAD-AMOUNT + 4 行正常
	badFile := "../testdata/test_orders_skip.txt"
	badData := "ORD001,1000,2026-01-01\n" +
		"ORD002,BAD-AMOUNT,2026-01-02\n" +
		"ORD003,1500,2026-01-03\n" +
		"ORD004,3000,2026-01-04\n" +
		"ORD005,2500,2026-01-05\n"
	require.NoError(t, os.WriteFile(badFile, []byte(badData), 0644))

	workflowID := fmt.Sprintf("hzwtest-skip-%d", time.Now().UnixNano())
	run, err := facade.StartWorkflow(context.Background(), workflowID, "skip-wf",
		batch.BatchInput{Params: map[string]any{
			"file_path": badFile, "start_line": 0, "line_count": 999999,
		}})
	require.NoError(t, err)

	var result batch.BatchResult
	require.NoError(t, run.Get(context.Background(), &result))

	slog.Info("Skip 完成", "processed", result.Processed, "skipped", result.Skipped, "output", result.Output)
	t.Log("══════════ Skip 端到端 ══════════")
	t.Logf("  Processed: %d (应 4)", result.Processed)
	t.Logf("  Skipped: %d (应 1)", result.Skipped)
	t.Logf("  Output: %+v", result.Output)

	require.Equal(t, 4, result.Processed, "坏记录跳过，处理 4 条正常记录")
	require.Equal(t, 1, result.Skipped, "跳过 1 条坏记录")
	// 汇总只含正常记录：1000+1500+3000+2500=8000
	require.Equal(t, 8000, asInt(result.Output["total_amount"]), "汇总不含坏记录")
	require.Equal(t, 4, asInt(result.Output["count"]), "计数 4")
	t.Logf("  ✅ 坏记录被 Skip，批不中断：processed=4 skipped=1 amount=8000")
}
