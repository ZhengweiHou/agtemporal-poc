// hzwtest_p0_batch 端到端断点恢复验证。
//
// 场景：引擎 Activity 处理 5 行（chunkSize=2 → 3 个 chunk），Writer 在第 2 个 chunk 失败。
//   chunk 1（行0,1）→ 成功，heartbeat 记录 processed=2
//   chunk 2（行2,3）→ Writer 失败 → Activity 失败
//   Temporal 重试 → HasHeartbeatDetails=true → 读 processed=2 → Seek(2) → 从行2 续跑
//   chunk 2'（行2,3）+ chunk 3（行4）→ 成功
//
// 验证点：
//   1. 端到端断点恢复（真实 Temporal Activity 重试 + heartbeat 持久化）
//   2. 已提交 chunk（行0,1）不重跑——sumWriter 汇总只含行2,3,4 的金额 7000
//   3. PositionAware Seek 被正确调用
package hzwtest_p0_batch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// failOnceWriterFactory 第一次创建的 Writer 在第 2 个 Write 失败，之后创建的成功汇总。
// attempts 是原子计数器（跨 Activity 重试共享）。
type failOnceWriterFactory struct {
	attempts atomic.Int32
}

func (f *failOnceWriterFactory) NewWriter(ctx context.Context, input batch.BatchInput) (batch.Writer, error) {
	n := f.attempts.Add(1)
	if n == 1 {
		// 第一次执行：第 2 个 chunk 失败
		return &failWriter{writeCount: 0}, nil
	}
	// 重试：正常汇总
	return &sumWriter{}, nil
}

// failWriter 第 2 次 Write 失败（模拟瞬时故障）。
type failWriter struct {
	writeCount int
}

func (w *failWriter) Write(ctx context.Context, items []any) error {
	w.writeCount++
	if w.writeCount == 2 {
		return errors.New("simulated transient write failure")
	}
	return nil
}

// TestEndToEndResume 验证真实 Temporal 上的断点恢复。
func TestEndToEndResume(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// chunkSize=2 → 5 行分 3 个 chunk；第 2 个 chunk 失败
	b := batch.NewBuilder(
		batch.WithChunkSize(2),
		batch.WithMaxAttempts(2),
		batch.WithHeartbeatTimeout(30*time.Second),
	)
	factory := &failOnceWriterFactory{}
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, factory,
		batch.WithActivityName("resume-engine"),
	)
	require.NoError(t, err)

	wm.RegisterActivity(engineDef)
	workflowDef := b.BuildWorkflow("resume-engine", batch.WithWorkflowName("resume-wf"))
	wm.RegisterWorkflow(workflowDef)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := "../testdata/test_orders.txt"
	workflowID := fmt.Sprintf("hzwtest-resume-%d", time.Now().UnixNano())

	engineInput := batch.BatchInput{Params: map[string]any{
		"file_path": filePath, "start": 0, "line_count": 999999,
	}}

	run, err := facade.StartWorkflow(context.Background(), workflowID, "resume-wf", engineInput)
	require.NoError(t, err)

	var result batch.BatchResult
	require.NoError(t, run.Get(context.Background(), &result))

	slog.Info("断点恢复完成", "processed", result.Processed, "output", result.Output)
	t.Log("══════════ 端到端断点恢复 ══════════")
	t.Logf("  Processed: %d (应 5)", result.Processed)
	t.Logf("  Output: %+v", result.Output)

	require.Equal(t, 5, result.Processed, "断点恢复后应处理全部 5 行")

	// 关键验证：sumWriter 只汇总了从断点（行2）续跑的部分 1500+3000+2500=7000
	// 已提交的 chunk（行0,1 = 1000+2000=3000）没有重跑
	require.Equal(t, 7000, asInt(result.Output["total_amount"]),
		"应从断点续跑，只汇总行2,3,4（1500+3000+2500=7000），已提交行0,1 不重跑")
	require.Equal(t, 3, asInt(result.Output["count"]),
		"应从断点续跑，只计数 3 行（行2,3,4）")
	t.Logf("  ✅ 已提交 chunk（行0,1）未重跑，从行2 续跑成功")
}
