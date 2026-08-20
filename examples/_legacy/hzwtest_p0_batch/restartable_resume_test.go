// hzwtest_p0_batch RestartableReader 端到端验证。
//
// 场景：cursorReader（游标定位，状态是"最后主键"非 int 条数），Writer 第 2 个 chunk 失败，
//   重试时 RestoreState 从游标恢复——验证 ReaderState 经 heartbeat 在真实 Temporal 持久化并恢复。
//
// 对比 PositionAware（Seek 按条数）：RestartableReader 按游标（主键）定位，计数与定位分离。
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

// cursorListReader 读内存有序列表（模拟按主键排序的 DB 查询），逐条读。
// 状态是"最后主键"（string），而非条数——实现 RestartableReader。
type cursorListReader struct {
	items   []any
	pos     int
	lastKey string
}

func (r *cursorListReader) Read(ctx context.Context) ([]any, error) {
	if r.pos >= len(r.items) {
		return nil, nil
	}
	item := r.items[r.pos]
	r.pos++
	r.lastKey = item.(string)
	return []any{item}, nil
}

func (r *cursorListReader) SaveState() map[string]any {
	return map[string]any{"last_key": r.lastKey}
}

func (r *cursorListReader) RestoreState(state map[string]any) error {
	if state == nil {
		return nil
	}
	lastKey, _ := state["last_key"].(string)
	for i, item := range r.items {
		if item.(string) == lastKey {
			r.pos = i + 1
			return nil
		}
	}
	r.pos = 0
	return nil
}

// cursorReaderFactory 创建 cursorListReader（数据固定为 a-e）。
type cursorReaderFactory struct{}

func (f *cursorReaderFactory) NewReader(ctx context.Context, input batch.BatchInput) (batch.Reader, error) {
	return &cursorListReader{items: []any{"a", "b", "c", "d", "e"}}, nil
}

// cursorFailWriterFactory 第一次创建的 Writer 第 2 个 chunk 失败，之后成功计数。
type cursorFailWriterFactory struct {
	attempts atomic.Int32
}

func (f *cursorFailWriterFactory) NewWriter(ctx context.Context, input batch.BatchInput) (batch.Writer, error) {
	n := f.attempts.Add(1)
	if n == 1 {
		return &cursorFailWriter{writeCount: 0}, nil
	}
	return &cursorCountWriter{}, nil
}

type cursorFailWriter struct{ writeCount int }

func (w *cursorFailWriter) Write(ctx context.Context, items []any) error {
	w.writeCount++
	if w.writeCount == 2 {
		return errors.New("simulated write failure")
	}
	return nil
}

// cursorCountWriter 计数写入条数（ResultProvider）。
type cursorCountWriter struct{ count int }

func (w *cursorCountWriter) Write(ctx context.Context, items []any) error {
	w.count += len(items)
	return nil
}

func (w *cursorCountWriter) Result() map[string]any { return map[string]any{"count": w.count} }

// TestEndToEndRestartableResume 验证真实 Temporal 上 RestartableReader 游标恢复。
func TestEndToEndRestartableResume(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	// chunkSize=2 → 5 条分 3 个 chunk；第 2 个 chunk 失败
	b := batch.NewBuilder(batch.WithChunkSize(2), batch.WithMaxAttempts(2))
	factory := &cursorFailWriterFactory{}
	engineDef, err := b.BuildActivity(
		&cursorReaderFactory{}, &cursorProcessor{}, factory,
		batch.WithActivityName("cursor-engine"),
	)
	require.NoError(t, err)

	wm.RegisterActivity(engineDef)
	workflowDef := b.BuildWorkflow("cursor-engine", batch.WithWorkflowName("cursor-wf"))
	wm.RegisterWorkflow(workflowDef)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	workflowID := fmt.Sprintf("hzwtest-cursor-%d", time.Now().UnixNano())
	run, err := facade.StartWorkflow(context.Background(), workflowID, "cursor-wf", batch.BatchInput{})
	require.NoError(t, err)

	var result batch.BatchResult
	require.NoError(t, run.Get(context.Background(), &result))

	slog.Info("游标断点恢复完成", "processed", result.Processed, "output", result.Output)
	t.Log("══════════ RestartableReader 端到端 ══════════")
	t.Logf("  Processed: %d (应 5)", result.Processed)
	t.Logf("  Output: %+v", result.Output)

	require.Equal(t, 5, result.Processed, "游标恢复后处理全部 5 条")
	// 第 2 次（成功）Writer 只写了从游标恢复后的 3 条（c,d,e），而非全部 5 条
	// 已提交 chunk（a,b）不重跑
	require.Equal(t, 3, asInt(result.Output["count"]),
		"游标恢复后只写 3 条（c,d,e），已提交 a,b 不重跑")
	t.Logf("  ✅ 游标状态经 heartbeat 持久化恢复：从 c 续跑，已提交 a,b 不重跑")
}

// cursorProcessor 原样返回 item（string）。
type cursorProcessor struct{}

func (p *cursorProcessor) Process(ctx context.Context, item any) (any, error) {
	return item, nil
}
