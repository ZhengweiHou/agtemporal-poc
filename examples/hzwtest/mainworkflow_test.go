// 测试案例骨架——展示 Change-002 升级后的预期 API 用法。
// 标记为 Skip：待 batch/core 改动完成后启用。
//
// 案例：
//
//	MainWorkflow(filePath)
//	  ├─ step1CheckFile   → Activity
//	  ├─ step2Parallel
//	  │   ├─ step2aShardProcess  → Child Workflow (Partitioner + Parallel 引擎)
//	  │   └─ step2bSumAmounts    → Activity
//	  └─ step3PrintReport → Activity (从 FlowCtx 汇集)
package hzwtest

// import (
// 	"context"
// 	"fmt"
// 	"testing"

// 	"go.temporal.io/sdk/workflow"

// 	"github.com/ZhengweiHou/agtemporal/batch"
// 	"github.com/ZhengweiHou/agtemporal/core"
// )

// // ═══════════════════════════════════════════════════════════════
// // 自定义 Activity
// // ═══════════════════════════════════════════════════════════════

// func step1CheckFile(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
// 	_ = input.Params["file_path"].(string)
// 	return batch.BatchResult{Output: map[string]any{"exists": true}}, nil
// }

// func step2bSumAmounts(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
// 	return batch.BatchResult{Output: map[string]any{
// 		"total_amount": float64(10000), "count": float64(20),
// 	}}, nil
// }

// func step3PrintReport(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
// 	p := input.Params
// 	msg := fmt.Sprintf("file=%v exists=%v shards=%v amount=%v count=%v",
// 		p["file_path"], p["exists"], p["shard_count"], p["total_amount"], p["count"])
// 	return batch.BatchResult{Output: map[string]any{"report": msg}}, nil
// }

// // ═══════════════════════════════════════════════════════════════
// // 引擎 Activity —— Builder (R→P→W)
// // ═══════════════════════════════════════════════════════════════

// type step2aReaderFactory struct{}
// func (f *step2aReaderFactory) NewReader(ctx context.Context, input batch.BatchInput) (batch.Reader, error) {
// 	n := int(input.Params["line_count"].(float64))
// 	lines := make([]string, n)
// 	for i := 0; i < n; i++ { lines[i] = fmt.Sprintf("LINE-%d", i) }
// 	return &step2aReader{lines: lines}, nil
// }
// type step2aReader struct{ lines []string; pos int }
// func (r *step2aReader) Read(ctx context.Context) ([]any, error) {
// 	const chunk = 3
// 	if r.pos >= len(r.lines) { return nil, nil }
// 	end := r.pos + chunk; if end > len(r.lines) { end = len(r.lines) }
// 	out := make([]any, end-r.pos)
// 	for i, s := range r.lines[r.pos:end] { out[i] = s }
// 	r.pos = end; return out, nil
// }

// type step2aProcessor struct{}
// func (p step2aProcessor) Process(ctx context.Context, item any) (any, error) { return item, nil }

// type step2aWriterFactory struct{}
// func (f step2aWriterFactory) NewWriter(ctx context.Context, input batch.BatchInput) (batch.Writer, error) {
// 	return &step2aWriter{}, nil
// }
// type step2aWriter struct{}
// func (w *step2aWriter) Write(ctx context.Context, items []any) error { return nil }

// // ═══════════════════════════════════════════════════════════════
// // Child Workflow —— 分片引擎处理
// // ═══════════════════════════════════════════════════════════════

// func step2aShardProcess(ctx workflow.Context, input batch.BatchInput) (batch.BatchResult, error) {
// 	fc := batch.NewFlowCtx()
// 	shards := splitFile(int(input.Params["total_lines"].(float64)), input.Params["shard_count"].(int))
// 	phases := batch.Partition("s", "step2a-engine", shards)
// 	if err := batch.Parallel(ctx, fc, phases...); err != nil {
// 		return batch.BatchResult{}, err
// 	}
// 	return batch.BatchResult{Output: map[string]any{
// 		"shard_count": float64(len(shards)), "completed": true,
// 	}}, nil
// }

// func splitFile(total, n int) []batch.BatchInput {
// 	per := total / n; if total%n != 0 { per++ }
// 	var out []batch.BatchInput
// 	for i := 0; i < n; i++ {
// 		start, count := i*per, per
// 		if start+count > total { count = total - start }
// 		if count <= 0 { break }
// 		out = append(out, batch.BatchInput{Params: map[string]any{
// 			"shard_id": float64(i), "start_line": float64(start), "line_count": float64(count),
// 		}})
// 	}
// 	return out
// }

// // ═══════════════════════════════════════════════════════════════
// // 编排 Workflow
// // ═══════════════════════════════════════════════════════════════

// func MainWorkflow(ctx workflow.Context, filePath string) (map[string]any, error) {
// 	fc := batch.NewFlowCtx()
// 	fc.Put("file_path", filePath)

// 	p1 := &batch.Phase{Name: "step1-检查文件", GetIn: func(fc *batch.FlowCtx) batch.BatchInput {
// 		fp, _ := fc.Get("file_path")
// 		return batch.BatchInput{Params: map[string]any{"file_path": fp}}
// 	}}
// 	if err := batch.Pipeline(ctx, fc, p1); err != nil { return nil, err }

// 	p2a := &batch.Phase{Name: "step2a-分片处理", WF: step2aShardProcess, GetIn: func(fc *batch.FlowCtx) batch.BatchInput {
// 		fp, _ := fc.Get("file_path")
// 		return batch.BatchInput{Params: map[string]any{"file_path": fp, "total_lines": float64(20), "shard_count": 4}}
// 	}}
// 	p2b := &batch.Phase{Name: "step2b-金额汇总", GetIn: func(fc *batch.FlowCtx) batch.BatchInput {
// 		fp, _ := fc.Get("file_path")
// 		return batch.BatchInput{Params: map[string]any{"file_path": fp}}
// 	}}
// 	if err := batch.Parallel(ctx, fc, p2a, p2b); err != nil { return nil, err }

// 	p3 := &batch.Phase{Name: "step3-打印结果", GetIn: func(fc *batch.FlowCtx) batch.BatchInput {
// 		params := map[string]any{}
// 		for _, k := range []string{"step1-检查文件", "step2a-分片处理", "step2b-金额汇总"} {
// 			if v, ok := fc.Get(k); ok { if m, ok := v.(map[string]any); ok { for mk, mv := range m { params[mk] = mv } } }
// 		}
// 		if v, ok := fc.Get("file_path"); ok { params["file_path"] = v }
// 		return batch.BatchInput{Params: params}
// 	}}
// 	if err := batch.Pipeline(ctx, fc, p3); err != nil { return nil, err }

// 	return fc.All(), nil
// }

// // ═══════════════════════════════════════════════════════════════
// // Test
// // ═══════════════════════════════════════════════════════════════

// func TestMainWorkflow(t *testing.T) {
// 	t.Skip("待 batch/core 改动完成后启用")

// 	def, _ := batch.NewBuilder(batch.WithStartToCloseTimeout(5*time.Minute)).BuildActivity(
// 		&step2aReaderFactory{}, step2aProcessor{}, step2aWriterFactory{},
// 		batch.WithActivityName("step2a-engine"),
// 	)

// 	cfg := core.NewConfig()
// 	cfg.Server.HostPort = "172.17.0.1:7233"
// 	cfg.Worker.TaskQueue = "hzwtest"

// 	cf, _ := core.NewClientFacade(cfg)
// 	wm, _ := core.NewWorkerManager(cf, cfg)
// 	wm.RegisterWorkflow(MainWorkflow)

// 	reg := batch.NewPhaseRegistry()
// 	reg.AddActivity("step1-检查文件", step1CheckFile, nil)
// 	reg.AddActivity("step2b-金额汇总", step2bSumAmounts, nil)
// 	reg.AddActivity("step3-打印结果", step3PrintReport, nil)
// 	reg.AddActivity("step2a-engine", def.Fn, nil)
// 	reg.AddChildWorkflow("step2a-分片处理", step2aShardProcess, nil)
// 	wm.InstallPhaseRegistry(reg)

// 	_ = cf.StartWorkflow(context.Background(), "main-1", MainWorkflow, "/data/orders.txt")
// }
