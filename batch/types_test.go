package batch

import (
	"context"
	"encoding/json"
	"io"
	"testing"
)

// stubReader 实现 Reader + RestartableReader（嵌入 OffsetState）+ io.Closer，供测试复用。
type stubReader struct {
	OffsetState // 嵌入：自动获得 SaveState/RestoreState（条数定位）
	lines       []any
}

func (r *stubReader) Read(ctx context.Context) ([]any, error) {
	if r.Offset >= len(r.lines) {
		return nil, nil
	}
	item := r.lines[r.Offset]
	r.Offset++
	return []any{item}, nil
}

func (r *stubReader) Close() error { return nil }

// stubReaderFactory 实现 ReaderFactory；openErr 非 nil 时 NewReader 返回错误。
type stubReaderFactory struct {
	openErr error
	reads   []any
}

func (f *stubReaderFactory) NewReader(ctx context.Context, input BatchInput) (Reader, error) {
	if f.openErr != nil {
		return nil, f.openErr
	}
	return &stubReader{lines: append([]any(nil), f.reads...)}, nil
}

// stubProcessor 实现 Processor。
type stubProcessor struct{}

func (p *stubProcessor) Process(ctx context.Context, item any) (any, error) {
	return item, nil
}

// stubProcessorFactory 实现 ProcessorFactory。
type stubProcessorFactory struct{}

func (f *stubProcessorFactory) NewProcessor(ctx context.Context, input BatchInput) (Processor, error) {
	return &stubProcessor{}, nil
}

// stubWriter 实现 Writer。
type stubWriter struct{}

func (w *stubWriter) Write(ctx context.Context, items []any) error { return nil }

// stubWriterFactory 实现 WriterFactory。
type stubWriterFactory struct{}

func (f *stubWriterFactory) NewWriter(ctx context.Context, input BatchInput) (Writer, error) {
	return &stubWriter{}, nil
}

// stubTM 实现 TransactionManager。
type stubTM struct{}

func (t *stubTM) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

// 编译期断言：保证接口实现关系（生产接口未定义时此文件无法编译，作为 RED）。
var (
	_ Reader             = (*stubReader)(nil)
	_ RestartableReader  = (*stubReader)(nil)
	_ io.Closer          = (*stubReader)(nil)
	_ ReaderFactory      = (*stubReaderFactory)(nil)
	_ Processor          = (*stubProcessor)(nil)
	_ ProcessorFactory   = (*stubProcessorFactory)(nil)
	_ Writer             = (*stubWriter)(nil)
	_ WriterFactory      = (*stubWriterFactory)(nil)
	_ TransactionManager = (*stubTM)(nil)
)

func TestBatchInput_JSONRoundTrip(t *testing.T) {
	orig := BatchInput{Params: map[string]any{"date": "2026-08-04", "shard_id": 3}}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got BatchInput
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Params["date"] != "2026-08-04" {
		t.Fatalf("params mismatch: got %v", got.Params)
	}
	// JSON 序列化后数值变 float64
	if v, ok := got.Params["shard_id"].(float64); !ok || int(v) != 3 {
		t.Fatalf("shard_id mismatch: got %v (%T)", got.Params["shard_id"], got.Params["shard_id"])
	}
}

// TestPartition_JSONRoundTrip 验证 Partition（T15 方案 B——分区带名字）可序列化。
func TestPartition_JSONRoundTrip(t *testing.T) {
	orig := Partition{Name: "filecopy-partition1", Data: map[string]any{"file_path": "/data/one", "start": 100}}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Partition
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Name != "filecopy-partition1" {
		t.Fatalf("name mismatch: got %q", got.Name)
	}
	if got.Data["file_path"] != "/data/one" {
		t.Fatalf("data mismatch: got %v", got.Data)
	}
}

// TestFlowCtx_JSONRoundTrip 验证 FlowCtx 跨进程可序列化（快照传递——input/outputs 不丢）。
// 回归保护：非导出字段默认不序列化——曾导致快照跨进程后 input/outputs 全丢（nil map panic）。
func TestFlowCtx_JSONRoundTrip(t *testing.T) {
	fc := NewFlowCtx(map[string]any{"file_path": "x.txt", "start": 0})
	fc.Put("step1", map[string]any{"total_lines": 5, "valid_count": 4})

	data, err := json.Marshal(fc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got FlowCtx
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// input 保留
	if got.Input()["file_path"] != "x.txt" {
		t.Fatalf("input lost after round-trip: %v", got.Input())
	}
	// outputs 保留
	v, ok := got.Output("step1")
	if !ok {
		t.Fatal("outputs lost after round-trip")
	}
	m := v.(map[string]any)
	if asIntAny(m["total_lines"]) != 5 {
		t.Fatalf("output value mismatch: %v", m)
	}
	// 路径访问在反序列化后仍工作
	if n, _ := got.Int("step1.total_lines"); n != 5 {
		t.Fatalf("path access after round-trip: %d, want 5", n)
	}
}

// TestFlowCtx_PathAccess 验证路径访问（T12）：精确 key / 点路径 / input 前缀。
func TestFlowCtx_PathAccess(t *testing.T) {
	fc := NewFlowCtx(map[string]any{"file_path": "x.txt", "nested": map[string]any{"k": "v"}})
	fc.Put("step1", map[string]any{"total_lines": 5, "detail": map[string]any{"count": 3}})

	// 精确 key（Phase 输出整体）
	if _, ok := fc.Output("step1"); !ok {
		t.Fatal("exact key lookup failed")
	}
	// 点路径（Phase 输出嵌套）
	if n, ok := fc.Int("step1.total_lines"); !ok || n != 5 {
		t.Fatalf("dot path int: %d, %v", n, ok)
	}
	if n, ok := fc.Int("step1.detail.count"); !ok || n != 3 {
		t.Fatalf("nested dot path int: %d, %v", n, ok)
	}
	// input 前缀
	if s, ok := fc.Str("input.file_path"); !ok || s != "x.txt" {
		t.Fatalf("input prefix str: %q, %v", s, ok)
	}
	// input 嵌套
	if s, ok := fc.Str("input.nested.k"); !ok || s != "v" {
		t.Fatalf("input nested str: %q, %v", s, ok)
	}
	// 不存在的路径
	if _, ok := fc.Str("step1.no_such"); ok {
		t.Fatal("missing path should return !ok")
	}
	// JSON float64 转换（模拟序列化后的值）
	fc.Put("step2", map[string]any{"amount": float64(100)})
	if n, ok := fc.Int("step2.amount"); !ok || n != 100 {
		t.Fatalf("float64 int conversion: %d, %v", n, ok)
	}
}
