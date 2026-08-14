package batch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

// stubReader 实现 Reader + PositionAware + io.Closer，供测试复用。
type stubReader struct {
	lines []any
	pos   int
}

func (r *stubReader) Read(ctx context.Context) ([]any, error) {
	if r.pos >= len(r.lines) {
		return nil, nil
	}
	items := r.lines[r.pos:]
	r.pos = len(r.lines)
	return items, nil
}

func (r *stubReader) Seek(offset int) error {
	if offset < 0 || offset > len(r.lines) {
		return errors.New("offset out of range")
	}
	r.pos = offset
	return nil
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
	_ PositionAware      = (*stubReader)(nil)
	_ io.Closer          = (*stubReader)(nil)
	_ ReaderFactory      = (*stubReaderFactory)(nil)
	_ Processor          = (*stubProcessor)(nil)
	_ ProcessorFactory   = (*stubProcessorFactory)(nil)
	_ Writer             = (*stubWriter)(nil)
	_ WriterFactory      = (*stubWriterFactory)(nil)
	_ TransactionManager = (*stubTM)(nil)
	_ ChunkActivity      = func(ctx context.Context, input BatchInput) (BatchResult, error) { return BatchResult{}, nil }
)

func TestChunkActivity_Type(t *testing.T) {
	var fn ChunkActivity = func(ctx context.Context, input BatchInput) (BatchResult, error) {
		return BatchResult{}, nil
	}
	if fn == nil {
		t.Fatal("ChunkActivity function type mismatch")
	}
}

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
