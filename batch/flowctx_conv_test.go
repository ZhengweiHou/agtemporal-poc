package batch

import (
	"testing"

	"go.temporal.io/sdk/converter"
)

// TestFlowCtx_TemporalConverterRoundTrip 用 Temporal 的 DataConverter（而非裸 json.Marshal）
// 验证 FlowCtx 跨进程序列化——Activity/Child WF 入参走的是 converter 而非直接 json.Marshal，
// 曾发现裸 json round-trip 通过但 Temporal 边界 input 丢失。
func TestFlowCtx_TemporalConverterRoundTrip(t *testing.T) {
	conv := converter.GetDefaultDataConverter()

	fc := NewFlowCtx(map[string]any{"file_path": "x.txt", "start": 0})
	fc.Put("step1", map[string]any{"total_lines": 5})

	payload, err := conv.ToPayload(fc)
	if err != nil {
		t.Fatalf("ToPayload: %v", err)
	}
	t.Logf("payload data: %s", string(payload.GetData()))

	var got *FlowCtx
	if err := conv.FromPayload(payload, &got); err != nil {
		t.Fatalf("FromPayload: %v", err)
	}
	if got == nil {
		t.Fatal("got nil")
	}
	if got.Input()["file_path"] != "x.txt" {
		t.Fatalf("input lost after Temporal round-trip: %v", got.Input())
	}
	if _, ok := got.Output("step1"); !ok {
		t.Fatal("outputs lost after Temporal round-trip")
	}
}
