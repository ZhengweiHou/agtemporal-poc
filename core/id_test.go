package core

import (
	"testing"
)

// 期望格式：{prefix}-{16 hex}。长度恒定 16+1+len(prefix)。
func TestIDSpec_DeriveWorkflowID_DefaultAllIdentity(t *testing.T) {
	spec := &IDSpec{Prefix: "orders"}

	// 默认全识别：全部入参参与推导（无 NonIdentityKeys）
	id1, err := spec.DeriveWorkflowID(map[string]any{"file_path": "orders.txt", "date": "2026-08-17", "extra": "x"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	id2, err := spec.DeriveWorkflowID(map[string]any{"date": "2026-08-17", "file_path": "orders.txt", "extra": "x"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if id1 != id2 {
		t.Errorf("参数顺序变化应推导相同 ID: %q vs %q", id1, id2)
	}

	// 识别参数变化（extra 不同）→ 不同 ID（默认全识别，extra 参与）
	id3, err := spec.DeriveWorkflowID(map[string]any{"file_path": "orders.txt", "date": "2026-08-17", "extra": "y"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if id1 == id3 {
		t.Error("默认全识别：extra 变化应推导不同 ID")
	}

	// 格式校验：{prefix}-{16 hex}
	if len(id1) != len("orders-")+16 {
		t.Errorf("ID 长度异常: %q (len=%d)", id1, len(id1))
	}
	t.Logf("  推导: %s", id1)
}

func TestIDSpec_DeriveWorkflowID_NonIdentityExcluded(t *testing.T) {
	spec := &IDSpec{Prefix: "orders", NonIdentityKeys: []string{"run_id"}}

	// 非识别参数（run_id）不影响 ID
	id1, err := spec.DeriveWorkflowID(map[string]any{"file_path": "orders.txt", "date": "2026-08-17", "run_id": "abc"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	id2, err := spec.DeriveWorkflowID(map[string]any{"file_path": "orders.txt", "date": "2026-08-17", "run_id": "xyz"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if id1 != id2 {
		t.Errorf("非识别参数不应影响 ID: %q vs %q", id1, id2)
	}

	// 识别参数变化（date）→ 不同 ID
	id3, err := spec.DeriveWorkflowID(map[string]any{"file_path": "orders.txt", "date": "2026-08-18", "run_id": "abc"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if id1 == id3 {
		t.Error("识别参数变化应推导不同 ID")
	}
	t.Logf("  推导: %s", id1)
}

func TestIDSpec_DeriveWorkflowID_LengthConstant(t *testing.T) {
	// 长短识别参数值 → 相同长度（hash 恒定）
	short, _ := (&IDSpec{Prefix: "job"}).DeriveWorkflowID(map[string]any{"path": "a.txt"})
	long, _ := (&IDSpec{Prefix: "job"}).DeriveWorkflowID(map[string]any{
		"path": "/data/orders/2026/08/orders_20260817_final_version_2_very_long_filename.txt"})
	if len(short) != len(long) {
		t.Errorf("hash 应恒定长度: %d vs %d", len(short), len(long))
	}
	t.Logf("  短值: %s\n  长值: %s", short, long)
}

func TestIDSpec_DeriveWorkflowID_EmptyPrefix(t *testing.T) {
	spec := &IDSpec{}
	_, err := spec.DeriveWorkflowID(map[string]any{"a": "1"})
	if err == nil {
		t.Error("Prefix 为空应报错")
	}
}

func TestIDSpec_DeriveWorkflowID_CustomDerive(t *testing.T) {
	spec := &IDSpec{
		Prefix: "job",
		Derive: func(params map[string]any) (string, error) {
			return "custom-" + params["id"].(string), nil
		},
	}
	id, err := spec.DeriveWorkflowID(map[string]any{"id": "42"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if id != "custom-42" {
		t.Errorf("自定义推导 = %q, want custom-42", id)
	}
}
