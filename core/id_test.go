package core

import (
	"testing"
)

func TestIDSpec_DeriveWorkflowID_Default(t *testing.T) {
	spec := &IDSpec{Prefix: "orders", IdentityKeys: []string{"file_path", "date"}}

	// 相同识别参数 → 相同 WorkflowID（稳定）
	id1, err := spec.DeriveWorkflowID(map[string]any{"file_path": "orders.txt", "date": "2026-08-17", "extra": "x"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	id2, err := spec.DeriveWorkflowID(map[string]any{"date": "2026-08-17", "file_path": "orders.txt", "extra": "y"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if id1 != id2 {
		t.Errorf("相同识别参数应推导相同 ID: %q vs %q", id1, id2)
	}
	t.Logf("  推导: %s", id1)

	// 识别参数变化 → 不同 WorkflowID
	id3, err := spec.DeriveWorkflowID(map[string]any{"file_path": "orders.txt", "date": "2026-08-18"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if id1 == id3 {
		t.Errorf("识别参数变化应推导不同 ID")
	}

	// 非识别参数不影响 ID
	id4, err := spec.DeriveWorkflowID(map[string]any{"file_path": "orders.txt", "date": "2026-08-17", "extra": "zzz"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if id1 != id4 {
		t.Errorf("非识别参数不应影响 ID: %q vs %q", id1, id4)
	}
}

func TestIDSpec_DeriveWorkflowID_StableOrder(t *testing.T) {
	spec := &IDSpec{Prefix: "job", IdentityKeys: []string{"b", "a"}}
	id, err := spec.DeriveWorkflowID(map[string]any{"a": "1", "b": "2"})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	// key 按排序：a 在前
	want := "job|a=1|b=2"
	if id != want {
		t.Errorf("推导 = %q, want %q（key 按排序拼接）", id, want)
	}
}

func TestIDSpec_DeriveWorkflowID_MissingIdentity(t *testing.T) {
	spec := &IDSpec{Prefix: "job", IdentityKeys: []string{"file_path"}}
	_, err := spec.DeriveWorkflowID(map[string]any{})
	if err == nil {
		t.Error("识别参数缺失应报错")
	}
}

func TestIDSpec_DeriveWorkflowID_EmptyPrefix(t *testing.T) {
	spec := &IDSpec{IdentityKeys: []string{"a"}}
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
