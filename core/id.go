package core

import (
	"fmt"
	"sort"
	"strings"
)

// IDSpec 定义 WorkflowID 的推导规则（对标 Spring Batch JobParameters → JobInstance）。
//
// 核心语义：识别参数（IdentityKeys）相同的输入 → 推导出相同的 WorkflowID（= 同一 JobInstance）。
// 非识别参数不参与推导，仅作为运行入参传递。
//
// 默认推导规则：识别参数按 key 排序，拼接 key=value，前缀加在开头：
//
//	输入 {file_path: "orders.txt", date: "2026-08-17", extra: "x"}，识别 [date, file_path]
//	→ "orders|date=2026-08-17|file_path=orders.txt"
//
// Derive 可覆盖默认规则（自定义推导）。
type IDSpec struct {
	// Prefix 是 WorkflowID 前缀（通常为作业名），防不同作业 ID 冲突。必填。
	Prefix string
	// IdentityKeys 是参与推导的识别参数 key（对标 JobParameters 的识别性参数）。
	IdentityKeys []string
	// Derive 覆盖默认推导规则（可选）。非 nil 时 IdentityKeys 忽略。
	Derive func(params map[string]any) (string, error)
}

// DeriveWorkflowID 从输入参数推导 WorkflowID。
func (s *IDSpec) DeriveWorkflowID(params map[string]any) (string, error) {
	if s.Derive != nil {
		return s.Derive(params)
	}
	if s.Prefix == "" {
		return "", fmt.Errorf("IDSpec.Prefix 不能为空")
	}

	keys := append([]string(nil), s.IdentityKeys...)
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString(s.Prefix)
	for _, k := range keys {
		v, ok := params[k]
		if !ok {
			return "", fmt.Errorf("识别参数 %q 缺失", k)
		}
		sb.WriteString("|")
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(fmt.Sprintf("%v", v))
	}
	return sb.String(), nil
}
