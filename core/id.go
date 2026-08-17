package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// IDSpec 定义 WorkflowID 的推导规则（对标 Spring Batch JOB_NAME + JOB_KEY）。
//
// 核心语义：识别参数相同的输入 → 推导出相同的 WorkflowID（= 同一 JobInstance）。
// 非识别参数（NonIdentityKeys）不参与推导，仅作为运行入参传递。
//
// 默认推导规则（对标 Spring Batch JOB_KEY = hash(识别性参数序列化)）：
//
//	识别参数 = 全部入参 - NonIdentityKeys，按 key 排序序列化 key=value&...
//	→ SHA256 → 前 16 hex → "{prefix}-{hash}"
//
//	输入 {file_path: "orders.txt", date: "2026-08-17", run_id: "x"}，非识别 [run_id]
//	→ "orders-a3f2b8c9d4e5f6a7"
//
// 用 hash 而非可读拼接的原因：长度恒定（识别参数值再长也不怕）、
// 逻辑一致（单条规则，无"超长退化"分支）、对标 Spring Batch JOB_KEY。
// 可读性由 Prefix 锚定 + 参数值可查（Workflow 输入）承担，不由 ID 承担。
//
// Derive 可覆盖默认规则（自定义推导）。
type IDSpec struct {
	// Prefix 是 WorkflowID 前缀（通常为作业名），业务语义锚点 + 多作业 hash 空间隔离。必填。
	Prefix string
	// NonIdentityKeys 是不参与推导的非识别参数 key（对标 JobParameter.identifying=false）。
	// 为空 = 全部入参参与识别（对标 Spring Batch 默认全 identifying）。
	NonIdentityKeys []string
	// Derive 覆盖默认推导规则（可选）。非 nil 时忽略默认规则。
	Derive func(params map[string]any) (string, error)
}

// DeriveWorkflowID 从输入参数推导 WorkflowID。
// 格式：{prefix}-{sha256(识别参数序列化) 前 16 hex}。
func (s *IDSpec) DeriveWorkflowID(params map[string]any) (string, error) {
	if s.Derive != nil {
		return s.Derive(params)
	}
	if s.Prefix == "" {
		return "", fmt.Errorf("IDSpec.Prefix 不能为空")
	}

	// 识别参数 = 全部入参 - NonIdentityKeys（默认全识别，对标 Spring Batch）
	keys := make([]string, 0, len(params))
	for k := range params {
		if !containsStr(s.NonIdentityKeys, k) {
			keys = append(keys, k)
		}
	}
	// ⚠️ 基石，勿删：map 遍历顺序随机，不排序则相同参数序列化结果不定
	// → SHA256 不同 → WorkflowID 不同 → 幂等性直接失效（相同识别参数无法命中同一 JobInstance）。
	// 排序保证"相同识别参数 → 相同 WorkflowID"这一核心语义的确定性。
	// id_test.go 的 TestIDSpec_DeriveWorkflowID_DefaultAllIdentity 是回归保护。
	sort.Strings(keys)

	// 序列化识别参数（排序保证顺序变化不影响 ID）
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteString("&")
		}
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(fmt.Sprintf("%v", params[k]))
	}

	// SHA256 → 前 16 hex（64 bit 熵）。
	// 权衡：16 hex 够用的依据 = 批处理场景 ID 数量有限（幂等语义下 ID 数 ≈ 识别参数组合数，
	// 而非每次启动一个），百万级 ID 碰撞概率 < 10⁻⁷；截断的代价是不同参数组合可能碰撞到同一 ID，
	// 后果是"误判为同一 JobInstance"（被策略层拒绝/复用），不会产生数据错误。
	// 若未来参数组合量级上升或对碰撞零容忍，可升 32 hex（128 bit，对标 Spring Batch JOB_KEY）。
	sum := sha256.Sum256([]byte(sb.String()))
	return s.Prefix + "-" + hex.EncodeToString(sum[:])[:16], nil
}

// containsStr 判断切片是否包含目标字符串。
func containsStr(s []string, target string) bool {
	for _, v := range s {
		if v == target {
			return true
		}
	}
	return false
}
