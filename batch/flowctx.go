package batch

import (
	"encoding/json"
	"strings"
)

// FlowCtx Phase 间数据传递上下文（E1 落地——重构设计 §7）。
// input  是原始作业入参（显式字段——消灭"input"魔法 key）
// outputs 是 Phase 结果注册表（key = Phase name）
//
// 执行单元（Tasklet/WorkflowFn/PartitionerFn）统一收 *FlowCtx 快照：
//   - Activity/Child WF 入参经 Temporal 序列化传递（值拷贝 = 快照隔离，天然只读）
//   - 执行单元自己决定拿什么（fc.Input()/Str()/Int()/Output()——消灭 getIn）
//   - 输出走返回值（不修改快照）——Phase 框架层写入 outputs
type FlowCtx struct {
	input   map[string]any
	outputs map[string]any
}

// NewFlowCtx 创建 FlowCtx（input 为原始作业入参，nil 归一为空 map——防 nil map 写入 panic）。
func NewFlowCtx(input map[string]any) *FlowCtx {
	if input == nil {
		input = map[string]any{}
	}
	return &FlowCtx{
		input:   input,
		outputs: make(map[string]any),
	}
}

// MarshalJSON 自定义序列化——字段私有（input/outputs）但需跨进程传递
// （Activity/Child WF 入参走 Temporal JSON converter）。非导出字段默认不序列化，
// 不自定义序列化则快照跨进程后 input/outputs 全丢（nil map panic 根因——实测踩坑）。
func (c *FlowCtx) MarshalJSON() ([]byte, error) {
	aux := struct {
		Input   map[string]any `json:"input"`
		Outputs map[string]any `json:"outputs"`
	}{Input: c.input, Outputs: c.outputs}
	return json.Marshal(aux)
}

// UnmarshalJSON 自定义反序列化（对应 MarshalJSON）。
func (c *FlowCtx) UnmarshalJSON(data []byte) error {
	var aux struct {
		Input   map[string]any `json:"input"`
		Outputs map[string]any `json:"outputs"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.Input == nil {
		aux.Input = map[string]any{}
	}
	if aux.Outputs == nil {
		aux.Outputs = map[string]any{}
	}
	c.input = aux.Input
	c.outputs = aux.Outputs
	return nil
}

// Input 返回原始作业入参（只读约定——执行单元不应修改）。
func (c *FlowCtx) Input() map[string]any {
	return c.input
}

// Output 读取 Phase 输出（key = Phase name）。
func (c *FlowCtx) Output(name string) (any, bool) {
	v, ok := c.outputs[name]
	return v, ok
}

// Str 按路径读取字符串值（返回 (值, ok)）。
// 路径解析（T12 路径访问——递归解包）：
//   - 精确 key：fc.Str("step1-校验文件")——Phase 输出整体
//   - 点路径：fc.Str("step1.total_lines")——Phase 输出嵌套解包
//   - input 前缀：fc.Str("input.file_path")——作业入参
//   - 嵌套：fc.Str("子flow.孙flow.某key")
func (c *FlowCtx) Str(path string) (string, bool) {
	v, ok := c.lookup(path)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// Int 按路径读取整数值（返回 (值, ok)）。
// 处理 JSON 序列化后的 float64 / int / int64。
func (c *FlowCtx) Int(path string) (int, bool) {
	v, ok := c.lookup(path)
	if !ok {
		return 0, false
	}
	return asIntAny(v), true
}

// Put 存入 Phase 输出（nil 忽略）。key = Phase name。
func (c *FlowCtx) Put(name string, v any) {
	if v == nil {
		return
	}
	c.outputs[name] = v
}

// All 返回全部 Phase 输出（用于 Workflow 最终返回值）。
func (c *FlowCtx) All() map[string]any {
	return c.outputs
}

// lookup 路径解析：精确 key → input 前缀 → 点路径递归（outputs 根）。
func (c *FlowCtx) lookup(path string) (any, bool) {
	// ① 精确 key（Phase 输出）
	if v, ok := c.outputs[path]; ok {
		return v, true
	}
	// ② input.xxx 前缀（作业入参，支持嵌套）
	const inputPrefix = "input."
	if strings.HasPrefix(path, inputPrefix) {
		if v, ok := lookupPath(c.input, path[len(inputPrefix):]); ok {
			return v, true
		}
	}
	// ③ 点路径递归（outputs 根——Phase 输出嵌套）
	if v, ok := lookupPath(c.outputs, path); ok {
		return v, true
	}
	return nil, false
}

// lookupPath 从 map 根按点路径递归解包（A.B.C → map[A][B][C]）。
func lookupPath(root map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return nil, false
	}
	cur, ok := root[parts[0]]
	if !ok {
		return nil, false
	}
	for _, p := range parts[1:] {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// clone 深拷贝快照——Parallel 子 Phase 并行隔离写入区 / 分片坐标注入（不污染父）。
// 子 Phase 只读共享上游快照，写入自己的 name key，避免并发写主 FlowCtx。
func (c *FlowCtx) clone() *FlowCtx {
	return &FlowCtx{
		input:   cloneMap(c.input),
		outputs: cloneMap(c.outputs),
	}
}

// merge 把子 FlowCtx 的全部写入合并回主 FlowCtx。
// 安全前提：子 Phase 输出 key = 自身 name（唯一），预填的上游快照与原值相同，覆盖无害。
func (c *FlowCtx) merge(o *FlowCtx) {
	for k, v := range o.outputs {
		c.outputs[k] = v
	}
}

// cloneMap 深拷贝 map[string]any（值共享引用——执行单元约定不修改值内部）。
func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
