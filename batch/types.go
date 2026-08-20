// Package batch 提供基于 Temporal 的批处理引擎（构建期定义过程、执行期注入数据）。
package batch

import (
	"context"
)

// Reader 数据读取。无进度状态（外置到 Heartbeat）。
// 契约：
//   - len(items) == 0 且 err == nil → 数据源耗尽（EOF），引擎正常结束
//   - err != nil → 引擎返回 error，触发 Temporal 重试
type Reader interface {
	Read(ctx context.Context) ([]any, error)
}

// Processor 逐条转换。返回值原样进 chunk 交给 Writer——引擎不解释任何值语义。
// 契约（对标 Spring Batch ItemProcessor 返回 null 过滤）：
//   - 返回 (result, nil) → 结果进 chunk，原样交给 Writer
//   - 返回 (nil, nil)    → 过滤：该项不写 Writer、不计 Processed，计 Filtered
//   - 返回 (_, err)      → 引擎返回 error（SkipPolicy 可跳过，否则触发重试）
//   - **必须无状态**：跨重试复用同一实例。有状态需求（去重/计数/统计）通过外部共享存储或 Input 表达，不存实例内部。
type Processor interface {
	Process(ctx context.Context, item any) (any, error)
}

// Writer 批量持久化。必须幂等——Temporal 重试/重调度会重复提交已处理的 chunk。
type Writer interface {
	Write(ctx context.Context, items []any) error
}

// ResultProvider 可选的业务结果提供者。
// Writer 实现此接口时，引擎在循环结束后读取其 Result 填入返回 map 的 Output 字段——
// 这是引擎 Activity 向编排层返回业务聚合结果（如金额汇总）的通道。
// 未实现 → 返回 map 仅含统计（processed/skipped/filtered）。
type ResultProvider interface {
	Result() map[string]any
}

// ReaderFactory 每次执行创建新 Reader 实例。
// 实现此接口传给 NewChunkPhase → 引擎每次执行调 NewReader(ctx, input) 创建独立 Reader。
// 不实现 → 直接当 Reader 共享实例用（适用于无状态/小文件场景）。
// 对齐 Spring Batch Step Scope。
type ReaderFactory interface {
	NewReader(ctx context.Context, input BatchInput) (Reader, error)
}

// ProcessorFactory 每次执行创建新 Processor 实例（极少数有状态场景）。
type ProcessorFactory interface {
	NewProcessor(ctx context.Context, input BatchInput) (Processor, error)
}

// WriterFactory 每次执行创建新 Writer 实例（文件等有状态 Writer）。
type WriterFactory interface {
	NewWriter(ctx context.Context, input BatchInput) (Writer, error)
}

// RestartableReader 可自定义断点状态的 Reader（对标 Spring Batch ItemStream）。
// 对称接口：
//   - SaveState：chunk 提交后引擎调用，返回当前恢复状态（任意 map）
//   - RestoreState：重启时引擎调用，传入上次保存的状态，Reader 自行恢复定位
//
// 状态是任意 map[string]any——Reader 自己定义 key-value（如 DB 游标、文件字节偏移、分区 key），
// 框架不解释语义，只负责持久化（Heartbeat）与调用时机。
// 未实现 → 从头重跑（processed 保持 0，计数与重跑范围一致），Writer 幂等兜底。
//
// 线性数据源（文件行号、数组下标）可嵌入 OffsetState，自动获得"条数定位"的 SaveState/RestoreState，
// 无需手写两个方法。
type RestartableReader interface {
	Reader
	SaveState() map[string]any
	RestoreState(state map[string]any) error
}

// OffsetState 条数定位的通用实现——线性数据源嵌入它，自动获得 SaveState/RestoreState。
// 用户 Read 里维护 Offset（已读条数），框架在心跳/恢复时自动保存/恢复。
// 例如：FileReader 嵌入 OffsetState，Read 里读一行 Offset++，断点恢复自动从 Offset 继续。
type OffsetState struct {
	Offset int
}

// SaveState 保存当前条数偏移。
func (s *OffsetState) SaveState() map[string]any {
	return map[string]any{"offset": s.Offset}
}

// RestoreState 恢复条数偏移。
func (s *OffsetState) RestoreState(state map[string]any) error {
	if state == nil {
		return nil
	}
	s.Offset = asIntAny(state["offset"])
	return nil
}

// TransactionManager 事务注入（可选，nil = 无事务）。
type TransactionManager interface {
	WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}

// SkipPolicy 判断 Processor 错误是否可跳过（坏记录）。
// 对标 Spring Batch SkipPolicy：业务数据问题（如格式错误）可跳过，系统故障（DB 连接）不可跳过。
//   - 返回 true → 引擎跳过该记录，skipCount+1，继续处理
//   - 返回 false → 引擎返回 error，触发 Temporal 重试
// skipCount 是本次执行已跳过的记录数（从 0 开始），可用于跳过上限控制。
type SkipPolicy interface {
	ShouldSkip(err error, item any, skipCount int) bool
}

// BatchInput 执行期数据参数（内部契约——Reader/Processor/Writer Factory 使用）。
// Params 为 map[string]any——支持字符串、数值（分片坐标 start_line/line_count 等）。
// 注意：经 Temporal JSON 序列化后，数值型还原为 float64，取值时需类型转换。
type BatchInput struct {
	Params map[string]any `json:"params"`
}

// Partition 分片（T15 方案 B——分区带名字）。
// Name 是分区身份（partitioner 生成、与数据绑定返回）——聚合对齐 map[分区名]result、
// 分片 Child Workflow ID 派生（{主ID}-shard-{分区名}——跨 Run 幂等）的基础。
// Data 是坐标包（该分片执行单元的输入，如 file_path/start/line_count）。
type Partition struct {
	Name string         `json:"name"`
	Data map[string]any `json:"data"`
}

// ChunkProgress Heartbeat payload。Processed 是引擎写条数，Filtered 是过滤条数；
// Processed+Filtered = 已读条数（定位基准）；
// ReaderState 是 Reader 自定义状态（RestartableReader.SaveState 产物，可 nil）。
// 计数与定位分离：Processed/Filtered/Skipped 由引擎维护，ReaderState 由 Reader 定义。
type ChunkProgress struct {
	Processed   int            `json:"processed"`
	Filtered    int            `json:"filtered,omitempty"`
	Skipped     int            `json:"skipped,omitempty"` // 恢复时沿用，避免统计虚低
	ReaderState map[string]any `json:"reader_state,omitempty"`
}

// engineResult 引擎内部统计结构（不暴露——返回前拼 map）。
type engineResult struct {
	Processed int
	Skipped   int
	Filtered  int
	Output    map[string]any
}

// toMap 统计 + Output 拼 map（{processed, skipped, filtered} + Output 扁平化）——
// 与 FlowCtx 值形态一致（都是 map[string]any）。
func (r engineResult) toMap() map[string]any {
	out := map[string]any{"processed": r.Processed, "skipped": r.Skipped, "filtered": r.Filtered}
	for k, v := range r.Output {
		out[k] = v
	}
	return out
}
