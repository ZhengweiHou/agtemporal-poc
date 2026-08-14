// Package batch 提供基于 Temporal 的批处理引擎（构建期定义过程、执行期注入数据）。
package batch

import (
	"context"

	"go.temporal.io/sdk/workflow"
)

// Reader 数据读取。无进度状态（外置到 Heartbeat）。
// 契约：
//   - len(items) == 0 且 err == nil → 数据源耗尽（EOF），引擎正常结束
//   - err != nil → 引擎返回 error，触发 Temporal 重试
type Reader interface {
	Read(ctx context.Context) ([]any, error)
}

// Processor 逐条转换。返回值原样进 chunk 交给 Writer——引擎不解释任何值语义。
// 契约：
//   - 返回 (result, nil) → 结果进 chunk，原样交给 Writer
//   - 返回 (nil, nil)    → ❌ 业务代码编写不合理。引擎不解释 nil，它会进入 chunk
//   - 返回 (_, err)      → 引擎返回 error，触发重试
//   - **必须无状态**：跨重试复用同一实例。有状态需求（去重/计数/统计）通过外部共享存储或 Input 表达，不存实例内部。
type Processor interface {
	Process(ctx context.Context, item any) (any, error)
}

// Writer 批量持久化。必须幂等——Temporal 重试/重调度会重复提交已处理的 chunk。
type Writer interface {
	Write(ctx context.Context, items []any) error
}

// ResultProvider 可选的业务结果提供者。
// Writer 实现此接口时，引擎在循环结束后读取其 Result 填入 BatchResult.Output——
// 这是引擎 Activity 向编排层返回业务聚合结果（如金额汇总）的通道。
// 未实现 → BatchResult.Output 为 nil，仅返回 Processed 计数。
type ResultProvider interface {
	Result() map[string]any
}

// ReaderFactory 每次执行创建新 Reader 实例。
// 实现此接口传给 BuildActivity → 引擎每次执行调 NewReader(ctx, input) 创建独立 Reader。
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

// PositionAware 断点恢复。实现此接口的 Reader 可以精确跳到已提交位置。
// 语义：Seek(offset) 跳到第 offset 条 raw 数据（offset = 已提交条数）。
// 引擎不解析内部实现——FileReader 跳行号，DBReader 跳分页 offset。
// 未实现 → 从头重跑（processed 保持 0，计数与重跑范围一致），Writer 幂等兜底。
type PositionAware interface {
	Reader
	Seek(offset int) error
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

// ChunkActivity 引擎 Activity：func(ctx context.Context, input BatchInput) (BatchResult, error)。
// 引擎循环（IO/心跳/事务）所在。叶子——可被任意 Workflow 通过 ExecuteActivity 调用。
// 由 Builder.BuildActivity 生成，闭包捕获 Reader模板/Processor/Writer/buildConfig。
// 注意：SDK v1.44.0 的 activity 包无 Context 类型，Activity 第一参数使用标准 context.Context
// （SDK 传入的实现即 activity context 实例，activity.RecordHeartbeat 等照常工作）。
type ChunkActivity = func(ctx context.Context, input BatchInput) (BatchResult, error)

// BatchWorkflow 编排壳 Workflow：func(ctx workflow.Context, input BatchInput) (BatchResult, error)。
// 薄壳——内部 ExecuteActivity(ChunkActivity, input) 透传结果。StartWorkflow 入口。
// 由 Builder.BuildWorkflow 生成。无 IO、无心跳（Workflow 域）。
type BatchWorkflow = func(ctx workflow.Context, input BatchInput) (BatchResult, error)

// BatchInput 执行期数据参数。框架统一 struct，Factory/Reader 通过 Params 取值。
// Params 为 map[string]any——支持字符串、数值（分片坐标 start_line/line_count 等）。
// 注意：经 Temporal JSON 序列化后，数值型还原为 float64，取值时需类型转换。
type BatchInput struct {
	Params map[string]any `json:"params"`
}

// BatchResult ChunkActivity 与 BatchWorkflow 共用返回值。
// Processed 是引擎成功处理条数；Skipped 是跳过的坏记录数；Output 是 Writer 通过 ResultProvider 提供的业务聚合结果（可 nil）。
type BatchResult struct {
	Processed int            `json:"processed"`
	Skipped   int            `json:"skipped,omitempty"`
	Output    map[string]any `json:"output,omitempty"`
}

// ChunkProgress Heartbeat payload。Processed 是唯一恢复依据。
type ChunkProgress struct {
	Processed int `json:"processed"`
}
