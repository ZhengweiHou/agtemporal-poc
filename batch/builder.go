package batch

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/core"
)

// Builder 持有不可变 buildConfig 和自增序号，统一管理 Activity 和 Workflow 的构建。
// 非并发安全——构建期应在单 goroutine 中使用。
type Builder struct {
	bc  buildConfig
	seq int
}

// NewBuilder 创建 Builder。opts 写入内部 buildConfig（以 DefaultConfig 为 base）。
func NewBuilder(opts ...BuilderOption) *Builder {
	bc := buildConfig{Config: DefaultConfig()}
	for _, opt := range opts {
		opt(&bc)
	}
	return &Builder{bc: bc}
}

// DefaultActivityOpts 从 buildConfig 复制 Activity 默认值（独立副本，修改不影响基座）。
func (b *Builder) DefaultActivityOpts() ActivityOptions {
	return ActivityOptions{
		ChunkSize:          b.bc.ChunkSize,
		TransactionManager: b.bc.TransactionManager,
	}
}

// DefaultWorkflowOpts 从 buildConfig 复制 Workflow 默认值（独立副本，修改不影响基座）。
func (b *Builder) DefaultWorkflowOpts() WorkflowOptions {
	return WorkflowOptions{
		HeartbeatTimeout:     b.bc.HeartbeatTimeout,
		StartToCloseTimeout:  b.bc.StartToCloseTimeout,
		MaxAttempts:          b.bc.MaxAttempts,
		RetryInitialInterval: b.bc.RetryInitialInterval,
	}
}

// BuildActivity 构建 ChunkActivity（引擎循环，叶子 Activity）。
// 参数 r/p/w 可以是对应的接口或 Factory 接口；引擎运行时自动检测。
// opts 覆写 ActivityOptions（不传即用 DefaultActivityOpts）。
//
// 返回 *core.ActivityDef——注册名 Name 必填（用户 WithActivityName 指定或自动生成）。
// 注意：自动生成名仅单 Builder 内唯一，跨 Builder 未显式命名会产生同名，请用 WithActivityName。
func (b *Builder) BuildActivity(
	reader, processor, writer interface{},
	opts ...ActivityOption,
) (*core.ActivityDef, error) {
	if reader == nil {
		return nil, errors.New("batch: reader is nil")
	}
	if processor == nil {
		return nil, errors.New("batch: processor is nil")
	}
	if writer == nil {
		return nil, errors.New("batch: writer is nil")
	}
	if !isReaderLike(reader) {
		return nil, errors.New("batch: reader must implement Reader or ReaderFactory")
	}
	if !isProcessorLike(processor) {
		return nil, errors.New("batch: processor must implement Processor or ProcessorFactory")
	}
	if !isWriterLike(writer) {
		return nil, errors.New("batch: writer must implement Writer or WriterFactory")
	}

	// 基底 + 覆写
	ao := b.DefaultActivityOpts()
	for _, opt := range opts {
		opt(&ao)
	}

	// Name 必填：用户指定 → 自动生成
	if ao.Name == "" {
		b.seq++
		ao.Name = fmt.Sprintf("chunk-activity-%d", b.seq)
	}

	if ao.ChunkSize <= 0 {
		return nil, errors.New("batch: chunk size must be positive")
	}

	// 闭包捕获（模板 + ao），不经过 Temporal 序列化
	closure := func(ctx context.Context, input BatchInput) (BatchResult, error) {
		// Factory 检测：自动创建执行期实例（对齐 Spring Batch Step Scope）
		r, err := resolveReader(reader, ctx, input)
		if err != nil {
			return BatchResult{}, err
		}
		p, err := resolveProcessor(processor, ctx, input)
		if err != nil {
			return BatchResult{}, err
		}
		w, err := resolveWriter(writer, ctx, input)
		if err != nil {
			return BatchResult{}, err
		}

		// 引擎循环
		result, err := runChunkLoop(ctx, r, p, w, ao.TransactionManager, ao.ChunkSize)

		// 业务聚合结果：Writer 实现 ResultProvider 时，读其 Result 填入 Output
		if rp, ok := w.(ResultProvider); ok {
			result.Output = rp.Result()
		}

		// Close 错误收集：无论主流程是否成功都关闭执行期实例（释放资源）；
		// 仅当主流程成功时 Close 错误才覆盖返回值（触发重试 + 幂等兜底）。
		if cerr := closeExecInstances(w, p, r); cerr != nil && err == nil {
			return BatchResult{}, cerr
		}
		return result, err
	}
	return &core.ActivityDef{Fn: closure, Options: core.ActivityDefOptions{Name: ao.Name}}, nil
}

// closeExecInstances 逆序关闭执行期实例中实现 io.Closer 者，返回首个错误。
func closeExecInstances(instances ...any) error {
	for i := len(instances) - 1; i >= 0; i-- {
		if c, ok := instances[i].(io.Closer); ok {
			if err := c.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

// BuildWorkflow 构建 BatchWorkflow（编排壳）。
// activityName 是 BuildActivity 产出的注册名（须非空），用于 ExecuteActivity 字符串调用——
// 走 SDK 原生字符串名路径，兼容 DisableRegistrationAliasing。
func (b *Builder) BuildWorkflow(
	activityName string,
	opts ...WorkflowOption,
) *core.WorkflowDef {
	wo := b.DefaultWorkflowOpts()
	for _, opt := range opts {
		opt(&wo)
	}

	if wo.Name == "" {
		b.seq++
		wo.Name = fmt.Sprintf("batch-workflow-%d", b.seq)
	}

	// 薄壳：ExecuteActivity 透传结果
	closure := func(ctx workflow.Context, input BatchInput) (BatchResult, error) {
		var result BatchResult
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: wo.StartToCloseTimeout,
				HeartbeatTimeout:    wo.HeartbeatTimeout,
				RetryPolicy: &temporal.RetryPolicy{
					MaximumAttempts: int32(wo.MaxAttempts),
					InitialInterval: wo.RetryInitialInterval,
				},
			}),
			activityName,
			input, // Input 作为 Activity 参数，走 Temporal 序列化
		).Get(ctx, &result)
		return result, err
	}
	return &core.WorkflowDef{Fn: closure, Options: core.WorkflowDefOptions{Name: wo.Name}}
}

func isReaderLike(v interface{}) bool {
	_, ok1 := v.(Reader)
	_, ok2 := v.(ReaderFactory)
	return ok1 || ok2
}

func isProcessorLike(v interface{}) bool {
	_, ok1 := v.(Processor)
	_, ok2 := v.(ProcessorFactory)
	return ok1 || ok2
}

func isWriterLike(v interface{}) bool {
	_, ok1 := v.(Writer)
	_, ok2 := v.(WriterFactory)
	return ok1 || ok2
}

func resolveReader(v interface{}, ctx context.Context, input BatchInput) (Reader, error) {
	if rf, ok := v.(ReaderFactory); ok {
		return rf.NewReader(ctx, input)
	}
	return v.(Reader), nil
}

func resolveProcessor(v interface{}, ctx context.Context, input BatchInput) (Processor, error) {
	if pf, ok := v.(ProcessorFactory); ok {
		return pf.NewProcessor(ctx, input)
	}
	return v.(Processor), nil
}

func resolveWriter(v interface{}, ctx context.Context, input BatchInput) (Writer, error) {
	if wf, ok := v.(WriterFactory); ok {
		return wf.NewWriter(ctx, input)
	}
	return v.(Writer), nil
}
