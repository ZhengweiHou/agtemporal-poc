package batch

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/core"
)

// PhaseMode 编排单元类型。
type PhaseMode int

const (
	// PhaseActivity 叶子——ExecuteActivity 调用 Activity（引擎/自定义统一持 def）。
	PhaseActivity PhaseMode = iota
	// PhaseWorkflow 叶子——ExecuteChildWorkflow 调用子 Workflow。
	PhaseWorkflow
	// PhasePipeline 复合——串行执行子 Phase。
	PhasePipeline
	// PhaseParallel 复合——并行执行子 Phase。
	PhaseParallel
	// PhaseShard 复合——Partitioner 拆分 + 并行引擎 + 聚合。
	PhaseShard
)

// GetIn 从 FlowCtx 提取 Phase 输入。返回 (input, error)。
// input 是传给 Activity 的 BatchInput.Params（map[string]any）或 Child Workflow 的入参。
type GetIn func(fc *FlowCtx) (map[string]any, error)

// Phase 是编排单元：叶子（Activity/Child Workflow）或复合（Pipeline/Parallel/Shard）。
// name 是 FlowCtx key——Phase 执行结果以 name 存入 FlowCtx，下游 GetIn 通过它读取。
type Phase struct {
	name  string
	mode  PhaseMode
	def   *core.ActivityDef // 叶子：Activity 定义（引擎/自定义统一，注册名在 Options.Name）；复合：忽略
	fn    interface{}       // 叶子：Child Workflow 函数引用；复合：忽略
	getIn GetIn             // 叶子：输入提取
	steps []*Phase          // 复合：子 Phase 列表

	partitioner Partitioner // PhaseShard：拆分器

	ao workflow.ActivityOptions // Activity 执行配置
}

// NewActivityPhase 创建 Activity 叶子 Phase（引擎或自定义统一）。
// name：FlowCtx key（结果存入）；def：Activity 定义（BuildActivity 引擎 / BuildTasklet 自定义）；
// getIn：输入提取（返回的 map 即 BatchInput.Params）。
// Activity 结果 BatchResult{Processed/Skipped/Filtered/Output} 转成 map 存入 FlowCtx：
//
//	{processed: N, skipped: N, filtered: N, <output 字段扁平化>}
func NewActivityPhase(name string, def *core.ActivityDef, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseActivity, def: def, getIn: getIn}
}

// NewWorkflowPhase 创建 Child Workflow 叶子 Phase。规则同 NewActivityPhase。
// Child WorkflowID 自动派生：{主 WorkflowID}-{name}（可寻址、可查询、可 Reset）。
func NewWorkflowPhase(name string, fn interface{}, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseWorkflow, fn: fn, getIn: getIn}
}

// NewShardPhase 创建分片复合 Phase：Partitioner 拆分 → 并行引擎 Activity → 聚合。
// name：FlowCtx key（聚合结果存入）；partitioner：拆分器（纯内存）；def：分片引擎定义；getIn：初始输入提取。
// 聚合规则：processed/skipped 求和；Output 中数值字段求和。
func NewShardPhase(name string, partitioner Partitioner, def *core.ActivityDef, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseShard, partitioner: partitioner, def: def, getIn: getIn}
}

// Pipeline 串行组合多个 Phase（返回复合 Phase）。
func Pipeline(phases ...*Phase) *Phase {
	return &Phase{mode: PhasePipeline, steps: phases}
}

// Parallel 并行组合多个 Phase（返回复合 Phase）。
func Parallel(phases ...*Phase) *Phase {
	return &Phase{mode: PhaseParallel, steps: phases}
}

// FlowCtx Phase 间数据传递上下文。
// 无锁纯 map——Workflow 内 coroutine 协同执行（Pipeline 串行、Parallel 回调同一 goroutine），无并发写。
type FlowCtx struct {
	outputs map[string]any
}

// NewFlowCtx 创建空 FlowCtx。
func NewFlowCtx() *FlowCtx {
	return &FlowCtx{outputs: make(map[string]any)}
}

// Put 存入 Phase 输出（nil 忽略）。
func (c *FlowCtx) Put(name string, v any) {
	if v == nil {
		return
	}
	c.outputs[name] = v
}

// Get 读取 Phase 输出。
func (c *FlowCtx) Get(name string) (any, bool) {
	v, ok := c.outputs[name]
	return v, ok
}

// All 返回全部输出（用于 Workflow 最终返回值）。
func (c *FlowCtx) All() map[string]any {
	return c.outputs
}

// run 递归执行 Phase。
func (p *Phase) run(ctx workflow.Context, fc *FlowCtx) error {
	switch p.mode {
	case PhasePipeline:
		for _, step := range p.steps {
			if err := step.run(ctx, fc); err != nil {
				return err
			}
		}
		return nil

	case PhaseParallel:
		// 并行：先取输入 → 全部调度（Future 并发）→ 收集结果
		type task struct {
			phase *Phase
			get   func(workflow.Context) (map[string]any, error)
		}
		tasks := make([]task, 0, len(p.steps))
		for _, step := range p.steps {
			in, err := step.getIn(fc)
			if err != nil {
				return err
			}
			tasks = append(tasks, task{phase: step, get: step.schedule(ctx, in)})
		}
		for i := range tasks {
			out, err := tasks[i].get(ctx)
			if err != nil {
				return err
			}
			fc.Put(tasks[i].phase.name, out)
		}
		return nil

	case PhaseShard:
		// 分片：Partitioner 拆分 → 并行引擎 Activity → 聚合
		in, err := p.getIn(fc)
		if err != nil {
			return err
		}
		coords, err := p.partitioner.Partition(in)
		if err != nil {
			return err
		}
		if len(coords) == 0 {
			fc.Put(p.name, map[string]any{"processed": 0, "skipped": 0})
			return nil
		}
		// 并行调度引擎（每个分片一个引擎 Activity）
		gets := make([]func(workflow.Context) (map[string]any, error), 0, len(coords))
		for _, coord := range coords {
			gets = append(gets, p.scheduleEngine(ctx, coord))
		}
		// 收集 + 聚合
		results := make([]map[string]any, 0, len(coords))
		for _, get := range gets {
			out, err := get(ctx)
			if err != nil {
				return err
			}
			results = append(results, out)
		}
		fc.Put(p.name, aggregateShardResults(results))
		return nil

	default: // 叶子：Activity / Workflow
		in, err := p.getIn(fc)
		if err != nil {
			return err
		}
		out, err := p.schedule(ctx, in)(ctx)
		if err != nil {
			return err
		}
		fc.Put(p.name, out)
		return nil
	}
}

// schedule 调度叶子 Phase，返回"获取结果"闭包（Future 已在调度时发出，实现并发）。
func (p *Phase) schedule(ctx workflow.Context, input map[string]any) func(workflow.Context) (map[string]any, error) {
	switch p.mode {
	case PhaseActivity:
		// 统一契约：BatchInput 输入，BatchResult 输出转 map（引擎与自定义 Activity 同一签名）
		fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, p.activityOptions()), p.def.Options.Name, BatchInput{Params: input})
		return func(ctx workflow.Context) (map[string]any, error) {
			var result BatchResult
			if err := fut.Get(ctx, &result); err != nil {
				return nil, err
			}
			return batchResultToMap(result), nil
		}

	case PhaseWorkflow:
		// Child Workflow：ID 自动派生 {主 WorkflowID}-{name}（可寻址/可查询/可 Reset），
		// 幂等策略 AllowDuplicateFailedOnly 级联（主重跑时已完成 Child 不重建）。
		mainID := workflow.GetInfo(ctx).WorkflowExecution.ID
		childOpts := workflow.ChildWorkflowOptions{
			WorkflowID:            mainID + "-" + p.name,
			WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		}
		fut := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, childOpts), p.fn, input)
		return func(ctx workflow.Context) (map[string]any, error) {
			var out map[string]any
			return out, fut.Get(ctx, &out)
		}

	default:
		return func(workflow.Context) (map[string]any, error) { return nil, nil }
	}
}

// activityOptions 返回 Activity 执行配置：基础 + RetryPolicy（MaximumAttempts）。
// 所有 Activity（引擎/自定义统一）应用重试上限——防坏数据永久重试（Temporal 默认无限重试，Workflow 永不结束）。
func (p *Phase) activityOptions() workflow.ActivityOptions {
	ao := p.ao
	if ao.StartToCloseTimeout == 0 {
		ao.StartToCloseTimeout = 5 * time.Minute
	}
	if p.def != nil && p.def.Options.MaximumAttempts > 0 {
		ao.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: p.def.Options.MaximumAttempts}
	}
	return ao
}

// scheduleEngine 调度引擎 Activity（PhaseShard 用），返回结果闭包。
func (p *Phase) scheduleEngine(ctx workflow.Context, coord map[string]any) func(workflow.Context) (map[string]any, error) {
	fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, p.activityOptions()), p.def.Options.Name, BatchInput{Params: coord})
	return func(ctx workflow.Context) (map[string]any, error) {
		var result BatchResult
		if err := fut.Get(ctx, &result); err != nil {
			return nil, err
		}
		return batchResultToMap(result), nil
	}
}

// batchResultToMap Activity 结果转 FlowCtx 存取的 map（processed/skipped/filtered + Output 扁平化）。
// 统一转换——消除 schedule 与 scheduleEngine 的重复。
func batchResultToMap(result BatchResult) map[string]any {
	out := map[string]any{"processed": result.Processed, "skipped": result.Skipped, "filtered": result.Filtered}
	for k, v := range result.Output {
		out[k] = v
	}
	return out
}

// aggregateShardResults 聚合分片结果：processed/skipped 求和，Output 数值字段求和。
func aggregateShardResults(results []map[string]any) map[string]any {
	agg := map[string]any{"processed": 0, "skipped": 0}
	for _, r := range results {
		agg["processed"] = asIntAny(agg["processed"]) + asIntAny(r["processed"])
		agg["skipped"] = asIntAny(agg["skipped"]) + asIntAny(r["skipped"])
		for k, v := range r {
			if k == "processed" || k == "skipped" {
				continue
			}
			if n, ok := toFloat64(v); ok {
				agg[k] = asIntAny(agg[k]) + int(n)
			}
		}
	}
	return agg
}

// asIntAny 把 any 转 int（处理 JSON 序列化后的 float64 / int / int64）。
func asIntAny(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	}
	return 0
}

// toFloat64 判断 any 是否为数值并转 float64。
func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	}
	return 0, false
}

// Compile 把 Phase 树编译成 Workflow 函数。
// 返回的 Workflow 接收 map[string]any 入参，执行 Phase 树，返回 FlowCtx.All()。
// 入参可通过 getIn 中 fc.Get("input") 读取。
func Compile(root *Phase) func(workflow.Context, map[string]any) (map[string]any, error) {
	return func(ctx workflow.Context, input map[string]any) (map[string]any, error) {
		fc := NewFlowCtx()
		fc.Put("input", input)
		if err := root.run(ctx, fc); err != nil {
			return nil, err
		}
		return fc.All(), nil
	}
}

// CollectDefs 收集 Phase 树中所有 Activity 叶子（含分片引擎）的 def（用于注册）。
// 引擎与自定义 Activity 统一——一次收集，注册一体（P0-1）。
func (p *Phase) CollectDefs() []*core.ActivityDef {
	var out []*core.ActivityDef
	var walk func(*Phase)
	walk = func(ph *Phase) {
		if len(ph.steps) > 0 {
			for _, s := range ph.steps {
				walk(s)
			}
			return
		}
		if (ph.mode == PhaseActivity || ph.mode == PhaseShard) && ph.def != nil {
			out = append(out, ph.def)
		}
	}
	walk(p)
	return out
}

// CollectWorkflows 收集 Phase 树中所有叶子 Child Workflow 的函数引用（用于注册）。
func (p *Phase) CollectWorkflows() []interface{} {
	var out []interface{}
	var walk func(*Phase)
	walk = func(ph *Phase) {
		if len(ph.steps) > 0 {
			for _, s := range ph.steps {
				walk(s)
			}
			return
		}
		if ph.mode == PhaseWorkflow {
			out = append(out, ph.fn)
		}
	}
	walk(p)
	return out
}
