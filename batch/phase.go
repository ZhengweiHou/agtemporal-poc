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
	// PhaseActivity 叶子——ExecuteActivity 调用自定义 Activity。
	PhaseActivity PhaseMode = iota
	// PhaseWorkflow 叶子——ExecuteChildWorkflow 调用子 Workflow。
	PhaseWorkflow
	// PhaseEngine 叶子——引擎 Activity（BuildActivity 产物），BatchInput/BatchResult 与 map 转换。
	PhaseEngine
	// PhasePipeline 复合——串行执行子 Phase。
	PhasePipeline
	// PhaseParallel 复合——并行执行子 Phase。
	PhaseParallel
	// PhaseShard 复合——Partitioner 拆分 + 并行引擎 + 聚合。
	PhaseShard
)

// GetIn 从 FlowCtx 提取 Phase 输入。返回 (input, error)。
// input 是传给 Activity/Workflow 的入参（map[string]any）；对引擎 Phase 则是 BatchInput.Params。
type GetIn func(fc *FlowCtx) (map[string]any, error)

// Phase 是编排单元：叶子（Activity/Child Workflow/引擎）或复合（Pipeline/Parallel/Shard）。
// name 是 FlowCtx key——Phase 执行结果以 name 存入 FlowCtx，下游 GetIn 通过它读取。
type Phase struct {
	name  string
	mode  PhaseMode
	fn    interface{} // 叶子：Activity/Workflow 函数引用；复合：忽略
	getIn GetIn       // 叶子：输入提取
	steps []*Phase    // 复合：子 Phase 列表

	partitioner Partitioner    // PhaseShard：拆分器
	engine      *core.ActivityDef // PhaseEngine/PhaseShard：引擎定义（注册名在 Options.Name）

	ao workflow.ActivityOptions // Activity/引擎执行配置
}

// NewActivityPhase 创建 Activity 叶子 Phase。
// name：FlowCtx key（结果存入）；fn：Activity 函数；getIn：输入提取。
func NewActivityPhase(name string, fn interface{}, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseActivity, fn: fn, getIn: getIn}
}

// NewWorkflowPhase 创建 Child Workflow 叶子 Phase。规则同 NewActivityPhase。
// Child WorkflowID 自动派生：{主 WorkflowID}-{name}（可寻址、可查询、可 Reset）。
func NewWorkflowPhase(name string, fn interface{}, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseWorkflow, fn: fn, getIn: getIn}
}

// NewEnginePhase 创建引擎 Activity 叶子 Phase。
// engine 是 BuildActivity 产出的 core.ActivityDef——注册名取自 Options.Name（引擎闭包函数名不可靠）。
// getIn 返回的 map 即 BatchInput.Params；引擎结果 BatchResult{Processed, Output} 转成 map 存入 FlowCtx：
//
//	{processed: N, <output 字段扁平化>}
//
// 引擎注册由 Job.RegisterTo 或 CollectEngines 一体化完成。
func NewEnginePhase(name string, engine *core.ActivityDef, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseEngine, engine: engine, getIn: getIn}
}

// NewShardPhase 创建分片复合 Phase：Partitioner 拆分 → 并行引擎 Activity → 聚合。
// name：FlowCtx key（聚合结果存入）；partitioner：拆分器（纯内存）；engine：引擎定义；getIn：初始输入提取。
// 聚合规则：processed/skipped 求和；Output 中数值字段求和。
func NewShardPhase(name string, partitioner Partitioner, engine *core.ActivityDef, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseShard, partitioner: partitioner, engine: engine, getIn: getIn}
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

	default: // 叶子：Activity / Workflow / Engine
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
		fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, p.activityOptions()), p.fn, input)
		return func(ctx workflow.Context) (map[string]any, error) {
			var out map[string]any
			return out, fut.Get(ctx, &out)
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

	case PhaseEngine:
		// 引擎 Activity：BatchInput 输入，BatchResult 输出转 map
		fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, p.engineActivityOptions()), p.engine.Options.Name, BatchInput{Params: input})
		return func(ctx workflow.Context) (map[string]any, error) {
			var result BatchResult
			if err := fut.Get(ctx, &result); err != nil {
				return nil, err
			}
			return batchResultToMap(result), nil
		}

	default:
		return func(workflow.Context) (map[string]any, error) { return nil, nil }
	}
}

// activityOptions 返回 Activity 执行配置（零值 + 默认超时）。
func (p *Phase) activityOptions() workflow.ActivityOptions {
	if p.ao.StartToCloseTimeout == 0 {
		p.ao.StartToCloseTimeout = 5 * time.Minute
	}
	return p.ao
}

// engineActivityOptions 返回引擎 Activity 执行配置：基础配置 + RetryPolicy（MaximumAttempts）。
// 引擎 Activity 由框架产出，重试上限取自 ActivityDefOptions.MaximumAttempts——
// 防坏数据永久重试（Temporal 默认无限重试，会导致 Workflow 永不结束）。
func (p *Phase) engineActivityOptions() workflow.ActivityOptions {
	ao := p.activityOptions()
	if p.engine != nil && p.engine.Options.MaximumAttempts > 0 {
		ao.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: p.engine.Options.MaximumAttempts}
	}
	return ao
}

// scheduleEngine 调度引擎 Activity（PhaseShard 用），返回结果闭包。
// 引擎结果 BatchResult{Processed, Skipped, Output} 转成 map。
func (p *Phase) scheduleEngine(ctx workflow.Context, coord map[string]any) func(workflow.Context) (map[string]any, error) {
	fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, p.engineActivityOptions()), p.engine.Options.Name, BatchInput{Params: coord})
	return func(ctx workflow.Context) (map[string]any, error) {
		var result BatchResult
		if err := fut.Get(ctx, &result); err != nil {
			return nil, err
		}
		return batchResultToMap(result), nil
	}
}

// batchResultToMap 引擎结果转 FlowCtx 存取的 map（processed/skipped + Output 扁平化）。
// 统一转换——消除 schedule 与 scheduleEngine 的重复。
func batchResultToMap(result BatchResult) map[string]any {
	out := map[string]any{"processed": result.Processed, "skipped": result.Skipped}
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

// CollectActivities 收集 Phase 树中所有叶子 Activity 的函数引用（用于注册）。
func (p *Phase) CollectActivities() []interface{} {
	var out []interface{}
	var walk func(*Phase)
	walk = func(ph *Phase) {
		if len(ph.steps) > 0 {
			for _, s := range ph.steps {
				walk(s)
			}
			return
		}
		if ph.mode == PhaseActivity {
			out = append(out, ph.fn)
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

// CollectEngines 收集 Phase 树中所有引擎定义（PhaseEngine 叶子 + PhaseShard 的引擎），用于注册。
func (p *Phase) CollectEngines() []*core.ActivityDef {
	var out []*core.ActivityDef
	var walk func(*Phase)
	walk = func(ph *Phase) {
		if len(ph.steps) > 0 {
			for _, s := range ph.steps {
				walk(s)
			}
			return
		}
		if ph.engine != nil {
			out = append(out, ph.engine)
		}
	}
	walk(p)
	return out
}
