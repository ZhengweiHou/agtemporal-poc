package batch

import (
	"time"

	"go.temporal.io/sdk/workflow"
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
)

// GetIn 从 FlowCtx 提取 Phase 输入。返回 (input, error)。
// input 是传给 Activity/Workflow 的入参（map[string]any）；对引擎 Phase 则是 BatchInput.Params。
type GetIn func(fc *FlowCtx) (map[string]any, error)

// Phase 是编排单元：叶子（Activity/Child Workflow/引擎）或复合（Pipeline/Parallel）。
// name 是 FlowCtx key——Phase 执行结果以 name 存入 FlowCtx，下游 GetIn 通过它读取。
type Phase struct {
	name  string
	mode  PhaseMode
	fn    interface{} // 叶子：Activity/Workflow 函数引用，或引擎注册名（字符串）；复合：忽略
	getIn GetIn       // 叶子：输入提取
	steps []*Phase    // 复合：子 Phase 列表

	ao workflow.ActivityOptions // Activity/引擎执行配置
}

// NewActivityPhase 创建 Activity 叶子 Phase。
// name：FlowCtx key（结果存入）；fn：Activity 函数；getIn：输入提取。
func NewActivityPhase(name string, fn interface{}, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseActivity, fn: fn, getIn: getIn}
}

// NewWorkflowPhase 创建 Child Workflow 叶子 Phase。规则同 NewActivityPhase。
func NewWorkflowPhase(name string, fn interface{}, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseWorkflow, fn: fn, getIn: getIn}
}

// NewEnginePhase 创建引擎 Activity 叶子 Phase。
// engineName 是 BuildActivity 产出的注册名（字符串）——引擎闭包函数名不可靠，用注册名字符串调用。
// getIn 返回的 map 即 BatchInput.Params；引擎结果 BatchResult{Processed, Output} 转成 map 存入 FlowCtx：
//   {processed: N, <output 字段扁平化>}
// 引擎 Activity 本身需单独 RegisterActivity（BuildActivity 产出的 core.ActivityDef）。
func NewEnginePhase(name string, engineName string, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseEngine, fn: engineName, getIn: getIn}
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
		fut := workflow.ExecuteChildWorkflow(ctx, p.fn, input)
		return func(ctx workflow.Context) (map[string]any, error) {
			var out map[string]any
			return out, fut.Get(ctx, &out)
		}

	case PhaseEngine:
		// 引擎 Activity：BatchInput 输入，BatchResult 输出转 map
		fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, p.activityOptions()), p.fn, BatchInput{Params: input})
		return func(ctx workflow.Context) (map[string]any, error) {
			var result BatchResult
			if err := fut.Get(ctx, &result); err != nil {
				return nil, err
			}
			out := map[string]any{"processed": result.Processed}
			for k, v := range result.Output {
				out[k] = v
			}
			return out, nil
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
