package batch

import (
	"errors"
	"fmt"
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

	shardWf     interface{} // PhaseShard：分片 Child Workflow（内部生成，捕获 def）
	shardWfName string      // PhaseShard：分片 Child Workflow 注册名（{def名}-shard-wf）

	subRoot *Phase // NewFlowPhase：子树（供 CollectDefs/CollectWorkflowDefs 遍历）

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
// fn：业务手写 flow 函数（`func(ctx workflow.Context, input map[string]any) (map[string]any, error)`）——
// 用于需要内部控制（循环/分支/动态调度）的步骤（逃逸舱，暴露 SDK）。
// 静态子流程（Pipeline/Parallel 树）优先用 NewFlowPhase（无需写 workflow 函数）。
func NewWorkflowPhase(name string, fn interface{}, getIn GetIn) *Phase {
	return &Phase{name: name, mode: PhaseWorkflow, fn: fn, getIn: getIn}
}

// NewFlowPhase 把子编排树包装成 Child Workflow（对标 Spring FlowStep）。
// name：FlowCtx key；root：子 Phase 树（Pipeline/Parallel/叶子）。
// 输入契约：透传父 FlowCtx 的 input（框架内部处理，无需用户写 getIn）——
// 子树 compileFlow 以该 input 注入子树 FlowCtx，内层叶子从子树 FlowCtx 读。
// 需要"非 input 输入"（读上游 Phase 输出）时用 NewFlowPhaseWithInput。
// 输出语义：单叶子子树 → 叶子输出扁平（直接是该 Phase 的结果）；
// 多 Phase 子树 → 各 Phase 输出合并（不含 input）。
// 子树 defs 自动收集注册（CollectDefs/CollectWorkflowDefs 遍历 subRoot）。
func NewFlowPhase(name string, root *Phase) *Phase {
	return &Phase{
		name:    name,
		mode:    PhaseWorkflow,
		fn:      compileFlow(root),
		getIn:   passthroughInput, // 内部透传（用户无感知）
		subRoot: root,             // 供 CollectDefs/CollectWorkflowDefs 遍历子树
	}
}

// NewFlowPhaseWithInput 同 NewFlowPhase，但显式指定输入提取（非 input 输入场景）。
func NewFlowPhaseWithInput(name string, root *Phase, getIn GetIn) *Phase {
	return &Phase{
		name:    name,
		mode:    PhaseWorkflow,
		fn:      compileFlow(root),
		getIn:   getIn,
		subRoot: root,
	}
}

// passthroughInput 透传父 FlowCtx 的 input（NewFlowPhase 默认输入，内部函数不暴露）。
func passthroughInput(fc *FlowCtx) (map[string]any, error) {
	input, ok := fc.Get("input")
	if !ok {
		return nil, fmt.Errorf("batch: NewFlowPhase 透传输入缺失——父 FlowCtx 无 \"input\"（用 NewFlowPhaseWithInput 显式指定）")
	}
	m, ok := input.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("batch: NewFlowPhase 透传输入类型错误: %T（用 NewFlowPhaseWithInput 显式指定）", input)
	}
	return m, nil
}

// compileFlow 子树编译成 Child WF 函数。
// 顶层 recover 同 Compile：子树内 getIn/Partitioner panic → 快速失败（防 WFT 无限重试）。
func compileFlow(root *Phase) func(workflow.Context, map[string]any) (map[string]any, error) {
	return func(ctx workflow.Context, input map[string]any) (out map[string]any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("batch: workflow panic（检查 getIn/Partitioner 类型断言）: %v", r)
			}
		}()
		fc := NewFlowCtx()
		fc.Put("input", input)
		if err := root.run(ctx, fc); err != nil {
			return nil, err
		}
		all := fc.All()
		delete(all, "input")
		// 单叶子子树：叶子输出扁平（下游 getIn 直接读字段，无需解嵌套）
		if isLeafPhase(root) {
			if v, ok := all[root.name]; ok {
				if m, ok := v.(map[string]any); ok {
					return m, nil
				}
			}
		}
		return all, nil
	}
}

// isLeafPhase 判断 Phase 是否为叶子（Activity/Child WF/分片）。
func isLeafPhase(p *Phase) bool {
	return p.mode == PhaseActivity || p.mode == PhaseWorkflow || p.mode == PhaseShard
}

// NewShardPhase 创建分片复合 Phase：Partitioner 拆分 → 并行分片 Child Workflow → 聚合。
// name：FlowCtx key（聚合结果存入）；partitioner：拆分器（纯内存）；def：分片执行单元定义（引擎/tasklet 统一）；
// getIn：初始输入提取。
// 每个分片是一个 Child Workflow（可推导 ID：{主 WorkflowID}-shard-{n}）：
//   - 可寻址（Describe/Reset 单个分片）
//   - 幂等级联（主重跑时已完成分片被拒，识别 AlreadyStarted 并跳过；失败分片重跑）
// 分片 Child 内部 ExecuteActivity(def.Options.Name)——分片 = 任意执行单元的并行包装。
// 聚合规则：processed/skipped 求和；Output 中数值字段求和。
func NewShardPhase(name string, partitioner Partitioner, def *core.ActivityDef, getIn GetIn) *Phase {
	shardWf := func(ctx workflow.Context, input map[string]any) (map[string]any, error) {
		ao := workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Minute}
		if def.Options.MaximumAttempts > 0 {
			ao.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: def.Options.MaximumAttempts}
		}
		var result BatchResult
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, ao), def.Options.Name, BatchInput{Params: input}).Get(ctx, &result)
		if err != nil {
			return nil, err
		}
		return batchResultToMap(result), nil
	}
	return &Phase{
		name: name, mode: PhaseShard, partitioner: partitioner, def: def, getIn: getIn,
		shardWf:     shardWf,
		shardWfName: def.Options.Name + "-shard-wf", // 从引擎注册名派生（唯一）
	}
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

// clone 浅拷贝当前内容——Parallel 子 Phase 并行时的隔离写入区：
// 子 Phase 只读共享上游快照，写入自己的 name key，避免并发写主 FlowCtx。
func (c *FlowCtx) clone() *FlowCtx {
	out := &FlowCtx{outputs: make(map[string]any, len(c.outputs))}
	for k, v := range c.outputs {
		out.outputs[k] = v
	}
	return out
}

// merge 把子 FlowCtx 的全部写入合并回主 FlowCtx。
// 安全前提：子 Phase 输出 key = 自身 name（唯一），预填的上游快照与原值相同，覆盖无害。
func (c *FlowCtx) merge(o *FlowCtx) {
	for k, v := range o.outputs {
		c.outputs[k] = v
	}
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
		// 并行：子 Phase 各在自己的确定性 goroutine 递归 run（支持叶子/复合组合编排），
		// 结果经 workflow.Channel 收集，FlowCtx 隔离写入避免并发写竞态。
		// 失败快速传播（对标 Spring Batch）：任一子 Phase 失败 → cancel 其余分支的子 ctx，
		// 其 Activity/Child Workflow 收到取消快速终止（减少资源浪费与副作用窗口）。
		type presult struct {
			fc  *FlowCtx
			err error
		}
		results := workflow.NewChannel(ctx)
		cancels := make([]workflow.CancelFunc, len(p.steps))
		for i, step := range p.steps {
			step := step
			childCtx, cancel := workflow.WithCancel(ctx)
			cancels[i] = cancel
			workflow.Go(childCtx, func(ctx workflow.Context) {
				subFC := fc.clone()
				err := step.run(ctx, subFC)
				results.Send(ctx, presult{fc: subFC, err: err})
			})
		}
		for range p.steps {
			var r presult
			results.Receive(ctx, &r)
			if r.err != nil {
				// 失败快速传播：cancel 其余分支的 future（Workflow 侧停止等待），立即失败。
				// 限制（实测确认）：Workflow 内 WithCancel 只取消 future，不传播到已调度的
				// Activity 执行（Activity 在 worker 继续跑完副作用窗口）——由 Activity 幂等兜底；
				// Child Workflow 分支可被完整取消（RequestCancelExternalWorkflowExecution 传播）。
				for _, cancel := range cancels {
					cancel()
				}
				return r.err
			}
			fc.merge(r.fc)
		}
		return nil

	case PhaseShard:
		// 分片：Partitioner 拆分 → 并行分片 Child Workflow（可推导 ID + 幂等级联）→ 聚合
		in, err := p.getIn(fc)
		if err != nil {
			return err
		}
		coords, err := p.partitioner.Partition(in)
		if err != nil {
			return err
		}
		if len(coords) == 0 {
			// 结果结构统一（与正常分片一致）：processed/skipped/skipped_shards 全 0
			fc.Put(p.name, map[string]any{"processed": 0, "skipped": 0, "skipped_shards": 0})
			return nil
		}
		// 并行调度分片 Child Workflow（Future 并发）
		mainID := workflow.GetInfo(ctx).WorkflowExecution.ID
		gets := make([]func(workflow.Context) (map[string]any, bool, error), 0, len(coords))
		skippedShards := 0
		for i, coord := range coords {
			i, coord := i, coord
			childOpts := workflow.ChildWorkflowOptions{
				WorkflowID:            fmt.Sprintf("%s-shard-%d", mainID, i),
				WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
			}
			// 分片 Child 重试上限：继承 def.Options.MaximumAttempts——防坏数据无限重试
			// （Temporal 默认无限重试，分片失败会拖住主 WF 永不结束，同引擎 Activity 教训）。
			if p.def != nil && p.def.Options.MaximumAttempts > 0 {
				childOpts.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: p.def.Options.MaximumAttempts}
			}
			fut := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, childOpts), p.shardWfName, coord)
			gets = append(gets, func(ctx workflow.Context) (map[string]any, bool, error) {
				var out map[string]any
				err := fut.Get(ctx, &out)
				if err != nil {
					// 幂等级联：分片已完成（上次 Run 成功）→ 识别 AlreadyStarted 并跳过（不重跑）
					var alreadyStarted *temporal.ChildWorkflowExecutionAlreadyStartedError
					if errors.As(err, &alreadyStarted) {
						return nil, true, nil
					}
					return nil, false, err
				}
				return out, false, nil
			})
		}
		// 收集 + 聚合（跳过的分片不计入本次聚合——其结果在上次 Run 已提交，需外部状态才完整）
		results := make([]map[string]any, 0, len(coords)-skippedShards)
		for _, get := range gets {
			out, skipped, err := get(ctx)
			if err != nil {
				return err
			}
			if skipped {
				skippedShards++
				continue
			}
			results = append(results, out)
		}
		agg := aggregateShardResults(results)
		agg["skipped_shards"] = skippedShards
		fc.Put(p.name, agg)
		return nil

	default: // 叶子：Activity / Workflow
		in, err := p.getIn(fc)
		if err != nil {
			return err
		}
		out, skipped, err := p.schedule(ctx, in)(ctx)
		if err != nil {
			return err
		}
		if skipped {
			// 幂等跳过（PhaseWorkflow：同 ID 上次 Run 已完成）——结果不可得。
			// 写入标记让下游 getIn 可感知；若下游断言具体 key 会 nil panic（快速失败）。
			fc.Put(p.name, map[string]any{"skipped": true})
			return nil
		}
		fc.Put(p.name, out)
		return nil
	}
}

// schedule 调度叶子 Phase，返回"获取结果"闭包（Future 已在调度时发出，实现并发）。
// 返回 (out, skipped, err)：skipped=true 表示 Phase 被幂等跳过（同 ID 上次 Run 已完成），
// 结果不可得（在上次 Run 的 History）——下游依赖时通过 getIn 感知（nil 或 skipped 标记）。
func (p *Phase) schedule(ctx workflow.Context, input map[string]any) func(workflow.Context) (map[string]any, bool, error) {
	switch p.mode {
	case PhaseActivity:
		// 统一契约：BatchInput 输入，BatchResult 输出转 map（引擎与自定义 Activity 同一签名）
		fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, p.activityOptions()), p.def.Options.Name, BatchInput{Params: input})
		return func(ctx workflow.Context) (map[string]any, bool, error) {
			var result BatchResult
			if err := fut.Get(ctx, &result); err != nil {
				return nil, false, err
			}
			return batchResultToMap(result), false, nil
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
		return func(ctx workflow.Context) (map[string]any, bool, error) {
			var out map[string]any
			err := fut.Get(ctx, &out)
			if err != nil {
				// 幂等级联（对标 PhaseShard）：同 ID 上次 Run 已完成 → AlreadyStarted → 跳过。
				// AllowDuplicateFailedOnly 下 AlreadyStarted ⇒ 之前 Completed（成功）；
				// Running 窗口（并发分支被终止前）极小，严谨场景由客户端确认。
				var alreadyStarted *temporal.ChildWorkflowExecutionAlreadyStartedError
				if errors.As(err, &alreadyStarted) {
					return nil, true, nil
				}
				return nil, false, err
			}
			return out, false, nil
		}

	default:
		return func(workflow.Context) (map[string]any, bool, error) { return nil, false, nil }
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
//
// 顶层 recover：getIn/Partition 等 Phase 代码 panic（如类型断言）→ 转为 Workflow 失败。
// 不 recover 的后果：Workflow 函数 panic → SDK 判定 WFT 失败 → 无限重试 + History 暴涨
// （GrpcMessageTooLarge 卡死，实际踩过）。快速失败让错误在 run.Get 可见。
func Compile(root *Phase) func(workflow.Context, map[string]any) (map[string]any, error) {
	return func(ctx workflow.Context, input map[string]any) (out map[string]any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("batch: workflow panic（检查 getIn/Partitioner 类型断言）: %v", r)
			}
		}()
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
		if ph.subRoot != nil {
			walk(ph.subRoot) // NewFlowPhase：子树 defs 也收集
		}
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

// CollectWorkflowDefs 收集 Phase 树中所有 Child Workflow 定义（用户 Child WF + 分片 ShardWF），用于注册。
// 用户 Child WF：裸函数（普通函数名可靠，Name 留空走 SDK 反射）；
// NewFlowPhase：Compile 闭包（函数名不可靠，显式 Name={name}-flow-wf）；
// 分片 ShardWF：闭包（函数名不可靠，显式 Name={def名}-shard-wf）。
func (p *Phase) CollectWorkflowDefs() []*core.WorkflowDef {
	var out []*core.WorkflowDef
	var walk func(*Phase)
	walk = func(ph *Phase) {
		if ph.subRoot != nil {
			walk(ph.subRoot) // NewFlowPhase：子树里的 Child WF/分片也注册
		}
		if len(ph.steps) > 0 {
			for _, s := range ph.steps {
				walk(s)
			}
			return
		}
		if ph.mode == PhaseWorkflow {
			opts := core.WorkflowDefOptions{}
			if ph.subRoot != nil {
				opts.Name = ph.name + "-flow-wf" // Compile 闭包——显式名防注册冲突
			}
			out = append(out, &core.WorkflowDef{Fn: ph.fn, Options: opts})
		}
		if ph.mode == PhaseShard && ph.shardWf != nil {
			out = append(out, &core.WorkflowDef{Fn: ph.shardWf, Options: core.WorkflowDefOptions{Name: ph.shardWfName}})
		}
	}
	walk(p)
	return out
}
