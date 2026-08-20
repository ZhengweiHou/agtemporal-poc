package batch

import (
	"context"
	"errors"
	"fmt"
	"io"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/core"
)

// ═══════════════════════════════════════════════════════
// 执行单元签名（统一收 FlowCtx 快照——T13，消灭 getIn；返回统一 map——BatchResult 消灭）
// ═══════════════════════════════════════════════════════

// Tasklet 自定义执行单元（对标 Spring Tasklet——自己决定拿什么，fc.Str/Int 自取）。
type Tasklet = func(ctx context.Context, fc *FlowCtx) (map[string]any, error)

// WorkflowFn 手写 Child WF 逃逸舱（动态控制——循环/分支/动态调度，暴露 SDK）。
type WorkflowFn = func(ctx workflow.Context, fc *FlowCtx) (map[string]any, error)

// PartitionerFn 分片拆分（T8 决议 + T15 方案 B——输出带名字的分区列表）。
// 纯内存操作（Workflow 域执行、确定性要求，不做 IO）——不需要 ctx：
//   ⚠️ SDK 事实：Temporal 的 workflow.Context 不满足标准 context.Context 接口
//   （Done() 返回 internal.Channel 而非 <-chan struct{}），PartitionerFn 收 context.Context
//   无法在 Workflow 域直接调用。纯内存拆分本无取消/IO 语义，去掉 ctx 最干净。
// 每个 Partition.Data 是单个分片的坐标包（如 {shard_id, start, line_count, file_path}），
// 框架注入分片执行单元的 fc.Input()。
type PartitionerFn = func(fc *FlowCtx) ([]Partition, error)

// PhaseMode 编排单元类型。
type PhaseMode int

const (
	// PhaseActivity 叶子——ExecuteActivity 调用 Activity（Tasklet/Chunk 统一持 def）。
	PhaseActivity PhaseMode = iota
	// PhaseWorkflow 叶子——ExecuteChildWorkflow 调用子 Workflow（Flow/Workflow）。
	PhaseWorkflow
	// PhasePipeline 复合——串行执行子 Phase。
	PhasePipeline
	// PhaseParallel 复合——并行执行子 Phase。
	PhaseParallel
	// PhaseShard 复合——Partitioner 拆分 + 按 handler 形态并行 + 聚合。
	PhaseShard
)

// Phase 是编排单元：叶子（Activity/Child Workflow）或复合（Pipeline/Parallel/Shard）。
// name 是 FlowCtx key——Phase 执行结果以 name 存入 FlowCtx，下游通过 fc.Output(name) 读取。
type Phase struct {
	name  string
	mode  PhaseMode
	def   *core.ActivityDef // 叶子 Activity：定义（Tasklet/Chunk 内部构建，注册名在 Options.Name）
	fn    interface{}       // 叶子 Workflow：函数引用（WorkflowFn 或 compileFlow 产物）
	steps []*Phase          // 复合：子 Phase 列表

	partitioner *Phase // PhaseShard：拆分器（Phase 形态——Activity/Flow 统一；PartitionerFn 经 NewPartitionerPhase 包装）
	handler     *Phase // PhaseShard：分片执行单元（T16——分片执行形态 = handler 类型）

	regName string // Workflow 叶子注册名（默认派生 {name}-wf / {name}-flow-wf；Activity 在 def.Options.Name）

	subRoot *Phase // NewFlowPhase：子树（供 CollectDefs/CollectWorkflowDefs 遍历）

	ao workflow.ActivityOptions       // Activity 执行配置
	wo workflow.ChildWorkflowOptions  // Child WF 执行配置（构建期部分——ID/ReusePolicy 调度时补）
}

// ═══════════════════════════════════════════════════════
// 叶子构建器（配置内聚在各自构建处——Builder 内部化）
// ═══════════════════════════════════════════════════════

// NewTaskletPhase 创建自定义执行单元叶子（对标 Spring TaskletStep）。
// name：FlowCtx key（结果存入）+ 默认注册名；fn：统一签名执行函数；
// opts：ActivityOption（WithActivityName 覆盖注册名 / 重试 / 超时）。
func NewTaskletPhase(name string, fn Tasklet, opts ...ActivityOption) *Phase {
	if fn == nil {
		panic("batch: NewTaskletPhase fn 不能为 nil")
	}
	cfg := defaultActivityConfig()
	for _, o := range opts {
		o(&cfg)
	}
	regName := cfg.name
	if regName == "" {
		regName = name
	}
	def := &core.ActivityDef{Fn: fn, Options: core.ActivityDefOptions{
		Name:            regName,
		MaximumAttempts: cfg.maxAttempts,
	}}
	return &Phase{name: name, mode: PhaseActivity, def: def, ao: activityOptions(cfg)}
}

// NewChunkPhase 创建数据分块引擎叶子（R-P-W——对标 Spring chunk()，原 NewEnginePhase）。
// name：FlowCtx key + 默认注册名；reader/processor/writer：接口或 Factory（引擎自动检测）；
// opts：ActivityOption（ChunkSize/SkipPolicy/TransactionManager/心跳/重试/超时）。
func NewChunkPhase(name string, reader, processor, writer interface{}, opts ...ActivityOption) *Phase {
	if reader == nil {
		panic("batch: NewChunkPhase reader 不能为 nil")
	}
	if processor == nil {
		panic("batch: NewChunkPhase processor 不能为 nil")
	}
	if writer == nil {
		panic("batch: NewChunkPhase writer 不能为 nil")
	}
	if !isReaderLike(reader) {
		panic("batch: NewChunkPhase reader 必须实现 Reader 或 ReaderFactory")
	}
	if !isProcessorLike(processor) {
		panic("batch: NewChunkPhase processor 必须实现 Processor 或 ProcessorFactory")
	}
	if !isWriterLike(writer) {
		panic("batch: NewChunkPhase writer 必须实现 Writer 或 WriterFactory")
	}

	cfg := defaultActivityConfig()
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.chunkSize <= 0 {
		panic("batch: NewChunkPhase ChunkSize 必须 > 0（用 WithActivityChunkSize 设置）")
	}
	regName := cfg.name
	if regName == "" {
		regName = name
	}

	// 引擎闭包：收 FlowCtx 快照（fc.Input() → Factory 的 BatchInput 契约——内部转换）
	closure := func(ctx context.Context, fc *FlowCtx) (map[string]any, error) {
		input := BatchInput{Params: fc.Input()}
		r, err := resolveReader(reader, ctx, input)
		if err != nil {
			return nil, err
		}
		p, err := resolveProcessor(processor, ctx, input)
		if err != nil {
			return nil, err
		}
		w, err := resolveWriter(writer, ctx, input)
		if err != nil {
			return nil, err
		}

		result, err := runChunkLoop(ctx, r, p, w, cfg.transactionManager, cfg.chunkSize, cfg.skipPolicy)

		// 业务聚合结果：Writer 实现 ResultProvider 时，读其 Result 填入 Output
		if rp, ok := w.(ResultProvider); ok {
			result.Output = rp.Result()
		}

		// Close 错误收集：逆序关闭执行期实例（仅主流程成功时 Close 错误才覆盖返回值）
		if cerr := closeExecInstances(w, p, r); cerr != nil && err == nil {
			return nil, cerr
		}
		return result.toMap(), err
	}

	def := &core.ActivityDef{Fn: closure, Options: core.ActivityDefOptions{
		Name:            regName,
		MaximumAttempts: cfg.maxAttempts,
	}}
	return &Phase{name: name, mode: PhaseActivity, def: def, ao: activityOptions(cfg)}
}

// activityOptions 从 activityConfig 构建 SDK ActivityOptions（防坏数据永久重试）。
func activityOptions(cfg activityConfig) workflow.ActivityOptions {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: cfg.startToClose,
		HeartbeatTimeout:    cfg.heartbeatTimeout,
	}
	if cfg.maxAttempts > 0 {
		ao.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: cfg.maxAttempts}
	}
	return ao
}

// ═══════════════════════════════════════════════════════
// 编排单元构建器（Child WF——配置内聚 WorkflowOption）
// ═══════════════════════════════════════════════════════

// NewFlowPhase 把静态子编排树包装成 Child Workflow（对标 Spring FlowStep，零 SDK 代码）。
// name：FlowCtx key；root：子 Phase 树（Pipeline/Parallel/叶子）。
// 输入契约：透传父 FlowCtx 快照（Child 入参 = 父 fc——全量序列化，子树执行单元自取）。
// 输出语义：单叶子子树 → 叶子输出扁平；多 Phase 子树 → 各 Phase 输出合并。
// 子树 defs 自动收集注册（CollectDefs/CollectWorkflowDefs 遍历 subRoot）。
func NewFlowPhase(name string, root *Phase, opts ...WorkflowOption) *Phase {
	if root == nil {
		panic("batch: NewFlowPhase root 不能为 nil")
	}
	cfg := defaultWorkflowConfig()
	for _, o := range opts {
		o(&cfg)
	}
	regName := cfg.name
	if regName == "" {
		regName = name + "-flow-wf" // Compile 闭包——显式名防注册冲突
	}
	return &Phase{
		name:    name,
		mode:    PhaseWorkflow,
		fn:      compileFlow(root),
		regName: regName,
		subRoot: root,
		wo:      childOptions(cfg),
	}
}

// NewWorkflowPhase 创建手写 Child WF 叶子（逃逸舱——动态控制：循环/分支/动态调度）。
// fn：业务手写 flow 函数（收 FlowCtx 快照——自取输入）。
// 静态子流程（Pipeline/Parallel 树）优先用 NewFlowPhase（无需写 workflow 函数）。
func NewWorkflowPhase(name string, fn WorkflowFn, opts ...WorkflowOption) *Phase {
	if fn == nil {
		panic("batch: NewWorkflowPhase fn 不能为 nil")
	}
	cfg := defaultWorkflowConfig()
	for _, o := range opts {
		o(&cfg)
	}
	regName := cfg.name
	if regName == "" {
		regName = name + "-wf"
	}
	return &Phase{name: name, mode: PhaseWorkflow, fn: fn, regName: regName, wo: childOptions(cfg)}
}

// childOptions 从 workflowConfig 构建 SDK ChildWorkflowOptions（Child 重试——修复 D2）。
// WorkflowID / ReusePolicy 是运行期派生，不在构建期设置。
func childOptions(cfg workflowConfig) workflow.ChildWorkflowOptions {
	wo := workflow.ChildWorkflowOptions{}
	if cfg.maxAttempts > 0 {
		wo.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: cfg.maxAttempts}
	}
	if cfg.startToClose > 0 {
		wo.WorkflowRunTimeout = cfg.startToClose // Child 总时长硬上限
	}
	return wo
}

// compileFlow 子树编译成 Child WF 函数（收 FlowCtx 快照——input 显式字段，无魔法 key）。
// 顶层 recover 同 Compile：子树内 Partitioner 等 panic → 快速失败（防 WFT 无限重试）。
func compileFlow(root *Phase) func(workflow.Context, *FlowCtx) (map[string]any, error) {
	return func(ctx workflow.Context, fc *FlowCtx) (out map[string]any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("batch: workflow panic（检查 Partitioner/执行单元类型断言）: %v", r)
			}
		}()
		if err := root.run(ctx, fc); err != nil {
			return nil, err
		}
		all := fc.All()
		// 单叶子子树：叶子输出扁平（下游 fc.Str/Int 直接读字段，无需解嵌套）
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

// isLeafPhase 判断 Phase 是否为叶子（Activity/Child WF）。
func isLeafPhase(p *Phase) bool {
	return p.mode == PhaseActivity || p.mode == PhaseWorkflow
}

// ═══════════════════════════════════════════════════════
// 数据并行（Shard——T8 决议 + T15 方案 B + T16 handler 形态）
// ═══════════════════════════════════════════════════════

// NewShardPhase 创建分片复合 Phase：Partitioner（Phase 形态）拆分 → 按 handler 形态执行 → 聚合。
// name：FlowCtx key（聚合结果存入）；partitioner：拆分器（统一 *Phase——一切皆 Phase）：
//
//	NewPartitionerPhase(name, fn)  → 纯内存函数（Workflow 域直接调度）
//	NewTaskletPhase / NewChunkPhase → IO 拆分（读文件/查 DB 拆坐标——Activity 执行）
//	NewFlowPhase / NewWorkflowPhase → 独立拆分 Child WF（PartitionFlow——replay 保留分区结果）
//
// 输出契约（T8 决议：partition 输出不进 FlowCtx，直接给 shard 消费）：
// partition Phase 返回 map[string]any，约定 key "partitions" → []map[string]any（每项 {name, data}）。
// 业务手写 IO 拆分时返回该契约；NewPartitionerPhase 自动封装。
//
// handler：分片执行单元——**分片执行形态 = handler 类型（T16）**：
//
//	Activity 类（Tasklet/Chunk）→ 每分片 ExecuteActivity（跨 Run 失败全量重跑——已知限制）
//	Child WF 类（Flow/Workflow）→ 每分片 Child Workflow（ID 派生 {主ID}-shard-{分区名}——跨 Run 幂等）
//	组合（Pipeline/Parallel）  → 主 WF 内展开（语义允许，POC 案例先用叶子）
//
// 每个分片的坐标（Partition.Data）注入执行单元 fc.Input()（覆盖同名 key）。
// 聚合规则：processed/skipped 求和；其余数值字段求和。
// 配置内聚：partitioner/handler 各自构建处自配（ActivityOption/WorkflowOption）——无 ShardOption
// （分片级配置冗余：Child 重试在 handler 构建时配 WithWorkflowMaxAttempts）。
func NewShardPhase(name string, partitioner *Phase, handler *Phase) *Phase {
	if partitioner == nil {
		panic("batch: NewShardPhase partitioner 不能为 nil")
	}
	if handler == nil {
		panic("batch: NewShardPhase handler 不能为 nil")
	}
	return &Phase{
		name:        name,
		mode:        PhaseShard,
		partitioner: partitioner,
		handler:     handler,
	}
}

// NewPartitionerPhase 把纯内存 PartitionerFn 包装成 Activity Phase（便捷——纯函数零成本）。
// 内部闭包调用 fn(fc) 并把 []Partition 封装为输出契约（{"partitions": [...]}）供 shard 提取。
// 纯内存拆分（Workflow 域执行、确定性要求）不需要 IO；需要 IO 拆坐标时用
// NewTaskletPhase/NewChunkPhase/NewFlowPhase 包装（Activity/Child WF 形态可 IO）。
func NewPartitionerPhase(name string, fn PartitionerFn) *Phase {
	if fn == nil {
		panic("batch: NewPartitionerPhase fn 不能为 nil")
	}
	closure := func(ctx context.Context, fc *FlowCtx) (map[string]any, error) {
		parts, err := fn(fc)
		if err != nil {
			return nil, err
		}
		return partitionListToMap(parts), nil
	}
	def := &core.ActivityDef{Fn: closure, Options: core.ActivityDefOptions{
		Name:            name,
		MaximumAttempts: defaultActivityConfig().maxAttempts,
	}}
	return &Phase{name: name, mode: PhaseActivity, def: def, ao: activityOptions(defaultActivityConfig())}
}

// partitionListToMap 输出契约封装：{"partitions": [{"name":..., "data":{...}}, ...]}。
func partitionListToMap(parts []Partition) map[string]any {
	list := make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		list = append(list, map[string]any{"name": p.Name, "data": p.Data})
	}
	return map[string]any{"partitions": list}
}

// extractPartitions 从 partition Phase 输出提取分区列表。
// 兼容 JSON 序列化后的形态（[]any + map[string]any 元素——data 内数值变 float64，业务用 asIntAny 处理）。
func extractPartitions(out map[string]any) ([]Partition, error) {
	raw, ok := out["partitions"]
	if !ok {
		return nil, fmt.Errorf("batch: partition Phase 输出缺少 partitions key（契约: {\"partitions\": [...]}）")
	}
	list, ok := raw.([]any)
	if !ok {
		// 同进程形态（testsuite 直调）：[]map[string]any
		if lm, ok := raw.([]map[string]any); ok {
			list = make([]any, 0, len(lm))
			for _, m := range lm {
				list = append(list, m)
			}
		} else {
			return nil, fmt.Errorf("batch: partitions 类型错误: %T", raw)
		}
	}
	parts := make([]Partition, 0, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("batch: partitions[%d] 类型错误: %T", i, item)
		}
		name, _ := m["name"].(string)
		data, _ := m["data"].(map[string]any)
		if data == nil {
			data = map[string]any{}
		}
		parts = append(parts, Partition{Name: name, Data: data})
	}
	return parts, nil
}

// ═══════════════════════════════════════════════════════
// 拓扑组合 + Compile
// ═══════════════════════════════════════════════════════

// Pipeline 串行组合多个 Phase（返回复合 Phase）。
func Pipeline(phases ...*Phase) *Phase {
	return &Phase{mode: PhasePipeline, steps: phases}
}

// Parallel 并行组合多个 Phase（返回复合 Phase）。
func Parallel(phases ...*Phase) *Phase {
	return &Phase{mode: PhaseParallel, steps: phases}
}

// Compile 把 Phase 树编译成 Workflow 函数。
// 返回的 Workflow 接收 map[string]any 入参（Job.Start params），执行 Phase 树，返回 fc.All()。
// 顶层 recover：Partitioner 等 Phase 代码 panic → 转为 Workflow 失败（快速失败，防 History 暴涨）。
func Compile(root *Phase) func(workflow.Context, map[string]any) (map[string]any, error) {
	return func(ctx workflow.Context, input map[string]any) (out map[string]any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("batch: workflow panic（检查 Partitioner/执行单元类型断言）: %v", r)
			}
		}()
		fc := NewFlowCtx(input)
		if err := root.run(ctx, fc); err != nil {
			return nil, err
		}
		return fc.All(), nil
	}
}

// ═══════════════════════════════════════════════════════
// run / schedule（递归执行）
// ═══════════════════════════════════════════════════════

// run 递归执行 Phase（无 getIn——执行单元收 fc 快照自取）。
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
		// 失败快速传播（对标 Spring Batch）：任一子 Phase 失败 → cancel 其余分支的子 ctx。
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
		return p.runShard(ctx, fc)

	default: // 叶子：Activity / Workflow
		out, skipped, err := p.schedule(ctx, fc)(ctx)
		if err != nil {
			return err
		}
		if skipped {
			// 幂等跳过（Child WF：同 ID 上次 Run 已完成）——结果不可得。
			// 写入标记让下游可感知；若下游断言具体 key 会 nil panic（快速失败）。
			fc.Put(p.name, map[string]any{"skipped": true})
			return nil
		}
		fc.Put(p.name, out)
		return nil
	}
}

// runShard 分片执行（T16——形态由 handler 类型决定；partition 也是 Phase 形态）。
func (p *Phase) runShard(ctx workflow.Context, fc *FlowCtx) error {
	// 1. 执行 partition（Phase 形态——schedule 统一调度：
	//    Activity 类（NewPartitionerPhase/NewTaskletPhase）→ 可 IO 拆坐标
	//    Child WF 类（NewFlowPhase/NewWorkflowPhase）→ PartitionFlow 独立 Child（replay 保留分区结果））
	out, skipped, err := p.partitioner.schedule(ctx, fc)(ctx)
	if err != nil {
		return err
	}
	if skipped {
		// PartitionFlow AlreadyStarted（跨 Run 重跑）——分区列表不可得（在旧 Run History）
		// 聚合缺失下沉（已知限制——跨 Run 结果传递，外部状态存储才是完整解）
		fc.Put(p.name, map[string]any{"processed": 0, "skipped": 0, "skipped_shards": 0, "partitions_skipped": true})
		return nil
	}
	parts, err := extractPartitions(out)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		// 结果结构统一（与正常分片一致）：processed/skipped/skipped_shards 全 0
		fc.Put(p.name, map[string]any{"processed": 0, "skipped": 0, "skipped_shards": 0})
		return nil
	}
	// 分区名补齐（T15：空名自动派生 {Phase name}-{i}——保证 Child ID 唯一性）
	for i := range parts {
		if parts[i].Name == "" {
			parts[i].Name = fmt.Sprintf("%s-%d", p.name, i)
		}
	}

	switch p.handler.mode {
	case PhaseActivity:
		// Activity 类 handler：每分片一次 ExecuteActivity（跨 Run 全量重跑——已知限制）
		results := make([]map[string]any, 0, len(parts))
		for _, part := range parts {
			subFC := fc.clone()
			mergeCoords(subFC, part.Data)
			out, _, err := p.handler.schedule(ctx, subFC)(ctx)
			if err != nil {
				return err
			}
			results = append(results, out)
		}
		agg := aggregateShardResults(results)
		agg["skipped_shards"] = 0
		fc.Put(p.name, agg)
		return nil

	case PhaseWorkflow:
		// Child WF 类 handler：每分片一个 Child Workflow（ID 派生自分区名——跨 Run 幂等）
		mainID := workflow.GetInfo(ctx).WorkflowExecution.ID
		skippedShards := 0
		results := make([]map[string]any, 0, len(parts))
		for _, part := range parts {
			subFC := fc.clone()
			mergeCoords(subFC, part.Data)
			childOpts := p.handler.wo
			childOpts.WorkflowID = fmt.Sprintf("%s-shard-%s", mainID, part.Name) // 分区名 → Child ID
			childOpts.WorkflowIDReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
			fut := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, childOpts), p.handler.regName, subFC)
			var out map[string]any
			err := fut.Get(ctx, &out)
			if err != nil {
				// 幂等级联：分片已完成（上次 Run 成功）→ 识别 AlreadyStarted 并跳过（不重跑）
				var alreadyStarted *temporal.ChildWorkflowExecutionAlreadyStartedError
				if errors.As(err, &alreadyStarted) {
					skippedShards++
					continue
				}
				return err
			}
			results = append(results, out)
		}
		agg := aggregateShardResults(results)
		agg["skipped_shards"] = skippedShards
		fc.Put(p.name, agg)
		return nil

	default:
		// 组合 handler（Pipeline/Parallel）：主 WF 内展开执行（C15——语义允许，POC 案例先用叶子）。
		// 组合 Phase 无自身 name——每个子 Phase 输出作为独立 result 聚合（统计字段自然求和）。
		results := make([]map[string]any, 0, len(parts)*2)
		for _, part := range parts {
			subFC := fc.clone()
			mergeCoords(subFC, part.Data)
			if err := p.handler.run(ctx, subFC); err != nil {
				return err
			}
			for _, m := range subFC.All() {
				if mm, ok := m.(map[string]any); ok {
					results = append(results, mm)
				}
			}
		}
		agg := aggregateShardResults(results)
		agg["skipped_shards"] = 0
		fc.Put(p.name, agg)
		return nil
	}
}

// mergeCoords 坐标注入执行单元 fc.Input()（覆盖同名 key——分片特有输入优先）。
func mergeCoords(fc *FlowCtx, coords map[string]any) {
	for k, v := range coords {
		fc.input[k] = v
	}
}

// schedule 调度叶子 Phase，返回"获取结果"闭包（Future 已在调度时发出，实现并发）。
// 返回 (out, skipped, err)：skipped=true 表示 Phase 被幂等跳过（同 ID 上次 Run 已完成），
// 结果不可得（在上次 Run 的 History）——下游依赖时通过 fc.Output 感知（nil 或 skipped 标记）。
func (p *Phase) schedule(ctx workflow.Context, fc *FlowCtx) func(workflow.Context) (map[string]any, bool, error) {
	switch p.mode {
	case PhaseActivity:
		// 统一契约：fc 快照入参（序列化即快照），map 输出
		fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, p.ao), p.def.Options.Name, fc)
		return func(ctx workflow.Context) (map[string]any, bool, error) {
			var out map[string]any
			if err := fut.Get(ctx, &out); err != nil {
				return nil, false, err
			}
			return out, false, nil
		}

	case PhaseWorkflow:
		// Child Workflow：ID 自动派生 {主 WorkflowID}-{name}（可寻址/可查询/可 Reset），
		// 幂等策略 AllowDuplicateFailedOnly 级联（主重跑时已完成 Child 不重建）。
		mainID := workflow.GetInfo(ctx).WorkflowExecution.ID
		childOpts := p.wo
		childOpts.WorkflowID = mainID + "-" + p.name
		childOpts.WorkflowIDReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
		fut := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, childOpts), p.regName, fc)
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

// ═══════════════════════════════════════════════════════
// 收集（注册一体化）
// ═══════════════════════════════════════════════════════

// CollectDefs 收集 Phase 树中所有 Activity 叶子（含 Shard 的 partitioner/handler）的 def（用于注册）。
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
		}
		if ph.handler != nil {
			walk(ph.handler) // PhaseShard：分片执行单元（任意形态）也收集
		}
		if ph.partitioner != nil {
			walk(ph.partitioner) // PhaseShard：拆分器（IO Tasklet / 纯函数包装）也收集
		}
		if ph.mode == PhaseActivity && ph.def != nil {
			out = append(out, ph.def)
		}
	}
	walk(p)
	return out
}

// CollectWorkflowDefs 收集 Phase 树中所有 Child Workflow 定义（用户 Child WF + Flow 子树 + Shard 的 partitioner/handler），用于注册。
// 用户 Child WF（NewWorkflowPhase）：业务函数（普通函数名可靠，Name 留空走 SDK 反射）——
//   重构后统一收 FlowCtx 快照，注册名 = regName 显式派生（防闭包冲突）。
// NewFlowPhase：Compile 闭包（函数名不可靠，显式 Name = {name}-flow-wf）。
func (p *Phase) CollectWorkflowDefs() []*core.WorkflowDef {
	var out []*core.WorkflowDef
	var walk func(*Phase)
	walk = func(ph *Phase) {
		if ph.subRoot != nil {
			walk(ph.subRoot)
		}
		if len(ph.steps) > 0 {
			for _, s := range ph.steps {
				walk(s)
			}
		}
		if ph.handler != nil {
			walk(ph.handler) // PhaseShard：分片执行单元
		}
		if ph.partitioner != nil {
			walk(ph.partitioner) // PhaseShard：拆分器（PartitionFlow 等 Child WF 形态）
		}
		if ph.mode == PhaseWorkflow {
			out = append(out, &core.WorkflowDef{Fn: ph.fn, Options: core.WorkflowDefOptions{Name: ph.regName}})
		}
	}
	walk(p)
	return out
}

// ═══════════════════════════════════════════════════════
// 聚合与类型转换 helper
// ═══════════════════════════════════════════════════════

// aggregateShardResults 聚合分片结果：processed/skipped 求和，其余数值字段求和。
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

// ═══════════════════════════════════════════════════════
// Reader/Processor/Writer 检测与解析（原 Builder 内部机制——内部化）
// ═══════════════════════════════════════════════════════

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
