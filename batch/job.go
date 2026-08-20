package batch

import (
	"context"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/core"
)

// JobOption 是 NewJob 的可选参数。
type JobOption func(*jobConfig)

type jobConfig struct {
	nonIdentityKeys []string
}

// WithNonIdentityParams 声明非识别参数 key（不参与 WorkflowID 推导，对标 Spring Batch
// JobParameter.identifying=false）。不调用 = 全部入参参与识别（默认全识别）。
// 典型非识别参数：时间戳、运行批次号等"每次启动不同"的值——若它们参与推导，会破坏幂等。
func WithNonIdentityParams(keys ...string) JobOption {
	return func(c *jobConfig) { c.nonIdentityKeys = keys }
}

// Job 是批处理作业（对标 Spring Batch Job）。
// 持有编排树（Phase）+ 识别参数声明；提供一体化注册（RegisterTo）与启动（Start，自动推导 WorkflowID）。
type Job struct {
	name   string
	id     *core.IDSpec
	root   *Phase

	wfName string                                                              // 主 WF 注册名（显式——Compile 闭包反射名不可靠，防与子树闭包冲突）
	wf     func(workflow.Context, map[string]any) (map[string]any, error) // Compile 结果
}

// NewJob 创建批处理作业。
// name：作业名（WorkflowID 前缀 + 主 WF 注册名 {name}-main-wf）；root：编排树（Compile 根）；opts：识别参数等。
// 构造期校验（编程错误立即暴露，而非运行时/启动时才报错）：name 非空、root 非 nil。
func NewJob(name string, root *Phase, opts ...JobOption) *Job {
	if name == "" {
		panic("batch: NewJob name 不能为空（WorkflowID 前缀）")
	}
	if root == nil {
		panic("batch: NewJob root 不能为空（编排树）")
	}
	cfg := &jobConfig{}
	for _, o := range opts {
		o(cfg)
	}
	return &Job{
		name:   name,
		id:     &core.IDSpec{Prefix: name, NonIdentityKeys: cfg.nonIdentityKeys},
		root:   root,
		wfName: name + "-main-wf",
		wf:     Compile(root),
	}
}

// Name 返回作业名。
func (j *Job) Name() string { return j.name }

// WorkflowName 返回主 WF 注册名（显式名——防闭包反射名冲突）。
func (j *Job) WorkflowName() string { return j.wfName }

// Workflow 返回编译后的 Workflow 函数（用于注册）。
func (j *Job) Workflow() interface{} { return j.wf }

// Defs 返回编排树收集的 Activity 定义（引擎 + 自定义统一，用于注册）。
func (j *Job) Defs() []*core.ActivityDef { return j.root.CollectDefs() }

// WorkflowDefs 返回编排树收集的 Child Workflow 定义（用户 Child WF + 分片 ShardWF，用于注册）。
func (j *Job) WorkflowDefs() []*core.WorkflowDef { return j.root.CollectWorkflowDefs() }

// RegisterTo 一体化注册到 WorkerManager（Workflow + Activity + Child Workflow）。
// 解决注册碎片化——编排树一次注册完成（引擎与自定义 Activity 统一经 CollectDefs）。
// 主 WF 用显式注册名 {name}-main-wf（Compile 闭包反射名不可靠——实测踩坑：
// 闭包反射短名可能与子树 compileFlow 闭包冲突，导致主 WF 匹配到错误 Workflow）。
func (j *Job) RegisterTo(wm *core.WorkerManager) {
	wm.RegisterWorkflow(&core.WorkflowDef{Fn: j.wf, Options: core.WorkflowDefOptions{Name: j.wfName}})
	for _, def := range j.Defs() {
		wm.RegisterActivity(def)
	}
	for _, def := range j.WorkflowDefs() {
		wm.RegisterWorkflow(def)
	}
}

// Start 推导 WorkflowID（识别参数）并启动作业，应用断批重跑默认策略。
// 相同识别参数 → 相同 WorkflowID；失败后允许重跑、成功后拒绝（AllowDuplicateFailedOnly）。
// 用字符串注册名启动（而非函数引用）——显式匹配主 WF，杜绝反射名歧义。
func (j *Job) Start(ctx context.Context, facade *core.ClientFacade, params map[string]any) (client.WorkflowRun, error) {
	id, err := j.id.DeriveWorkflowID(params)
	if err != nil {
		return nil, err
	}
	return facade.StartWorkflow(ctx, id, j.wfName, params, core.WithDefaultResumePolicy())
}

// DeriveWorkflowID 仅推导 WorkflowID（不启动），供查询/续批等场景使用。
func (j *Job) DeriveWorkflowID(params map[string]any) (string, error) {
	return j.id.DeriveWorkflowID(params)
}
