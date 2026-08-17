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
	name string
	id   *core.IDSpec
	root *Phase

	wf func(workflow.Context, map[string]any) (map[string]any, error) // Compile 结果
}

// NewJob 创建批处理作业。
// name：作业名（WorkflowID 前缀）；root：编排树（Compile 根）；opts：识别参数等。
func NewJob(name string, root *Phase, opts ...JobOption) *Job {
	cfg := &jobConfig{}
	for _, o := range opts {
		o(cfg)
	}
	return &Job{
		name: name,
		id:   &core.IDSpec{Prefix: name, NonIdentityKeys: cfg.nonIdentityKeys},
		root: root,
		wf:   Compile(root),
	}
}

// Name 返回作业名。
func (j *Job) Name() string { return j.name }

// Workflow 返回编译后的 Workflow 函数（用于注册）。
func (j *Job) Workflow() interface{} { return j.wf }

// Activities 返回编排树收集的 Activity 函数引用（用于注册）。
func (j *Job) Activities() []interface{} { return j.root.CollectActivities() }

// Workflows 返回编排树收集的 Child Workflow 函数引用（用于注册）。
func (j *Job) Workflows() []interface{} { return j.root.CollectWorkflows() }

// Engines 返回编排树收集的引擎定义（用于注册）。
func (j *Job) Engines() []*core.ActivityDef { return j.root.CollectEngines() }

// RegisterTo 一体化注册到 WorkerManager（Workflow + Activity + Child Workflow + 引擎）。
// 解决注册碎片化——编排树 + 引擎一次注册完成。
func (j *Job) RegisterTo(wm *core.WorkerManager) {
	wm.RegisterWorkflow(j.wf)
	for _, fn := range j.Activities() {
		wm.RegisterActivity(fn)
	}
	for _, fn := range j.Workflows() {
		wm.RegisterWorkflow(fn)
	}
	for _, def := range j.Engines() {
		wm.RegisterActivity(def)
	}
}

// Start 推导 WorkflowID（识别参数）并启动作业，应用断批重跑默认策略。
// 相同识别参数 → 相同 WorkflowID；失败后允许重跑、成功后拒绝（AllowDuplicateFailedOnly）。
func (j *Job) Start(ctx context.Context, facade *core.ClientFacade, params map[string]any) (client.WorkflowRun, error) {
	id, err := j.id.DeriveWorkflowID(params)
	if err != nil {
		return nil, err
	}
	return facade.StartWorkflow(ctx, id, j.wf, params, core.WithDefaultResumePolicy())
}

// DeriveWorkflowID 仅推导 WorkflowID（不启动），供查询/续批等场景使用。
func (j *Job) DeriveWorkflowID(params map[string]any) (string, error) {
	return j.id.DeriveWorkflowID(params)
}
