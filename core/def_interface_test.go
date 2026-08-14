package core

import (
	"context"
	"testing"

	"go.temporal.io/sdk/workflow"
)

// 验证：框架层可自定义 Def 实现接口，保留强类型，同时获得注册能力。
// 这是「接口 + 默认实现」设计的核心价值——batch 自定义 Def 不再需要类型擦除。

// customWorkflowFn 模拟 batch 的强类型函数签名。
type customWorkflowFn func(ctx workflow.Context, input string) (string, error)

// customWorkflowDef 自定义 Def——保留强类型 Fn，实现 WorkflowRegistrable。
type customWorkflowDef struct {
	fn   customWorkflowFn
	name string
}

func (d *customWorkflowDef) WorkflowFunc() interface{} { return d.fn }
func (d *customWorkflowDef) WorkflowOptions() WorkflowDefOptions {
	return WorkflowDefOptions{Name: d.name}
}

// customActivityFn 模拟 batch 的强类型 Activity 签名。
type customActivityFn func(ctx context.Context, input string) (string, error)

// customActivityDef 自定义 Def——保留强类型 Fn，实现 ActivityRegistrable。
type customActivityDef struct {
	fn   customActivityFn
	name string
}

func (d *customActivityDef) ActivityFunc() interface{} { return d.fn }
func (d *customActivityDef) ActivityOptions() ActivityDefOptions {
	return ActivityDefOptions{Name: d.name}
}

func TestWorkerManager_CustomWorkflowDef(t *testing.T) {
	fw := &fakeWorker{}
	wm := &WorkerManager{worker: fw}

	// 自定义 Def，Fn 保持强类型 customWorkflowFn
	def := &customWorkflowDef{fn: func(ctx workflow.Context, s string) (string, error) { return s, nil }, name: "custom-wf"}
	wm.RegisterWorkflow(def)

	if !fw.workflowOpts {
		t.Fatal("自定义 Def 应走 RegisterWorkflowWithOptions 分支")
	}
	if fw.workflowName != "custom-wf" {
		t.Fatalf("registered name = %q, want custom-wf", fw.workflowName)
	}
}

func TestWorkerManager_CustomActivityDef(t *testing.T) {
	fw := &fakeWorker{}
	wm := &WorkerManager{worker: fw}

	def := &customActivityDef{fn: func(ctx context.Context, s string) (string, error) { return s, nil }, name: "custom-act"}
	wm.RegisterActivity(def)

	if !fw.activityOpts {
		t.Fatal("自定义 Def 应走 RegisterActivityWithOptions 分支")
	}
	if fw.activityName != "custom-act" {
		t.Fatalf("registered name = %q, want custom-act", fw.activityName)
	}
}

// 验证默认实现 ActivityDef 仍走接口路径。
func TestWorkerManager_DefaultDefImplementsInterface(t *testing.T) {
	var _ ActivityRegistrable = (*ActivityDef)(nil)
	var _ WorkflowRegistrable = (*WorkflowDef)(nil)
	// 编译期断言：默认实现满足接口
}
