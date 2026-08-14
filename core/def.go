package core

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// WorkflowDefOptions 是 Workflow 注册的 core 自有配置。
// 语义由 core 定义，不直接暴露 Temporal SDK 类型；注册时经 toRegisterOptions 映射为 SDK 选项。
// 零值即默认（Name 空 → SDK 回退反射取函数名）。
type WorkflowDefOptions struct {
	Name                          string // 注册名；空则 SDK 回退默认（反射取函数名）
	DisableAlreadyRegisteredCheck bool   // 关闭重复注册检查；false = 默认
}

// ActivityDefOptions 是 Activity 注册的 core 自有配置。规则同 WorkflowDefOptions。
type ActivityDefOptions struct {
	Name                          string // 注册名；空则 SDK 回退默认
	DisableAlreadyRegisteredCheck bool   // 关闭重复注册检查；false = 默认
}

// WorkflowRegistrable 是「可注册的 Workflow」抽象。
// WorkerManager 通过此接口识别注册单元，而非具体类型断言——
// 框架层（batch 等）可复用默认实现 WorkflowDef，或自定义实现以保留强类型。
type WorkflowRegistrable interface {
	WorkflowFunc() interface{}
	WorkflowOptions() WorkflowDefOptions
}

// ActivityRegistrable 是「可注册的 Activity」抽象。规则同 WorkflowRegistrable。
type ActivityRegistrable interface {
	ActivityFunc() interface{}
	ActivityOptions() ActivityDefOptions
}

// WorkflowDef 是「Workflow 函数 + 注册选项」的绑定，WorkflowRegistrable 的默认实现。
type WorkflowDef struct {
	Fn      interface{}
	Options WorkflowDefOptions
}

// ActivityDef 是「Activity 函数 + 注册选项」的绑定，ActivityRegistrable 的默认实现。
type ActivityDef struct {
	Fn      interface{}
	Options ActivityDefOptions
}

// WorkflowFunc 实现 WorkflowRegistrable。
func (d *WorkflowDef) WorkflowFunc() interface{} { return d.Fn }

// WorkflowOptions 实现 WorkflowRegistrable。
func (d *WorkflowDef) WorkflowOptions() WorkflowDefOptions { return d.Options }

// ActivityFunc 实现 ActivityRegistrable。
func (d *ActivityDef) ActivityFunc() interface{} { return d.Fn }

// ActivityOptions 实现 ActivityRegistrable。
func (d *ActivityDef) ActivityOptions() ActivityDefOptions { return d.Options }

// toRegisterOptions 把 core 自有语义映射为 SDK 的 RegisterOptions。
// 零值直接赋值——空 Name / false 即默认，SDK 内部自行回退。
// 这是「SDK 类型仅在 core 内部可见」这一承诺的落点——上层（batch 等）不碰 SDK 类型。
func (o WorkflowDefOptions) toRegisterOptions() workflow.RegisterOptions {
	return workflow.RegisterOptions{
		Name:                          o.Name,
		DisableAlreadyRegisteredCheck: o.DisableAlreadyRegisteredCheck,
	}
}

// toRegisterOptions 同 WorkflowDefOptions，映射为 SDK 的 activity.RegisterOptions。
func (o ActivityDefOptions) toRegisterOptions() activity.RegisterOptions {
	return activity.RegisterOptions{
		Name:                          o.Name,
		DisableAlreadyRegisteredCheck: o.DisableAlreadyRegisteredCheck,
	}
}
