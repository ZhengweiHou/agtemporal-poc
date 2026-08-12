package core

import (
	"context"

	"go.temporal.io/sdk/client"
	enumspb "go.temporal.io/api/enums/v1"
)

// ClientFacade 封装 Temporal Client 的核心操作，提供简化 API + 逃生舱。
type ClientFacade struct {
	cli    client.Client
	config *Config
}

// StartWorkflowOption 是 StartWorkflow 的可选参数。
// P0 不传 opts，使用 Config 中的默认值。
// P1 可通过 option 覆盖默认行为（如 WorkflowID 冲突策略、自定义 TaskQueue）。
type StartWorkflowOption func(*client.StartWorkflowOptions)

// WithWorkflowIDReusePolicy 设置 WorkflowID 重用策略。
func WithWorkflowIDReusePolicy(policy enumspb.WorkflowIdReusePolicy) StartWorkflowOption {
	return func(opts *client.StartWorkflowOptions) {
		opts.WorkflowIDReusePolicy = policy
	}
}

// NewClientFacade 创建并验证 Temporal 连接。
func NewClientFacade(cfg *Config) (*ClientFacade, error) {
	cli, err := client.Dial(client.Options{
		HostPort:  cfg.Server.HostPort,
		Namespace: cfg.Server.Namespace,
		Logger:    NewSlogAdapter(nil),
	})
	if err != nil {
		return nil, err
	}

	_, err = cli.CheckHealth(context.Background(), &client.CheckHealthRequest{})
	if err != nil {
		cli.Close()
		return nil, err
	}

	return &ClientFacade{cli: cli, config: cfg}, nil
}

// StartWorkflow 启动一个新的 Workflow。
// workflowID 必须全局唯一。
func (c *ClientFacade) StartWorkflow(
	ctx context.Context,
	workflowID string,
	workflowFn interface{},
	input interface{},
	opts ...StartWorkflowOption,
) (client.WorkflowRun, error) {
	wfOpts := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: c.config.Worker.TaskQueue,
	}
	for _, o := range opts {
		o(&wfOpts)
	}
	return c.cli.ExecuteWorkflow(ctx, wfOpts, workflowFn, input)
}

// CancelWorkflow 优雅取消正在运行的 Workflow。
func (c *ClientFacade) CancelWorkflow(ctx context.Context, workflowID string) error {
	return c.cli.CancelWorkflow(ctx, workflowID, "")
}

// GetRawClient 逃生舱：返回原始 client.Client。
func (c *ClientFacade) GetRawClient() client.Client {
	return c.cli
}

// Close 释放连接资源。
func (c *ClientFacade) Close() {
	c.cli.Close()
}
