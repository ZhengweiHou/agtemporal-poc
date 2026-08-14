package core

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// WorkerManager 管理 Temporal Worker 的完整生命周期。
type WorkerManager struct {
	worker worker.Worker
	client client.Client
	config *Config
}

// NewWorkerManager 创建 WorkerManager。
func NewWorkerManager(clientFacade *ClientFacade, cfg *Config) (*WorkerManager, error) {
	w := worker.New(clientFacade.cli, cfg.Worker.TaskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize:     cfg.Worker.MaxConcurrentActivity,
		MaxConcurrentWorkflowTaskExecutionSize: cfg.Worker.MaxConcurrentWorkflow,
	})
	return &WorkerManager{
		worker: w,
		client: clientFacade.cli,
		config: cfg,
	}, nil
}

// RegisterWorkflow 注册 Workflow 函数。
// 若 wf 实现 WorkflowRegistrable，用其 WorkflowOptions（core 自有语义）映射为 SDK 选项注册；
// 否则走 SDK 默认注册（裸函数，反射取函数名）。
func (w *WorkerManager) RegisterWorkflow(wf interface{}) {
	if def, ok := wf.(WorkflowRegistrable); ok {
		w.worker.RegisterWorkflowWithOptions(def.WorkflowFunc(), def.WorkflowOptions().toRegisterOptions())
		return
	}
	w.worker.RegisterWorkflow(wf)
}

// RegisterActivity 注册 Activity 函数。
// 若 act 实现 ActivityRegistrable，用其 ActivityOptions（core 自有语义）映射为 SDK 选项注册；
// 否则走 SDK 默认注册。
func (w *WorkerManager) RegisterActivity(act interface{}) {
	if def, ok := act.(ActivityRegistrable); ok {
		w.worker.RegisterActivityWithOptions(def.ActivityFunc(), def.ActivityOptions().toRegisterOptions())
		return
	}
	w.worker.RegisterActivity(act)
}

// Start 启动 Worker，开始轮询 Task Queue。
// 同步调用——调用方用 goroutine 包一层。
func (w *WorkerManager) Start() error {
	return w.worker.Run(nil)
}

// Stop 优雅停止 Worker。
func (w *WorkerManager) Stop() {
	w.worker.Stop()
}

// GetRawWorker 逃生舱：返回原始 worker.Worker。
func (w *WorkerManager) GetRawWorker() worker.Worker {
	return w.worker
}
