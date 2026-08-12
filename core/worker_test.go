package core

import (
	"context"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/batch"
)

func makeFacade(t *testing.T) *ClientFacade {
	t.Helper()
	cfg := NewConfig()
	cf, err := NewClientFacade(cfg)
	if err != nil {
		t.Skipf("Temporal server not available, skipping: %v", err)
	}
	t.Cleanup(func() { cf.Close() })
	return cf
}

func TestWorkerManager_NewWorkerManager(t *testing.T) {
	cf := makeFacade(t)
	cfg := NewConfig()

	wm, err := NewWorkerManager(cf, cfg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if wm == nil {
		t.Fatal("expected non-nil WorkerManager")
	}
}

func TestWorkerManager_RegisterWorkflow(t *testing.T) {
	cf := makeFacade(t)
	cfg := NewConfig()

	wm, err := NewWorkerManager(cf, cfg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	wm.RegisterWorkflow(func(ctx workflow.Context) error { return nil })
}

func TestWorkerManager_RegisterActivity(t *testing.T) {
	cf := makeFacade(t)
	cfg := NewConfig()

	wm, err := NewWorkerManager(cf, cfg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	wm.RegisterActivity(func(ctx context.Context) error { return nil })
}

func TestWorkerManager_GetRawWorker(t *testing.T) {
	cf := makeFacade(t)
	cfg := NewConfig()

	wm, err := NewWorkerManager(cf, cfg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	rw := wm.GetRawWorker()
	if rw == nil {
		t.Error("expected non-nil worker.Worker from GetRawWorker")
	}
}

// fakeWorker 捕获注册调用，验证 WorkerManager 解包逻辑（无需真实 server）。
type fakeWorker struct {
	activityName string
	workflowName string
	activityOpts bool // true = 走 RegisterActivityWithOptions 分支
	workflowOpts bool
}

func (f *fakeWorker) RegisterActivity(a interface{}) {
	f.activityOpts = false
	f.activityName = ""
}

func (f *fakeWorker) RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions) {
	f.activityOpts = true
	f.activityName = options.Name
}

func (f *fakeWorker) RegisterWorkflow(w interface{}) {
	f.workflowOpts = false
	f.workflowName = ""
}

func (f *fakeWorker) RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions) {
	f.workflowOpts = true
	f.workflowName = options.Name
}

func (f *fakeWorker) RegisterNexusService(service *nexus.Service) {}

func (f *fakeWorker) RegisterDynamicActivity(a interface{}, options activity.DynamicRegisterOptions) {
}

func (f *fakeWorker) RegisterDynamicWorkflow(a interface{}, options workflow.DynamicRegisterOptions) {
}

func (f *fakeWorker) Start() error { return nil }

func (f *fakeWorker) Run(interruptCh <-chan interface{}) error { return nil }

func (f *fakeWorker) Stop() {}

func dummyActivity(ctx context.Context, input batch.BatchInput) (batch.BatchResult, error) {
	return batch.BatchResult{}, nil
}

func dummyWorkflow(ctx workflow.Context, input batch.BatchInput) (batch.BatchResult, error) {
	return batch.BatchResult{}, nil
}

func TestWorkerManager_RegisterActivity_DefName(t *testing.T) {
	fw := &fakeWorker{}
	wm := &WorkerManager{worker: fw}

	def := &batch.ChunkActivityDef{Fn: dummyActivity, Name: "adjustment"}
	wm.RegisterActivity(def)

	if !fw.activityOpts {
		t.Fatal("expected RegisterActivityWithOptions branch for Def")
	}
	if fw.activityName != "adjustment" {
		t.Fatalf("registered name = %q, want adjustment", fw.activityName)
	}
}

func TestWorkerManager_RegisterActivity_BareFunc(t *testing.T) {
	fw := &fakeWorker{}
	wm := &WorkerManager{worker: fw}

	wm.RegisterActivity(dummyActivity)

	if fw.activityOpts {
		t.Fatal("expected SDK default RegisterActivity branch for bare func")
	}
}

func TestWorkerManager_RegisterWorkflow_DefName(t *testing.T) {
	fw := &fakeWorker{}
	wm := &WorkerManager{worker: fw}

	def := &batch.BatchWorkflowDef{Fn: dummyWorkflow, Name: "my-batch"}
	wm.RegisterWorkflow(def)

	if !fw.workflowOpts {
		t.Fatal("expected RegisterWorkflowWithOptions branch for Def")
	}
	if fw.workflowName != "my-batch" {
		t.Fatalf("registered name = %q, want my-batch", fw.workflowName)
	}
}

func TestWorkerManager_RegisterWorkflow_BareFunc(t *testing.T) {
	fw := &fakeWorker{}
	wm := &WorkerManager{worker: fw}

	wm.RegisterWorkflow(dummyWorkflow)

	if fw.workflowOpts {
		t.Fatal("expected SDK default RegisterWorkflow branch for bare func")
	}
}
