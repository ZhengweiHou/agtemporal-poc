package fx

import (
	"context"

	"github.com/aif-go/ag-core/ag/ag_conf"
	"github.com/ZhengweiHou/agtemporal/adapter"
	"github.com/ZhengweiHou/agtemporal/core"
	"go.uber.org/fx"
)

// ClientModule 提供 Temporal Client + Config，不启动 Worker。
// 适用于只需要触发/查询 Workflow 的服务（调度器、管理后台、API 网关）。
//
// 提供的类型：
//   - *core.Config — 从 ag_conf 绑定的配置
//   - *core.ClientFacade — Temporal Client 门面
//
// 副作用：
//
//	OnStop → ClientFacade.Close()
var ClientModule = fx.Module("agtemporal-client",
	fx.Provide(
		func(binder ag_conf.IBinder) (*core.Config, error) {
			return adapter.ConfigFromAgConf(binder)
		},
		fx.Annotate(
			func(cfg *core.Config) (*core.ClientFacade, error) {
				return core.NewClientFacade(cfg)
			},
			fx.OnStop(func(cf *core.ClientFacade) { cf.Close() }),
		),
	),
)

// WorkerModule 在 ClientModule 基础上增加 Worker 管理器。
// 适用于执行 Workflow/Activity 的 Worker 节点。
// 前提：需要先注册 ClientModule。
//
// 提供的类型：
//   - *core.WorkerManager — Worker 管理器
//
// 副作用：
//
//	OnStart → WorkerManager.Start()（在 goroutine 中运行）
//	OnStop  → WorkerManager.Stop()
var WorkerModule = fx.Module("agtemporal-worker",
	fx.Provide(
		func(cf *core.ClientFacade, cfg *core.Config) (*core.WorkerManager, error) {
			return core.NewWorkerManager(cf, cfg)
		},
	),
	fx.Invoke(func(lc fx.Lifecycle, wm *core.WorkerManager) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				go wm.Start()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				wm.Stop()
				return nil
			},
		})
	}),
)

// Module 同时提供 Client + Worker（等同于 ClientModule + WorkerModule）。
// 适用于同时需要触发和执行 Workflow 的单体服务。
var Module = fx.Options(ClientModule, WorkerModule)
