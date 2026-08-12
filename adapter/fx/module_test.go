package fx

import (
	"testing"

	"github.com/aif-go/ag-core/ag/ag_conf"
	"github.com/ZhengweiHou/agtemporal/core"
	"go.uber.org/fx"
)

// mockBinder 实现 ag_conf.IBinder 用于测试。
type mockBinder struct{}

func (m *mockBinder) Bind(target any, prefix ...string) error {
	_ = target.(*core.Config)
	return nil
}

func (m *mockBinder) GetEnv() ag_conf.IConfigurableEnvironment {
	return nil
}

var _ ag_conf.IBinder = (*mockBinder)(nil)

func binderProvider() ag_conf.IBinder {
	return &mockBinder{}
}

func TestClientModule_Provides(t *testing.T) {
	app := fx.New(
		fx.Provide(binderProvider),
		ClientModule,
		fx.Invoke(func(cfg *core.Config, cf *core.ClientFacade) {
			if cfg == nil {
				t.Error("expected non-nil *core.Config")
			}
			if cf == nil {
				t.Error("expected non-nil *core.ClientFacade")
			}
		}),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestClientModule_CloseOnStop(t *testing.T) {
	app := fx.New(
		fx.Provide(binderProvider),
		ClientModule,
		fx.Invoke(func(cf *core.ClientFacade) {
			if cf.GetRawClient() == nil {
				t.Error("expected non-nil raw client")
			}
		}),
	)
	if err := app.Start(t.Context()); err != nil {
		t.Skipf("Temporal server not available, skipping: %v", err)
	}
	if err := app.Stop(t.Context()); err != nil {
		t.Error("expected clean stop, got error:", err)
	}
}
