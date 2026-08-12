package adapter

import (
	"testing"

	"github.com/aif-go/ag-core/ag/ag_conf"
	"github.com/ZhengweiHou/agtemporal/core"
)

// mockBinder 实现 ag_conf.IBinder 用于测试。
type mockBinder struct {
	bindFn func(target any, prefix ...string) error
}

func (m *mockBinder) Bind(target any, prefix ...string) error {
	return m.bindFn(target, prefix...)
}

func (m *mockBinder) GetEnv() ag_conf.IConfigurableEnvironment {
	return nil
}

var _ ag_conf.IBinder = (*mockBinder)(nil)


func TestConfigFromAgConf_Success(t *testing.T) {
	binder := &mockBinder{
		bindFn: func(target any, prefix ...string) error {
			if len(prefix) > 0 && prefix[0] != ConfigPrefix {
				t.Errorf("expected prefix '%s', got '%s'", ConfigPrefix, prefix[0])
			}
			cfg := target.(*core.Config)
			cfg.Server.HostPort = "temporal:7233"
			cfg.Server.Namespace = "production"
			return nil
		},
	}

	cfg, err := ConfigFromAgConf(binder)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.Server.HostPort != "temporal:7233" {
		t.Errorf("expected HostPort 'temporal:7233', got '%s'", cfg.Server.HostPort)
	}
	if cfg.Server.Namespace != "production" {
		t.Errorf("expected Namespace 'production', got '%s'", cfg.Server.Namespace)
	}
}

func TestConfigFromAgConf_InvalidConfig(t *testing.T) {
	binder := &mockBinder{
		bindFn: func(target any, prefix ...string) error {
			cfg := target.(*core.Config)
			cfg.Server.HostPort = ""
			return nil
		},
	}

	_, err := ConfigFromAgConf(binder)
	if err == nil {
		t.Error("expected error for empty HostPort, got nil")
	}
}
