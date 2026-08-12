package adapter

import (
	"github.com/aif-go/ag-core/ag/ag_conf"
	"github.com/ZhengweiHou/agtemporal/core"
)

// ConfigPrefix 是 ag_conf 中 temporal 配置的前缀。
const ConfigPrefix = "temporal"

// ConfigFromAgConf 从 ag_conf 绑定 temporal.* 配置到 core.Config。
func ConfigFromAgConf(binder ag_conf.IBinder) (*core.Config, error) {
	cfg := core.NewConfig()
	if err := binder.Bind(cfg, ConfigPrefix); err != nil {
		return nil, err
	}
	return cfg, cfg.Validate()
}
