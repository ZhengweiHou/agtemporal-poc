package core

import "errors"

// Config 是 Temporal 连接的完整配置。
// 通过 adapter 从 ag_conf 绑定，也可手动构造。
type Config struct {
	Server ServerConfig `yaml:"server"`
	Worker WorkerConfig `yaml:"worker"`
	Client ClientConfig `yaml:"client"` // P0 留位，P1 启用
	Logger LoggerConfig `yaml:"logger"`
}

// ServerConfig Temporal Server 连接配置。
type ServerConfig struct {
	HostPort  string    `yaml:"hostPort"`  // 默认 "localhost:7233"
	Namespace string    `yaml:"namespace"` // 默认 "default"
	TLS       TLSConfig `yaml:"tls"`       // P1: TLS/mTLS 配置；P0 零值表示不启用
}

// TLSConfig TLS 连接配置。
// P1: 证书路径、跳过验证等。
type TLSConfig struct{}

// WorkerConfig Worker 运行时配置。
type WorkerConfig struct {
	TaskQueue             string `yaml:"taskQueue"`
	MaxConcurrentActivity int    `yaml:"maxConcurrentActivity"` // 默认 20
	MaxConcurrentWorkflow int    `yaml:"maxConcurrentWorkflow"` // 默认 10
}

// ClientConfig Client 连接参数。
// P1: 连接超时、健康检查间隔等。
type ClientConfig struct{}

// LoggerConfig 日志配置。
type LoggerConfig struct {
	Name     string `yaml:"name"`     // 默认 "agtemporal"，对应 agslog 命名日志器
	LogLevel string `yaml:"logLevel"` // 默认 "info"
	Debug    bool   `yaml:"debug"`    // 默认 false
}

// NewConfig 创建带有默认值的 Config。
func NewConfig() *Config {
	return &Config{
		Server: ServerConfig{
			HostPort:  "localhost:7233",
			Namespace: "default",
		},
		Worker: WorkerConfig{
			TaskQueue:             "agtemporal-queue",
			MaxConcurrentActivity: 20,
			MaxConcurrentWorkflow: 10,
		},
		Logger: LoggerConfig{
			Name:  "agtemporal",
			Debug: false,
		},
	}
}

// Validate 检查必填字段。
func (c *Config) Validate() error {
	if c.Server.HostPort == "" {
		return errors.New("cfg.Server.HostPort is required")
	}
	return nil
}
