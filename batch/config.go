package batch

import "time"

// Config 批处理可调参数（有默认值、可从配置文件绑定）。
type Config struct {
	ChunkSize            int           // 攒批阈值（对齐 Spring Batch commit-interval），默认 100
	HeartbeatTimeout     time.Duration // 每 chunk 心跳间隔，Server 用此值判死，默认 15s
	StartToCloseTimeout  time.Duration // Activity 总时长硬上限（安全网），默认 24h
	MaxAttempts          int           // 最大重试次数，默认 3
	RetryInitialInterval time.Duration // 首次重试间隔，默认 1s
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		ChunkSize:            100,
		HeartbeatTimeout:     15 * time.Second,
		StartToCloseTimeout:  24 * time.Hour,
		MaxAttempts:          3,
		RetryInitialInterval: time.Second,
	}
}
