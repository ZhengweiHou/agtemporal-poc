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

// buildConfig Builder 级不可变基座（NewBuilder 后永不修改）。
// 包含共享默认参数与运行时依赖，不暴露给用户。
type buildConfig struct {
	Config
	TransactionManager TransactionManager // nil = 无事务
}

// ActivityOptions BuildActivity 的专属配置。
// 从 buildConfig 预填充默认值，再由 ...ActivityOption 覆写差异。
type ActivityOptions struct {
	Name               string             // Activity 注册名（"" = Builder 自动生成）
	ChunkSize          int                // 攒批阈值，覆盖 Builder 级默认
	TransactionManager TransactionManager // 覆盖 Builder 级默认（nil = 无事务）
}

// WorkflowOptions BuildWorkflow 的专属配置。
// 从 buildConfig 预填充默认值，再由 ...WorkflowOption 覆写差异。
type WorkflowOptions struct {
	Name                 string        // Workflow 注册名（"" = Builder 自动生成）
	HeartbeatTimeout     time.Duration // Server 判死窗口
	StartToCloseTimeout  time.Duration // Activity 总时长硬上限（安全网）
	MaxAttempts          int           // 最大重试次数
	RetryInitialInterval time.Duration // 首次重试间隔
}

// BuilderOption 作用于 buildConfig（NewBuilder 用）。三层独立类型——编译期阻断误用。
type BuilderOption func(*buildConfig)

// ActivityOption 作用于 ActivityOptions（BuildActivity 用）。
type ActivityOption func(*ActivityOptions)

// WorkflowOption 作用于 WorkflowOptions（BuildWorkflow 用）。
type WorkflowOption func(*WorkflowOptions)

// WithChunkSize 设置 Builder 级攒批阈值。
func WithChunkSize(n int) BuilderOption {
	return func(bc *buildConfig) { bc.ChunkSize = n }
}

// WithHeartbeatTimeout 设置 Builder 级心跳超时（Server 判死窗口）。
func WithHeartbeatTimeout(d time.Duration) BuilderOption {
	return func(bc *buildConfig) { bc.HeartbeatTimeout = d }
}

// WithStartToCloseTimeout 设置 Builder 级 Activity 总时长硬上限（安全网）。
func WithStartToCloseTimeout(d time.Duration) BuilderOption {
	return func(bc *buildConfig) { bc.StartToCloseTimeout = d }
}

// WithMaxAttempts 设置 Builder 级最大重试次数。
func WithMaxAttempts(n int) BuilderOption {
	return func(bc *buildConfig) { bc.MaxAttempts = n }
}

// WithRetryInitialInterval 设置 Builder 级首次重试间隔。
func WithRetryInitialInterval(d time.Duration) BuilderOption {
	return func(bc *buildConfig) { bc.RetryInitialInterval = d }
}

// WithTransactionManager 注入 Builder 级事务管理器（nil = 无事务）。
func WithTransactionManager(tm TransactionManager) BuilderOption {
	return func(bc *buildConfig) { bc.TransactionManager = tm }
}

// WithActivityName 设置 Activity 注册名。
// 未调用时 Builder 自动生成（如 "chunk-activity-1"）。
func WithActivityName(name string) ActivityOption {
	return func(ao *ActivityOptions) { ao.Name = name }
}

// WithActivityChunkSize 覆盖 Builder 级 ChunkSize。
func WithActivityChunkSize(n int) ActivityOption {
	return func(ao *ActivityOptions) { ao.ChunkSize = n }
}

// WithActivityTM 覆盖 Builder 级 TransactionManager（nil = 无事务）。
func WithActivityTM(tm TransactionManager) ActivityOption {
	return func(ao *ActivityOptions) { ao.TransactionManager = tm }
}

// WithWorkflowName 设置 Workflow 注册名。
// 未调用时 Builder 自动生成（如 "batch-workflow-N"）。
func WithWorkflowName(name string) WorkflowOption {
	return func(wo *WorkflowOptions) { wo.Name = name }
}

// WithWorkflowHeartbeatTimeout 覆盖 Builder 级 HeartbeatTimeout。
func WithWorkflowHeartbeatTimeout(d time.Duration) WorkflowOption {
	return func(wo *WorkflowOptions) { wo.HeartbeatTimeout = d }
}

// WithWorkflowStartToCloseTimeout 覆盖 Builder 级 StartToCloseTimeout。
func WithWorkflowStartToCloseTimeout(d time.Duration) WorkflowOption {
	return func(wo *WorkflowOptions) { wo.StartToCloseTimeout = d }
}

// WithWorkflowMaxAttempts 覆盖 Builder 级 MaxAttempts。
func WithWorkflowMaxAttempts(n int) WorkflowOption {
	return func(wo *WorkflowOptions) { wo.MaxAttempts = n }
}

// WithWorkflowRetryInitialInterval 覆盖 Builder 级 RetryInitialInterval。
func WithWorkflowRetryInitialInterval(d time.Duration) WorkflowOption {
	return func(wo *WorkflowOptions) { wo.RetryInitialInterval = d }
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
