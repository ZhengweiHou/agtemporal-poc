package batch

import (
	"time"
)

// ═══════════════════════════════════════════════════════
// Options 体系（重构设计 §6——配置内聚在各自构建处）
//
// 注意（Go 语言约束）：同包内不允许同名函数——文档草案的"三组 WithName 同名函数"
// 无法编译，采用前缀方案（WithActivity*/WithWorkflow*/WithShard*）实现类型隔离。
// 语义与文档一致：执行载体决定配置类型——Activity 叶子用 ActivityOption，
// Child WF 用 WorkflowOption，分片用 ShardOption；配置不跨层传播。
// ═══════════════════════════════════════════════════════

// activityConfig Activity 叶子（Tasklet/Chunk）的构建配置。
type activityConfig struct {
	name               string           // 注册名（"" = 默认 Phase name 派生）
	maxAttempts        int32            // 重试上限（默认 DefaultConfig.MaxAttempts=3，防坏数据无限重试）
	startToClose       time.Duration    // Activity 总时长硬上限（安全网），默认 24h
	heartbeatTimeout   time.Duration    // 心跳超时（Chunk 引擎）
	chunkSize          int              // 攒批阈值（Chunk 引擎），默认 100
	skipPolicy         SkipPolicy       // 坏记录跳过策略（Chunk 引擎）
	transactionManager TransactionManager // 事务注入（Chunk 引擎，nil = 无事务）
}

// workflowConfig Child WF（Flow/Workflow）的构建配置。
type workflowConfig struct {
	name         string        // 注册名（"" = 默认派生 {name}-flow-wf / {name}-wf）
	maxAttempts  int32         // Child 重试上限（修复 D2），默认 3
	startToClose time.Duration // Child 总时长硬上限（0 = 不设，依赖内部 Activity 超时）
}

// ActivityOption 作用于 activityConfig（NewTaskletPhase/NewChunkPhase）。
type ActivityOption func(*activityConfig)

// WorkflowOption 作用于 workflowConfig（NewFlowPhase/NewWorkflowPhase）。
type WorkflowOption func(*workflowConfig)

// ── 默认值 ──

func defaultActivityConfig() activityConfig {
	dc := DefaultConfig()
	return activityConfig{
		maxAttempts:      int32(dc.MaxAttempts),
		startToClose:     dc.StartToCloseTimeout,
		heartbeatTimeout: dc.HeartbeatTimeout,
		chunkSize:        dc.ChunkSize,
	}
}

func defaultWorkflowConfig() workflowConfig {
	return workflowConfig{maxAttempts: 3}
}

// ── ActivityOption ──

// WithActivityName 覆盖 Activity 注册名（默认 = Phase name）。
func WithActivityName(name string) ActivityOption {
	return func(c *activityConfig) { c.name = name }
}

// WithActivityMaxAttempts 覆盖 Activity 重试上限。
func WithActivityMaxAttempts(n int) ActivityOption {
	return func(c *activityConfig) { c.maxAttempts = int32(n) }
}

// WithActivityStartToCloseTimeout 覆盖 Activity 总时长硬上限。
func WithActivityStartToCloseTimeout(d time.Duration) ActivityOption {
	return func(c *activityConfig) { c.startToClose = d }
}

// WithActivityHeartbeatTimeout 覆盖心跳超时（Chunk 引擎）。
func WithActivityHeartbeatTimeout(d time.Duration) ActivityOption {
	return func(c *activityConfig) { c.heartbeatTimeout = d }
}

// WithActivityChunkSize 覆盖攒批阈值（Chunk 引擎）。
func WithActivityChunkSize(n int) ActivityOption {
	return func(c *activityConfig) { c.chunkSize = n }
}

// WithActivitySkipPolicy 设置坏记录跳过策略（Chunk 引擎）。
func WithActivitySkipPolicy(sp SkipPolicy) ActivityOption {
	return func(c *activityConfig) { c.skipPolicy = sp }
}

// WithActivityTM 注入事务管理器（Chunk 引擎，nil = 无事务）。
func WithActivityTM(tm TransactionManager) ActivityOption {
	return func(c *activityConfig) { c.transactionManager = tm }
}

// ── WorkflowOption ──

// WithWorkflowName 覆盖 Child WF 注册名（默认派生 {name}-flow-wf / {name}-wf）。
func WithWorkflowName(name string) WorkflowOption {
	return func(c *workflowConfig) { c.name = name }
}

// WithWorkflowMaxAttempts 覆盖 Child 重试上限（修复 D2）。
func WithWorkflowMaxAttempts(n int) WorkflowOption {
	return func(c *workflowConfig) { c.maxAttempts = int32(n) }
}

// WithWorkflowStartToCloseTimeout 覆盖 Child 总时长硬上限。
func WithWorkflowStartToCloseTimeout(d time.Duration) WorkflowOption {
	return func(c *workflowConfig) { c.startToClose = d }
}
