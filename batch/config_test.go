package batch

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ChunkSize != 100 {
		t.Fatalf("ChunkSize = %d, want 100", cfg.ChunkSize)
	}
	if cfg.HeartbeatTimeout != 15*time.Second {
		t.Fatalf("HeartbeatTimeout = %v, want 15s", cfg.HeartbeatTimeout)
	}
	if cfg.StartToCloseTimeout != 24*time.Hour {
		t.Fatalf("StartToCloseTimeout = %v, want 24h", cfg.StartToCloseTimeout)
	}
	if cfg.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", cfg.MaxAttempts)
	}
	if cfg.RetryInitialInterval != time.Second {
		t.Fatalf("RetryInitialInterval = %v, want 1s", cfg.RetryInitialInterval)
	}
}

// ═══ ActivityOption（作用于 activityConfig）═══

func TestActivityOption_Defaults(t *testing.T) {
	cfg := defaultActivityConfig()
	if cfg.maxAttempts != 3 {
		t.Fatalf("maxAttempts = %d, want 3", cfg.maxAttempts)
	}
	if cfg.chunkSize != 100 {
		t.Fatalf("chunkSize = %d, want 100", cfg.chunkSize)
	}
	if cfg.startToClose != 24*time.Hour {
		t.Fatalf("startToClose = %v, want 24h", cfg.startToClose)
	}
}

func TestActivityOption_WithActivityName(t *testing.T) {
	cfg := defaultActivityConfig()
	WithActivityName("adjustment")(&cfg)
	if cfg.name != "adjustment" {
		t.Fatalf("name = %q, want adjustment", cfg.name)
	}
}

func TestActivityOption_WithActivityMaxAttempts(t *testing.T) {
	cfg := defaultActivityConfig()
	WithActivityMaxAttempts(5)(&cfg)
	if cfg.maxAttempts != 5 {
		t.Fatalf("maxAttempts = %d, want 5", cfg.maxAttempts)
	}
}

func TestActivityOption_WithActivityChunkSize(t *testing.T) {
	cfg := defaultActivityConfig()
	WithActivityChunkSize(50)(&cfg)
	if cfg.chunkSize != 50 {
		t.Fatalf("chunkSize = %d, want 50", cfg.chunkSize)
	}
}

func TestActivityOption_WithActivitySkipPolicy(t *testing.T) {
	cfg := defaultActivityConfig()
	sp := &skipAllPolicy{}
	WithActivitySkipPolicy(sp)(&cfg)
	if cfg.skipPolicy != sp {
		t.Fatal("skipPolicy not injected")
	}
}

func TestActivityOption_WithActivityTM(t *testing.T) {
	cfg := defaultActivityConfig()
	tm := &stubTM{}
	WithActivityTM(tm)(&cfg)
	if cfg.transactionManager != tm {
		t.Fatal("transactionManager not injected")
	}
}

func TestActivityOption_WithActivityHeartbeatTimeout(t *testing.T) {
	cfg := defaultActivityConfig()
	WithActivityHeartbeatTimeout(30 * time.Second)(&cfg)
	if cfg.heartbeatTimeout != 30*time.Second {
		t.Fatalf("heartbeatTimeout = %v, want 30s", cfg.heartbeatTimeout)
	}
}

// ═══ WorkflowOption（作用于 workflowConfig——Child 重试，修复 D2）═══

func TestWorkflowOption_Defaults(t *testing.T) {
	cfg := defaultWorkflowConfig()
	if cfg.maxAttempts != 3 {
		t.Fatalf("maxAttempts = %d, want 3 (防 Child 无限重试)", cfg.maxAttempts)
	}
}

func TestWorkflowOption_WithWorkflowName(t *testing.T) {
	cfg := defaultWorkflowConfig()
	WithWorkflowName("my-batch")(&cfg)
	if cfg.name != "my-batch" {
		t.Fatalf("name = %q, want my-batch", cfg.name)
	}
}

func TestWorkflowOption_WithWorkflowMaxAttempts(t *testing.T) {
	cfg := defaultWorkflowConfig()
	WithWorkflowMaxAttempts(5)(&cfg)
	if cfg.maxAttempts != 5 {
		t.Fatalf("maxAttempts = %d, want 5", cfg.maxAttempts)
	}
}

func TestWorkflowOption_WithWorkflowStartToCloseTimeout(t *testing.T) {
	cfg := defaultWorkflowConfig()
	WithWorkflowStartToCloseTimeout(6 * time.Hour)(&cfg)
	if cfg.startToClose != 6*time.Hour {
		t.Fatalf("startToClose = %v, want 6h", cfg.startToClose)
	}
}
