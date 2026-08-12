package batch

import (
	"reflect"
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

func TestBuilderOption_WithChunkSize(t *testing.T) {
	bc := buildConfig{Config: DefaultConfig()}
	WithChunkSize(500)(&bc)
	if bc.ChunkSize != 500 {
		t.Fatalf("ChunkSize = %d, want 500", bc.ChunkSize)
	}
}

func TestBuilderOption_WithTimeouts(t *testing.T) {
	bc := buildConfig{Config: DefaultConfig()}
	WithHeartbeatTimeout(30 * time.Second)(&bc)
	if bc.HeartbeatTimeout != 30*time.Second {
		t.Fatalf("HeartbeatTimeout = %v, want 30s", bc.HeartbeatTimeout)
	}
	WithStartToCloseTimeout(6 * time.Hour)(&bc)
	if bc.StartToCloseTimeout != 6*time.Hour {
		t.Fatalf("StartToCloseTimeout = %v, want 6h", bc.StartToCloseTimeout)
	}
}

func TestBuilderOption_WithMaxAttemptsAndRetryInterval(t *testing.T) {
	bc := buildConfig{Config: DefaultConfig()}
	WithMaxAttempts(5)(&bc)
	if bc.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %d, want 5", bc.MaxAttempts)
	}
	WithRetryInitialInterval(2 * time.Second)(&bc)
	if bc.RetryInitialInterval != 2*time.Second {
		t.Fatalf("RetryInitialInterval = %v, want 2s", bc.RetryInitialInterval)
	}
}

func TestBuilderOption_WithTransactionManager(t *testing.T) {
	bc := buildConfig{Config: DefaultConfig()}
	if bc.TransactionManager != nil {
		t.Fatal("default TransactionManager should be nil")
	}
	tm := &stubTM{}
	WithTransactionManager(tm)(&bc)
	if bc.TransactionManager != tm {
		t.Fatal("TransactionManager not injected")
	}
}

func TestActivityOption_WithActivityName(t *testing.T) {
	ao := ActivityOptions{}
	WithActivityName("adjustment")(&ao)
	if ao.Name != "adjustment" {
		t.Fatalf("Name = %q, want adjustment", ao.Name)
	}
}

func TestActivityOption_WithActivityChunkSize(t *testing.T) {
	ao := ActivityOptions{}
	WithActivityChunkSize(50)(&ao)
	if ao.ChunkSize != 50 {
		t.Fatalf("ChunkSize = %d, want 50", ao.ChunkSize)
	}
}

func TestActivityOption_WithActivityTM(t *testing.T) {
	ao := ActivityOptions{}
	tm := &stubTM{}
	WithActivityTM(tm)(&ao)
	if ao.TransactionManager != tm {
		t.Fatal("TransactionManager not injected")
	}
}

func TestActivityOptions_NoHeartbeatTimeout(t *testing.T) {
	typ := reflect.TypeOf(ActivityOptions{})
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).Name == "HeartbeatTimeout" {
			t.Fatal("ActivityOptions must not have HeartbeatTimeout")
		}
	}
}

func TestWorkflowOption_WithWorkflowName(t *testing.T) {
	wo := WorkflowOptions{}
	WithWorkflowName("my-batch")(&wo)
	if wo.Name != "my-batch" {
		t.Fatalf("Name = %q, want my-batch", wo.Name)
	}
}

func TestWorkflowOption_WithWorkflowTimeouts(t *testing.T) {
	wo := WorkflowOptions{}
	WithWorkflowHeartbeatTimeout(30 * time.Second)(&wo)
	if wo.HeartbeatTimeout != 30*time.Second {
		t.Fatalf("HeartbeatTimeout = %v, want 30s", wo.HeartbeatTimeout)
	}
	WithWorkflowStartToCloseTimeout(6 * time.Hour)(&wo)
	if wo.StartToCloseTimeout != 6*time.Hour {
		t.Fatalf("StartToCloseTimeout = %v, want 6h", wo.StartToCloseTimeout)
	}
}

func TestWorkflowOption_WithWorkflowRetry(t *testing.T) {
	wo := WorkflowOptions{}
	WithWorkflowMaxAttempts(5)(&wo)
	if wo.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %d, want 5", wo.MaxAttempts)
	}
	WithWorkflowRetryInitialInterval(2 * time.Second)(&wo)
	if wo.RetryInitialInterval != 2*time.Second {
		t.Fatalf("RetryInitialInterval = %v, want 2s", wo.RetryInitialInterval)
	}
}
