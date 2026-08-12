package core

import "testing"

func TestNewConfig_DefaultValues(t *testing.T) {
	cfg := NewConfig()

	if cfg.Server.HostPort != "localhost:7233" {
		t.Errorf("expected default HostPort 'localhost:7233', got '%s'", cfg.Server.HostPort)
	}
	if cfg.Server.Namespace != "default" {
		t.Errorf("expected default Namespace 'default', got '%s'", cfg.Server.Namespace)
	}
	if cfg.Worker.TaskQueue != "agtemporal-queue" {
		t.Errorf("expected default TaskQueue 'agtemporal-queue', got '%s'", cfg.Worker.TaskQueue)
	}
	if cfg.Worker.MaxConcurrentActivity != 20 {
		t.Errorf("expected default MaxConcurrentActivity 20, got %d", cfg.Worker.MaxConcurrentActivity)
	}
	if cfg.Worker.MaxConcurrentWorkflow != 10 {
		t.Errorf("expected default MaxConcurrentWorkflow 10, got %d", cfg.Worker.MaxConcurrentWorkflow)
	}
	if cfg.Logger.Name != "agtemporal" {
		t.Errorf("expected default Logger.Name 'agtemporal', got '%s'", cfg.Logger.Name)
	}
}

func TestConfig_Validate_EmptyHostPort(t *testing.T) {
	cfg := NewConfig()
	cfg.Server.HostPort = ""

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for empty HostPort, got nil")
	}
}
