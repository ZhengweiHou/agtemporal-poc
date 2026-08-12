package core

import (
	"testing"

	"go.temporal.io/sdk/client"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestClientFacade_NewClientFacade_InvalidConfig(t *testing.T) {
	cfg := NewConfig()
	cfg.Server.HostPort = "invalid-host:9999"

	_, err := NewClientFacade(cfg)
	if err == nil {
		t.Error("expected error for invalid HostPort, got nil")
	}
}

func TestClientFacade_GetRawClient(t *testing.T) {
	cfg := NewConfig()

	cf, err := NewClientFacade(cfg)
	if err != nil {
		t.Skipf("Temporal server not available, skipping: %v", err)
	}
	defer cf.Close()

	rawClient := cf.GetRawClient()
	if rawClient == nil {
		t.Error("expected non-nil client.Client from GetRawClient")
	}
}

func TestWithWorkflowIDReusePolicy(t *testing.T) {
	opt := WithWorkflowIDReusePolicy(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
	opts := &client.StartWorkflowOptions{}
	opt(opts)

	if opts.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE {
		t.Errorf("expected WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, got %v", opts.WorkflowIDReusePolicy)
	}
}
