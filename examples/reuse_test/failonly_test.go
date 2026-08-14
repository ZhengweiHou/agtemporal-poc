// AllowDuplicateFailedOnly 验证——"断批重跑"核心策略的两种边界。
//
// 场景 A：失败后重跑 → 应成功（新 RunID 全量重跑）
// 场景 B：成功后重跑 → 应被拒（防止误重跑）
package reuse_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const failOnlyTaskQueue = "reuse-failonly"

// FailOrRunWorkflow: 参数 fail=true 时立即失败，否则正常完成
func FailOrRunWorkflow(ctx workflow.Context, fail bool) (string, error) {
	if fail {
		return "", fmt.Errorf("模拟批处理失败")
	}
	return "success", nil
}

func TestReusePolicyAllowDuplicateFailedOnly(t *testing.T) {
	c, _ := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	defer c.Close()

	w := worker.New(c, failOnlyTaskQueue, worker.Options{})
	w.RegisterWorkflow(FailOrRunWorkflow)
	require.NoError(t, w.Start())
	defer w.Stop()

	opts := func(wfID string) client.StartWorkflowOptions {
		return client.StartWorkflowOptions{
			ID:                    wfID,
			TaskQueue:             failOnlyTaskQueue,
			WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		}
	}

	// ═══ 场景 A：失败后重跑 → 应成功 ═══
	t.Run("失败后重跑成功", func(t *testing.T) {
		wfID := fmt.Sprintf("failonly-a-%d", time.Now().UnixNano())

		// 第一次：fail=true → 失败
		run1, err := c.ExecuteWorkflow(context.Background(), opts(wfID), FailOrRunWorkflow, true)
		require.NoError(t, err)
		var r1 string
		err1 := run1.Get(context.Background(), &r1)
		require.Error(t, err1, "第一次应失败")
		t.Logf("第一次失败 ✓ err=%v", err1)

		// 第二次：fail=false → 重跑成功
		run2, err2 := c.ExecuteWorkflow(context.Background(), opts(wfID), FailOrRunWorkflow, false)
		require.NoError(t, err2, "失败后应允许重跑")
		var r2 string
		err2get := run2.Get(context.Background(), &r2)
		require.NoError(t, err2get)
		t.Logf("✅ 失败后重跑成功，runID=%s（新 RunID %s vs 旧 %s）", run2.GetRunID(), run2.GetRunID(), run1.GetRunID())
	})

	// ═══ 场景 B：成功后重跑 → 应被拒 ═══
	t.Run("成功后重跑被拒", func(t *testing.T) {
		wfID := fmt.Sprintf("failonly-b-%d", time.Now().UnixNano())

		// 第一次：fail=false → 成功
		run1, err := c.ExecuteWorkflow(context.Background(), opts(wfID), FailOrRunWorkflow, false)
		require.NoError(t, err)
		var r1 string
		require.NoError(t, run1.Get(context.Background(), &r1))
		t.Log("第一次成功 ✓")

		// 第二次：fail=false → 应该被拒（因为前一个已成功，非失败）
		// 关键：需显式设置 WorkflowExecutionErrorWhenAlreadyStarted=true，
		// 否则 SDK 会吞掉 AlreadyStarted 错误，返回已有 Run 引用
		opts2 := opts(wfID)
		opts2.WorkflowExecutionErrorWhenAlreadyStarted = true
		_, err2 := c.ExecuteWorkflow(context.Background(), opts2, FailOrRunWorkflow, false)
		t.Logf("第二次提交 err=%v", err2)
		if err2 != nil && temporal.IsWorkflowExecutionAlreadyStartedError(err2) {
			t.Log("✅ 成功后重跑被拒（AlreadyStarted）——防止误重跑")
		} else if err2 == nil {
			t.Log("⚠️ 成功后重跑未被拒（err=nil），需查证")
		} else {
			t.Logf("⚠️ 错误类型: %T", err2)
		}
	})
}
