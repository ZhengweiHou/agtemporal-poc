// hzwtest_p0_batch 方向 B 实测——分片=Child Workflow 的幂等/寻址语义。
//
// 验证点：
//   ① Child 冲突错误语义：已完成 Child 被再次调用（同 ID + AllowDuplicateFailedOnly），
//      Future.Get 返回什么？能否与普通失败区分？
//   ② 主 WF 幂等级联：主 WF 重跑时，已完成分片 Child 被拒 → 主 WF 能否识别并优雅跳过？
//   ③ 分片级可寻址：可推导 ID（{主ID}-shard-{n}）能否 Describe 查询单个分片？
package hzwtest_p0_batch

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/core"
)

// probeShardChildWf 分片 Child WF：fail=true 时失败（模拟坏分片）。
func probeShardChildWf(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	if input["fail"] == true {
		return nil, errors.New("simulated shard failure")
	}
	return map[string]any{"shard_ok": true}, nil
}

// probeParentWf 主 WF：调度 shard-0/shard-1（可推导 ID + AllowDuplicateFailedOnly），记录各分片错误。
// fail_shard 非空 → 该分片失败，主 WF 整体失败（制造"失败后重跑"场景）。
func probeParentWf(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	failShard, _ := input["fail_shard"].(string)
	result := map[string]any{}
	for _, name := range []string{"shard-0", "shard-1"} {
		childID := workflow.GetInfo(ctx).WorkflowExecution.ID + "-" + name
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID:            childID,
			WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		})
		var out map[string]any
		err := workflow.ExecuteChildWorkflow(childCtx, probeShardChildWf, map[string]any{
			"fail": failShard == name,
		}).Get(ctx, &out)
		if err != nil {
			// 结构化区分：Child 已启动/已完成被拒 vs 真实失败
			var alreadyStarted *temporal.ChildWorkflowExecutionAlreadyStartedError
			if errors.As(err, &alreadyStarted) {
				result[name+"_already_started"] = true // 幂等跳过信号
			} else {
				result[name+"_err"] = err.Error()
			}
		} else {
			result[name+"_ok"] = true
		}
	}
	if failShard != "" {
		return result, errors.New("parent failed due to shard: " + failShard)
	}
	return result, nil
}

// TestShardWF_ChildConflictAndIdempotency 验证①②③：
//   第一次跑（shard-1 失败）→ 主 WF 失败
//   第二次跑（同主 ID，修复）→ shard-0 已完成（观察冲突错误），shard-1 重跑成功
//   结束后 Describe 单个分片（验证③）
func TestShardWF_ChildConflictAndIdempotency(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)
	wm.RegisterWorkflow(probeParentWf)
	wm.RegisterWorkflow(probeShardChildWf)
	go func() { _ = wm.Start() }()
	defer wm.Stop()

	parentID := fmt.Sprintf("shardwf-test-%d", time.Now().UnixNano())

	// ═══ 第一次跑：shard-1 失败 → 主 WF 失败 ═══
	run1, err := facade.StartWorkflow(context.Background(), parentID, probeParentWf,
		map[string]any{"fail_shard": "shard-1"},
		core.WithDefaultResumePolicy())
	require.NoError(t, err)
	err1 := run1.Get(context.Background(), nil)
	require.Error(t, err1, "第一次跑应因 shard-1 失败而整体失败")
	fmt.Printf("第一次跑: 失败 ✅ (shard-1 注入失败)\n")

	// ═══ 第二次跑：同主 ID（失败后允许），shard-0 已完成 / shard-1 重跑 ═══
	run2, err := facade.StartWorkflow(context.Background(), parentID, probeParentWf,
		map[string]any{"fail_shard": ""},
		core.WithDefaultResumePolicy())
	require.NoError(t, err)

	var result map[string]any
	err2 := run2.Get(context.Background(), &result)
	require.NoError(t, err2, "第二次跑应成功（shard-1 重跑成功）")

	// ═══ 验证①：shard-0（已完成）冲突错误语义——errors.As 结构化区分 ═══
	alreadyStarted, hasFlag := result["shard-0_already_started"]
	fmt.Printf("验证① shard-0 冲突: already_started=%v (errors.As 结构化区分 ✅)\n", alreadyStarted)
	require.Equal(t, true, hasFlag, "已完成 Child 被拒应命中 ChildWorkflowExecutionAlreadyStartedError")
	require.Equal(t, true, alreadyStarted, "errors.As 应命中 already started")

	// ═══ 验证②：主 WF 幂等级联 ═══
	shard1OK, _ := result["shard-1_ok"]
	fmt.Printf("验证② shard-1 重跑结果: ok=%v\n", shard1OK)
	require.Equal(t, true, shard1OK, "shard-1（上次失败）应允许重跑并成功")

	// ═══ 验证③：分片级可寻址（Describe 单个分片） ═══
	childID := parentID + "-shard-0"
	desc, err := facade.GetRawClient().DescribeWorkflowExecution(context.Background(), childID, "")
	require.NoError(t, err, "可推导 Child ID 应可查询")
	status := desc.WorkflowExecutionInfo.Status
	fmt.Printf("验证③ Describe shard-0: status=%v (可寻址 ✅)\n", status)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, status)
}

