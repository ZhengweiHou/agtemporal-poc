// Search Attributes 作为"跳 step 依据"的方案验证。
//
// 场景：
//   SkipWorkflow 顺序执行 P1 → P2 → P3，每步完成后 UpsertSearchAttributes 记录 CompletedSteps。
//   P2 遇坏数据失败 → 旧 Run 的 SA = "P1"。
//   修复数据 → 查询旧 Run SA → 作为 skip_steps 入参重跑 → 新 Run 跳过 P1，从 P2 继续。
//
// 验证点：
//   1. 自定义 SA 能否通过 OperatorService 注册
//   2. 失败后，旧 Run 的 SA 是否跨 Run 保留、能否查到
//   3. 用查到的 SA 作为 skip 依据，新 Run 能否正确跳过已完成步骤
package sa_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	saTaskQueue   = "sa-skip-demo"
	saAttrName    = "CompletedSteps" // 自定义 SA：记录已完成步骤（逗号分隔）
	saProgressLog = "../testdata/sa_progress.txt"
	saDataFile    = "../testdata/sa_bad.txt"
)

// ── 各步骤 Activity：写副作用，便于观察是否真的执行 ──

func saStepActivity(ctx context.Context, step string, filePath string) (string, error) {
	// P2 读文件，遇坏数据失败；其余步骤直接成功
	if step == "P2" {
		content, err := os.ReadFile(filePath)
		if err != nil {
			return "", err
		}
		if strings.Contains(string(content), "BAD-AMOUNT") {
			return "", fmt.Errorf("P2 金额解析失败: BAD-AMOUNT")
		}
	}

	// 副作用：记录"这一步真的执行了"
	f, err := os.OpenFile(saProgressLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fmt.Fprintf(f, "%s-executed\n", step)
	return step + "-done", nil
}

// ── Workflow：顺序执行 P1→P2→P3，每步完成 Upsert SA ──

func SkipWorkflow(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	filePath := asStr(input["file_path"])
	skipSteps := asStr(input["skip_steps"]) // 逗号分隔，如 "P1" 表示跳过 P1

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	completed := []string{}

	steps := []string{"P1", "P2", "P3"}
	for _, step := range steps {
		// 若在 skip 列表里，跳过
		if strings.Contains(","+skipSteps+",", ","+step+",") {
			workflow.GetLogger(ctx).Info("跳过已完成步骤", "step", step)
			completed = append(completed, step)
			continue
		}

		var out string
		err := workflow.ExecuteActivity(ctx, saStepActivity, step, filePath).Get(ctx, &out)
		if err != nil {
			workflow.GetLogger(ctx).Error("步骤失败", "step", step, "err", err)
			return map[string]any{"completed": completed}, err
		}
		completed = append(completed, step)

		// 每步完成后 Upsert SA
		upsertErr := workflow.UpsertSearchAttributes(ctx, map[string]interface{}{
			saAttrName: strings.Join(completed, ","),
		})
		if upsertErr != nil {
			workflow.GetLogger(ctx).Error("Upsert SA 失败", "err", upsertErr)
		}
	}

	return map[string]any{"completed": completed}, nil
}

func asStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// ── 注册自定义 SA ──

func registerSA(t *testing.T, c client.Client) {
	_, err := c.OperatorService().AddSearchAttributes(context.Background(),
		&operatorservice.AddSearchAttributesRequest{
			Namespace: "default",
			SearchAttributes: map[string]enumspb.IndexedValueType{
				saAttrName: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
		})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Logf("注册 SA（可能已存在）: %v", err)
	}
}

// ── 查询旧 Run 的 SA ──

func getSA(c client.Client, wfID, runID string) string {
	desc, err := c.DescribeWorkflowExecution(context.Background(), wfID, runID)
	if err != nil {
		return ""
	}
	sa := desc.GetWorkflowExecutionInfo().GetSearchAttributes()
	if sa == nil {
		return ""
	}
	for k, v := range sa.GetIndexedFields() {
		if k == saAttrName {
			// Payload 是 JSON 编码的，需用 data converter 解码
			var s string
			if err := converter.GetDefaultDataConverter().FromPayload(v, &s); err != nil {
				return ""
			}
			return s
		}
	}
	return ""
}

func TestSAAsSkipBasis(t *testing.T) {
	os.Remove(saProgressLog)

	c, err := client.Dial(client.Options{HostPort: "172.17.0.1:7233", Namespace: "default"})
	require.NoError(t, err)
	defer c.Close()

	registerSA(t, c)

	w := worker.New(c, saTaskQueue, worker.Options{})
	w.RegisterWorkflow(SkipWorkflow)
	w.RegisterActivity(saStepActivity)
	require.NoError(t, w.Start())
	defer w.Stop()

	wfID := fmt.Sprintf("sa-skip-%d", time.Now().UnixNano())

	// ═══ 第一次：P2 坏数据 → 失败 ═══
	badData := "ORD001,1000,2026-01-01\nORD002,BAD-AMOUNT,2026-01-02\n"
	require.NoError(t, os.WriteFile(saDataFile, []byte(badData), 0644))

	run1, _ := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: saTaskQueue,
	}, SkipWorkflow, map[string]any{"file_path": saDataFile, "skip_steps": ""})

	var r1 map[string]any
	err1 := run1.Get(context.Background(), &r1)
	require.Error(t, err1, "P2 坏数据应失败")
	t.Logf("第一次失败 ✓ runID=%s", run1.GetRunID())

	// 副作用：P1 执行了，P2 失败
	side, _ := os.ReadFile(saProgressLog)
	t.Logf("失败后副作用: %q（应只有 P1-executed）", strings.TrimSpace(string(side)))

	// ═══ 查询旧 Run 的 SA ═══
	oldSA := getSA(c, wfID, run1.GetRunID())
	t.Logf("旧 Run SA[CompletedSteps] = %q", oldSA)

	// ═══ 修复数据 ═══
	fixedData := "ORD001,1000,2026-01-01\nORD002,2000,2026-01-02\n"
	require.NoError(t, os.WriteFile(saDataFile, []byte(fixedData), 0644))

	// ═══ 第二次：用旧 SA 作为 skip_steps 重跑 ═══
	run2, _ := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: saTaskQueue,
	}, SkipWorkflow, map[string]any{"file_path": saDataFile, "skip_steps": oldSA})

	var r2 map[string]any
	err2 := run2.Get(context.Background(), &r2)
	require.NoError(t, err2, "修复后应成功")
	t.Logf("第二次成功 ✓ runID=%s", run2.GetRunID())

	// ═══ 评估：副作用行数 ═══
	side2, _ := os.ReadFile(saProgressLog)
	lines := strings.Split(strings.TrimSpace(string(side2)), "\n")
	t.Log("══════════════════════════════")
	t.Logf("总副作用: %q", lines)
	t.Logf("总执行步骤数: %d（P1=1 + P2=1 + P3=1，理想为 3 若跳过了 P1 则 P1 只出现一次）", len(lines))

	p1Count := 0
	for _, l := range lines {
		if l == "P1-executed" {
			p1Count++
		}
	}
	if p1Count == 1 {
		t.Log("✅ 方案可行：P1 只执行一次，第二次重跑跳过了 P1")
	} else {
		t.Logf("⚠️ P1 执行了 %d 次，skip 未生效", p1Count)
	}
}
