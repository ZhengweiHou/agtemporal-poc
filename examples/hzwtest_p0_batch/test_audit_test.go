// hzwtest_p0_batch 边界审查测试——以专业测试角度验证框架健壮性。
//
// 覆盖：
//   1. getIn panic → Workflow 快速失败（非无限重试卡死）——Compile 顶层 recover 验证
//   2. 空文件（0 行）→ 分片 coords 空 → 成功且 processed=0
//   3. 全坏数据 + SkipPolicy → 全部跳过（processed=0, skipped=N）
//   4. 分片数 > 行数 → 有效分片截断（不产生空分片）
package hzwtest_p0_batch

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

// getInPanic 故意写错的 getIn：断言不存在的 key → nil 类型断言 panic。
func getInPanic(fc *batch.FlowCtx) (map[string]any, error) {
	input, _ := fc.Get("input")
	missing, _ := fc.Get("step1-校验文件")
	_ = missing.(map[string]any) // ← nil panic！
	return map[string]any{"file_path": input.(map[string]any)["file_path"]}, nil
}

// TestAudit_GetInPanic_FastFail 验证 getIn panic → Workflow 快速失败（非无限重试卡死）。
func TestAudit_GetInPanic_FastFail(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	sumDef, err := b.BuildTasklet(step2bSumAmounts, batch.WithActivityName("audit-panic-sum"))
	require.NoError(t, err)

	// 编排里放一个 getIn panic 的 Phase（模拟用户 getIn 写错）
	flow := batch.Pipeline(
		batch.NewActivityPhase("step1-校验文件", sumDef, getInPanic), // getIn panic
	)
	job := batch.NewJob("audit-panic", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := fmt.Sprintf("../testdata/test_audit_panic_%d.txt", time.Now().UnixNano())
	require.NoError(t, os.WriteFile(filePath, []byte("ORD001,1000,2026-01-01\n"), 0644))
	defer os.Remove(filePath)

	run, err := job.Start(context.Background(), facade, map[string]any{"file_path": filePath})
	require.NoError(t, err)

	// 关键：快速失败（不卡死）——Get 在 timeout 内返回错误
	done := make(chan error, 1)
	go func() {
		var result map[string]any
		done <- run.Get(context.Background(), &result)
	}()
	select {
	case err := <-done:
		require.Error(t, err, "getIn panic 应导致 Workflow 失败")
		t.Logf("✅ 快速失败: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("❌ Workflow 卡死（getIn panic 未快速失败）——Compile recover 未生效")
	}
}

// TestAudit_EmptyFile 空文件：分片 coords 空 → 成功且 processed=0。
func TestAudit_EmptyFile(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithChunkSize(3), batch.WithMaxAttempts(2))
	validateDef, err := b.BuildTasklet(step1ValidateFile, batch.WithActivityName("audit-empty-v"))
	require.NoError(t, err)
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("audit-empty-engine"),
		batch.WithActivitySkipPolicy(anySkip{}),
	)
	require.NoError(t, err)

	flow := batch.Pipeline(
		batch.NewActivityPhase("step1-校验文件", validateDef, getInFilePath),
		batch.NewShardPhase("shard", &designPartitioner{shardCount: 3}, engineDef, getInDesignValidate),
	)
	job := batch.NewJob("audit-empty", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := fmt.Sprintf("../testdata/test_audit_empty_%d.txt", time.Now().UnixNano())
	require.NoError(t, os.WriteFile(filePath, []byte(""), 0644)) // 空文件
	defer os.Remove(filePath)

	run, err := job.Start(context.Background(), facade, map[string]any{"file_path": filePath})
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result), "空文件应成功")
	s := result["shard"].(map[string]any)
	require.Equal(t, float64(0), s["processed"], "空文件 processed=0")
	t.Logf("✅ 空文件: processed=0, skipped_shards=%v", s["skipped_shards"])
}

// TestAudit_AllBadRecords_SkipAll 全坏数据 + SkipPolicy → 全部跳过。
func TestAudit_AllBadRecords_SkipAll(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithChunkSize(2), batch.WithMaxAttempts(2))
	validateDef, err := b.BuildTasklet(step1ValidateFile, batch.WithActivityName("audit-bad-v"))
	require.NoError(t, err)
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("audit-bad-engine"),
		batch.WithActivitySkipPolicy(anySkip{}),
	)
	require.NoError(t, err)

	flow := batch.Pipeline(
		batch.NewActivityPhase("step1-校验文件", validateDef, getInFilePath),
		batch.NewShardPhase("shard", &designPartitioner{shardCount: 2}, engineDef, getInDesignValidate),
	)
	job := batch.NewJob("audit-bad", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := fmt.Sprintf("../testdata/test_audit_bad_%d.txt", time.Now().UnixNano())
	data := "ORD001,BAD-AMOUNT,2026-01-01\n" +
		"ORD002,BAD-AMOUNT,2026-01-02\n" +
		"ORD003,BAD-AMOUNT,2026-01-03\n" +
		"ORD004,BAD-AMOUNT,2026-01-04\n"
	require.NoError(t, os.WriteFile(filePath, []byte(data), 0644))
	defer os.Remove(filePath)

	run, err := job.Start(context.Background(), facade, map[string]any{"file_path": filePath})
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result), "全坏数据应成功（SkipPolicy）")
	s := result["shard"].(map[string]any)
	require.Equal(t, float64(0), s["processed"], "全坏 processed=0")
	require.Equal(t, float64(4), s["skipped"], "全坏 skipped=4")
	t.Logf("✅ 全坏数据: processed=0, skipped=4")
}

// TestAudit_ShardMoreThanLines 分片数 > 行数 → 有效分片截断。
func TestAudit_ShardMoreThanLines(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	b := batch.NewBuilder(batch.WithChunkSize(2), batch.WithMaxAttempts(2))
	validateDef, err := b.BuildTasklet(step1ValidateFile, batch.WithActivityName("audit-many-v"))
	require.NoError(t, err)
	engineDef, err := b.BuildActivity(
		&shardReaderFactory{}, &amountProcessor{}, &sumWriterFactory{},
		batch.WithActivityName("audit-many-engine"),
		batch.WithActivitySkipPolicy(anySkip{}),
	)
	require.NoError(t, err)

	flow := batch.Pipeline(
		batch.NewActivityPhase("step1-校验文件", validateDef, getInFilePath),
		batch.NewShardPhase("shard", &designPartitioner{shardCount: 10}, engineDef, getInDesignValidate), // 10 片 > 3 行
	)
	job := batch.NewJob("audit-many", flow)
	job.RegisterTo(wm)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	filePath := fmt.Sprintf("../testdata/test_audit_many_%d.txt", time.Now().UnixNano())
	data := "ORD001,1000,2026-01-01\nORD002,2000,2026-01-02\nORD003,3000,2026-01-03\n"
	require.NoError(t, os.WriteFile(filePath, []byte(data), 0644))
	defer os.Remove(filePath)

	run, err := job.Start(context.Background(), facade, map[string]any{"file_path": filePath})
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result), "分片>行数应成功")
	s := result["shard"].(map[string]any)
	require.Equal(t, float64(3), s["processed"], "3 行全部处理")
	// 有效分片 = 行数（3 个），非 10 个空分片
	require.Equal(t, float64(0), s["skipped_shards"], "无跳过（3 行 3 分片全部执行）")
	t.Logf("✅ 分片>行数: processed=3, 有效分片截断")
}

// TestAudit_EmptyParams_NoIdentity 无识别参数：所有作业共享固定 ID。
// 验证：DeriveWorkflowID(空) 固定（e3b0c44298fc1c14 = SHA256("") 前 16），
// 即所有无参数实例推导出同一 WorkflowID → 幂等冲突（设计问题：应 NewJob/Start 校验）。
func TestAudit_EmptyParams_NoIdentity(t *testing.T) {
	id1, err := (&core.IDSpec{Prefix: "audit-noid"}).DeriveWorkflowID(map[string]any{})
	require.NoError(t, err)
	id2, err := (&core.IDSpec{Prefix: "audit-noid"}).DeriveWorkflowID(map[string]any{})
	require.NoError(t, err)

	t.Logf("空参数 ID: %s", id1)
	require.Equal(t, id1, id2, "空参数两次推导相同")
	require.Equal(t, "audit-noid-e3b0c44298fc1c14", id1, "固定 ID（SHA256 空串前 16 hex）")

	// 危险语义：两个不同作业名相同前缀时，无参数 → 同一 ID → 互相拒绝
	t.Logf("⚠️ 设计问题确认：无识别参数 → 固定 ID（%s）→ 所有实例互相拒绝，应 NewJob/Start 校验", id1)
}
