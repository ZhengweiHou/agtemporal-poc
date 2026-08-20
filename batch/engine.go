package batch

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
)

// runChunkLoop 引擎循环：读 → 逐条处理 → 攒批 → 事务写 → 心跳，最后提交尾部剩余。
// 三层防御：
//   - 层1：循环顶部 ctx.Err() 检查——Server 取消/心跳超时立即停
//   - 层2：RecordHeartbeat 后 ctx.Err() 检查——心跳失败会 cancel context，立即收手
//   - 层3：Writer 幂等——at-least-once 重试/重调度的最终防线
//
// 断点恢复（Q27）：HasHeartbeatDetails 为真时读取 ChunkProgress；
// 仅当 reader 实现 RestartableReader 才 RestoreState(ReaderState) 并沿用 Processed/Filtered 计数；
// 非 RestartableReader 从头重跑、processed 保持 0 重新累加——计数与重跑范围一致。
//
// 返回 engineResult（内部统计结构）——由调用方 toMap() 拼 map（BatchResult 消灭）。
func runChunkLoop(
	ctx context.Context,
	reader Reader,
	proc Processor,
	writer Writer,
	tm TransactionManager, // nil = 无事务
	chunkSize int,
	skipPolicy SkipPolicy, // nil = 不跳过，任何 Processor 错误都中断重试
) (engineResult, error) {

	// ── 步骤 1：断点恢复 ──
	processed := 0
	skipped := 0
	filtered := 0
	if activity.HasHeartbeatDetails(ctx) {
		var progress ChunkProgress
		if err := activity.GetHeartbeatDetails(ctx, &progress); err != nil {
			// 心跳数据损坏：显式失败（触发重试恢复），不静默从头跑
			return engineResult{}, fmt.Errorf("batch: 读取心跳详情失败: %w", err)
		}

		// 恢复：仅当 Reader 实现 RestartableReader（自己定义位置）才恢复并沿用计数。
		if rs, ok := reader.(RestartableReader); ok {
			if err := rs.RestoreState(progress.ReaderState); err != nil {
				return engineResult{}, err
			}
			processed = progress.Processed
			filtered = progress.Filtered
			skipped = progress.Skipped
		}
		// 未实现 RestartableReader → 从头重跑（processed 保持 0）。
		// 数据正确性由 Writer 幂等兜底；processed 若沿用 heartbeat 值会导致计数虚高。
	}

	// ── 步骤 2：主循环 ──
	var items, chunk []any
	for {
		// 活性检测（层 1）
		if ctx.Err() != nil {
			return engineResult{Processed: processed, Skipped: skipped, Filtered: filtered}, ctx.Err()
		}

		// 读（缓存空了才调 Reader）
		if len(items) == 0 {
			var err error
			items, err = reader.Read(ctx)
			if err != nil {
				return engineResult{Processed: processed, Skipped: skipped, Filtered: filtered}, err
			}
			if len(items) == 0 {
				break // EOF
			}
		}

		// 逐条处理（引擎不解释返回值语义——原样进 chunk）
		item := items[0]
		items = items[1:]
		r, err := proc.Process(ctx, item)
		if err != nil {
			// Skip：坏记录可跳过（SkipPolicy 判 true）→ 记录并继续，不中断
			if skipPolicy != nil && skipPolicy.ShouldSkip(err, item, skipped) {
				skipped++
				continue
			}
			return engineResult{Processed: processed, Skipped: skipped, Filtered: filtered}, err
		}
		if r == nil {
			// 过滤：Processor 返回 nil → 不写 chunk、不计 processed，计 filtered
			filtered++
			continue
		}
		chunk = append(chunk, r)

		// 攒够 ChunkSize → 事务写入
		if len(chunk) >= chunkSize {
			if err := writeChunk(ctx, writer, tm, chunk); err != nil {
				return engineResult{Processed: processed, Skipped: skipped, Filtered: filtered}, err
			}
			processed += len(chunk)
			chunk = nil

			// 心跳（Processed 是写条数，Filtered 是过滤条数，ReaderState 是 Reader 自定义状态）。
			// RecordHeartbeat 不返回 error——心跳失败会 cancel context，通过 ctx.Err() 检测（层 2）。
			progress := ChunkProgress{Processed: processed, Filtered: filtered, Skipped: skipped}
			if rs, ok := reader.(RestartableReader); ok {
				progress.ReaderState = rs.SaveState()
			}
			activity.RecordHeartbeat(ctx, progress)
			if ctx.Err() != nil {
				return engineResult{Processed: processed, Skipped: skipped, Filtered: filtered}, ctx.Err()
			}
		}
	}

	// ── 步骤 3：尾部剩余 chunk 兜底提交 ──
	if len(chunk) > 0 {
		if err := writeChunk(ctx, writer, tm, chunk); err != nil {
			return engineResult{Processed: processed, Skipped: skipped, Filtered: filtered}, err
		}
		processed += len(chunk)
	}

	return engineResult{Processed: processed, Skipped: skipped, Filtered: filtered}, nil
}

// writeChunk 在一个事务内（tm 非 nil）或直接（tm=nil）写入 chunk。
func writeChunk(ctx context.Context, writer Writer, tm TransactionManager, chunk []any) error {
	writeFn := func(ctx context.Context) error {
		return writer.Write(ctx, chunk)
	}
	if tm != nil {
		return tm.WithTransaction(ctx, writeFn)
	}
	return writeFn(ctx)
}
