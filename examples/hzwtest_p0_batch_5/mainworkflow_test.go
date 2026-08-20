// hzwtest_p0_batch_5 —— 日终批量对账综合案例（完整对标设计文档：agtemporal POC 对账业务综合案例设计）。
//
// 单文件自包含：不依赖其他测试包的符号。
//
// 业务流程（6 阶段，覆盖全部 Phase 场景）：
//
//	Job: 日终对账（识别参数 date + run_id）
//	  ├─ ① 数据准备: Parallel(flow 内部导出 ∥ flow 外部加载)
//	  ├─ ② 预处理:   Parallel(Tasklet 内部统计 ∥ Tasklet 外部统计)
//	  ├─ ③ 核心对账: Shard(partition=IO Tasklet 读外部工件拆坐标, handler=Chunk 对账引擎 R-P-W)
//	  ├─ ④ 差异处理: Pipeline(合并 Tasklet → Parallel(分类统计 ∥ flow 差错清单))
//	  ├─ ⑤ 报告:     Tasklet 汇总 → Workflow 渠道子报告（手写逃逸舱——循环渠道动态调度）
//	  └─ ⑥ 通知:     Tasklet
//
// 对账规则: trade_no 匹配 + 金额核对 → 一致(过滤) / 金额不符 / 外部多账（差异落工件）
// 内部多账 = 内部总数 − 匹配数（分类统计反推）
//
// 测试数据（100 级，§2.2）: 外部 100（一致 80 + 金额不符 8 + 外部多账 12）/ 内部 92（+ 内部多账 4）
// 渠道: POS/EBANK/ATM（供渠道子报告逃逸舱）
//
// 工件（D5: 文件名含 run_id）: internal/external/diff-{分片}/diff-all/checklist/report/report-{渠道}
package hzwtest_p0_batch_5

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.temporal.io/sdk/workflow"

	"github.com/ZhengweiHou/agtemporal/batch"
	"github.com/ZhengweiHou/agtemporal/core"
)

const taskQueue = "hzwtest-p0-batch-5"

// ── 业务常量 ──
const (
	shardCount = 4 // 对账分片数（设计文档 §2.2）
)

var channels = []string{"POS", "EBANK", "ATM"}

// ── map 取值 helper ──

func asInt(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	case string:
		n, _ := strconv.Atoi(x)
		return n
	}
	return 0
}

func asStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asStrings(v any) []string {
	if list, ok := v.([]any); ok {
		out := make([]string, 0, len(list))
		for _, item := range list {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	if list, ok := v.([]string); ok {
		return list
	}
	return nil
}

// ═══════════════════════════════════════════════════════
// 测试数据生成（100 级——设计文档 §2.2）
// ═══════════════════════════════════════════════════════

// genReconData 生成内部原始流水 + 外部对账文件。
// 返回 (internalRaw, externalRaw) 路径。
func genReconData(t *testing.T, dir string) (string, string) {
	t.Helper()
	// 一致 80: T0001-T0080（双方同额）
	// 金额不符 8: T0081-T0088（内部 X，外部 X+100）
	// 内部多账 4: T0089-T0092（仅内部）
	// 外部多账 12: T0093-T0104（仅外部）
	intPath := filepath.Join(dir, "int_raw.txt")
	extPath := filepath.Join(dir, "ext_raw.txt")

	var intLines, extLines []string
	for i := 1; i <= 80; i++ { // 一致
		no := fmt.Sprintf("T%04d", i)
		amount := 1000 + i*7
		intLines = append(intLines, fmt.Sprintf("%s,ACC%04d,%d,%s,%s", no, i, amount, "2026-08-20", channels[i%3]))
		extLines = append(extLines, fmt.Sprintf("%s,ACC%04d,%d,%s,%s", no, i, amount, "2026-08-20", channels[i%3]))
	}
	for i := 81; i <= 88; i++ { // 金额不符（外部多 100）
		no := fmt.Sprintf("T%04d", i)
		intLines = append(intLines, fmt.Sprintf("%s,ACC%04d,%d,%s,%s", no, i, 5000+i, "2026-08-20", channels[i%3]))
		extLines = append(extLines, fmt.Sprintf("%s,ACC%04d,%d,%s,%s", no, i, 5000+i+100, "2026-08-20", channels[i%3]))
	}
	for i := 89; i <= 92; i++ { // 内部多账（仅内部）
		no := fmt.Sprintf("T%04d", i)
		intLines = append(intLines, fmt.Sprintf("%s,ACC%04d,%d,%s,%s", no, i, 3000+i, "2026-08-20", channels[i%3]))
	}
	for i := 93; i <= 104; i++ { // 外部多账（仅外部）
		no := fmt.Sprintf("T%04d", i)
		extLines = append(extLines, fmt.Sprintf("%s,ACC%04d,%d,%s,%s", no, i, 2000+i, "2026-08-20", channels[i%3]))
	}

	require.NoError(t, os.WriteFile(intPath, []byte(strings.Join(intLines, "\n")+"\n"), 0644))
	require.NoError(t, os.WriteFile(extPath, []byte(strings.Join(extLines, "\n")+"\n"), 0644))
	require.Equal(t, 92, len(intLines), "内部原始 92 笔")
	require.Equal(t, 100, len(extLines), "外部原始 100 笔")
	return intPath, extPath
}

// ═══════════════════════════════════════════════════════
// 对账引擎组件（R-P-W——handler=Chunk）
// ═══════════════════════════════════════════════════════

// reconRecord 外部流水记录（Reader 产出）。
type reconRecord struct {
	TradeNo string
	Amount  int
	Channel string
}

// diffRecord 差异记录（Processor 产出 → Writer 落工件）。
type diffRecord struct {
	TradeNo   string
	Type      string // mismatch / ext_only
	ExtAmount int
	IntAmount int
}

// reconReaderFactory 读分片外部记录段。实现 ReaderFactory + 内部索引路径传递。
type reconReaderFactory struct{}

func (f *reconReaderFactory) NewReader(ctx context.Context, input batch.BatchInput) (batch.Reader, error) {
	filePath := asStr(input.Params["file_path"])
	start := asInt(input.Params["start"])
	lineCount := asInt(input.Params["line_count"])
	return newReconReader(filePath, start, lineCount)
}

type reconReader struct {
	batch.OffsetState // 断点定位（行号）
	records []reconRecord
}

func newReconReader(filePath string, start, lineCount int) (*reconReader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var all []reconRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if len(fields) < 3 { // 外部工件格式: trade_no,amount,channel
			continue
		}
		amt, _ := strconv.Atoi(fields[1])
		all = append(all, reconRecord{TradeNo: fields[0], Amount: amt, Channel: fields[2]})
	}
	end := start + lineCount
	if end > len(all) {
		end = len(all)
	}
	if start > len(all) {
		start = len(all)
	}
	return &reconReader{records: all[start:end]}, nil
}

func (r *reconReader) Read(ctx context.Context) ([]any, error) {
	if r.Offset >= len(r.records) {
		return nil, nil
	}
	rec := r.records[r.Offset]
	r.Offset++
	return []any{rec}, nil
}

// reconProcessorFactory 每次执行创建 Processor（加载内部流水索引——Activity 域 IO）。
type reconProcessorFactory struct{}

func (f *reconProcessorFactory) NewProcessor(ctx context.Context, input batch.BatchInput) (batch.Processor, error) {
	idx, err := loadInternalIndex(asStr(input.Params["internal_file"]))
	if err != nil {
		return nil, err
	}
	return &reconProcessor{internalIdx: idx}, nil
}

// reconProcessor 对账：trade_no 匹配 + 金额核对。
// 一致 → 返回 nil（过滤语义——不写差异、计 filtered）；
// 金额不符/外部多账 → 差异记录。
// 实现 ResultProvider——累计全部外部记录金额（含被过滤的一致记录——Writer 看不到）。
type reconProcessor struct {
	internalIdx map[string]int // trade_no → amount
	extTotal    int            // 全部外部记录金额（含一致）
}

func loadInternalIndex(path string) (map[string]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	idx := map[string]int{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if len(fields) < 2 {
			continue
		}
		amt, _ := strconv.Atoi(fields[1])
		idx[fields[0]] = amt
	}
	return idx, nil
}

func (p *reconProcessor) Process(ctx context.Context, item any) (any, error) {
	rec := item.(reconRecord)
	p.extTotal += rec.Amount // 全量累计（含一致）
	intAmount, ok := p.internalIdx[rec.TradeNo]
	if !ok {
		return diffRecord{TradeNo: rec.TradeNo, Type: "ext_only", ExtAmount: rec.Amount, IntAmount: 0}, nil
	}
	if intAmount != rec.Amount {
		return diffRecord{TradeNo: rec.TradeNo, Type: "mismatch", ExtAmount: rec.Amount, IntAmount: intAmount}, nil
	}
	return nil, nil // 一致——过滤
}

// Result Processor 侧统计（引擎合并进 Output——含被过滤记录）。
func (p *reconProcessor) Result() map[string]any {
	return map[string]any{"ext_total": p.extTotal}
}

// reconWriterFactory 创建 reconWriter（差异落分片工件文件）。
type reconWriterFactory struct{}

func (f *reconWriterFactory) NewWriter(ctx context.Context, input batch.BatchInput) (batch.Writer, error) {
	diffFile := fmt.Sprintf("%s/%s-diff-%s.txt",
		asStr(input.Params["work_dir"]), asStr(input.Params["run_id"]), asStr(input.Params["shard_name"]))
	fh, err := os.Create(diffFile)
	if err != nil {
		return nil, err
	}
	return &reconWriter{f: fh}, nil
}

type reconWriter struct {
	f         *os.File
	matched   int // 一致（过滤）——由 ResultProvider 从外部记录反推？——在 Result() 里用引擎统计不可得，改在 Processor 侧计
	mismatch  int
	extOnly   int
	extAmount int
}

func (w *reconWriter) Write(ctx context.Context, items []any) error {
	for _, it := range items {
		d := it.(diffRecord)
		if _, err := fmt.Fprintf(w.f, "%s,%s,%d,%d\n", d.TradeNo, d.Type, d.ExtAmount, d.IntAmount); err != nil {
			return err
		}
		w.extAmount += d.ExtAmount
		switch d.Type {
		case "mismatch":
			w.mismatch++
		case "ext_only":
			w.extOnly++
		}
	}
	return nil
}

func (w *reconWriter) Close() error { return w.f.Close() }

func (w *reconWriter) Result() map[string]any {
	return map[string]any{
		"mismatch":   w.mismatch,
		"ext_only":   w.extOnly,
		"ext_amount": w.extAmount,
	}
}

// ═══════════════════════════════════════════════════════
// 阶段① 数据准备（flow 子树——Child WF）
// ═══════════════════════════════════════════════════════

// exportInternal 内部流水导出（读原始内部文件 → 标准化工件）。
func exportInternal(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	intRaw, _ := fc.Str("input.int_raw")
	runID, _ := fc.Str("input.run_id")
	workDir, _ := fc.Str("input.work_dir")
	out := filepath.Join(workDir, runID+"-internal.txt")

	count, amount := 0, 0
	f, err := os.Open(intRaw)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if len(fields) < 3 {
			continue
		}
		amt, _ := strconv.Atoi(fields[2])
		lines = append(lines, fmt.Sprintf("%s,%d", fields[0], amt))
		count++
		amount += amt
	}
	requireOK := os.WriteFile(out, []byte(strings.Join(lines, "\n")+"\n"), 0644)
	if requireOK != nil {
		return nil, requireOK
	}
	return map[string]any{"exported": count, "int_amount": amount, "int_work": out}, nil
}

// loadExternal 外部对账文件加载（读原始 → 校验 → 标准化工件）。
func loadExternal(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	extRaw, _ := fc.Str("input.ext_raw")
	runID, _ := fc.Str("input.run_id")
	workDir, _ := fc.Str("input.work_dir")
	out := filepath.Join(workDir, runID+"-external.txt")

	count, amount, bad := 0, 0, 0
	f, err := os.Open(extRaw)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Split(line, ",")
		if len(fields) < 5 {
			bad++
			continue
		}
		amt, err := strconv.Atoi(fields[2])
		if err != nil {
			bad++
			continue
		}
		lines = append(lines, fmt.Sprintf("%s,%d,%s", fields[0], amt, fields[4]))
		count++
		amount += amt
	}
	if err := os.WriteFile(out, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		return nil, err
	}
	return map[string]any{"loaded": count, "ext_amount": amount, "bad_lines": bad, "ext_work": out}, nil
}

// ═══════════════════════════════════════════════════════
// 阶段② 预处理统计（Tasklet）
// ═══════════════════════════════════════════════════════

func internalStats(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	work, _ := fc.Str("input.int_work_path")
	count, amount := 0, 0
	f, err := os.Open(work)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if len(fields) < 2 {
			continue
		}
		amt, _ := strconv.Atoi(fields[1])
		count++
		amount += amt
	}
	return map[string]any{"int_count": count, "int_amount": amount}, nil
}

func externalStats(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	work, _ := fc.Str("input.ext_work_path")
	count, amount := 0, 0
	chSet := map[string]bool{}
	f, err := os.Open(work)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if len(fields) < 3 {
			continue
		}
		amt, _ := strconv.Atoi(fields[1])
		count++
		amount += amt
		chSet[fields[2]] = true
	}
	var chList []string
	for ch := range chSet {
		chList = append(chList, ch)
	}
	sort.Strings(chList)
	return map[string]any{"ext_count": count, "ext_amount": amount, "channels": chList}, nil
}

// ═══════════════════════════════════════════════════════
// 阶段③ 核心对账（Shard——partition=IO Tasklet + handler=Chunk）
// ═══════════════════════════════════════════════════════

// splitExternal IO 拆分：读外部工件统计行数 → 拆 4 片坐标。
// 输出契约: {"partitions": [{"name","data"}]}（不进 FlowCtx——T8 决议）
func splitExternal(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	extWork, _ := fc.Str("input.ext_work_path")
	intWork, _ := fc.Str("input.int_work_path")
	runID, _ := fc.Str("input.run_id")
	workDir, _ := fc.Str("input.work_dir")

	f, err := os.Open(extWork)
	if err != nil {
		return nil, err
	}
	total := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if len(sc.Text()) > 0 {
			total++
		}
	}
	f.Close()

	per := total / shardCount
	if total%shardCount != 0 {
		per++
	}
	var parts []batch.Partition
	for i := 0; i < shardCount; i++ {
		start := i * per
		count := per
		if rem := total - start; count > rem {
			count = rem
		}
		if count <= 0 {
			break
		}
		name := fmt.Sprintf("shard-%d", i)
		parts = append(parts, batch.Partition{
			Name: name,
			Data: map[string]any{
				"file_path":    extWork,
				"start":        start,
				"line_count":   count,
				"internal_file": intWork,
				"shard_name":   name,
				"run_id":       runID,
				"work_dir":     workDir,
			},
		})
	}
	// 契约封装（与框架 batch.partitionListToMap 一致）
	list := make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		list = append(list, map[string]any{"name": p.Name, "data": p.Data})
	}
	return map[string]any{"partitions": list}, nil
}

// ═══════════════════════════════════════════════════════
// 阶段④ 差异处理（Pipeline: 合并 → Parallel(分类 ∥ 清单)）
// ═══════════════════════════════════════════════════════

// mergeDiffs 合并 N 个分片差异文件 → 完整差异工件（排序——设计文档 F4）。
func mergeDiffs(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	runID, _ := fc.Str("input.run_id")
	workDir, _ := fc.Str("input.work_dir")

	// glob 分片差异文件
	pattern := filepath.Join(workDir, runID+"-diff-*.txt")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	var all []string
	total := 0
	for _, fpath := range files {
		f, err := os.Open(fpath)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if len(sc.Text()) > 0 {
				all = append(all, sc.Text())
				total++
			}
		}
		f.Close()
	}
	// 按 trade_no 排序
	sort.Strings(all)

	out := filepath.Join(workDir, runID+"-diff-all.txt")
	if err := os.WriteFile(out, []byte(strings.Join(all, "\n")+"\n"), 0644); err != nil {
		return nil, err
	}
	return map[string]any{"merged_files": len(files), "total_diff_lines": total, "diff_all": out}, nil
}

// classifyDiffs 差异分类统计（读完整差异工件）。
func classifyDiffs(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	diffAll, _ := fc.Str("input.diff_all_path")
	f, err := os.Open(diffAll)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	mismatch, extOnly := 0, 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if len(fields) < 2 {
			continue
		}
		switch fields[1] {
		case "mismatch":
			mismatch++
		case "ext_only":
			extOnly++
		}
	}
	return map[string]any{"mismatch_count": mismatch, "ext_only_count": extOnly}, nil
}

// genChecklist 差错清单（读完整差异工件 → 格式化清单工件）。
func genChecklist(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	diffAll, _ := fc.Str("input.diff_all_path")
	runID, _ := fc.Str("input.run_id")
	workDir, _ := fc.Str("input.work_dir")

	f, err := os.Open(diffAll)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if len(sc.Text()) > 0 {
			lines = append(lines, sc.Text())
		}
	}
	out := filepath.Join(workDir, runID+"-checklist.txt")
	if err := os.WriteFile(out, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		return nil, err
	}
	return map[string]any{"checklist_lines": len(lines), "checklist": out}, nil
}

// ═══════════════════════════════════════════════════════
// 阶段⑤ 报告（Tasklet 汇总 + Workflow 渠道子报告——逃逸舱）
// ═══════════════════════════════════════════════════════

// buildReport 汇总全部阶段统计 → 报告工件。
func buildReport(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	runID, _ := fc.Str("input.run_id")
	workDir, _ := fc.Str("input.work_dir")

	sv, _ := fc.Output("核心对账")
	shard, _ := sv.(map[string]any)
	cv, _ := fc.Output("差异分类")
	cls, _ := cv.(map[string]any)
	ev, _ := fc.Output("外部统计")
	extStats, _ := ev.(map[string]any)
	iv, _ := fc.Output("内部统计")
	intStats, _ := iv.(map[string]any)

	matched := asInt(shard["filtered"]) // 一致 = 过滤数
	mismatch := asInt(cls["mismatch_count"])
	extOnly := asInt(cls["ext_only_count"])
	intCount := asInt(intStats["int_count"])
	intOnly := intCount - matched - mismatch // 内部多账 = 内部总数 − 匹配数（D6）

	report := fmt.Sprintf(
		"对账报告 date=%s\n外部流水: %d 笔 金额=%d\n内部流水: %d 笔 金额=%d\n"+
			"一致: %d\n金额不符: %d\n外部多账: %d\n内部多账: %d\n",
		mustStr(fc, "input.date"),
		asInt(extStats["ext_count"]), asInt(extStats["ext_amount"]),
		intCount, asInt(intStats["int_amount"]),
		matched, mismatch, extOnly, intOnly,
	)
	out := filepath.Join(workDir, runID+"-report.txt")
	if err := os.WriteFile(out, []byte(report), 0644); err != nil {
		return nil, err
	}
	return map[string]any{
		"report":     report,
		"matched":    matched,
		"mismatch":   mismatch,
		"ext_only":   extOnly,
		"int_only":   intOnly,
		"report_file": out,
	}, nil
}

// channelReportActivity 渠道子报告（普通 Activity 函数——手写 workflow 内部调用，需注册）。
func channelReportActivity(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	extWork, _ := fc.Str("input.ext_work_path")
	runID, _ := fc.Str("input.run_id")
	workDir, _ := fc.Str("input.work_dir")
	channel, _ := fc.Str("input.channel")

	f, err := os.Open(extWork)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	count, amount := 0, 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if len(fields) < 3 || fields[2] != channel {
			continue
		}
		amt, _ := strconv.Atoi(fields[1])
		count++
		amount += amt
	}
	out := filepath.Join(workDir, fmt.Sprintf("%s-report-%s.txt", runID, channel))
	if err := os.WriteFile(out, []byte(fmt.Sprintf("渠道 %s: %d 笔, 金额 %d\n", channel, count, amount)), 0644); err != nil {
		return nil, err
	}
	return map[string]any{"channel": channel, "count": count, "amount": amount}, nil
}

// channelReportWorkflow 渠道子报告（手写逃逸舱——循环渠道动态调度）。
// 渠道列表从 fc 读（阶段② 外部统计输出）；逐渠道 ExecuteActivity 生成子报告。
// ⚠️ 内部 ExecuteActivity 必须显式 ActivityOptions（超时）——否则永不调度。
func channelReportWorkflow(ctx workflow.Context, fc *batch.FlowCtx) (map[string]any, error) {
	ev, _ := fc.Output("外部统计")
	extStats, _ := ev.(map[string]any)
	chList := asStrings(extStats["channels"])
	results := map[string]any{}
	for _, ch := range chList {
		// 渠道注入快照（WorkflowFn 收的 fc 是 Child 入参副本——修改不影响父）
		fc.Input()["channel"] = ch
		actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Minute,
		})
		var out map[string]any
		if err := workflow.ExecuteActivity(actCtx, channelReportActivity, fc).Get(ctx, &out); err != nil {
			return nil, err
		}
		results[ch] = out
	}
	return results, nil
}

// ═══════════════════════════════════════════════════════
// 阶段⑥ 通知（Tasklet）
// ═══════════════════════════════════════════════════════

func notify(ctx context.Context, fc *batch.FlowCtx) (map[string]any, error) {
	rv, _ := fc.Output("对账报告")
	rep, _ := rv.(map[string]any)
	summary := fmt.Sprintf("对账完成: 一致 %d / 金额不符 %d / 外部多账 %d / 内部多账 %d",
		asInt(rep["matched"]), asInt(rep["mismatch"]), asInt(rep["ext_only"]), asInt(rep["int_only"]))
	slog.Info("通知", "summary", summary)
	return map[string]any{"summary": summary}, nil
}

// ═══════════════════════════════════════════════════════
// helper
// ═══════════════════════════════════════════════════════

func mustStr(fc *batch.FlowCtx, path string) string {
	s, _ := fc.Str(path)
	return s
}

func newConfig() *core.Config {
	cfg := core.NewConfig()
	cfg.Server.HostPort = "172.17.0.1:7233"
	cfg.Worker.TaskQueue = taskQueue
	return cfg
}

// ═══════════════════════════════════════════════════════
// 编排（设计文档 §4.1 Phase 树）
// ═══════════════════════════════════════════════════════

func buildFlow() *batch.Phase {
	// ═══ 阶段① 数据准备（Parallel: flow ∥ flow）═══
	prepare := batch.Parallel(
		batch.NewFlowPhase("内部导出", batch.NewTaskletPhase("导出内部", exportInternal, batch.WithActivityMaxAttempts(2))),
		batch.NewFlowPhase("外部加载", batch.NewTaskletPhase("加载外部", loadExternal, batch.WithActivityMaxAttempts(2))),
	)
	// ═══ 阶段② 预处理（Parallel: Tasklet ∥ Tasklet）═══
	stats := batch.Parallel(
		batch.NewTaskletPhase("内部统计", internalStats, batch.WithActivityMaxAttempts(2)),
		batch.NewTaskletPhase("外部统计", externalStats, batch.WithActivityMaxAttempts(2)),
	)
	// ═══ 阶段③ 核心对账（Shard: partition=IO Tasklet + handler=Chunk）═══
	recon := batch.NewShardPhase("核心对账",
		batch.NewTaskletPhase("拆坐标", splitExternal, batch.WithActivityMaxAttempts(2)),
		batch.NewChunkPhase("对账引擎",
			&reconReaderFactory{}, &reconProcessorFactory{}, &reconWriterFactory{},
			batch.WithActivityChunkSize(20), batch.WithActivityMaxAttempts(2),
		),
	)
	// ═══ 阶段④ 差异处理（Pipeline: 合并 → Parallel(分类 ∥ flow 清单)）═══
	diff := batch.Pipeline(
		batch.NewTaskletPhase("差异合并", mergeDiffs, batch.WithActivityMaxAttempts(2)),
		batch.Parallel(
			batch.NewTaskletPhase("差异分类", classifyDiffs, batch.WithActivityMaxAttempts(2)),
			batch.NewFlowPhase("差错清单", batch.NewTaskletPhase("生成清单", genChecklist, batch.WithActivityMaxAttempts(2))),
		),
	)
	// ═══ 阶段⑤ 报告（Tasklet 汇总 → Workflow 渠道子报告）═══
	report := batch.Pipeline(
		batch.NewTaskletPhase("对账报告", buildReport, batch.WithActivityMaxAttempts(2)),
		batch.NewWorkflowPhase("渠道子报告", channelReportWorkflow, batch.WithWorkflowMaxAttempts(2)),
	)
	// ═══ 阶段⑥ 通知 ═══
	notifyPhase := batch.NewTaskletPhase("通知", notify, batch.WithActivityMaxAttempts(2))

	return batch.Pipeline(prepare, stats, recon, diff, report, notifyPhase)
}

// ═══════════════════════════════════════════════════════
// 端到端验证（设计文档 §5 验收标准）
// ═══════════════════════════════════════════════════════

func TestReconciliation_FullFlow(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	job := batch.NewJob("recon", buildFlow())
	job.RegisterTo(wm)
	wm.RegisterActivity(channelReportActivity) // 手写 workflow 内部 Activity（树外——显式注册）

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	// ═══ 测试数据（100 级）═══
	workDir := fmt.Sprintf("../testdata/recon_%d", time.Now().UnixNano())
	require.NoError(t, os.MkdirAll(workDir, 0755))
	defer os.RemoveAll(workDir) // 清理必须在测试函数
	intRaw, extRaw := genReconData(t, workDir)
	runID := fmt.Sprintf("r%d", time.Now().UnixNano())

	params := map[string]any{
		"date":      "2026-08-20",
		"run_id":    runID,
		"work_dir":  workDir,
		"int_raw":   intRaw,
		"ext_raw":   extRaw,
		"int_work_path": filepath.Join(workDir, runID+"-internal.txt"),
		"ext_work_path": filepath.Join(workDir, runID+"-external.txt"),
		"diff_all_path": filepath.Join(workDir, runID+"-diff-all.txt"),
	}

	run, err := job.Start(context.Background(), facade, params)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, run.Get(context.Background(), &result))
	t.Log("══════════ 日终对账综合案例 ══════════")
	t.Logf("  WorkflowID: %s", run.GetID())
	for k, v := range result {
		t.Logf("  %s: %+v", k, v)
	}

	// ═══ A1: 对账正确性 ═══
	s, ok := result["核心对账"].(map[string]any)
	require.True(t, ok, "核心对账 应存在")
	require.Equal(t, float64(80), s["filtered"], "一致 80（过滤）")
	require.Equal(t, float64(8), s["mismatch"], "金额不符 8")
	require.Equal(t, float64(12), s["ext_only"], "外部多账 12")

	// ═══ A2: 分片完整覆盖（外部全量核对 = processed + filtered = 20 + 80 = 100）═══
	require.Equal(t, float64(20), s["processed"], "差异总数 20")
	require.Equal(t, 100, asIntAny(s["processed"])+asIntAny(s["filtered"]), "外部核对总数 100")

	// ═══ A5: 合并正确性 ═══
	m, ok := result["差异合并"].(map[string]any)
	require.True(t, ok, "差异合并 应存在")
	require.Equal(t, float64(4), m["merged_files"], "合并 4 个分片差异文件")
	require.Equal(t, float64(20), m["total_diff_lines"], "完整差异 20 行")

	// ═══ A4: 工件落盘 ═══
	for _, name := range []string{runID + "-internal.txt", runID + "-external.txt", runID + "-diff-all.txt", runID + "-checklist.txt", runID + "-report.txt"} {
		_, err := os.Stat(filepath.Join(workDir, name))
		require.NoError(t, err, "工件应存在: %s", name)
	}
	for i := 0; i < 4; i++ {
		_, err := os.Stat(filepath.Join(workDir, fmt.Sprintf("%s-diff-shard-%d.txt", runID, i)))
		require.NoError(t, err, "分片差异工件应存在: shard-%d", i)
	}

	// ═══ A8: 逃逸舱（渠道子报告）═══
	cr, ok := result["渠道子报告"].(map[string]any)
	require.True(t, ok, "渠道子报告 应存在")
	for _, ch := range channels {
		sub, ok := cr[ch].(map[string]any)
		require.True(t, ok, "渠道 %s 子报告应存在", ch)
		require.True(t, asInt(sub["count"]) > 0, "渠道 %s 笔数 > 0", ch)
		_, err := os.Stat(filepath.Join(workDir, fmt.Sprintf("%s-report-%s.txt", runID, ch)))
		require.NoError(t, err, "渠道子报告工件应存在: %s", ch)
	}

	// ═══ A1 补: 内部多账（报告反推——D6）═══
	rep, ok := result["对账报告"].(map[string]any)
	require.True(t, ok, "对账报告 应存在")
	require.Equal(t, float64(4), rep["int_only"], "内部多账 4")

	// ═══ A3: 金额一致性（引擎全量金额 ext_total = 外部统计金额——双路一致，含被过滤记录）═══
	extStats, ok := result["外部统计"].(map[string]any)
	require.True(t, ok, "外部统计 应存在")
	require.Equal(t, asIntAny(extStats["ext_amount"]), asIntAny(s["ext_total"]), "双路金额一致")

	// ═══ A6/A7: 阶段输出存在（并行/嵌套全部执行）═══
	for _, phase := range []string{"内部导出", "外部加载", "内部统计", "外部统计", "差异分类", "差错清单", "通知"} {
		_, ok := result[phase].(map[string]any)
		require.True(t, ok, "阶段输出应存在: %s", phase)
	}

	t.Log("  ✅ 综合案例通过：6 阶段全流程 + 对账正确性 + 分片/合并 + 逃逸舱 + 工件落盘")
}

// ═══════════════════════════════════════════════════════
// 负例：坏数据 → 明确失败（设计文档 A10，可选）
// ═══════════════════════════════════════════════════════

func TestReconciliation_BadExternalFile(t *testing.T) {
	facade, err := core.NewClientFacade(newConfig())
	require.NoError(t, err)
	defer facade.Close()

	wm, err := core.NewWorkerManager(facade, newConfig())
	require.NoError(t, err)

	job := batch.NewJob("recon-bad", buildFlow())
	job.RegisterTo(wm)
	wm.RegisterActivity(channelReportActivity)

	go func() { _ = wm.Start() }()
	defer wm.Stop()

	workDir := fmt.Sprintf("../testdata/recon_bad_%d", time.Now().UnixNano())
	require.NoError(t, os.MkdirAll(workDir, 0755))
	defer os.RemoveAll(workDir)

	// 外部文件含坏行（金额非数字）——loadExternal 计 bad_lines 不失败；
	// 用坏坐标触发：外部文件格式错乱导致拆分/对账失败——此处改为"外部文件不存在"（加载失败传播）
	runID := fmt.Sprintf("r%d", time.Now().UnixNano())
	intRaw := filepath.Join(workDir, "int_raw.txt")
	require.NoError(t, os.WriteFile(intRaw, []byte("T0001,A001,1000,2026-08-20,POS\n"), 0644))

	// 外部原始文件缺失 → 阶段① 外部加载失败 → 主 WF 失败
	run, err := job.Start(context.Background(), facade, map[string]any{
		"date":          "2026-08-20",
		"run_id":        runID,
		"work_dir":      workDir,
		"int_raw":       intRaw,
		"ext_raw":       filepath.Join(workDir, "ext_raw_missing.txt"),
		"int_work_path": filepath.Join(workDir, runID+"-internal.txt"),
		"ext_work_path": filepath.Join(workDir, runID+"-external.txt"),
		"diff_all_path": filepath.Join(workDir, runID+"-diff-all.txt"),
	})
	require.NoError(t, err)

	var result map[string]any
	err = run.Get(context.Background(), &result)
	require.Error(t, err, "外部文件缺失应导致失败")
	t.Logf("✅ 坏数据失败传播: %v", err)
}

func asIntAny(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	}
	return 0
}
