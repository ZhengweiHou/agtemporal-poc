package batch

import (
	"testing"
)

// TestPartitionContract 验证 partition 输出契约的封装/提取对称（NewPartitionerPhase 内部链路）。
func TestPartitionContract(t *testing.T) {
	parts := []Partition{
		{Name: "shard-0", Data: map[string]any{"start": 0, "line_count": 2, "file_path": "x.txt"}},
		{Name: "shard-1", Data: map[string]any{"start": 2, "line_count": 2, "file_path": "x.txt"}},
		{Name: "", Data: map[string]any{"start": 4, "line_count": 1, "file_path": "x.txt"}}, // 空名——runShard 派生
	}

	out := partitionListToMap(parts)
	got, err := extractPartitions(out)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].Name != "shard-0" || asIntAny(got[0].Data["start"]) != 0 {
		t.Fatalf("part[0] mismatch: %+v", got[0])
	}
	if got[1].Name != "shard-1" || asIntAny(got[1].Data["line_count"]) != 2 {
		t.Fatalf("part[1] mismatch: %+v", got[1])
	}
	if got[2].Name != "" {
		t.Fatalf("part[2] name should stay empty (runShard derives): %+v", got[2])
	}
}

// TestPartitionContract_MissingKey 契约缺失 partitions key → 明确报错。
func TestPartitionContract_MissingKey(t *testing.T) {
	_, err := extractPartitions(map[string]any{"foo": 1})
	if err == nil {
		t.Fatal("expected error for missing partitions key")
	}
}
