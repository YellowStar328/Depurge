package replay

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"depurge/internal/dataset"
	"depurge/internal/state"
)

// TestPreExecuteVsSerialRWSet 对比多个区块在串行执行与预执行下的读写集差异。
//
// 串行：交易共享状态（前一笔的提交影响后一笔，replayBlock）。
// 预执行：每笔交易从同一份 witness 初始状态独立执行（PreExecute）。
//
// 集合语义对比 FlatReadKeys/FlatWriteKeys（与 buildCompare 的 rwsets 口径一致），
// 仅输出读写集不一致的交易差异明细，并统计每个区块不一致的交易数与全局汇总。
// dataset 路径可用 -dataset 覆盖，默认使用仓库内 datasets/test-24000000-24000009；
// -blocks 可限定区块范围（如 24000001-24000003），空则全部。
//# 默认全部区块
// go test ./internal/replay/ -run TestPreExecuteVsSerialRWSet -v

// # 指定 dataset 与区块范围
// go test ./internal/replay/ -run TestPreExecuteVsSerialRWSet -v \
//   -args -dataset /path/to/dataset -blocks 24000001-24000003

const maxKeysShown = 10 // 每笔差异交易最多展示的 key 数，避免输出爆炸

var (
	flagDataset = flag.String("dataset", filepath.Join("..", "..", "datasets", "test-24000000-24000009"), "dataset 目录路径")
	flagBlocks  = flag.String("blocks", "", "限定区块范围，如 24000001-24000003 或 24000001，空则全部")
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestPreExecuteVsSerialRWSet(t *testing.T) {
	datasetDir := *flagDataset
	blockFilter := *flagBlocks

	loader, err := dataset.NewLoader(datasetDir)
	if err != nil {
		t.Skipf("dataset not available: %v", err)
	}
	blockNums, err := loader.BlockList(blockFilter)
	if err != nil {
		t.Fatalf("list blocks: %v", err)
	}
	if len(blockNums) == 0 {
		t.Skipf("no blocks found in %s", datasetDir)
	}

	cfg := DefaultConfig()
	cfg.StateConfig = state.DefaultConfig()
	r := NewReplayer(loader, cfg)

	var (
		loadedBlocks int
		totalTxs     int
		totalDiffers int
		perBlock     []string
	)
	for _, blockNum := range blockNums {
		blk, err := loader.LoadBlock(blockNum)
		if err != nil {
			t.Logf("block %d not available, skip: %v", blockNum, err)
			continue
		}
		loadedBlocks++

		serial := r.replayBlock(blk)
		pre := r.PreExecute(blk)
		if len(serial) != len(pre) {
			t.Fatalf("block %d tx count mismatch: serial=%d pre=%d", blockNum, len(serial), len(pre))
		}
		t.Logf("block %d: %d txs, witness accounts=%d", blockNum, len(blk.Transactions), len(blk.Witness.Accounts))

		differs := 0
		for i := range serial {
			s, p := &serial[i], &pre[i]

			readDiff := diffKeySets(s.FlatReadKeys, p.FlatReadKeys)
			writeDiff := diffKeySets(s.FlatWriteKeys, p.FlatWriteKeys)
			hasReadDiff := len(readDiff.serialOnly) > 0 || len(readDiff.preOnly) > 0
			hasWriteDiff := len(writeDiff.serialOnly) > 0 || len(writeDiff.preOnly) > 0
			if !hasReadDiff && !hasWriteDiff {
				continue
			}
			differs++

			t.Logf("=== tx #%d %s : RW-set differs ===", i, s.TxHash)
			if hasReadDiff {
				logKeyDiff(t, "read ", s.FlatReadKeys, p.FlatReadKeys, readDiff, maxKeysShown)
			}
			if hasWriteDiff {
				logKeyDiff(t, "write", s.FlatWriteKeys, p.FlatWriteKeys, writeDiff, maxKeysShown)
			}
		}

		t.Logf("--- block %d: total=%d | rw-set identical=%d | differs=%d ---",
			blockNum, len(serial), len(serial)-differs, differs)
		perBlock = append(perBlock, fmt.Sprintf("block %d: differs=%d/%d (%.1f%%)",
			blockNum, differs, len(serial), pct(differs, len(serial))))
		totalTxs += len(serial)
		totalDiffers += differs
	}

	// 全局汇总：每个区块读写集不一致的交易数
	t.Logf("========== summary: %d blocks | total=%d txs | rw-set differs=%d (%.1f%%) ==========",
		loadedBlocks, totalTxs, totalDiffers, pct(totalDiffers, totalTxs))
	for _, line := range perBlock {
		t.Logf("    %s", line)
	}
}

func pct(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b) * 100
}

// keySetDiff 两个扁平 key 列表的集合差异。
type keySetDiff struct {
	serialOnly []string // 仅串行执行读到/写到（预执行没有）
	preOnly    []string // 仅预执行读到/写到（串行没有）
}

func diffKeySets(serial, pre []string) keySetDiff {
	sset := make(map[string]struct{}, len(serial))
	for _, k := range serial {
		sset[k] = struct{}{}
	}
	pset := make(map[string]struct{}, len(pre))
	for _, k := range pre {
		pset[k] = struct{}{}
	}
	var d keySetDiff
	for _, k := range serial {
		if _, ok := pset[k]; !ok {
			d.serialOnly = append(d.serialOnly, k)
		}
	}
	for _, k := range pre {
		if _, ok := sset[k]; !ok {
			d.preOnly = append(d.preOnly, k)
		}
	}
	sort.Strings(d.serialOnly)
	sort.Strings(d.preOnly)
	return d
}

func logKeyDiff(t *testing.T, kind string, serial, pre []string, d keySetDiff, maxShown int) {
	t.Logf("    %s set: serial=%d keys pre=%d keys | serial-only=%d pre-only=%d",
		kind, len(serial), len(pre), len(d.serialOnly), len(d.preOnly))
	if len(d.serialOnly) > 0 {
		t.Logf("      serial-only: %s%s", previewKeys(d.serialOnly, maxShown), ellipsis(len(d.serialOnly), maxShown))
	}
	if len(d.preOnly) > 0 {
		t.Logf("      pre-only   : %s%s", previewKeys(d.preOnly, maxShown), ellipsis(len(d.preOnly), maxShown))
	}
}

func previewKeys(keys []string, n int) string {
	if len(keys) > n {
		keys = keys[:n]
	}
	return strings.Join(keys, ", ")
}

func ellipsis(total, shown int) string {
	if total > shown {
		return " ..."
	}
	return ""
}
