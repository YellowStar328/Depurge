package replay

import (
	"testing"
	"time"

	"depurge/internal/dataset"
	"depurge/internal/state"
	"depurge/internal/vegeta"
)

// TestVegetaBlockEndToEnd 端到端运行 Vegeta 管线并校验守恒关系与耗时明细。
//
// 守恒不变量：
//   - Committed + SerialTotal == TxCount（每笔交易要么并行验证提交，要么进串行兜底）；
//   - PreExecFailed + EmptyRWSet + Aborted + Cascaded + Committed + Degraded == TxCount；
//   - Degraded == 0（波次死锁防御不应触发）。
//
// dataset 路径可用 -dataset 覆盖，默认使用仓库内 datasets/test-24000000-24000009；
// -blocks 可限定区块范围，空则全部。
//
//	go test ./internal/replay/ -run TestVegetaBlockEndToEnd -v
//	go test ./internal/replay/ -run TestVegetaBlockEndToEnd -v \
//	  -args -dataset /path/to/dataset -blocks 24000001-24000003
func TestVegetaBlockEndToEnd(t *testing.T) {
	loader, blockNums := loadVegetaBlocks(t)
	if len(blockNums) == 0 {
		t.Skip("no blocks available")
	}

	cfg := DefaultConfig() // Runs=1, RecordRW=true
	cfg.StateConfig = state.DefaultConfig()
	r := NewReplayer(loader, cfg)
	vcfg := DefaultVegetaConfig()
	vcfg.Parallelism = 4 // 固定并发度，避免 CI 机器核数差异影响复现

	for _, blockNum := range blockNums {
		blk, err := loader.LoadBlock(blockNum)
		if err != nil {
			t.Logf("block %d not available, skip: %v", blockNum, err)
			continue
		}
		res, err := r.RunVegetaBlock(blk, vcfg)
		if err != nil {
			t.Fatalf("block %d RunVegetaBlock: %v", blockNum, err)
		}

		if got := res.Committed + res.SerialTotal; got != res.TxCount {
			t.Errorf("block %d: committed(%d)+serial(%d) != txs(%d)",
				blockNum, res.Committed, res.SerialTotal, res.TxCount)
		}
		if got := res.PreExecFailed + res.EmptyRWSet + res.Aborted + res.Cascaded + res.Committed + res.Degraded; got != res.TxCount {
			t.Errorf("block %d: outcome partition sums to %d, want %d", blockNum, got, res.TxCount)
		}
		if res.Degraded != 0 {
			t.Errorf("block %d: deadlock guard triggered: %s", blockNum, res.Warning)
		}

		t.Logf("block %d: %d txs | waves=%d(max %d) | parallel=%d aborted=%d(+%d) serial=%d | "+
			"pre=%s order=%s dag=%s par=%s(clone %s, merge %s) ser=%s | algo=%s incl-pre=%s | "+
			"baseline=%s speedup=%.2f | state-diff=%d",
			blockNum, res.TxCount, res.Waves, res.MaxWaveSize,
			res.Committed, res.Aborted, res.Cascaded, res.SerialTotal,
			nsDur(res.PreExecWallNs), nsDur(res.OrderNs), nsDur(res.DagNs),
			nsDur(res.ParallelWallNs), nsDur(res.CloneNs), nsDur(res.MergeNs),
			nsDur(res.SerialWallNs), nsDur(res.TotalAlgoNs), nsDur(res.TotalInclPreNs),
			nsDur(res.BaselineWallNs), res.Speedup, res.StateDiffKeys)
		if res.Warning != "" {
			t.Logf("  WARNING: %s", res.Warning)
		}
		for _, s := range res.FailSamples {
			t.Logf("  abort: %s", s)
		}
		for _, s := range res.StateDiffSample {
			t.Logf("  diff: %s", s)
		}
	}
}

// TestVegetaStateEquivalentOriginalOrder 验证 --edge-order original +
// --serial-order block（原始区块序定边 + 区块序兜底）口径下的状态等价性。
//
// 理论：原始序定边时，冲突对按区块序定向，并行提交的最终状态与链上串行
// 严格等价（写集两两不相交的批次，合并为并集）。
//
// 已知算法局限：预执行失败/空读写集的交易不进 DAG（冲突未知），它们被推迟
// 到最后的串行兜底段；但它们**之前**的并行提交交易可能在 veg 侧提交了与
// 链上不同的值（predicted 集来自 base 状态、不含失败交易影响 → 验证通过）。
// 故：无预执行失败的区块断言 diff==0；有失败的区块仅报告 diff（算法固有，
// 非回归）。
func TestVegetaStateEquivalentOriginalOrder(t *testing.T) {
	loader, blockNums := loadVegetaBlocks(t)
	if len(blockNums) == 0 {
		t.Skip("no blocks available")
	}
	if len(blockNums) > 3 {
		blockNums = blockNums[:3]
	}

	cfg := DefaultConfig()
	cfg.StateConfig = state.DefaultConfig()
	r := NewReplayer(loader, cfg)
	vcfg := DefaultVegetaConfig()
	vcfg.Parallelism = 4
	vcfg.EdgeOrder = vegeta.EdgeOrderOriginal
	vcfg.SerialOrder = vegeta.SerialOrderBlock

	for _, blockNum := range blockNums {
		blk, err := loader.LoadBlock(blockNum)
		if err != nil {
			continue
		}
		res, err := r.RunVegetaBlock(blk, vcfg)
		if err != nil {
			t.Fatalf("block %d RunVegetaBlock: %v", blockNum, err)
		}
		failedDirect := res.PreExecFailed + res.EmptyRWSet
		if res.StateDiffKeys != 0 {
			if failedDirect == 0 {
				// 无预执行失败却出现 diff → 真实回归
				t.Errorf("block %d: state diff = %d keys with 0 pre-exec-failed "+
					"(expect 0 under edge-order=original+serial-order=block)",
					blockNum, res.StateDiffKeys)
				for _, s := range res.StateDiffSample {
					t.Errorf("  %s", s)
				}
			} else {
				// 预执行失败交易的固有泄漏：仅报告，不断言
				t.Logf("block %d: state diff = %d keys (algorithmic leakage from "+
					"%d pre-exec-failed/empty txs; parallel=%d serial=%d)",
					blockNum, res.StateDiffKeys, failedDirect,
					res.Committed, res.SerialTotal)
				for _, s := range res.StateDiffSample {
					t.Logf("  %s", s)
				}
			}
		} else {
			t.Logf("block %d: state equivalent ✓ (parallel=%d serial=%d failed=%d)",
				blockNum, res.Committed, res.SerialTotal, failedDirect)
		}
	}
}

// loadVegetaBlocks 加载 dataset 与区块列表（复用 preexec_test.go 的 flag）。
func loadVegetaBlocks(t *testing.T) (*dataset.Loader, []uint64) {
	t.Helper()
	loader, err := dataset.NewLoader(*flagDataset)
	if err != nil {
		t.Skipf("dataset not available: %v", err)
	}
	blockNums, err := loader.BlockList(*flagBlocks)
	if err != nil {
		t.Fatalf("list blocks: %v", err)
	}
	return loader, blockNums
}

// nsDur 纳秒数值转 time.Duration（日志格式化用）。
func nsDur(ns int64) time.Duration { return time.Duration(ns) }
