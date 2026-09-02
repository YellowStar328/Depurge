// branchflip.go —— 分支翻转探测器：找出「预执行走一个分支、串行累积后走另一个分支」的交易。
//
// 方法（确定性，不依赖调度噪声）：
//   - 预执行读写集：每笔交易基于区块初始状态（witness 锚点）独立执行，
//     交易互不干扰 —— 即「预执行时走到的分支」；
//   - 串行真实读写集：按区块原始顺序执行，每笔基于前面交易累积后的状态
//     —— 即「有前面交易状态累计后实际走到的分支」；
//   - 对每笔交易算双向差异：
//       miss  = real ∖ pre（真实分支多摸的 key：真实走了、预执行没走的分支）
//       extra = pre ∖ real（预执行分支多摸的 key：预执行走了、真实没走的分支）
//     两者都非空 = 分支翻转的签名（预执行与真实落到了不同分支）。
//
// 这与 abort 的关系：vegeta/depurge 的验证是 real ⊆ spec（spec=预执行读写集）。
// miss 非空即验证失败 → abort。extra 非空进一步说明不是「漏了个动态槽」，
// 而是整条分支换了（预执行摸的 key 真实根本不摸）。
//
// 本探测器直接对比预执行与串行两个确定性口径，定位「哪些合约/交易」存在分支翻转，
// 供人工核对合约源码中的分支结构。

package replay

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/vm"

	"depurge/internal/dataset"
	"depurge/internal/llmsoundness"
	"depurge/internal/state"
	"depurge/internal/vegeta"
)

// BranchFlipConfig 驱动分支翻转探测。
type BranchFlipConfig struct {
	Parallelism    int    // 预执行并行度（<=0 = NumCPU）
	FilterNonce    bool   // 过滤 nonce 伪冲突键
	FilterCoinbase bool   // 过滤 coinbase balance 热点键
	LLMDir         string // LLM 静态分析目录（可选，用于合约名/源码定位）
	Contract       string // 只看指定合约（0x 地址）；空 = 全部
	MinTotal       int    // 上报阈值：miss+extra 总 key 数 >= 该值
	Top            int    // 按合约聚合表的展示条数
}

// branchFlipTx 一笔分支翻转交易。
type branchFlipTx struct {
	block uint64
	idx   int
	to    string
	name  string
	miss  []string // real ∖ pre：真实分支多摸的 key
	extra []string // pre ∖ real：预执行分支多摸的 key
}

// branchFlipContract 按合约聚合。
type branchFlipContract struct {
	addr     string
	name     string
	flips    int // 分支翻转交易数
	missKeys int // 累计 miss key 数
	cracked  int // miss 中反推出交易可见地址的（转账对手方形态）
}

// RunBranchFlip 探测并输出分支翻转交易。
func (r *Replayer) RunBranchFlip(cfg BranchFlipConfig, blockRange string, w io.Writer) error {
	if cfg.MinTotal < 1 {
		cfg.MinTotal = 1
	}
	if cfg.Top < 1 {
		cfg.Top = 30
	}
	focusOn := ""
	if cfg.Contract != "" {
		focusOn = strings.ToLower(cfg.Contract)
	}

	var contracts map[common.Address]*llmsoundness.Contract
	if cfg.LLMDir != "" {
		var err error
		if contracts, err = llmsoundness.LoadContracts(cfg.LLMDir); err != nil {
			return err
		}
	}

	parallelism := cfg.Parallelism
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}

	var (
		totalTxs   int
		bothOKTxs  int
		flips      []branchFlipTx
		byContract = make(map[string]*branchFlipContract)
	)

	err := r.loader.ForEachBlock(blockRange, func(blk *dataset.BlockData) error {
		blockCtx := BuildBlockContext(&blk.Header, r.chainConfig)
		opts := vegeta.Options{
			FilterNonce:    cfg.FilterNonce,
			FilterCoinbase: cfg.FilterCoinbase,
			Coinbase:       blockCtx.Coinbase.String(),
		}
		n := len(blk.Transactions)
		if n == 0 {
			return nil
		}

		// 1. 预执行：每笔基于初始状态，得到「预执行分支」读写集。
		base := state.NewMemoryStateDBWithTrie(false)
		loadWitness(base, &blk.Witness)
		pre, _, _ := r.vegetaPreExecute(blk, base, blockCtx, parallelism, opts)

		// 2. 串行重放：按原始顺序、状态累积，得到「真实分支」读写集。
		realKeys, realOK := r.serialRWSet(blk, blockCtx, opts)

		// 3. 逐笔双向差异。
		for i := range blk.Transactions {
			totalTxs++
			tx := &blk.Transactions[i]
			if pre[i].failed || !realOK[i] {
				continue
			}
			bothOKTxs++

			// 读∪写合并后去重（同一槽既读又写只算一个 key）。
			preKeys := vegeta.FilterKeys(
				append(append([]string{}, pre[i].filteredReads...), pre[i].filteredWrites...), opts)
			miss := vegeta.SubsetOf(realKeys[i], preKeys)  // real ∖ pre
			extra := vegeta.SubsetOf(preKeys, realKeys[i]) // pre ∖ real
			if len(miss) == 0 || len(extra) == 0 {
				continue
			}
			if len(miss)+len(extra) < cfg.MinTotal {
				continue
			}
			if focusOn != "" && strings.ToLower(tx.To) != focusOn {
				continue
			}

			name := ""
			if tx.To != "" && contracts != nil {
				if c := contracts[common.HexToAddress(tx.To)]; c != nil {
					name = c.Meta.ContractName
				}
			}
			flips = append(flips, branchFlipTx{
				block: blk.BlockNumber(), idx: i, to: tx.To, name: name,
				miss: miss, extra: extra,
			})

			c := byContract[strings.ToLower(tx.To)]
			if c == nil {
				c = &branchFlipContract{addr: tx.To, name: name}
				byContract[strings.ToLower(tx.To)] = c
			}
			c.flips++
			c.missKeys += len(miss)
			for _, k := range miss {
				if specDiffMissClass(k) != "storage" {
					continue
				}
				if _, slot, ok := specDiffParseStorageKey(k); !ok {
					continue
				} else if _, _, _, ok2 := specDiffCrackSlot(slot, tx); ok2 {
					c.cracked++
				}
			}
		}

		fmt.Printf("block %d: branchflip scan done (%d flips so far)\n", blk.BlockNumber(), len(flips))
		return nil
	})
	if err != nil {
		return err
	}

	writeBranchFlipReport(w, cfg, totalTxs, bothOKTxs, flips, byContract)
	return nil
}

// serialRWSet 按原始顺序串行重放，返回每笔交易的过滤后读写集（读∪写）与成功标记。
// 与预执行口径对齐：同一 executeTx 路径、同一 recorder 配置，仅状态锚点不同
// （串行=前面交易累积后的状态）。只统计「执行成功」的交易（失败交易读写集无分支语义）。
func (r *Replayer) serialRWSet(blk *dataset.BlockData, blockCtx vm.BlockContext, opts vegeta.Options) ([][]string, []bool) {
	n := len(blk.Transactions)
	keys := make([][]string, n)
	ok := make([]bool, n)

	db := state.NewMemoryStateDB()
	loadWitness(db, &blk.Witness)
	gp := new(core.GasPool).AddGas(uint64(blk.Header.GasLimit))

	for i := range blk.Transactions {
		out := r.executeTx(db, blockCtx, gp, &blk.Transactions[i], i, false)
		db.Finalise(true) // 串行语义：前一笔的状态提交，供后一笔累积依赖

		if out.msgErr != nil || out.execErr != nil || out.result == nil ||
			out.result.Failed() || out.recorder == nil {
			continue
		}
		rec := out.recorder
		rec.SetRootResult(out.result.UsedGas, false, "")
		rec.Freeze()
		real := append(append([]string{}, rec.FlatReadKeys()...), rec.FlatWriteKeys()...)
		keys[i] = vegeta.FilterKeys(real, opts)
		ok[i] = true
	}
	return keys, ok
}

func writeBranchFlipReport(w io.Writer, cfg BranchFlipConfig, totalTxs, bothOKTxs int,
	flips []branchFlipTx, byContract map[string]*branchFlipContract) {

	sep := strings.Repeat("=", 51)
	dash := strings.Repeat("-", 67)

	fmt.Fprintln(w, sep)
	fmt.Fprintln(w, ">>> BranchFlip: pre-exec branch vs serial-accumulated branch <<<")
	fmt.Fprintln(w, sep)
	fmt.Fprintln(w, "method: 每笔交易对比 预执行读写集(初始状态) vs 串行读写集(前序累积状态)")
	fmt.Fprintln(w, "  flip = miss(real∖pre) 与 extra(pre∖real) 均非空：预执行与真实落到不同分支")
	fmt.Fprintf(w, "txs=%d both-succeeded=%d branch-flips=%d\n", totalTxs, bothOKTxs, len(flips))

	// 按合约聚合。
	rows := make([]*branchFlipContract, 0, len(byContract))
	for _, c := range byContract {
		rows = append(rows, c)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].flips != rows[j].flips {
			return rows[i].flips > rows[j].flips
		}
		return rows[i].addr < rows[j].addr
	})
	limit := cfg.Top
	if limit > len(rows) {
		limit = len(rows)
	}
	fmt.Fprintln(w, dash)
	fmt.Fprintf(w, "contracts with branch flips (top %d by flip count):\n", limit)
	fmt.Fprintf(w, "%-44s %-20s %6s %8s %8s\n", "contract", "name", "flips", "missKeys", "cracked")
	for _, c := range rows[:limit] {
		name := c.name
		if name == "" {
			name = "(no LLM analysis)"
		}
		fmt.Fprintf(w, "%-44s %-20s %6d %8d %8d\n", c.addr, name, c.flips, c.missKeys, c.cracked)
	}

	// 逐笔明细（限制条数，防长尾爆量）。
	fmt.Fprintln(w, dash)
	const detailCap = 60
	fmt.Fprintf(w, "branch-flip transactions (first %d of %d):\n", minInt(detailCap, len(flips)), len(flips))
	for _, f := range flips[:minInt(detailCap, len(flips))] {
		name := f.name
		if name == "" {
			name = "(no LLM analysis)"
		}
		fmt.Fprintf(w, "blk=%d tx#%d to=%s %s\n", f.block, f.idx, f.to, name)
		fmt.Fprintf(w, "    pre-exec branch (extra, %d keys): pre-exec 走了、真实没走的分支\n", len(f.extra))
		for _, k := range f.extra {
			fmt.Fprintf(w, "        %s\n", k)
		}
		fmt.Fprintf(w, "    real branch (miss, %d keys): 真实走了、预执行没走的分支\n", len(f.miss))
		for _, k := range f.miss {
			fmt.Fprintf(w, "        %s\n", k)
		}
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
