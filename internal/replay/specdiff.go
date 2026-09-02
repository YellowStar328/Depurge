// specdiff：A 臂（LLM 保守集）vs C 臂（预执行保守集）的逐合约 abort 对比。
//
// 目的：筛选「换用 A 臂后 abort 次数能降低」的合约（LLM 保守集质量优于
// 预执行读写集的合约），为按合约混合选择 spec 来源提供依据。
//
// 方法（动态双臂重放，真实管线）：
//   - 每个区块用同一份预执行/调度代码路径（runDepurgeBlockOnce）分别跑
//     C 臂与 A 臂，记录每笔交易的 Scheduled/Aborted 结局（TxDetails）；
//   - abort 判定即管线 step3 的真实验证（spec⊇real 超集校验 + 重执行），
//     不是静态近似——包含调度/状态可见性的全部真实动力学；
//   - 按 tx.To 归因到合约：delta = abortA − abortC < 0 即「用 A 臂后该合约
//     相关交易的 abort 次数降低」；
//   - SkipBaseline=true：跳过串行基线与 state-diff（不影响 abort 统计）。
//
// 注意：两臂各自独立重放（各自从 witness 重建基准状态），调度并行度相同；
// 高并行度下个别交易的 abort 可能受调度时序影响，边界合约可复跑确认。
package replay

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"depurge/internal/dataset"
	depurge "depurge/internal/depurge"
	"depurge/internal/llmsoundness"
)

// SpecDiffConfig 是 specdiff 分析的配置。
type SpecDiffConfig struct {
	Parallelism    int    // 预执行/并行执行并行度（两臂相同，控制变量）
	FilterNonce    bool   // 过滤 nonce 伪冲突键
	FilterCoinbase bool   // 过滤 coinbase balance 热点键
	LLMDir         string // LLM 静态分析目录（必需）
	Arm            string // 第二臂：A=LLM 优先+回退；B=并集（默认 A）
	MinTx          int    // 逐合约输出的最低交易数阈值
	Top            int    // 第二臂更差表的展示条数
	All            bool   // 输出全部合约（含 delta=0）
}

// specDiffRescue 一笔「C 臂 abort 而第二臂不 abort」的被救回交易样本。
type specDiffRescue struct {
	block uint64
	idx   int
	to    string
	name  string
	llmOK bool     // 第二臂该笔 LLM 实例化成功（B 臂=并集含 LLM 键；A 臂=用 LLM 集）
	miss  []string // C 臂缺失 key（带槽反推标注）
}

const specDiffRescueSampleCap = 40

// specDiffContract 单合约（按 tx.To 归属）的两臂 abort 统计。
type specDiffContract struct {
	address common.Address
	name    string
	hasLLM  bool
	txs     int // 指向该合约的交易总数
	llmOK   int // LLM 实例化成功（A 臂实际用 LLM 集）的交易数
	schedC  int // C 臂进入并行调度的笔数
	schedA  int // A 臂进入并行调度的笔数
	cmtC    int // C 臂并行段提交数（真正的并行收益）
	cmtA    int // A 臂并行段提交数
	abortC  int // C 臂并行段 abort 数
	abortA  int // A 臂并行段 abort 数
	saves   int // C abort 而 A 不 abort
	hurts   int // A abort 而 C 不 abort
}

func (c *specDiffContract) delta() int { return c.abortA - c.abortC }

// dCmt 提交数净变化：>0 表示换臂后真正走并行提交成功的交易变多。
func (c *specDiffContract) dCmt() int { return c.cmtA - c.cmtC }

// specDiffTotals 全局汇总计数。
//
// 注：schedA/abortA 等 "A" 后缀字段指「第二臂」（A 或 B，由 cfg.Arm 决定）。
type specDiffTotals struct {
	blocks   int
	txs      int
	creation int // 合约创建（To 为空）：无 LLM 分析，两臂 spec 恒同
	schedC   int
	schedA   int
	cmtC     int // 并行段提交数（真正吃到并行收益的交易）
	cmtA     int
	abortC   int
	abortA   int
	saves    int
	hurts    int
	// 第二臂「新拉进并行调度」的交易（C 臂 spec 过滤后为空 → 直接串行，
	// 第二臂 spec 非空 → 入队）及其结局分解：这部分是换臂的净增量来源。
	newSched      int // da.Scheduled && !dc.Scheduled
	newSchedAbort int // 其中 abort（白付一次并行执行 + 仍回落串行）
	newSchedCmt   int // 其中提交成功（真正的增量收益）
	llmTried      int
	llmOK         int
	llmFail       map[string]int

	// 被救回交易（C abort 而第二臂不 abort）的分析：
	rescueCount int              // 被救回交易总数（= saves 的交易口径）
	rescueLLM   int              // 其中第二臂 LLM 分析成功者（LLM 键补上缺口）
	missHist    map[string]int   // C 臂缺失 key 类型直方图
	rescues     []specDiffRescue // 样本（上限 specDiffRescueSampleCap）

	// 全部 C 臂 abort 的缺失 key 分解（回答「abort 由什么类型的 key 引起」）：
	cAborted       int            // C 臂 abort 交易数（带 miss key 的）
	cMissClass     map[string]int // 全部 C abort 缺失 key 的类型直方图
	cMissCracked   int            // storage 缺失键反推出「交易可见地址 mapping 项」的个数
	cMissUncracked int            // storage 缺失键反推不出的个数（动态键/硬编码对手方）
	cMissRescued   int            // 落在「被第二臂救回」交易上的缺失 key 个数
}

// RunSpecDiff 对每个区块分别重放 C 臂与 A 臂（真实管线），按合约归因
// abort 差异，报告写 w。
func (r *Replayer) RunSpecDiff(cfg SpecDiffConfig, blockRange string, w io.Writer) error {
	if err := r.depurgePreconditions(); err != nil {
		return err
	}
	if cfg.LLMDir == "" {
		return fmt.Errorf("specdiff 需要 --llm-dir")
	}
	contracts, err := llmsoundness.LoadContracts(cfg.LLMDir)
	if err != nil {
		return fmt.Errorf("加载 LLM 分析数据: %w", err)
	}
	if len(contracts) == 0 {
		return fmt.Errorf("%s 下无可加载的 LLM 分析合约", cfg.LLMDir)
	}
	if cfg.Parallelism < 1 {
		cfg.Parallelism = 1
	}
	if cfg.MinTx < 1 {
		cfg.MinTx = 1
	}
	if cfg.Top < 1 {
		cfg.Top = 20
	}
	cfg.Arm = strings.ToUpper(strings.TrimSpace(cfg.Arm))
	if cfg.Arm == "" {
		cfg.Arm = string(depurge.ArmA)
	}
	if cfg.Arm != string(depurge.ArmA) && cfg.Arm != string(depurge.ArmB) {
		return fmt.Errorf("specdiff 第二臂必须是 A 或 B（got %q）", cfg.Arm)
	}

	baseCfg := DepurgeConfig{
		Parallelism:    cfg.Parallelism,
		FilterNonce:    cfg.FilterNonce,
		FilterCoinbase: cfg.FilterCoinbase,
		LLMDir:         cfg.LLMDir,
		SkipBaseline:   true, // 分析模式：跳过基线/state-diff，不影响 abort 统计
	}
	cfgC := baseCfg
	cfgC.Arm = string(depurge.ArmC)
	cfgA := baseCfg
	cfgA.Arm = cfg.Arm
	opts := baseCfg.filterOpts()

	rows := make(map[common.Address]*specDiffContract)
	var t specDiffTotals
	t.llmFail = make(map[string]int)
	t.missHist = make(map[string]int)
	t.cMissClass = make(map[string]int)

	err = r.loader.ForEachBlock(blockRange, func(blk *dataset.BlockData) error {
		resC, err := r.runDepurgeBlockOnce(blk, cfgC, contracts, opts)
		if err != nil {
			return err
		}
		resA, err := r.runDepurgeBlockOnce(blk, cfgA, contracts, opts)
		if err != nil {
			return err
		}

		for k, v := range resA.LLMFail {
			t.llmFail[k] += v
		}
		t.llmTried += resA.LLMTried
		t.llmOK += resA.LLMOK

		for i := range blk.Transactions {
			tx := &blk.Transactions[i]
			dc := resC.TxDetails[i]
			da := resA.TxDetails[i]
			t.txs++
			if dc.Scheduled {
				t.schedC++
			}
			if da.Scheduled {
				t.schedA++
			}
			if dc.Committed {
				t.cmtC++
			}
			if da.Committed {
				t.cmtA++
			}
			if dc.Aborted {
				t.abortC++
				if len(dc.Miss) > 0 {
					specDiffCollectCMiss(&t, tx, dc, da)
				}
			}
			if da.Aborted {
				t.abortA++
			}
			if dc.Aborted && !da.Aborted {
				t.saves++
				specDiffCollectRescue(&t, blk, tx, i, dc, da, contracts)
			}
			if da.Aborted && !dc.Aborted {
				t.hurts++
			}
			if da.Scheduled && !dc.Scheduled {
				t.newSched++
				switch {
				case da.Aborted:
					t.newSchedAbort++
				case da.Committed:
					t.newSchedCmt++
				}
			}
			if tx.To == "" {
				t.creation++
				continue
			}

			to := common.HexToAddress(tx.To)
			row := rows[to]
			if row == nil {
				row = &specDiffContract{address: to}
				if c := contracts[to]; c != nil {
					row.hasLLM = true
					row.name = c.Meta.ContractName
				}
				rows[to] = row
			}
			row.txs++
			if da.LLMOK {
				row.llmOK++
			}
			if dc.Scheduled {
				row.schedC++
			}
			if da.Scheduled {
				row.schedA++
			}
			if dc.Committed {
				row.cmtC++
			}
			if da.Committed {
				row.cmtA++
			}
			if dc.Aborted {
				row.abortC++
			}
			if da.Aborted {
				row.abortA++
			}
			switch {
			case dc.Aborted && !da.Aborted:
				row.saves++
			case da.Aborted && !dc.Aborted:
				row.hurts++
			}
		}
		t.blocks++
		fmt.Printf("block %d: specdiff done (C aborted=%d %s aborted=%d)\n",
			blk.BlockNumber(), resC.Aborted, cfg.Arm, resA.Aborted)
		return nil
	})
	if err != nil {
		return err
	}
	if t.blocks == 0 {
		fmt.Fprintln(w, "no blocks analyzed")
		return nil
	}
	specDiffWriteReport(w, cfg, rows, &t)
	return nil
}

// specDiffWriteReport 输出汇总 + 逐合约表。
func specDiffWriteReport(w io.Writer, cfg SpecDiffConfig,
	rows map[common.Address]*specDiffContract, t *specDiffTotals) {

	sep := strings.Repeat("=", 51)
	dash := strings.Repeat("-", 67)
	arm := cfg.Arm
	armLabel := map[string]string{"A": "arm A (LLM-first)", "B": "arm B (union)"}[arm]

	fmt.Fprintln(w, sep)
	fmt.Fprintf(w, ">>> SpecDiff: %s vs arm C (pre-exec) <<<\n", armLabel)
	fmt.Fprintln(w, sep)
	fmt.Fprintln(w, "method: dynamic two-arm replay (real pipeline, per-block fresh state)")
	fmt.Fprintln(w, "  abort = step3 真实验证失败（spec⊉real 或重执行失败），按 tx.To 归因")
	fmt.Fprintf(w, "blocks=%d txs=%d (contract-creation=%d)\n", t.blocks, t.txs, t.creation)
	fmt.Fprintf(w, "arm C: scheduled=%d committed=%d (%.2f%%) aborted=%d (%.2f%%)\n",
		t.schedC, t.cmtC, pctOf(t.cmtC, t.txs), t.abortC, pctOf(t.abortC, t.schedC))
	fmt.Fprintf(w, "arm %s: scheduled=%d committed=%d (%.2f%%) aborted=%d (%.2f%%)\n",
		arm, t.schedA, t.cmtA, pctOf(t.cmtA, t.txs), t.abortA, pctOf(t.abortA, t.schedA))
	fmt.Fprintf(w, "delta=%+d | saves=%d (C aborts, %s doesn't) hurts=%d (%s aborts, C doesn't)\n",
		t.abortA-t.abortC, t.saves, arm, t.hurts, arm)
	fmt.Fprintf(w, "committed delta=%+d (= scheduled delta %+d − abort delta %+d，占全部交易 %+.2f%%)\n",
		t.cmtA-t.cmtC, t.schedA-t.schedC, t.abortA-t.abortC, pctOf(t.cmtA-t.cmtC, t.txs))
	fmt.Fprintf(w, "newly scheduled by arm %s (C 未进调度): %d txs | committed=%d (净收益) aborted=%d (白跑一次并行仍回落串行)\n",
		arm, t.newSched, t.newSchedCmt, t.newSchedAbort)
	fmt.Fprintf(w, "LLM: tried=%d ok=%d | fail:", t.llmTried, t.llmOK)
	for _, reason := range depurge.AllLLMFailReasons() {
		if n := t.llmFail[reason.String()]; n > 0 {
			fmt.Fprintf(w, " %s=%d", reason.String(), n)
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  cross-check: 与 ./depurge replay --replay-depurge --spec-arm C / %s 的 aborted 同口径\n", arm)

	// 全部 C 臂 abort 的缺失 key 分解：abort 到底由什么类型的 key 引起。
	fmt.Fprintln(w, dash)
	fmt.Fprintf(w, "C-arm abort miss-key composition (%d aborted txs with miss keys, <=%d keys/tx sampled)\n",
		t.cAborted, missKeyCap)
	fmt.Fprint(w, "  by type:")
	for _, class := range []string{"storage", "acct-balance", "acct-other", "other"} {
		if n := t.cMissClass[class]; n > 0 {
			fmt.Fprintf(w, " %s=%d", class, n)
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  storage keys: cracked-to-tx-visible-address=%d（分支转账对手方形态） uncracked=%d（计数器派生/硬编码对手方）\n",
		t.cMissCracked, t.cMissUncracked)
	fmt.Fprintf(w, "  miss keys on txs rescued by arm %s: %d\n", arm, t.cMissRescued)

	// 被救回交易的缺失 key 分析：C 臂缺的 key 即第二臂额外覆盖的 key。
	fmt.Fprintln(w, dash)
	fmt.Fprintf(w, "rescued aborts (C aborts, arm %s doesn't): %d txs\n", arm, t.rescueCount)
	fmt.Fprintf(w, "  of which arm-%s LLM analysis OK: %d（LLM 键补上缺口）；其余 %d 为调度噪声（两臂 spec 恒同）\n",
		arm, t.rescueLLM, t.rescueCount-t.rescueLLM)
	fmt.Fprint(w, "  miss keys on rescued txs by type:")
	for _, class := range []string{"storage", "acct-balance", "acct-other", "other"} {
		if n := t.missHist[class]; n > 0 {
			fmt.Fprintf(w, " %s=%d", class, n)
		}
	}
	fmt.Fprintln(w)
	if len(t.rescues) > 0 {
		fmt.Fprintf(w, "  samples (first %d):\n", len(t.rescues))
		for _, s := range t.rescues {
			name := s.name
			if name == "" {
				name = "(no LLM analysis)"
			}
			fmt.Fprintf(w, "  blk=%d tx#%d to=%s %s llmOK=%v\n", s.block, s.idx, s.to, name, s.llmOK)
			for _, m := range s.miss {
				fmt.Fprintf(w, "      miss %s\n", m)
			}
		}
	}

	all := make([]*specDiffContract, 0, len(rows))
	for _, row := range rows {
		all = append(all, row)
	}

	// A 臂更优：delta < 0（用户要筛选的目标集合）。
	better := make([]*specDiffContract, 0)
	worse := make([]*specDiffContract, 0)
	neutral := 0
	for _, row := range all {
		switch {
		case row.delta() < 0:
			better = append(better, row)
		case row.delta() > 0:
			worse = append(worse, row)
		default:
			neutral++
		}
	}
	sort.Slice(better, func(i, j int) bool {
		if better[i].delta() != better[j].delta() {
			return better[i].delta() < better[j].delta()
		}
		return better[i].txs > better[j].txs
	})
	sort.Slice(worse, func(i, j int) bool {
		if worse[i].delta() != worse[j].delta() {
			return worse[i].delta() > worse[j].delta()
		}
		return worse[i].txs > worse[j].txs
	})

	fmt.Fprintln(w, dash)
	shown := 0
	netDelta := 0
	for _, row := range better {
		if row.txs >= cfg.MinTx {
			shown++
			netDelta += row.delta()
		}
	}
	fmt.Fprintf(w, "contracts where arm %s REDUCES aborts (delta<0, txs>=%d): %d contracts, net %+d aborts\n",
		arm, cfg.MinTx, shown, netDelta)
	specDiffWriteTable(w, better, cfg.MinTx, -1)

	fmt.Fprintln(w, dash)
	limit := cfg.Top
	if cfg.All {
		limit = -1
	}
	fmt.Fprintf(w, "contracts where arm %s INCREASES aborts (delta>0, txs>=%d): %d contracts, top %d shown\n",
		arm, cfg.MinTx, len(worse), topCount(worse, cfg.MinTx, limit))
	specDiffWriteTable(w, worse, cfg.MinTx, limit)

	if cfg.All {
		fmt.Fprintln(w, dash)
		rest := make([]*specDiffContract, 0, neutral)
		for _, row := range all {
			if row.delta() == 0 {
				rest = append(rest, row)
			}
		}
		sort.Slice(rest, func(i, j int) bool { return rest[i].txs > rest[j].txs })
		fmt.Fprintf(w, "neutral contracts (delta=0, txs>=%d): %d\n", cfg.MinTx, countMinTx(rest, cfg.MinTx))
		specDiffWriteTable(w, rest, cfg.MinTx, -1)
	} else {
		fmt.Fprintln(w, dash)
		fmt.Fprintf(w, "neutral contracts (delta=0): %d (omitted; use --all to list)\n", neutral)
	}
	fmt.Fprintln(w, sep)
}

// specDiffWriteTable 输出逐合约表。minTx 过滤；limit<0 表示不限条数。
func specDiffWriteTable(w io.Writer, rows []*specDiffContract, minTx, limit int) {
	fmt.Fprintf(w, "%-42s %-20s %6s %6s %6s %6s %6s %6s %6s %6s %6s %6s %6s %6s\n",
		"contract", "name", "txs", "llmOK", "schC", "schA", "cmtC", "cmtA", "dCmt", "abtC", "abtA", "delta", "saves", "hurts")
	printed := 0
	for _, row := range rows {
		if row.txs < minTx {
			continue
		}
		if limit >= 0 && printed >= limit {
			break
		}
		name := row.name
		if !row.hasLLM {
			name = "(no LLM analysis)"
		}
		if len(name) > 20 {
			name = name[:19] + "…"
		}
		fmt.Fprintf(w, "%-42s %-20s %6d %6d %6d %6d %6d %6d %+6d %6d %6d %+6d %6d %6d\n",
			row.address.Hex(), name, row.txs, row.llmOK, row.schedC, row.schedA,
			row.cmtC, row.cmtA, row.dCmt(), row.abortC, row.abortA, row.delta(), row.saves, row.hurts)
		printed++
	}
	if printed == 0 {
		fmt.Fprintln(w, "(none)")
	}
}

func countMinTx(rows []*specDiffContract, minTx int) int {
	n := 0
	for _, r := range rows {
		if r.txs >= minTx {
			n++
		}
	}
	return n
}

// specDiffCollectCMiss 分类一笔 C 臂 abort 交易的缺失 key，回答
// 「abort 由什么类型的 key 引起」：
//   - acct-balance：ETH 转账对手方键（分支转账的典型形态）；
//   - storage 且反推出交易可见地址：ERC20 转账/授权对手方键（分支典型）；
//   - storage 反推不出：动态键（计数器派生）或合约内硬编码对手方。
//
// 注：每笔缺失 key 最多保留 missKeyCap 个，计数为样本口径。
func specDiffCollectCMiss(t *specDiffTotals, tx *dataset.TxData, dc, da TxDetail) {
	t.cAborted++
	rescued := !da.Aborted
	for _, k := range dc.Miss {
		cls := specDiffMissClass(k)
		t.cMissClass[cls]++
		if rescued {
			t.cMissRescued++
		}
		if cls != "storage" {
			continue
		}
		if _, slot, ok := specDiffParseStorageKey(k); !ok {
			continue
		} else if _, _, _, ok2 := specDiffCrackSlot(slot, tx); ok2 {
			t.cMissCracked++
		} else {
			t.cMissUncracked++
		}
	}
}

// specDiffCollectRescue 记录一笔被第二臂救回的交易：C 臂缺失的 key 即
// 「第二臂 spec 额外覆盖、从而避免 abort」的 key。
func specDiffCollectRescue(t *specDiffTotals, blk *dataset.BlockData,
	tx *dataset.TxData, idx int, dc, da TxDetail,
	contracts map[common.Address]*llmsoundness.Contract) {

	t.rescueCount++
	if da.LLMOK {
		t.rescueLLM++
	}
	descs := make([]string, 0, len(dc.Miss))
	for _, k := range dc.Miss {
		t.missHist[specDiffMissClass(k)]++
		descs = append(descs, specDiffAnnotateMissKey(k, tx))
	}
	if len(t.rescues) < specDiffRescueSampleCap {
		name := ""
		if tx.To != "" {
			if c := contracts[common.HexToAddress(tx.To)]; c != nil {
				name = c.Meta.ContractName
			}
		}
		t.rescues = append(t.rescues, specDiffRescue{
			block: blk.BlockNumber(),
			idx:   idx,
			to:    tx.To,
			name:  name,
			llmOK: da.LLMOK,
			miss:  descs,
		})
	}
}

// specDiffMissClass 缺失 key 粗分类：
//   - acct-balance：某账户 ETH balance（ETH 转账对手方键）；
//   - storage：合约存储槽（ERC20 balances/allowed 等 mapping 项的形态，
//     分支转账对手方键与计数器派生键都落这类，需槽反推区分）；
//   - acct-other / other：code/nonce 等。
func specDiffMissClass(k string) string {
	switch {
	case strings.HasPrefix(k, "storage:"):
		return "storage"
	case strings.HasPrefix(k, "acct:") && strings.HasSuffix(k, ":balance"):
		return "acct-balance"
	case strings.HasPrefix(k, "acct:"):
		return "acct-other"
	default:
		return "other"
	}
}

// specDiffAnnotateMissKey 对 storage 缺失键做槽反推标注：若槽命中
// keccak(pad(addr)||pad(base))（Solidity mapping(address=>...) 标准布局），
// 说明该槽是「交易可见地址的 mapping 项」——分支转账对手方键的典型形态。
func specDiffAnnotateMissKey(k string, tx *dataset.TxData) string {
	cAddr, slot, ok := specDiffParseStorageKey(k)
	if !ok {
		return k
	}
	if mAddr, base, inCalldata, ok2 := specDiffCrackSlot(slot, tx); ok2 {
		src := "tx-addr"
		if inCalldata {
			src = "calldata"
		}
		return fmt.Sprintf("%s [cracked: %s.mapping@slot%d[%s] (%s)]",
			k, cAddr.Hex(), base, mAddr.Hex(), src)
	}
	return k + " [uncracked]"
}

// specDiffParseStorageKey 解析 "storage:<addr>:<slot>"。
func specDiffParseStorageKey(k string) (addr common.Address, slot common.Hash, ok bool) {
	parts := strings.Split(k, ":")
	if len(parts) != 3 || parts[0] != "storage" || !common.IsHexAddress(parts[1]) {
		return common.Address{}, common.Hash{}, false
	}
	return common.HexToAddress(parts[1]), common.HexToHash(parts[2]), true
}

// specDiffCrackSlot 反推 slot 是否为 keccak(pad(addr)||pad(base))，
// 候选地址取交易可见地址（from/to/calldata 中的地址字）。
func specDiffCrackSlot(slot common.Hash, tx *dataset.TxData) (
	addr common.Address, base uint64, inCalldata bool, ok bool) {

	cands := specDiffTxCandidates(tx)
	if len(cands) == 0 {
		return common.Address{}, 0, false, false
	}
	var keyWord [32]byte
	copy(keyWord[12:], cands[0].addr.Bytes()) // 占位，逐候选覆盖
	var baseWord [32]byte
	for base = 0; base <= 64; base++ {
		baseWord = [32]byte{}
		binary.BigEndian.PutUint64(baseWord[24:], base)
		for _, c := range cands {
			keyWord = [32]byte{}
			copy(keyWord[12:], c.addr.Bytes())
			if crypto.Keccak256Hash(keyWord[:], baseWord[:]) == slot {
				return c.addr, base, c.inCalldata, true
			}
		}
	}
	return common.Address{}, 0, false, false
}

// specDiffAddrCand 槽反推的候选地址。
type specDiffAddrCand struct {
	addr       common.Address
	inCalldata bool
}

// specDiffTxCandidates 收集交易可见地址：from、to、calldata 中每个
// 「高 12 字节为零」的 32 字节字（标准地址编码参数）。
func specDiffTxCandidates(tx *dataset.TxData) []specDiffAddrCand {
	var cands []specDiffAddrCand
	if tx.From != "" {
		cands = append(cands, specDiffAddrCand{addr: common.HexToAddress(tx.From)})
	}
	if tx.To != "" {
		cands = append(cands, specDiffAddrCand{addr: common.HexToAddress(tx.To)})
	}
	if b, err := hex.DecodeString(strings.TrimPrefix(tx.Input, "0x")); err == nil {
		// ABI calldata = 4 字节 selector + 32 字节对齐参数；参数区从偏移 4 起。
		start := 0
		if len(b) >= 4+32 {
			start = 4
		}
		for i := start; i+32 <= len(b); i += 32 {
			word := b[i : i+32]
			padded := true
			for _, x := range word[:12] {
				if x != 0 {
					padded = false
					break
				}
			}
			if !padded {
				continue
			}
			nonZero := false
			for _, x := range word[12:] {
				if x != 0 {
					nonZero = true
					break
				}
			}
			if nonZero {
				cands = append(cands, specDiffAddrCand{
					addr:       common.BytesToAddress(word[12:]),
					inCalldata: true,
				})
			}
		}
	}
	return cands
}

func topCount(rows []*specDiffContract, minTx, limit int) int {
	n := countMinTx(rows, minTx)
	if limit >= 0 && n > limit {
		return limit
	}
	return n
}
