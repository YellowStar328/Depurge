// vegeta.go 实现 NSDI'25 论文 Vegeta（speculate-order-replay）的区块级编排：
// 并行预执行猜保守读写集 → 依赖排序（贪心聚簇）→ 冲突 DAG → 按波次乐观并行
// 验证（验证失败作废回退串行，不级联）→ 串行兜底 → 成本核算与串行基线对比。
//
// 状态口径（用户确认）：全程纯内存（NewMemoryStateDBWithTrie(false)），
// 区块结束统一一次 MPT 提交（单独计时，不计入算法总时间）。
// 计时口径：算法总时间 = 依赖排序 + DAG 构建 + 批量并行验证 + 串行重放；
// 预执行单独计时（同时输出含预执行口径）；witness 加载不计入。
package replay

import (
	"fmt"
	"io"
	"math/big"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/holiman/uint256"

	"depurge/internal/dataset"
	"depurge/internal/state"
	"depurge/internal/vegeta"
)

// VegetaConfig Vegeta 调度配置。执行口径沿用 Replayer 自身 Config：
// 需要 Runs=1 且 RecordRW=true（读写集是调度核心输入）。
type VegetaConfig struct {
	Parallelism    int          // worker 数（预执行与波次验证共用；<=0 → runtime.NumCPU()）
	EdgeOrder      string       // vegeta.EdgeOrderNew（默认）| vegeta.EdgeOrderOriginal
	SerialOrder    string       // vegeta.SerialOrderBlock（默认）| vegeta.SerialOrderHash
	FilterNonce    bool         // 聚簇/建边/验证包含判断过滤 nonce 伪冲突 key
	FilterCoinbase bool         // 过滤 coinbase 的 balance tip 写 key（可交换累加；合并走增量）
	StateConfig    state.Config // recorder 采集配置（取自 Replayer.cfg，仅文档提示）
}

// DefaultVegetaConfig 返回默认配置：多核、聚簇序定边、区块序兜底、
// 过滤 nonce 伪冲突与 coinbase tip 热点。
func DefaultVegetaConfig() VegetaConfig {
	return VegetaConfig{
		Parallelism:    0, // <=0 → runtime.NumCPU()
		EdgeOrder:      vegeta.EdgeOrderNew,
		SerialOrder:    vegeta.SerialOrderBlock,
		FilterNonce:    true,
		FilterCoinbase: true,
	}
}

func (c VegetaConfig) normalize() VegetaConfig {
	if c.Parallelism <= 0 {
		c.Parallelism = runtime.NumCPU()
	}
	if c.EdgeOrder == "" {
		c.EdgeOrder = vegeta.EdgeOrderNew
	}
	if c.SerialOrder == "" {
		c.SerialOrder = vegeta.SerialOrderBlock
	}
	return c
}

// 交易去向（统计口径）。
const (
	TxOutcomeParallel = "parallel" // 波次并行验证通过并提交
	TxOutcomeAborted  = "aborted"  // 验证失败作废（不级联后继）进串行段
	TxOutcomeDirect   = "serial"   // 预执行失败/空集，直接进串行段
)

// VegetaBlockResult 单区块 Vegeta 运行结果：去向统计、各阶段耗时、
// 基线对比与最终状态 diff 诊断。
type VegetaBlockResult struct {
	BlockNumber uint64
	TxCount     int

	// 交易去向
	PreExecFailed int // 预执行失败（msgErr/execErr/EVM 失败）
	EmptyRWSet    int // 过滤 nonce 后读写集为空
	Committed     int // 波次并行验证通过提交
	Aborted       int // 验证失败作废（直接失败数；不级联作废后继）
	SerialTotal   int // 串行兜底段交易总数
	Degraded      int // 死锁防御兜底强制进串行段的交易数（正常为 0）
	Waves         int // 波次数
	MaxWaveSize   int // 最大波次交易数

	// 计时（纳秒）
	WitnessLoadNs  int64 // witness 灌入（分块控制开销，不计入算法时间）
	PreExecWallNs  int64 // 阶段1 预执行 wall（多核并行）
	PreExecSumNs   int64 // 阶段1 ApplyMessage 耗时总和
	OrderNs        int64 // 阶段2 依赖排序（贪心聚簇）
	DagNs          int64 // 阶段3 DAG 构建
	ParallelWallNs int64 // 阶段4 批量并行验证 wall（含克隆/执行/验证/合并提交）
	ParallelSumNs  int64 // 阶段4 ApplyMessage 耗时总和
	CloneNs        int64 // 阶段4 状态克隆分项
	MergeNs        int64 // 阶段4 合并提交分项
	SerialWallNs   int64 // 阶段5 串行兜底 wall（含 Finalise）
	SerialSumNs    int64 // 阶段5 ApplyMessage 耗时总和
	MptNs          int64 // 区块末一次 MPT 提交（不计入算法时间）

	// 成本核算与对比
	TotalAlgoNs    int64   // 算法总时间 = Order+Dag+Parallel+Serial（spec 口径，不含预执行）
	TotalInclPreNs int64   // 含预执行口径总时间
	BaselineWallNs int64   // 纯内存串行基线 wall（含 Finalise）
	BaselineSumNs  int64   // 基线 ApplyMessage 耗时总和
	Speedup        float64 // BaselineWallNs / TotalAlgoNs
	SpeedupInclPre float64 // BaselineWallNs / TotalInclPreNs

	// 正确性诊断
	StateDiffKeys   int      // 最终状态与串行基线不一致的 key 数（含仅一侧）
	StateDiffSample []string // 不一致 key 样本（前若干个）
	FailSamples     []string // 验证失败原因样本（前若干个）
	Warning         string   // 死锁防御触发等异常提示（空 = 无异常）

	// 并发顺序串行化验证：把 vegeta 实际提交顺序（并行段提交顺序 + 串行兜底顺序）
	// 串行重放一遍，与 vegeta 并发执行结果做 diff。仅验证、不计入算法耗时。
	SerializedOrderMatch  bool     // 串行化重放结果是否与并发结果完全一致
	SerializedDiffKeys    int      // 串行化 vs 并发的差异 key 数
	SerializedDiffSample  []string // 差异 key 样本
}

const (
	stateDiffSampleLimit = 8
	failSampleLimit      = 5
	missKeyPreviewLimit  = 3
)

// preExecResult 预执行单笔交易结果。
type preExecResult struct {
	failed         bool     // 预执行失败 → 直接进串行段
	err            string   // 失败原因
	reads          []string // 原始扁平读集（recorder 口径）
	writes         []string // 原始扁平写集
	filteredReads  []string // 过滤 nonce 后（调度/验证口径）
	filteredWrites []string
}

// waveOutcome 波次内单笔交易的验证结果。
type waveOutcome struct {
	txIdx   int
	valid   bool                 // 验证通过
	execNs  int64                // ApplyMessage 耗时
	cloneNs int64                // master.Clone 耗时
	db      *state.MemoryStateDB // valid 时：成员库（已 Finalise）
	// writeKeys：valid 时原始扁平写集（合并提交用）
	// additiveDeltas：可交换累加地址（coinbase）的本笔增量（成员最终 balance
	// − 克隆时 master 的 balance）。增量必须在执行时基于克隆基准预计算，
	// 不能在合并时用"src − 当前 master"重算（同批先合并的成员已推进 master）。
	writeKeys      []string
	additiveDeltas map[common.Address]*big.Int
	failReason     string // 作废原因
}

// RunVegetaBlock 在单个区块上运行完整 Vegeta 管线。
func (r *Replayer) RunVegetaBlock(blk *dataset.BlockData, vcfg VegetaConfig) (*VegetaBlockResult, error) {
	vcfg = vcfg.normalize()
	if err := r.vegetaPreconditions(); err != nil {
		return nil, err
	}
	opts := vegeta.Options{
		FilterNonce:    vcfg.FilterNonce,
		FilterCoinbase: vcfg.FilterCoinbase,
		EdgeOrder:      vcfg.EdgeOrder,
		SerialOrder:    vcfg.SerialOrder,
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	res := &VegetaBlockResult{
		BlockNumber: blk.BlockNumber(),
		TxCount:     len(blk.Transactions),
	}

	// 纯内存基准状态（witness 灌入 = 分块控制开销，单独计时，不计入算法时间）
	loadStart := time.Now()
	base := state.NewMemoryStateDBWithTrie(false)
	loadWitness(base, &blk.Witness)
	res.WitnessLoadNs = time.Since(loadStart).Nanoseconds()

	blockCtx := BuildBlockContext(&blk.Header, r.chainConfig)
	// coinbase 地址（EIP-55，与 FlatBalanceKey 格式一致）：tip 热点过滤与
	// 增量合并（additive balance）都按该地址识别；增量在成员执行时预计算
	// （见 waveOutcome.additiveDeltas 与 MergeCommittedFrom 契约）。
	opts.Coinbase = blockCtx.Coinbase.String()

	// ---- 阶段 1：并行预执行（猜保守读写集）----
	pre, preWall, preSum := r.vegetaPreExecute(blk, base, blockCtx, vcfg.Parallelism, opts)
	res.PreExecWallNs, res.PreExecSumNs = preWall, preSum

	// 分类：参与调度（预执行成功且过滤后读写集非空）vs 初始串行段
	infos := make([]vegeta.TxInfo, 0, len(blk.Transactions))
	directSerial := make([]int, 0)
	for i := range pre {
		pr := &pre[i]
		if pr.failed {
			res.PreExecFailed++
			directSerial = append(directSerial, i)
			continue
		}
		if len(pr.filteredReads) == 0 && len(pr.filteredWrites) == 0 {
			res.EmptyRWSet++
			directSerial = append(directSerial, i)
			continue
		}
		infos = append(infos, vegeta.TxInfo{
			Index:  i,
			Hash:   blk.Transactions[i].Hash,
			Reads:  pr.reads,
			Writes: pr.writes,
		})
	}

	// ---- 阶段 2：依赖排序（按 key 贪心聚簇）----
	t := time.Now()
	order := vegeta.ClusterOrder(infos, opts)
	res.OrderNs = time.Since(t).Nanoseconds()

	// ---- 阶段 3：构建冲突 DAG ----
	t = time.Now()
	g := vegeta.BuildGraph(order, infos, opts)
	res.DagNs = time.Since(t).Nanoseconds()

	// ---- 阶段 4：按波次乐观并行验证 ----
	// master 即基准状态库：预执行结束后 base 不再被并发读取；
	// 波内仅只读（并发 Clone），波间串行合并提交。
	// waveCommitted 记录每个波次 committed 交易的原始索引（波内按提交顺序），
	// 用于"按波次隔离串行"验证（不计入算法耗时）：复刻并发读隔离 + 顺序合并。
	master := base
	waveCommitted := make([][]int, 0, 8)
	parStart := time.Now()
	for g.Pending() {
		wave := g.Ready()
		if len(wave) == 0 {
			// 防御兜底：作废/提交均会释放后继入度，理论不可达；剩余 pending 强制进串行段。
			remaining := g.Remaining()
			res.Degraded = len(remaining)
			res.Warning = fmt.Sprintf("波次死锁防御触发：%d 笔 pending 交易强制进入串行段", len(remaining))
			for _, idx := range remaining {
				g.Invalidate(idx)
			}
			break
		}
		res.Waves++
		if len(wave) > res.MaxWaveSize {
			res.MaxWaveSize = len(wave)
		}

		outcomes := r.vegetaRunWave(blk, master, blockCtx, wave, pre, vcfg.Parallelism, opts)
		for _, oc := range outcomes {
			res.ParallelSumNs += oc.execNs
			res.CloneNs += oc.cloneNs
		}

		// 先判定本波全部作废者，再合并幸存者。作废不级联后继：
		// 后继继续在并行阶段执行（基于"前驱写入缺失"的视图），
		// 最终状态不保证与串行完全一致，仅尽可能接近（确定性不受影响）。
		var committed []int
		byTx := make(map[int]*waveOutcome, len(outcomes))
		for _, oc := range outcomes {
			byTx[oc.txIdx] = oc
			if oc.valid {
				committed = append(committed, oc.txIdx)
				continue
			}
			if len(g.Invalidate(oc.txIdx)) > 0 {
				res.Aborted++
			}
			if len(res.FailSamples) < failSampleLimit {
				res.FailSamples = append(res.FailSamples, fmt.Sprintf("tx#%d: %s", oc.txIdx, oc.failReason))
			}
		}
		// 合并幸存者写入（批内写集两两无交集，顺序无歧义；
		// coinbase balance 为增量累加，天然合并多成员 tip）
		mergeStart := time.Now()
		for _, idx := range committed {
			oc := byTx[idx]
			if err := master.MergeCommittedFrom(oc.db, oc.writeKeys, oc.additiveDeltas); err != nil {
				return nil, fmt.Errorf("block %d merge tx#%d: %w", blk.BlockNumber(), idx, err)
			}
			g.Commit(idx)
			res.Committed++
		}
		res.MergeNs += time.Since(mergeStart).Nanoseconds()
		waveCommitted = append(waveCommitted, committed)
	}
	res.ParallelWallNs = time.Since(parStart).Nanoseconds()

	// ---- 阶段 5：串行重放兜底 ----
	// 初始串行段（预执行失败/空集）+ 验证作废（不级联）；
	// block 序 = 与链上串行执行等价（用户确认口径）。
	serialList := vegeta.SortSerial(append(g.Aborted(), directSerial...),
		func(i int) string { return blk.Transactions[i].Hash }, opts)
	res.SerialTotal = len(serialList)
	gp := new(core.GasPool).AddGas(uint64(blk.Header.GasLimit)) // 共享区块 GasPool（对齐串行语义）
	t = time.Now()
	for _, idx := range serialList {
		out := r.executeTx(master, blockCtx, gp, &blk.Transactions[idx], idx, false)
		master.Finalise(true)
		if len(out.runs) > 0 {
			res.SerialSumNs += out.runs[0]
		}
	}
	res.SerialWallNs = time.Since(t).Nanoseconds()

	// ---- 区块末一次 MPT 提交（单独计时，不计入算法时间）----
	mptNs, err := vegetaBlockEndMPT(master)
	if err != nil {
		return nil, fmt.Errorf("block %d block-end MPT: %w", blk.BlockNumber(), err)
	}
	res.MptNs = mptNs

	// ---- 串行基线（同口径纯内存）与最终状态 diff 诊断 ----
	baselineDb, baseWall, baseSum := r.vegetaSerialBaseline(blk, blockCtx)
	res.BaselineWallNs, res.BaselineSumNs = baseWall, baseSum

	onlyVeg, onlyBase := state.DiffFlatStates(master.ExportFlatState(), baselineDb.ExportFlatState())
	res.StateDiffKeys = len(onlyVeg) + len(onlyBase)
	for _, k := range onlyVeg {
		if len(res.StateDiffSample) >= stateDiffSampleLimit {
			break
		}
		res.StateDiffSample = append(res.StateDiffSample, "veg-only "+k)
	}
	for _, k := range onlyBase {
		if len(res.StateDiffSample) >= stateDiffSampleLimit {
			break
		}
		res.StateDiffSample = append(res.StateDiffSample, "serial-only "+k)
	}

	// ---- 并发顺序串行化验证（仅验证、不计入算法耗时）----
	// 按波次隔离串行重放（复刻并发读隔离 + 顺序合并），与并发结果 master 做
	// diff：验证合并提交层是否与"串行重放"等价。
	serializedDb := r.vegetaSerializedOrderReplay(blk, blockCtx, waveCommitted, serialList, opts)
	onlyConc, onlySer := state.DiffFlatStates(master.ExportFlatState(), serializedDb.ExportFlatState())
	res.SerializedDiffKeys = len(onlyConc) + len(onlySer)
	res.SerializedOrderMatch = res.SerializedDiffKeys == 0
	for _, k := range onlyConc {
		if len(res.SerializedDiffSample) >= stateDiffSampleLimit {
			break
		}
		res.SerializedDiffSample = append(res.SerializedDiffSample, "concurrent-only "+k)
	}
	for _, k := range onlySer {
		if len(res.SerializedDiffSample) >= stateDiffSampleLimit {
			break
		}
		res.SerializedDiffSample = append(res.SerializedDiffSample, "serialized-only "+k)
	}

	// ---- 成本核算（仅统计各阶段耗时与总耗时，不计算 speedup）----
	res.TotalAlgoNs = res.OrderNs + res.DagNs + res.ParallelWallNs + res.SerialWallNs
	res.TotalInclPreNs = res.PreExecWallNs + res.TotalAlgoNs
	return res, nil
}

// RunVegeta 在区块范围上运行 Vegeta 管线，逐区块输出明细，
// 并向 w 输出汇总（耗时、加速比、正确性诊断）。
// runs 为每区块整管线重复轮数（>1 时取各轮耗时平均，减少测量噪声）。
func (r *Replayer) RunVegeta(vcfg VegetaConfig, blockRange string, runs int, w io.Writer) error {
	vcfg = vcfg.normalize()
	if err := r.vegetaPreconditions(); err != nil {
		return err
	}
	if runs < 1 {
		runs = 1
	}
	opts := vegeta.Options{
		FilterNonce:    vcfg.FilterNonce,
		FilterCoinbase: vcfg.FilterCoinbase,
		EdgeOrder:      vcfg.EdgeOrder,
		SerialOrder:    vcfg.SerialOrder,
	}
	if err := opts.Validate(); err != nil {
		return err
	}

	fmt.Fprintf(w, ">>> Replay Vegeta <<<\n")
	fmt.Fprintf(w, "Vegeta run | parallelism=%d runs=%d\n",
		vcfg.Parallelism, runs)

	var (
		blocks int
		totals VegetaBlockResult
	)
	totals.SerializedOrderMatch = true // 汇总口径：全部区块 match 才为 match
	err := r.loader.ForEachBlock(blockRange, func(blk *dataset.BlockData) error {
		// 整区块多轮：取各轮耗时平均，正确性诊断以最后一轮为准（各轮确定性一致）。
		res, perRun := r.runVegetaBlockRuns(blk, vcfg, runs)
		blocks++
		accumulateVegetResult(&totals, res)

		line := fmt.Sprintf(
			"block %d: %d txs | waves=%d(max %d) | parallel=%d aborted=%d serial=%d | "+
				"pre=%s order=%s dag=%s par=%s(clone %s, merge %s) ser=%s | "+
				"total=%s (excl. pre-exec) incl-pre=%s | state-diff=%d | serialized-order=%s",
			res.BlockNumber, res.TxCount, res.Waves, res.MaxWaveSize,
			res.Committed, res.Aborted, res.SerialTotal,
			time.Duration(res.PreExecWallNs), time.Duration(res.OrderNs), time.Duration(res.DagNs),
			time.Duration(res.ParallelWallNs), time.Duration(res.CloneNs), time.Duration(res.MergeNs),
			time.Duration(res.SerialWallNs),
			time.Duration(res.TotalAlgoNs), time.Duration(res.TotalInclPreNs), res.StateDiffKeys,
			serializedMatchLabel(res))
		fmt.Fprintln(w, line)
		if runs > 1 && len(perRun) > 0 {
			fmt.Fprintf(w, "  runs(n=%d): %s\n", runs, formatRuns(perRun))
		}
		fmt.Printf("block %d: vegeta done (speedup %.2fx, state-diff %d)\n",
			res.BlockNumber, res.Speedup, res.StateDiffKeys)

		if res.Warning != "" {
			fmt.Fprintf(w, "  WARNING: %s\n", res.Warning)
		}
		for _, s := range res.FailSamples {
			fmt.Fprintf(w, "  abort: %s\n", s)
		}
		for _, s := range res.StateDiffSample {
			fmt.Fprintf(w, "  diff: %s\n", s)
		}
		for _, s := range res.SerializedDiffSample {
			fmt.Fprintf(w, "  serialized-diff: %s\n", s)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if blocks == 0 {
		fmt.Fprintln(w, "no blocks replayed")
		return nil
	}

	sep := "-------------------------------------------------------------------"
	fmt.Fprintln(w, sep)
	fmt.Fprintf(w, "blocks=%d txs=%d | waves=%d | parallel=%d (%.1f%%) serial=%d (%.1f%%)\n",
		blocks, totals.TxCount, totals.Waves, totals.Committed,
		pctOf(totals.Committed, totals.TxCount), totals.SerialTotal,
		pctOf(totals.SerialTotal, totals.TxCount))
	fmt.Fprintf(w, "phase timing:\n")
	fmt.Fprintf(w, "  pre-exec  : wall=%s sum=%s (excluded from total)\n",
		time.Duration(totals.PreExecWallNs), time.Duration(totals.PreExecSumNs))
	fmt.Fprintf(w, "  order     : %s\n", time.Duration(totals.OrderNs))
	fmt.Fprintf(w, "  dag       : %s\n", time.Duration(totals.DagNs))
	fmt.Fprintf(w, "  parallel  : wall=%s sum=%s (clone=%s merge=%s)\n",
		time.Duration(totals.ParallelWallNs), time.Duration(totals.ParallelSumNs),
		time.Duration(totals.CloneNs), time.Duration(totals.MergeNs))
	fmt.Fprintf(w, "  serial    : wall=%s sum=%s\n",
		time.Duration(totals.SerialWallNs), time.Duration(totals.SerialSumNs))
	fmt.Fprintln(w, sep)
	fmt.Fprintf(w, "total (order+dag+parallel+serial) : %s\n", time.Duration(totals.TotalAlgoNs))
	fmt.Fprintf(w, "total incl. pre-exec              : %s\n", time.Duration(totals.TotalInclPreNs))
	fmt.Fprintf(w, "block-end MPT                     : %s (excluded from total)\n", time.Duration(totals.MptNs))
	fmt.Fprintf(w, "state diff keys                   : %d across all blocks\n", totals.StateDiffKeys)
	fmt.Fprintf(w, "serialized-order verification     : %s (diff keys %d, verified outside algo timing)\n",
		serializedMatchLabel(&totals), totals.SerializedDiffKeys)
	if totals.Degraded > 0 {
		fmt.Fprintf(w, "WARNING           : %d txs degraded to serial by deadlock guard\n", totals.Degraded)
	}
	fmt.Fprintln(w, strings.Repeat("=", 51))
	return nil
}

// vegetaPreconditions 校验 Replayer 配置满足 vegeta 执行口径。
func (r *Replayer) vegetaPreconditions() error {
	if !r.cfg.RecordRW {
		return fmt.Errorf("vegeta 需要 RecordRW=true（读写集是调度核心输入）")
	}
	if r.cfg.Runs != 1 {
		return fmt.Errorf("vegeta 需要 Runs=1（当前 %d）", r.cfg.Runs)
	}
	return nil
}

// vegetaPreExecute 阶段 1：多核并行预执行。
// 每笔交易基于同一份 witness 基准状态的独立克隆执行（skipNonce=true、
// 每笔满额 GasPool），互不影响；recorder 采集保守读写集。
// base 在本阶段并发只读（Clone 无写），预执行结果不回写 base。
func (r *Replayer) vegetaPreExecute(blk *dataset.BlockData, base *state.MemoryStateDB,
	blockCtx vm.BlockContext, workers int, opts vegeta.Options) ([]preExecResult, int64, int64) {

	n := len(blk.Transactions)
	results := make([]preExecResult, n)
	var (
		mu    sync.Mutex
		sumNs int64
		wg    sync.WaitGroup
		sem   = make(chan struct{}, workers)
	)

	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()

			db := base.Clone()
			gp := new(core.GasPool).AddGas(uint64(blk.Header.GasLimit))
			out := r.executeTx(db, blockCtx, gp, &blk.Transactions[i], i, true)

			var pr preExecResult
			switch {
			case out.msgErr != nil:
				pr.failed = true
				pr.err = fmt.Sprintf("build message: %v", out.msgErr)
			case out.execErr != nil:
				pr.failed = true
				pr.err = out.execErr.Error()
			case out.result == nil || out.result.Failed():
				pr.failed = true
				pr.err = "pre-execute failed"
			default:
				rec := out.recorder
				rec.SetRootResult(out.result.UsedGas, false, "")
				rec.Freeze()
				pr.reads = rec.FlatReadKeys()
				pr.writes = rec.FlatWriteKeys()
				pr.filteredReads = vegeta.FilterKeys(pr.reads, opts)
				pr.filteredWrites = vegeta.FilterKeys(pr.writes, opts)
			}

			mu.Lock()
			results[i] = pr
			if len(out.runs) > 0 {
				sumNs += out.runs[0]
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	return results, time.Since(start).Nanoseconds(), sumNs
}

// vegetaRunWave 阶段 4 单个波次：批内交易在 master.Clone()（基准 + 之前已提交
// 交易写入的最新视图）上并行重执行，随后做读写集包含性验证。
// 批内成员两两无边（无冲突 key 交集），互不读对方写入；
// master 在波内只读（并发 Clone 安全），合并提交发生在波间（单线程）。
func (r *Replayer) vegetaRunWave(blk *dataset.BlockData, master *state.MemoryStateDB,
	blockCtx vm.BlockContext, wave []int, pre []preExecResult,
	workers int, opts vegeta.Options) []*waveOutcome {

	outcomes := make([]*waveOutcome, len(wave))
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, workers)
	)
	for wi, idx := range wave {
		wg.Add(1)
		sem <- struct{}{}
		go func(wi, idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			oc := &waveOutcome{txIdx: idx}
			defer func() {
				mu.Lock()
				outcomes[wi] = oc
				mu.Unlock()
			}()

			cloneStart := time.Now()
			db := master.Clone()
			oc.cloneNs = time.Since(cloneStart).Nanoseconds()

			// 记录可交换累加地址（coinbase）在克隆时点的基准余额：
			// 验证通过后 delta = 成员最终值 − 该基准（MergeCommittedFrom 增量契约）。
			var cbAddr common.Address
			var preAdditiveBalance *uint256.Int
			if opts.FilterCoinbase && opts.Coinbase != "" {
				cbAddr = common.HexToAddress(opts.Coinbase)
				preAdditiveBalance = master.GetBalance(cbAddr)
			}

			gp := new(core.GasPool).AddGas(uint64(blk.Header.GasLimit))
			// 波次验证与预执行同口径：跳过 nonce 校验。nonce 是防重放序号、
			// 非业务依赖，同账户连续 nonce 不应因 witness 初始 nonce 固定而
			// 误判 abort（真实依赖由 balance/storage 读写集捕捉）。
			out := r.executeTx(db, blockCtx, gp, &blk.Transactions[idx], idx, true)
			if len(out.runs) > 0 {
				oc.execNs = out.runs[0]
			}

			// 验证准则：执行成功（execErr==nil 且 status=1）且
			// 实际读写集（过滤 nonce）完全落在预执行预测集内。
			switch {
			case out.msgErr != nil:
				oc.failReason = fmt.Sprintf("build message: %v", out.msgErr)
			case out.execErr != nil:
				oc.failReason = out.execErr.Error()
			case out.result == nil || out.result.Failed():
				oc.failReason = "re-execute failed"
			default:
				rec := out.recorder
				rec.SetRootResult(out.result.UsedGas, false, "")
				rec.Freeze()
				missR := vegeta.SubsetOf(
					vegeta.FilterKeys(rec.FlatReadKeys(), opts),
					pre[idx].filteredReads)
				missW := vegeta.SubsetOf(
					vegeta.FilterKeys(rec.FlatWriteKeys(), opts),
					pre[idx].filteredWrites)
				if len(missR) > 0 || len(missW) > 0 {
					oc.failReason = fmt.Sprintf("rw-set violation: read%v write%v",
						previewMissKeys(missR), previewMissKeys(missW))
				} else {
					// 通过：Finalise 消化成员状态（journal/EIP-158/自毁），
					// 交由 MergeCommittedFrom 字段级合并进 master；
					// 失败路径直接丢弃成员库（Clone 天然回退，无需显式回滚）。
					db.Finalise(true)
					oc.valid = true
					oc.db = db
					oc.writeKeys = rec.FlatWriteKeys() // 原始口径（含 nonce 写：nonce 走 +=1 累加合并）
					if preAdditiveBalance != nil {
						oc.additiveDeltas = map[common.Address]*big.Int{
							cbAddr: new(big.Int).Sub(
								db.GetBalance(cbAddr).ToBig(),
								preAdditiveBalance.ToBig()),
						}
					}
				}
			}
		}(wi, idx)
	}
	wg.Wait()
	return outcomes
}

// vegetaSerializedOrderReplay 并发顺序串行化验证：在新鲜状态库上精确复刻
// vegeta 并发执行的"读隔离 + 顺序合并"语义，验证合并提交层
// （MergeCommittedFrom / additiveDeltas / nonce 累加）是否与串行重放等价。
//
// 口径：按波次分批。每个波次内，每笔交易基于"波次开始时的 db 快照"独立执行
// （复刻并发时每笔 master.Clone() 的读隔离——同波交易互相不可见），波内全部
// 执行完后按 committed 顺序 MergeCommittedFrom 合并进 db（复刻并发合并）。
// 串行兜底段（serialList）在 db 上直接顺序执行（与并发串行段一致）。
// nonce 口径逐笔匹配：并行段交易 skipNonce=true，串行兜底段 skipNonce=false。
//
// 本函数仅用于正确性验证，其耗时不计入算法总耗时。
func (r *Replayer) vegetaSerializedOrderReplay(blk *dataset.BlockData, blockCtx vm.BlockContext,
	waveCommitted [][]int, serialList []int, opts vegeta.Options) *state.MemoryStateDB {

	db := state.NewMemoryStateDBWithTrie(false)
	loadWitness(db, &blk.Witness)

	var cbAddr common.Address
	if opts.FilterCoinbase && opts.Coinbase != "" {
		cbAddr = common.HexToAddress(opts.Coinbase)
	}

	// 并行段：按波次隔离串行（复刻并发读隔离 + 顺序合并）
	for _, wave := range waveCommitted {
		// 波次开始快照：本波所有交易的执行基准（复刻并发 master.Clone()）
		waveSnapshot := db.Clone()

		type mergeRec struct {
			db            *state.MemoryStateDB
			writeKeys     []string
			additiveDeltas map[common.Address]*big.Int
		}
		recs := make([]mergeRec, 0, len(wave))

		for _, idx := range wave {
			member := waveSnapshot.Clone() // 每笔基于波次开始快照（同波隔离）
			gp := new(core.GasPool).AddGas(uint64(blk.Header.GasLimit))

			var preAdditiveBalance *uint256.Int
			if cbAddr != (common.Address{}) {
				preAdditiveBalance = waveSnapshot.GetBalance(cbAddr)
			}

			out := r.executeTx(member, blockCtx, gp, &blk.Transactions[idx], idx, true)
			if out.recorder != nil {
				out.recorder.SetRootResult(out.result.UsedGas, false, "")
				out.recorder.Freeze()
			}
			member.Finalise(true)

			var deltas map[common.Address]*big.Int
			if preAdditiveBalance != nil {
				deltas = map[common.Address]*big.Int{
					cbAddr: new(big.Int).Sub(
						member.GetBalance(cbAddr).ToBig(),
						preAdditiveBalance.ToBig()),
				}
			}
			writeKeys := []string{}
			if out.recorder != nil {
				writeKeys = out.recorder.FlatWriteKeys()
			}
			recs = append(recs, mergeRec{db: member, writeKeys: writeKeys, additiveDeltas: deltas})
		}

		// 波内全部执行完，按提交顺序合并进 db（复刻并发合并）
		for _, rec := range recs {
			if err := db.MergeCommittedFrom(rec.db, rec.writeKeys, rec.additiveDeltas); err != nil {
				panic(err) // 不应发生：与 vegeta 合并同口径
			}
		}
	}

	// 串行兜底段：直接顺序执行（与并发串行段一致）
	gp := new(core.GasPool).AddGas(uint64(blk.Header.GasLimit))
	for _, idx := range serialList {
		r.executeTx(db, blockCtx, gp, &blk.Transactions[idx], idx, false)
		db.Finalise(true)
	}
	return db
}

// vegetaSerialBaseline 纯内存串行基线：与串行执行完全同口径
// （共享状态、每笔 Finalise、保留 nonce 校验、共享 GasPool、recorder 开启），
// 不做 MPT/receipt。返回最终状态库与耗时（wall 含 Finalise / EVM-only 分项）。
// witness 加载不计入（与 vegeta 的 WitnessLoadNs 对称）。
func (r *Replayer) vegetaSerialBaseline(blk *dataset.BlockData, blockCtx vm.BlockContext) (*state.MemoryStateDB, int64, int64) {
	db := state.NewMemoryStateDBWithTrie(false)
	loadWitness(db, &blk.Witness)
	gp := new(core.GasPool).AddGas(uint64(blk.Header.GasLimit))

	var sumNs int64
	start := time.Now()
	for i := range blk.Transactions {
		out := r.executeTx(db, blockCtx, gp, &blk.Transactions[i], i, false)
		db.Finalise(true)
		if len(out.runs) > 0 {
			sumNs += out.runs[0]
		}
	}
	return db, time.Since(start).Nanoseconds(), sumNs
}

// vegetaBlockEndMPT 区块末统一一次 MPT 提交（现代主网口径）。
// 基于最终状态导出灌入带 trie 的新库后 CommitMPT 一次；
// 灌入（InitAccount 的 storage trie 构建）视为状态准备不计时，
// 计时口径对齐 replayBlock 的区块末 CommitMPT。
func vegetaBlockEndMPT(final *state.MemoryStateDB) (int64, error) {
	db := state.NewMemoryStateDB() // 带 trie
	for _, snap := range final.ExportAccounts() {
		db.InitAccount(snap.Addr, snap.Balance, snap.Nonce, snap.Code, snap.Storage)
	}
	start := time.Now()
	db.CommitMPT()
	return time.Since(start).Nanoseconds(), nil
}

// ---- 内部工具 ----

// previewMissKeys 截取前几个 key 用于失败原因展示。
func previewMissKeys(keys []string) []string {
	if len(keys) > missKeyPreviewLimit {
		keys = keys[:missKeyPreviewLimit]
	}
	return keys
}

// runVegetaBlockRuns 对单个区块重复 runs 轮完整 Vegeta 管线，返回各计时
// 分项逐轮取平均后的结果，以及每轮算法总耗时明细（用于展示测量噪声）。
// 正确性诊断字段（去向统计、state-diff、warning、samples）取最后一轮
// （管线对同一区块确定性一致，各轮应完全相同）。
func (r *Replayer) runVegetaBlockRuns(blk *dataset.BlockData, vcfg VegetaConfig, runs int) (*VegetaBlockResult, []int64) {
	if runs <= 1 {
		res, err := r.RunVegetaBlock(blk, vcfg)
		if err != nil {
			panic(err) // 不应发生：RunVegeta 已在前置校验中约束输入
		}
		return res, nil
	}

	results := make([]*VegetaBlockResult, 0, runs)
	perRun := make([]int64, 0, runs)
	for i := 0; i < runs; i++ {
		res, err := r.RunVegetaBlock(blk, vcfg)
		if err != nil {
			panic(err)
		}
		results = append(results, res)
		perRun = append(perRun, res.TotalAlgoNs)
	}
	return averageVegetaBlockResults(blk.BlockNumber(), results), perRun
}

// averageVegetaBlockResults 对多轮结果求平均：计时分项取均值，
// 去向统计/诊断字段取最后一轮（确定性一致）。
func averageVegetaBlockResults(blockNum uint64, results []*VegetaBlockResult) *VegetaBlockResult {
	n := int64(len(results))
	avg := *results[len(results)-1] // 计数/诊断字段取最后一轮
	avg.BlockNumber = blockNum
	// 耗时字段 = 各轮求和 ÷ 轮数（与 averageDepurgeBlockResults 同口径）。
	// 注意不能只取最后一轮再除 n：vegeta 各轮耗时并非逐位一致，
	// 旧实现会把 runs>1 的耗时整体低估约 n 倍。
	sum := func(get func(*VegetaBlockResult) int64) int64 {
		var t int64
		for _, r := range results {
			t += get(r)
		}
		return t / n
	}
	avg.WitnessLoadNs = sum(func(r *VegetaBlockResult) int64 { return r.WitnessLoadNs })
	avg.PreExecWallNs = sum(func(r *VegetaBlockResult) int64 { return r.PreExecWallNs })
	avg.PreExecSumNs = sum(func(r *VegetaBlockResult) int64 { return r.PreExecSumNs })
	avg.OrderNs = sum(func(r *VegetaBlockResult) int64 { return r.OrderNs })
	avg.DagNs = sum(func(r *VegetaBlockResult) int64 { return r.DagNs })
	avg.ParallelWallNs = sum(func(r *VegetaBlockResult) int64 { return r.ParallelWallNs })
	avg.ParallelSumNs = sum(func(r *VegetaBlockResult) int64 { return r.ParallelSumNs })
	avg.CloneNs = sum(func(r *VegetaBlockResult) int64 { return r.CloneNs })
	avg.MergeNs = sum(func(r *VegetaBlockResult) int64 { return r.MergeNs })
	avg.SerialWallNs = sum(func(r *VegetaBlockResult) int64 { return r.SerialWallNs })
	avg.SerialSumNs = sum(func(r *VegetaBlockResult) int64 { return r.SerialSumNs })
	avg.MptNs = sum(func(r *VegetaBlockResult) int64 { return r.MptNs })
	avg.BaselineWallNs = sum(func(r *VegetaBlockResult) int64 { return r.BaselineWallNs })
	avg.BaselineSumNs = sum(func(r *VegetaBlockResult) int64 { return r.BaselineSumNs })
	// 派生字段重算（基于平均后的基础值）
	avg.TotalAlgoNs = avg.OrderNs + avg.DagNs + avg.ParallelWallNs + avg.SerialWallNs
	avg.TotalInclPreNs = avg.PreExecWallNs + avg.TotalAlgoNs
	return &avg
}

// formatRuns 将每轮算法耗时（纳秒）格式化为 "run1, run2, ... (avg X)"。
func formatRuns(perRun []int64) string {
	if len(perRun) == 0 {
		return ""
	}
	var sum int64
	parts := make([]string, len(perRun))
	for i, ns := range perRun {
		parts[i] = time.Duration(ns).String()
		sum += ns
	}
	avg := time.Duration(sum / int64(len(perRun)))
	return fmt.Sprintf("%s (avg %s)", strings.Join(parts, ", "), avg)
}

// accumulateVegetResult 累加区块结果到汇总。
func accumulateVegetResult(totals *VegetaBlockResult, res *VegetaBlockResult) {
	totals.TxCount += res.TxCount
	totals.PreExecFailed += res.PreExecFailed
	totals.EmptyRWSet += res.EmptyRWSet
	totals.Committed += res.Committed
	totals.Aborted += res.Aborted
	totals.SerialTotal += res.SerialTotal
	totals.Degraded += res.Degraded
	totals.Waves += res.Waves
	if res.MaxWaveSize > totals.MaxWaveSize {
		totals.MaxWaveSize = res.MaxWaveSize
	}
	totals.WitnessLoadNs += res.WitnessLoadNs
	totals.PreExecWallNs += res.PreExecWallNs
	totals.PreExecSumNs += res.PreExecSumNs
	totals.OrderNs += res.OrderNs
	totals.DagNs += res.DagNs
	totals.ParallelWallNs += res.ParallelWallNs
	totals.ParallelSumNs += res.ParallelSumNs
	totals.CloneNs += res.CloneNs
	totals.MergeNs += res.MergeNs
	totals.SerialWallNs += res.SerialWallNs
	totals.SerialSumNs += res.SerialSumNs
	totals.MptNs += res.MptNs
	totals.TotalAlgoNs += res.TotalAlgoNs
	totals.TotalInclPreNs += res.TotalInclPreNs
	totals.BaselineWallNs += res.BaselineWallNs
	totals.BaselineSumNs += res.BaselineSumNs
	totals.StateDiffKeys += res.StateDiffKeys
	totals.SerializedDiffKeys += res.SerializedDiffKeys
	if !res.SerializedOrderMatch {
		totals.SerializedOrderMatch = false
	}
}

func pctOf(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b) * 100
}

// serializedMatchLabel 返回并发顺序串行化验证的结论标签：
// "MATCH"（串行化重放与并发结果完全一致）或 "MISMATCH"。
func serializedMatchLabel(res *VegetaBlockResult) string {
	if res.SerializedOrderMatch {
		return "MATCH"
	}
	return "MISMATCH"
}
