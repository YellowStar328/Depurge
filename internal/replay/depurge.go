// depurge.go 实现 depurge 算法的区块级编排（对标 vegeta.go，独立实现互不影响）：
// step1 三臂获取保守读写集 spec_rw（A=LLM 优先+回退 / B=并集 / C=纯预执行）→
// step2 按 key 建队列依赖图（不区分读写）→ step3 事件驱动无屏障并行执行
// （队头就绪即克隆已提交状态执行，spec⊇real 验证提交/abort，提交即解锁后继）→
// step4 失败/空集/abort 交易按原始顺序串行兜底 → 串行基线 state-diff 诊断。
//
// 状态口径沿用 vegeta：全程纯内存（NewMemoryStateDBWithTrie(false)），
// 区块末统一一次 MPT 提交（单独计时，不计入算法总时间）；
// 合并提交复用 state.MergeCommittedFrom（nonce +=1 累加、coinbase 增量）。
// 计时口径：算法总时间 = 建图 + 并行 + 串行兜底；spec 获取（预执行 + LLM 分析）
// 单独计时（同时输出含 spec 口径）；witness 加载不计入。
package replay

import (
	"fmt"
	"io"
	"math/big"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/holiman/uint256"

	"depurge/internal/dataset"
	depurge "depurge/internal/depurge"
	"depurge/internal/llmsoundness"
	"depurge/internal/state"
	"depurge/internal/vegeta"
)

// DepurgeConfig depurge 调度配置。执行口径沿用 Replayer 自身 Config：
// 需要 Runs=1 且 RecordRW=true（读写集是调度核心输入）。
type DepurgeConfig struct {
	Arm            string       // spec 获取臂："A"|"B"|"C"（大小写不敏感；默认 C）
	LLMDir         string       // LLM 静态分析数据目录（A/B 臂必填）
	Parallelism    int          // worker 数（预执行与并行执行共用；<=0 → runtime.NumCPU()）
	FilterNonce    bool         // 调度/验证过滤 nonce 伪冲突 key
	FilterCoinbase bool         // 过滤 coinbase 的 balance tip 写 key（可交换累加；合并走增量）
	StateConfig    state.Config // recorder 采集配置（取自 Replayer.cfg，仅文档提示）
}

// DefaultDepurgeConfig 返回默认配置：C 臂（纯预执行）、多核、
// 过滤 nonce 伪冲突与 coinbase tip 热点。
func DefaultDepurgeConfig() DepurgeConfig {
	return DepurgeConfig{
		Arm:            "C",
		Parallelism:    0, // <=0 → runtime.NumCPU()
		FilterNonce:    true,
		FilterCoinbase: true,
	}
}

func (c DepurgeConfig) normalize() DepurgeConfig {
	if c.Parallelism <= 0 {
		c.Parallelism = runtime.NumCPU()
	}
	if c.Arm == "" {
		c.Arm = "C"
	}
	return c
}

// DepurgeBlockResult 单区块 depurge 运行结果：去向统计、各阶段耗时、
// 并行度指标、基线对比与最终状态 diff 诊断。
type DepurgeBlockResult struct {
	BlockNumber uint64
	TxCount     int

	// 交易去向（Scheduled = Committed + Aborted + Degraded）
	PreExecFailed int // step1 无可用读写集（预执行失败且 LLM 不可用）→ 直接串行
	SpecEmpty     int // spec 过滤后为空 → 直接串行
	Scheduled     int // 参与调度（spec 非空入队）
	Committed     int // 并行验证通过提交
	Aborted       int // spec⊉real 或执行失败作废 → 串行兜底
	SerialTotal   int // 串行兜底段交易总数（直接串行 + abort + 防御兜底）
	Degraded      int // 死锁防御兜底强制进串行段的交易数（正常为 0）

	// LLM 分析统计（仅 A/B 臂）
	LLMTried int            // 尝试 LLM 分析的交易数
	LLMOK    int            // LLM 分析成功数
	LLMFail  map[string]int // 失败类别名 → 笔数（depurge.LLMFailReason.String()）

	// 计时（纳秒）
	WitnessLoadNs  int64 // witness 灌入（不计入算法时间）
	SpecWallNs     int64 // step1 spec 获取 wall（预执行 + LLM 分析 + 桥接选择，不计入 total）
	PreExecSumNs   int64 // step1 预执行 ApplyMessage 耗时总和
	GraphNs        int64 // step2 key 队列建图
	ParallelWallNs int64 // step3 事件驱动并行 wall（含克隆/执行/验证/合并）
	ParallelSumNs  int64 // step3 ApplyMessage 耗时总和
	CloneNs        int64 // step3 状态克隆分项（各 worker 累加口径）
	MergeNs        int64 // step3 合并提交分项
	SerialWallNs   int64 // step4 串行兜底 wall（含 Finalise）
	SerialSumNs    int64 // step4 ApplyMessage 耗时总和
	MptNs          int64 // 区块末一次 MPT 提交（不计入算法时间）

	// 并行度指标（事件驱动，无波次）
	PeakInFlight   int   // 峰值在飞交易数
	InFlightAreaNs int64 // Σ 在飞数×持续时间（平均并行度 = 面积 / ParallelWallNs）
	Dispatches     int   // 就绪派发事件次数

	// 成本核算与对比
	TotalAlgoNs     int64   // 算法总时间 = Graph+Parallel+Serial（不含 spec 获取）
	TotalInclSpecNs int64   // 含 spec 获取口径总时间
	BaselineWallNs  int64   // 纯内存串行基线 wall（含 Finalise）
	BaselineSumNs   int64   // 基线 ApplyMessage 耗时总和
	Speedup         float64 // BaselineWallNs / TotalAlgoNs

	// 正确性诊断
	StateDiffKeys   int      // 最终状态与串行基线不一致的 key 数（含仅一侧）
	StateDiffSample []string // 不一致 key 样本（前若干个）
	FailSamples     []string // 作废原因样本（前若干个）
	Warning         string   // 死锁防御触发等异常提示（空 = 无异常）
}

// depurgeJob 是 dispatcher 派发给 worker 的单笔执行任务。
type depurgeJob struct {
	idx  int
	spec []string // 过滤后 spec keys（spec⊇real 判定基准）
}

// depurgeExecResult 是 worker 返回的单笔执行验证结果。
type depurgeExecResult struct {
	idx        int
	valid      bool
	failReason string
	execNs     int64
	cloneNs    int64
	db         *state.MemoryStateDB // valid 时：成员库（已 Finalise）
	// writeKeys：valid 时原始扁平写集（合并提交用，含 nonce 写：走 +=1 累加）
	// additiveDeltas：coinbase 本笔增量（成员最终 balance − 克隆时基准，
	// 必须在执行时基于克隆基准预计算，不能在合并时重算）。
	writeKeys      []string
	additiveDeltas map[common.Address]*big.Int
}

// RunDepurgeBlock 在单个区块上运行完整 depurge 管线。
// contracts 为 LLM 分析合约表（A/B 臂必填；C 臂传 nil）。
func (r *Replayer) RunDepurgeBlock(blk *dataset.BlockData, dcfg DepurgeConfig,
	contracts map[common.Address]*llmsoundness.Contract, runs int) (*DepurgeBlockResult, error) {

	dcfg = dcfg.normalize()
	if err := r.depurgePreconditions(); err != nil {
		return nil, err
	}
	arm, err := depurge.ParseArm(dcfg.Arm)
	if err != nil {
		return nil, err
	}
	dcfg.Arm = string(arm)
	if arm != depurge.ArmC && len(contracts) == 0 {
		return nil, fmt.Errorf("depurge arm %s 需要 LLM 分析合约表（--llm-dir）", arm)
	}
	res, _, err := r.runDepurgeBlockRuns(blk, dcfg, contracts, dcfg.filterOpts(), runs)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// depurgePreconditions 校验 Replayer 配置满足 depurge 执行口径。
func (r *Replayer) depurgePreconditions() error {
	if !r.cfg.RecordRW {
		return fmt.Errorf("depurge 需要 RecordRW=true（读写集是调度核心输入）")
	}
	if r.cfg.Runs != 1 {
		return fmt.Errorf("depurge 需要 Runs=1（当前 %d）", r.cfg.Runs)
	}
	return nil
}

// filterOpts 构造 nonce/coinbase 过滤选项。
// EdgeOrder/SerialOrder 对 depurge 无意义（队列固定原始序、兜底固定原始序），
// 填默认值仅为满足 vegeta.Options 零值语义。
func (c DepurgeConfig) filterOpts() vegeta.Options {
	return vegeta.Options{
		FilterNonce:    c.FilterNonce,
		FilterCoinbase: c.FilterCoinbase,
		EdgeOrder:      vegeta.EdgeOrderNew,
		SerialOrder:    vegeta.SerialOrderBlock,
	}
}

// depurgeReexecRate 重执行率 = Aborted / 参与调度且已出结果的交易数
// （预执行失败/空集属"未进调度"，不计入）。
func depurgeReexecRate(res *DepurgeBlockResult) float64 {
	return pctOf(res.Aborted, res.Committed+res.Aborted)
}

// depurgeAvgInFlight 平均并行度 = 在飞面积 / 并行墙钟。
func depurgeAvgInFlight(res *DepurgeBlockResult) float64 {
	if res.ParallelWallNs <= 0 {
		return 0
	}
	return float64(res.InFlightAreaNs) / float64(res.ParallelWallNs)
}

// RunDepurge 在区块范围上运行 depurge 管线，逐区块输出明细，
// 并向 w 输出汇总（耗时、重执行率、并行度、正确性诊断）。
// runs 为每区块整管线重复轮数（>1 时取各轮耗时平均，减少测量噪声）。
func (r *Replayer) RunDepurge(dcfg DepurgeConfig, blockRange string, runs int, w io.Writer) error {
	dcfg = dcfg.normalize()
	if err := r.depurgePreconditions(); err != nil {
		return err
	}
	arm, err := depurge.ParseArm(dcfg.Arm)
	if err != nil {
		return err
	}
	dcfg.Arm = string(arm)
	if runs < 1 {
		runs = 1
	}

	// A/B 臂加载 LLM 静态分析数据（一次加载，全部区块复用）。
	var contracts map[common.Address]*llmsoundness.Contract
	if arm != depurge.ArmC {
		if dcfg.LLMDir == "" {
			return fmt.Errorf("depurge arm %s 需要 --llm-dir", arm)
		}
		contracts, err = llmsoundness.LoadContracts(dcfg.LLMDir)
		if err != nil {
			return fmt.Errorf("加载 LLM 分析数据: %w", err)
		}
		if len(contracts) == 0 {
			return fmt.Errorf("%s 下无可加载的 LLM 分析合约", dcfg.LLMDir)
		}
	}
	opts := dcfg.filterOpts()

	fmt.Fprintf(w, ">>> Replay Depurge <<<\n")
	fmt.Fprintf(w, "Depurge run | arm=%s parallelism=%d runs=%d\n", arm, dcfg.Parallelism, runs)

	var (
		blocks int
		totals DepurgeBlockResult
	)
	totals.LLMFail = make(map[string]int)
	err = r.loader.ForEachBlock(blockRange, func(blk *dataset.BlockData) error {
		// 整区块多轮：取各轮耗时平均，正确性诊断以最后一轮为准（各轮确定性一致）。
		res, perRun, err := r.runDepurgeBlockRuns(blk, dcfg, contracts, opts, runs)
		if err != nil {
			return err
		}
		blocks++
		accumulateDepurgeResult(&totals, res)

		fmt.Fprintf(w,
			"block %d: %d txs | sched=%d committed=%d aborted=%d serial=%d | "+
				"spec=%s graph=%s par=%s(clone %s, merge %s) ser=%s | "+
				"total=%s (excl. spec) incl-spec=%s | state-diff=%d | "+
				"reexec=%.1f%% par-avg=%.2f peak=%d\n",
			res.BlockNumber, res.TxCount, res.Scheduled, res.Committed, res.Aborted, res.SerialTotal,
			time.Duration(res.SpecWallNs), time.Duration(res.GraphNs),
			time.Duration(res.ParallelWallNs), time.Duration(res.CloneNs), time.Duration(res.MergeNs),
			time.Duration(res.SerialWallNs),
			time.Duration(res.TotalAlgoNs), time.Duration(res.TotalInclSpecNs), res.StateDiffKeys,
			depurgeReexecRate(res), depurgeAvgInFlight(res), res.PeakInFlight)
		if runs > 1 && len(perRun) > 0 {
			fmt.Fprintf(w, "  runs(n=%d): %s\n", runs, formatRuns(perRun))
		}
		fmt.Printf("block %d: depurge done (speedup %.2fx, committed %d, aborted %d, serial %d, state-diff %d)\n",
			res.BlockNumber, res.Speedup, res.Committed, res.Aborted, res.SerialTotal, res.StateDiffKeys)

		if res.Warning != "" {
			fmt.Fprintf(w, "  WARNING: %s\n", res.Warning)
		}
		for _, s := range res.FailSamples {
			fmt.Fprintf(w, "  abort: %s\n", s)
		}
		for _, s := range res.StateDiffSample {
			fmt.Fprintf(w, "  diff: %s\n", s)
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

	sep := strings.Repeat("-", 67)
	fmt.Fprintln(w, sep)
	fmt.Fprintf(w, "blocks=%d txs=%d | committed=%d (%.1f%%) aborted=%d serial=%d (%.1f%%)\n",
		blocks, totals.TxCount, totals.Committed, pctOf(totals.Committed, totals.TxCount),
		totals.Aborted, totals.SerialTotal, pctOf(totals.SerialTotal, totals.TxCount))
	fmt.Fprintf(w, "re-execution rate: %.2f%% (aborted %d / scheduled %d)\n",
		depurgeReexecRate(&totals), totals.Aborted, totals.Committed+totals.Aborted)
	fmt.Fprintf(w, "spec acquisition: no-rwset=%d empty-spec=%d\n",
		totals.PreExecFailed, totals.SpecEmpty)
	if arm != depurge.ArmC {
		fmt.Fprintf(w, "LLM analysis (arm %s): tried=%d ok=%d | fail:", arm, totals.LLMTried, totals.LLMOK)
		for _, reason := range depurge.AllLLMFailReasons() {
			fmt.Fprintf(w, " %s=%d", reason.String(), totals.LLMFail[reason.String()])
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintln(w, "phase timing:")
	fmt.Fprintf(w, "  spec      : wall=%s (excluded from total; pre-exec sum=%s)\n",
		time.Duration(totals.SpecWallNs), time.Duration(totals.PreExecSumNs))
	fmt.Fprintf(w, "  graph     : %s\n", time.Duration(totals.GraphNs))
	fmt.Fprintf(w, "  parallel  : wall=%s sum=%s (clone=%s merge=%s) avg-par=%.2f peak=%d dispatches=%d\n",
		time.Duration(totals.ParallelWallNs), time.Duration(totals.ParallelSumNs),
		time.Duration(totals.CloneNs), time.Duration(totals.MergeNs),
		depurgeAvgInFlight(&totals), totals.PeakInFlight, totals.Dispatches)
	fmt.Fprintf(w, "  serial    : wall=%s sum=%s\n",
		time.Duration(totals.SerialWallNs), time.Duration(totals.SerialSumNs))
	fmt.Fprintln(w, sep)
	fmt.Fprintf(w, "total (graph+parallel+serial) : %s\n", time.Duration(totals.TotalAlgoNs))
	fmt.Fprintf(w, "total incl. spec              : %s\n", time.Duration(totals.TotalInclSpecNs))
	fmt.Fprintf(w, "block-end MPT                 : %s (excluded from total)\n", time.Duration(totals.MptNs))
	fmt.Fprintf(w, "state diff keys               : %d across all blocks\n", totals.StateDiffKeys)
	if totals.TotalAlgoNs > 0 {
		fmt.Fprintf(w, "speedup                       : %.2fx (baseline wall %s / total %s)\n",
			float64(totals.BaselineWallNs)/float64(totals.TotalAlgoNs),
			time.Duration(totals.BaselineWallNs), time.Duration(totals.TotalAlgoNs))
	}
	if totals.Degraded > 0 {
		fmt.Fprintf(w, "WARNING           : %d txs degraded to serial by deadlock guard\n", totals.Degraded)
	}
	fmt.Fprintln(w, strings.Repeat("=", 51))
	return nil
}

// runDepurgeBlockOnce 单区块单轮完整 depurge 管线。
func (r *Replayer) runDepurgeBlockOnce(blk *dataset.BlockData, dcfg DepurgeConfig,
	contracts map[common.Address]*llmsoundness.Contract, opts vegeta.Options) (*DepurgeBlockResult, error) {

	arm, _ := depurge.ParseArm(dcfg.Arm)
	res := &DepurgeBlockResult{
		BlockNumber: blk.BlockNumber(),
		TxCount:     len(blk.Transactions),
		LLMFail:     make(map[string]int),
	}

	// 纯内存基准状态（witness 灌入 = 分块控制开销，单独计时，不计入算法时间）
	loadStart := time.Now()
	base := state.NewMemoryStateDBWithTrie(false)
	loadWitness(base, &blk.Witness)
	res.WitnessLoadNs = time.Since(loadStart).Nanoseconds()

	blockCtx := BuildBlockContext(&blk.Header, r.chainConfig)
	// coinbase 地址（EIP-55，与 FlatBalanceKey 格式一致）：tip 热点过滤与
	// 增量合并（additive balance）都按该地址识别。
	opts.Coinbase = blockCtx.Coinbase.String()

	// ---- step1：三臂保守读写集 spec_rw（不计入 total）----
	specStart := time.Now()
	// 预执行对三臂都跑（recorder 口径读写集）；复用 vegeta 预执行（同口径：
	// skipNonce=true、每笔满额 GasPool、基于同一 witness 基准克隆）。
	pre, _, preSum := r.vegetaPreExecute(blk, base, blockCtx, dcfg.Parallelism, opts)
	res.PreExecSumNs = preSum

	n := len(blk.Transactions)
	specs := make([][]string, n)
	directSerial := make([]int, 0)
	infos := make([]depurge.TxInfo, 0, n)
	for i := range blk.Transactions {
		tx := &blk.Transactions[i]
		pr := &pre[i]

		// LLM 静态分析（仅 A/B 臂），失败逐类统计。
		var llmKeys []string
		llmOK := false
		if arm != depurge.ArmC {
			res.LLMTried++
			keys, reason := depurgeLLMKeys(contracts, tx)
			if reason == depurge.LLMOK {
				llmOK = true
				llmKeys = keys
				res.LLMOK++
			} else {
				res.LLMFail[reason.String()]++
			}
		}

		// 预执行读写并集（过滤口径，与 vegeta 预执行一致）。
		var preKeys []string
		if !pr.failed {
			preKeys = append(append([]string{}, pr.filteredReads...), pr.filteredWrites...)
		}

		// 臂选择：LLM 臂静态合成 sender balance（gas 真实调度键）。
		// 预执行失败且 LLM 不可用 → spec 为空 → 直接串行兜底。
		senderBal := state.FlatBalanceKey(common.HexToAddress(tx.From))
		spec := depurge.SelectSpec(arm, preKeys, llmKeys, llmOK, senderBal)
		if len(spec) == 0 {
			res.PreExecFailed++
			directSerial = append(directSerial, i)
			continue
		}
		// nonce/coinbase 伪冲突统一在调度层过滤（沿用 vegeta 口径）。
		spec = vegeta.FilterKeys(spec, opts)
		if len(spec) == 0 {
			res.SpecEmpty++
			directSerial = append(directSerial, i)
			continue
		}
		specs[i] = spec
		infos = append(infos, depurge.TxInfo{Index: i, Keys: spec})
	}
	res.SpecWallNs = time.Since(specStart).Nanoseconds()

	// ---- step2：key 队列依赖图（不区分读写，按原始序入队）----
	t := time.Now()
	sched := depurge.BuildScheduler(n, infos)
	res.GraphNs = time.Since(t).Nanoseconds()
	res.Scheduled = sched.InGraph()

	// ---- step3：事件驱动无屏障并行执行 ----
	// actor 模型：dispatcher（本 goroutine）串行处理提交/作废，独占调度器与
	// master 写路径（免锁）；worker 池并行执行。master 用 RWMutex 分离
	// 并行 Clone（读锁）与串行 Merge（写锁）。
	// 就绪交易在其所有 key 队列都在队头 → 其全部前驱已提交合并进 master，
	// Clone 即得"看到前驱提交结果"的隔离快照（满足算法语义）。
	master := base
	var masterMu sync.RWMutex
	parStart := time.Now()

	jobsCh := make(chan depurgeJob, res.Scheduled+1)
	resultsCh := make(chan depurgeExecResult, res.Scheduled+1)
	var wg sync.WaitGroup
	for wi := 0; wi < dcfg.Parallelism; wi++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobsCh {
				resultsCh <- r.depurgeExecOne(blk, master, &masterMu, blockCtx, job, opts)
			}
		}()
	}

	inFlight := 0
	last := parStart
	dispatch := func(idxs []int) {
		if len(idxs) == 0 {
			return
		}
		res.Dispatches++
		for _, idx := range idxs {
			sched.Dispatch(idx)
			jobsCh <- depurgeJob{idx: idx, spec: specs[idx]}
		}
	}

	initial := sched.Ready()
	dispatch(initial)
	inFlight = len(initial)
	res.PeakInFlight = inFlight

	for inFlight > 0 {
		er := <-resultsCh
		// 并行度面积累计：上一段在飞数 × 持续时间。
		now := time.Now()
		res.InFlightAreaNs += int64(inFlight) * now.Sub(last).Nanoseconds()
		last = now
		inFlight--

		res.ParallelSumNs += er.execNs
		res.CloneNs += er.cloneNs

		var newly []int
		if er.valid {
			// 提交：串行合并进 master（写锁），随后出队解锁后继。
			mergeStart := time.Now()
			masterMu.Lock()
			mergeErr := master.MergeCommittedFrom(er.db, er.writeKeys, er.additiveDeltas)
			masterMu.Unlock()
			res.MergeNs += time.Since(mergeStart).Nanoseconds()
			if mergeErr != nil {
				close(jobsCh)
				wg.Wait()
				return nil, fmt.Errorf("block %d merge tx#%d: %w", blk.BlockNumber(), er.idx, mergeErr)
			}
			res.Committed++
			newly = sched.Finish(er.idx, false)
		} else {
			// 作废：不级联后继（后继继续基于"前驱写入缺失"视图执行，
			// 最终状态尽力接近串行，确定性不受影响）；计入串行兜底。
			res.Aborted++
			if len(res.FailSamples) < failSampleLimit {
				res.FailSamples = append(res.FailSamples, fmt.Sprintf("tx#%d: %s", er.idx, er.failReason))
			}
			newly = sched.Finish(er.idx, true)
		}

		// 提交/作废即解锁后继并立即派发（无全局屏障，消除波次 straggler）。
		dispatch(newly)
		inFlight += len(newly)
		if inFlight > res.PeakInFlight {
			res.PeakInFlight = inFlight
		}
	}
	close(jobsCh)
	wg.Wait()
	res.ParallelWallNs = time.Since(parStart).Nanoseconds()

	// 死锁防御兜底：队列按原始序入队，阻塞恒指向更小序号，理论不可达；
	// 若触发则剩余 pending 强制进串行段（对齐 vegeta Degraded 语义）。
	if sched.Pending() > 0 {
		remaining := sched.DrainRemaining()
		res.Degraded = len(remaining)
		res.Warning = fmt.Sprintf("死锁防御触发：%d 笔 pending 交易强制进入串行段", len(remaining))
	}

	// ---- step4：串行兜底（直接串行 + abort + 防御兜底，按最初顺序）----
	// sched.Aborted() 已含防御兜底交易；与 directSerial 不相交。
	serialList := append([]int{}, directSerial...)
	serialList = append(serialList, sched.Aborted()...)
	sort.Ints(serialList)
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

	onlyDep, onlyBase := state.DiffFlatStates(master.ExportFlatState(), baselineDb.ExportFlatState())
	res.StateDiffKeys = len(onlyDep) + len(onlyBase)
	for _, k := range onlyDep {
		if len(res.StateDiffSample) >= stateDiffSampleLimit {
			break
		}
		res.StateDiffSample = append(res.StateDiffSample, "depurge-only "+k)
	}
	for _, k := range onlyBase {
		if len(res.StateDiffSample) >= stateDiffSampleLimit {
			break
		}
		res.StateDiffSample = append(res.StateDiffSample, "serial-only "+k)
	}

	// ---- 成本核算 ----
	res.TotalAlgoNs = res.GraphNs + res.ParallelWallNs + res.SerialWallNs
	res.TotalInclSpecNs = res.SpecWallNs + res.TotalAlgoNs
	if res.TotalAlgoNs > 0 {
		res.Speedup = float64(res.BaselineWallNs) / float64(res.TotalAlgoNs)
	}
	return res, nil
}

// depurgeExecOne worker 执行单笔就绪交易：克隆已提交状态 → 执行 →
// spec⊇real 超集验证。克隆在读锁下（可并行），合并由 dispatcher 写锁串行。
// 与预执行/波次验证同口径：skipNonce=true、每笔满额 GasPool。
func (r *Replayer) depurgeExecOne(blk *dataset.BlockData, master *state.MemoryStateDB,
	masterMu *sync.RWMutex, blockCtx vm.BlockContext, job depurgeJob,
	opts vegeta.Options) depurgeExecResult {

	er := depurgeExecResult{idx: job.idx}

	masterMu.RLock()
	cloneStart := time.Now()
	db := master.Clone()
	er.cloneNs = time.Since(cloneStart).Nanoseconds()
	// 记录可交换累加地址（coinbase）在克隆时点的基准余额：
	// 验证通过后 delta = 成员最终值 − 该基准（MergeCommittedFrom 增量契约）。
	var cbAddr common.Address
	var preAdditiveBalance *uint256.Int
	if opts.FilterCoinbase && opts.Coinbase != "" {
		cbAddr = common.HexToAddress(opts.Coinbase)
		preAdditiveBalance = master.GetBalance(cbAddr)
	}
	masterMu.RUnlock()

	gp := new(core.GasPool).AddGas(uint64(blk.Header.GasLimit))
	out := r.executeTx(db, blockCtx, gp, &blk.Transactions[job.idx], job.idx, true)
	if len(out.runs) > 0 {
		er.execNs = out.runs[0]
	}

	// 验证准则：执行成功（无错误且 status=1）且 spec_rw ⊇ real_rw
	// （真实读写并集过滤后完全落在 spec 内），否则作废进串行兜底。
	switch {
	case out.msgErr != nil:
		er.failReason = fmt.Sprintf("build message: %v", out.msgErr)
	case out.execErr != nil:
		er.failReason = out.execErr.Error()
	case out.result == nil || out.result.Failed():
		er.failReason = "re-execute failed"
	default:
		rec := out.recorder
		rec.SetRootResult(out.result.UsedGas, false, "")
		rec.Freeze()
		real := append(append([]string{}, rec.FlatReadKeys()...), rec.FlatWriteKeys()...)
		miss := vegeta.SubsetOf(vegeta.FilterKeys(real, opts), job.spec)
		if len(miss) > 0 {
			er.failReason = fmt.Sprintf("rw-set violation: %v", previewMissKeys(miss))
		} else {
			// 通过：Finalise 消化成员状态（journal/EIP-158/自毁），
			// 交由 MergeCommittedFrom 字段级合并进 master；
			// 失败路径直接丢弃成员库（Clone 天然回退，无需显式回滚）。
			db.Finalise(true)
			er.valid = true
			er.db = db
			er.writeKeys = rec.FlatWriteKeys() // 原始口径（含 nonce 写：走 +=1 累加合并）
			if preAdditiveBalance != nil {
				er.additiveDeltas = map[common.Address]*big.Int{
					cbAddr: new(big.Int).Sub(
						db.GetBalance(cbAddr).ToBig(),
						preAdditiveBalance.ToBig()),
				}
			}
		}
	}
	return er
}

// depurgeLLMKeys 对单笔交易做 LLM 静态分析实例化并桥接为 recorder key 格式
// （slot:<小写地址>:<slot> → storage:<EIP-55地址>:<slot>）。
// 失败逐类返回原因（供 step1 统计与 A 臂回退 / B 臂降级判定）。
func depurgeLLMKeys(contracts map[common.Address]*llmsoundness.Contract,
	tx *dataset.TxData) ([]string, depurge.LLMFailReason) {

	if tx.To == "" {
		return nil, depurge.LLMNotContract
	}
	c, ok := contracts[common.HexToAddress(tx.To)]
	if !ok {
		return nil, depurge.LLMNoContract
	}
	input := strings.TrimPrefix(tx.Input, "0x")
	if len(input) < 8 {
		return nil, depurge.LLMNoSelector
	}
	sel := "0x" + input[:8]
	if c.Selectors[sel] == nil {
		return nil, depurge.LLMNoSelector
	}
	data := common.FromHex(tx.Input)
	if len(data) < 4 {
		return nil, depurge.LLMDecodeFail
	}
	method, err := c.ABI.MethodById(data[:4])
	if err != nil {
		return nil, depurge.LLMDecodeFail
	}
	args, err := method.Inputs.Unpack(data[4:])
	if err != nil {
		return nil, depurge.LLMDecodeFail
	}
	if !c.HasStorage() {
		return nil, depurge.LLMNoStorage
	}
	sender := common.HexToAddress(tx.From)
	read := c.Instantiate(sel, "read", sender, args)
	write := c.Instantiate(sel, "write", sender, args)
	if read.Unresolved+write.Unresolved > 0 {
		return nil, depurge.LLMUnresolved
	}
	// InstResult.Keys 为集合（map），遍历桥接后去重排序（确定性）。
	seen := make(map[string]struct{}, len(read.Keys)+len(write.Keys))
	for k := range read.Keys {
		if bridged, ok := depurge.BridgeLLMKey(k); ok {
			seen[bridged] = struct{}{}
		}
	}
	for k := range write.Keys {
		if bridged, ok := depurge.BridgeLLMKey(k); ok {
			seen[bridged] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil, depurge.LLMEmpty
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, depurge.LLMOK
}

// runDepurgeBlockRuns 对单区块跑 runs 轮完整 depurge 管线：
// 各轮耗时取平均（摊平测量噪声），计数/诊断取最后一轮（各轮确定性一致）。
// 返回值：平均结果 + 每轮算法总时间（TotalAlgoNs）序列。
func (r *Replayer) runDepurgeBlockRuns(blk *dataset.BlockData, dcfg DepurgeConfig,
	contracts map[common.Address]*llmsoundness.Contract, opts vegeta.Options,
	runs int) (*DepurgeBlockResult, []int64, error) {

	if runs < 1 {
		runs = 1
	}
	all := make([]*DepurgeBlockResult, 0, runs)
	perRun := make([]int64, 0, runs)
	for i := 0; i < runs; i++ {
		res, err := r.runDepurgeBlockOnce(blk, dcfg, contracts, opts)
		if err != nil {
			return nil, nil, err
		}
		all = append(all, res)
		perRun = append(perRun, res.TotalAlgoNs)
	}
	if runs == 1 {
		return all[0], perRun, nil
	}
	return averageDepurgeBlockResults(all), perRun, nil
}

// averageDepurgeBlockResults 把同一区块的多轮结果平均为一个：
// 所有耗时字段取算术平均（摊平测量噪声），计数/诊断/样本取最后一轮
// （各轮确定性一致，仅耗时抖动）。
func averageDepurgeBlockResults(all []*DepurgeBlockResult) *DepurgeBlockResult {
	n := int64(len(all))
	last := *all[len(all)-1]
	avg := last
	sum := func(get func(*DepurgeBlockResult) int64) int64 {
		var t int64
		for _, r := range all {
			t += get(r)
		}
		return t / n
	}
	avg.WitnessLoadNs = sum(func(r *DepurgeBlockResult) int64 { return r.WitnessLoadNs })
	avg.SpecWallNs = sum(func(r *DepurgeBlockResult) int64 { return r.SpecWallNs })
	avg.PreExecSumNs = sum(func(r *DepurgeBlockResult) int64 { return r.PreExecSumNs })
	avg.GraphNs = sum(func(r *DepurgeBlockResult) int64 { return r.GraphNs })
	avg.ParallelWallNs = sum(func(r *DepurgeBlockResult) int64 { return r.ParallelWallNs })
	avg.ParallelSumNs = sum(func(r *DepurgeBlockResult) int64 { return r.ParallelSumNs })
	avg.CloneNs = sum(func(r *DepurgeBlockResult) int64 { return r.CloneNs })
	avg.MergeNs = sum(func(r *DepurgeBlockResult) int64 { return r.MergeNs })
	avg.SerialWallNs = sum(func(r *DepurgeBlockResult) int64 { return r.SerialWallNs })
	avg.SerialSumNs = sum(func(r *DepurgeBlockResult) int64 { return r.SerialSumNs })
	avg.MptNs = sum(func(r *DepurgeBlockResult) int64 { return r.MptNs })
	avg.BaselineWallNs = sum(func(r *DepurgeBlockResult) int64 { return r.BaselineWallNs })
	avg.BaselineSumNs = sum(func(r *DepurgeBlockResult) int64 { return r.BaselineSumNs })
	avg.InFlightAreaNs = sum(func(r *DepurgeBlockResult) int64 { return r.InFlightAreaNs })
	// 峰值并行度取各轮最大（峰值语义）。
	peak := 0
	for _, r := range all {
		if r.PeakInFlight > peak {
			peak = r.PeakInFlight
		}
	}
	avg.PeakInFlight = peak
	avg.TotalAlgoNs = avg.GraphNs + avg.ParallelWallNs + avg.SerialWallNs
	avg.TotalInclSpecNs = avg.SpecWallNs + avg.TotalAlgoNs
	if avg.TotalAlgoNs > 0 {
		avg.Speedup = float64(avg.BaselineWallNs) / float64(avg.TotalAlgoNs)
	}
	return &avg
}

// accumulateDepurgeResult 累加单区块结果到汇总。
func accumulateDepurgeResult(totals, res *DepurgeBlockResult) {
	totals.TxCount += res.TxCount
	totals.PreExecFailed += res.PreExecFailed
	totals.SpecEmpty += res.SpecEmpty
	totals.Scheduled += res.Scheduled
	totals.Committed += res.Committed
	totals.Aborted += res.Aborted
	totals.SerialTotal += res.SerialTotal
	totals.Degraded += res.Degraded
	totals.LLMTried += res.LLMTried
	totals.LLMOK += res.LLMOK
	for k, v := range res.LLMFail {
		totals.LLMFail[k] += v
	}
	totals.WitnessLoadNs += res.WitnessLoadNs
	totals.SpecWallNs += res.SpecWallNs
	totals.PreExecSumNs += res.PreExecSumNs
	totals.GraphNs += res.GraphNs
	totals.ParallelWallNs += res.ParallelWallNs
	totals.ParallelSumNs += res.ParallelSumNs
	totals.CloneNs += res.CloneNs
	totals.MergeNs += res.MergeNs
	totals.SerialWallNs += res.SerialWallNs
	totals.SerialSumNs += res.SerialSumNs
	totals.MptNs += res.MptNs
	totals.InFlightAreaNs += res.InFlightAreaNs
	totals.Dispatches += res.Dispatches
	if res.PeakInFlight > totals.PeakInFlight {
		totals.PeakInFlight = res.PeakInFlight
	}
	totals.TotalAlgoNs += res.TotalAlgoNs
	totals.TotalInclSpecNs += res.TotalInclSpecNs
	totals.BaselineWallNs += res.BaselineWallNs
	totals.BaselineSumNs += res.BaselineSumNs
	totals.StateDiffKeys += res.StateDiffKeys
}
