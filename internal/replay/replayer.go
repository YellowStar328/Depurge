// Package replay 实现区块/交易重放核心调度：从 witness 初始化内存状态、
// 按原始顺序执行交易、采集耗时与 slot 级读写集（含深层嵌套子调用）。
package replay

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"

	"depurge/internal/dataset"
	"depurge/internal/output"
	"depurge/internal/state"
	"depurge/internal/tracer"
)

// Config 重放配置。
type Config struct {
	Runs       int  // 每笔交易执行次数（>1 时输出各次耗时与中位数）
	RecordRW   bool // 是否采集读写集（false = 纯性能基准）
	Compare    bool // 是否与 dataset canonical/rwsets 对比
	MPTPerTx   bool // MPT 提交口径：true=每笔交易 CommitMPT（pre-Byzantium 语义）；false=区块结束统一一次（现代主网语义）
	StateConfig state.Config
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		Runs:        1,
		RecordRW:    true,
		StateConfig: state.DefaultConfig(),
	}
}

// Replayer 区块重放器。
type Replayer struct {
	loader      *dataset.Loader
	cfg         Config
	chainConfig *params.ChainConfig
}

// NewReplayer 创建重放器。chainId 来自 manifest，使用主网 ChainConfig 语义。
func NewReplayer(loader *dataset.Loader, cfg Config) *Replayer {
	cc := params.MainnetChainConfig
	if loader.Manifest != nil && loader.Manifest.ChainID != 1 {
		cc = &params.ChainConfig{
			ChainID:             new(big.Int).SetUint64(loader.Manifest.ChainID),
			HomesteadBlock:      params.MainnetChainConfig.HomesteadBlock,
			DAOForkBlock:        params.MainnetChainConfig.DAOForkBlock,
			DAOForkSupport:      true,
			EIP150Block:         params.MainnetChainConfig.EIP150Block,
			EIP155Block:         params.MainnetChainConfig.EIP155Block,
			EIP158Block:         params.MainnetChainConfig.EIP158Block,
			ByzantiumBlock:      params.MainnetChainConfig.ByzantiumBlock,
			ConstantinopleBlock: params.MainnetChainConfig.ConstantinopleBlock,
			PetersburgBlock:     params.MainnetChainConfig.PetersburgBlock,
			IstanbulBlock:       params.MainnetChainConfig.IstanbulBlock,
			BerlinBlock:         params.MainnetChainConfig.BerlinBlock,
			LondonBlock:         params.MainnetChainConfig.LondonBlock,
			ArrowGlacierBlock:   params.MainnetChainConfig.ArrowGlacierBlock,
			GrayGlacierBlock:    params.MainnetChainConfig.GrayGlacierBlock,
			MergeNetsplitBlock:  params.MainnetChainConfig.MergeNetsplitBlock,
			ShanghaiTime:        params.MainnetChainConfig.ShanghaiTime,
			CancunTime:          params.MainnetChainConfig.CancunTime,
			PragueTime:          params.MainnetChainConfig.PragueTime,
			Ethash:              params.MainnetChainConfig.Ethash,
		}
	}
	return &Replayer{loader: loader, cfg: cfg, chainConfig: cc}
}

// Run 流式重放指定区块范围并输出结果。
func (r *Replayer) Run(w *output.Writer, blockRange string) error {
	// 运行概要写入根目录 run-summary.log（覆盖写）
	sum, err := os.OpenFile("run-summary.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open run-summary.log: %w", err)
	}
	defer sum.Close()
	bw := bufio.NewWriter(sum)
	defer bw.Flush()

	startTime := time.Now()

	sep := strings.Repeat("=", 51)
	sub := strings.Repeat("-", 51)

	// 文件头
	fmt.Fprintf(bw, "Replay started at: %s\n", startTime.Format(time.RFC3339))
	fmt.Fprintf(bw, "%s\n", sep)
	fmt.Fprintf(bw, ">>> Replay Depurge <<<\n")
	if r.loader.Manifest != nil {
		fmt.Fprintf(bw, "Dataset range    : %d - %d\n", r.loader.Manifest.FromBlock, r.loader.Manifest.ToBlock)
	}
	fmt.Fprintf(bw, "%s\n", sub)

	var (
		totalSerialNs  int64 // EVM 执行（ApplyMessage）
		totalMptNs     int64 // MPT 树更新
		totalReceiptNs int64 // receipt 构建
	)
	err = r.loader.ForEachBlock(blockRange, func(blk *dataset.BlockData) error {
		results := r.replayBlock(blk)
		if err := w.WriteBlock(blk.BlockNumber(), results); err != nil {
			return err
		}

		// 累加该区块三段耗时
		var blockSerialNs, blockMptNs, blockReceiptNs int64
		for i := range results {
			blockSerialNs += results[i].ElapsedNs
			blockMptNs += results[i].MptNs
			blockReceiptNs += results[i].ReceiptNs
		}
		totalSerialNs += blockSerialNs
		totalMptNs += blockMptNs
		totalReceiptNs += blockReceiptNs

		fmt.Fprintf(bw, "block %-10d: %d txs | EVM: %s | MPT: %s | receipt: %s\n",
			blk.BlockNumber(), len(results),
			time.Duration(blockSerialNs), time.Duration(blockMptNs), time.Duration(blockReceiptNs))
		fmt.Printf("block %d: %d txs replayed -> %s\n", blk.BlockNumber(), len(results), w.Dir)
		return nil
	})
	if err != nil {
		return err
	}

	// 汇总
	chainTotal := totalSerialNs + totalMptNs + totalReceiptNs
	fmt.Fprintf(bw, "%s\n", sub)
	fmt.Fprintf(bw, "Total EVM exec   : %s\n", time.Duration(totalSerialNs))
	fmt.Fprintf(bw, "Total MPT update : %s\n", time.Duration(totalMptNs))
	fmt.Fprintf(bw, "Total receipt    : %s\n", time.Duration(totalReceiptNs))
	fmt.Fprintf(bw, "%s\n", sub)
	fmt.Fprintf(bw, "Chain-equivalent  : %s (EVM+MPT+receipt)\n", time.Duration(chainTotal))
	fmt.Fprintf(bw, "Serial exec only  : %s (EVM only)\n", time.Duration(totalSerialNs))
	fmt.Fprintf(bw, "Total elapsed     : %s\n", time.Since(startTime))
	fmt.Fprintf(bw, "%s\n", sep)

	return nil
}

// replayBlock 重放一个区块：从 witness 初始化状态，按序执行交易。
// stateAnchor=pre-first-user-tx + block-local-user-tx 语义：
// 同区块内交易共享状态（前一笔的提交影响后一笔），区块间独立。
func (r *Replayer) replayBlock(blk *dataset.BlockData) []output.TxResult {
	db := state.NewMemoryStateDB()
	loadWitness(db, &blk.Witness)

	blockCtx := BuildBlockContext(&blk.Header, r.chainConfig)
	gp := new(core.GasPool).AddGas(uint64(blk.Header.GasLimit))

	results := make([]output.TxResult, 0, len(blk.Transactions))
	for i := range blk.Transactions {
		tx := &blk.Transactions[i]
		res := r.replayTx(db, blockCtx, gp, blk, i, tx)
		results = append(results, res)
	}

	// 现代主网语义：区块内每笔交易只 Finalise，区块结束时统一做一次 MPT 提交。
	// 将这次区块级 MPT 耗时均摊到每笔交易，保持 Total MPT update 语义一致。
	if !r.cfg.MPTPerTx && len(results) > 0 {
		mptStart := time.Now()
		db.CommitMPT()
		perTx := time.Since(mptStart).Nanoseconds() / int64(len(results))
		for i := range results {
			results[i].MptNs = perTx
		}
	}
	return results
}

// replayTx 重放单笔交易（支持 --runs 多次执行取中位数）。
//
// 多 run 语义：前 N-1 次在快照上执行后回滚（仅计时），
// 最后一次正式执行（采集读写集并提交状态，供后续交易依赖）。
func (r *Replayer) replayTx(db *state.MemoryStateDB, blockCtx vm.BlockContext,
	gp *core.GasPool, blk *dataset.BlockData, txIdx int, tx *dataset.TxData) output.TxResult {

	res := output.TxResult{
		TxHash:      common.HexToHash(tx.Hash),
		TxIndex:     txIdx,
		BlockNumber: blk.BlockNumber(),
	}

	out := r.executeTx(db, blockCtx, gp, tx, txIdx)
	if out.msgErr != nil {
		res.Status = 2
		res.Err = fmt.Sprintf("build message: %v", out.msgErr)
		return res
	}

	// 串行语义收尾：状态合并（Finalise）→ MPT 提交（口径由 MPTPerTx 决定）→ receipt 构建
	db.Finalise(true)

	// MPT 树更新：
	//   true  = 每笔交易 CommitMPT（pre-Byzantium 语义）
	//   false = 区块结束时统一一次（现代主网语义），此处跳过，由 replayBlock 统一处理
	if r.cfg.MPTPerTx {
		mptStart := time.Now()
		db.CommitMPT()
		res.MptNs = time.Since(mptStart).Nanoseconds()
	}

	// receipt 构建：对齐链上 MakeReceipt 的 GetLogs + CreateBloom
	recStart := time.Now()
	_ = buildReceipt(blk, txIdx, db.Logs(), out.result, tx.Hash)
	res.ReceiptNs = time.Since(recStart).Nanoseconds()

	// 耗时 / 执行结果 / 读写集
	fillExecOutcome(&res, out)

	// 可选：与 dataset canonical / rwsets 对比
	if r.cfg.Compare {
		res.Compare = buildCompare(blk, txIdx, &res)
	}
	return res
}

// PreExecute 预执行一个区块的全部交易并采集读写集。
//
// 预执行语义：与串行 replayBlock 相反，每笔交易都基于同一份 witness
// 初始状态（pre-first-user-tx 锚点）独立执行，交易之间互不干扰、互不影响，
// 等价于把每笔交易都当作区块内第一笔来跑。
//
// 与串行执行的差异：
//   - 状态：每笔交易 Clone 一份初始状态快照，执行完即弃（不累积、不依赖前序交易）；
//   - GasPool：每笔交易独立满额（区块 gas limit）；
//   - 不做 Finalise/CommitMPT/buildReceipt（串行链上语义的产物），
//     MptNs/ReceiptNs 保持零值；
//   - 读写集采集口径与串行完全一致（slot + balance + nonce，per-tx recorder）。
//
// 返回 []output.TxResult（与串行结果同构、顺序与交易原始顺序一一对应），
// 便于直接对比串行/预执行的读写集差异（如冲突检测、乐观并发控制研究）。
//
// 当前为顺序调用，但每笔交易的 db/recorder 相互独立、无共享可变状态，
// 天然可并行（为后续并发预执行实验留出余地）。
func (r *Replayer) PreExecute(blk *dataset.BlockData) []output.TxResult {
	// 基础状态：纯内存模式（无 trie，预执行只需读写集、无需真实 state root），
	// witness 只灌一次，之后每笔交易从该快照克隆。
	base := state.NewMemoryStateDBWithTrie(false)
	loadWitness(base, &blk.Witness)

	blockCtx := BuildBlockContext(&blk.Header, r.chainConfig)

	results := make([]output.TxResult, 0, len(blk.Transactions))
	for i := range blk.Transactions {
		tx := &blk.Transactions[i]
		res := output.TxResult{
			TxHash:      common.HexToHash(tx.Hash),
			TxIndex:     i,
			BlockNumber: blk.BlockNumber(),
		}

		// 每笔交易：独立状态快照 + 独立满额 GasPool
		db := base.Clone()
		gp := new(core.GasPool).AddGas(uint64(blk.Header.GasLimit))

		out := r.executeTx(db, blockCtx, gp, tx, i)
		if out.msgErr != nil {
			res.Status = 2
			res.Err = fmt.Sprintf("build message: %v", out.msgErr)
			results = append(results, res)
			continue
		}

		// 耗时 / 执行结果 / 读写集（不做任何串行收尾）
		fillExecOutcome(&res, out)
		results = append(results, res)
	}
	return results
}

// execOutcome 单笔交易执行核心（executeTx）的产出。
type execOutcome struct {
	msgErr   error                 // 消息构造失败（Status=2 路径）；非 nil 时其余字段无效
	result   *core.ExecutionResult // 正式执行（最后一轮）的结果
	execErr  error                 // core 级错误（nonce/余额不匹配等）
	runs     []int64               // 各轮执行耗时
	recorder *state.AccessRecorder // 最后一轮的读写集采集器（RecordRW=false 时为 nil）
}

// executeTx 单笔交易执行核心（串行 replayTx 与预执行 PreExecute 共用）：
// 构造 msg → baseFee 对齐 → SetTxContext → 多 run ApplyMessage 计时
// （含 recorder + FrameTracer 注入）。
//
// 语义：
//   - Runs>1 时前 N-1 轮在快照上执行后回滚（仅计时），最后一轮正式执行；
//   - 正式执行后扣减 *gp（多轮用副本避免重复扣减区块 gas）；
//   - 不含串行语义的收尾（Finalise/CommitMPT/receipt），由调用方按需追加。
func (r *Replayer) executeTx(db *state.MemoryStateDB, blockCtx vm.BlockContext,
	gp *core.GasPool, tx *dataset.TxData, txIdx int) execOutcome {

	var out execOutcome

	msg, err := tx.ToMessage()
	if err != nil {
		out.msgErr = err
		return out
	}
	// 对齐 TransactionToMessage 的 baseFee 语义：
	// GasPrice = min(GasTipCap + baseFee, GasFeeCap)
	if blockCtx.BaseFee != nil && msg.GasFeeCap != nil && msg.GasTipCap != nil {
		msg.GasPrice = bigMin(new(big.Int).Add(msg.GasTipCap, blockCtx.BaseFee), msg.GasFeeCap)
	}
	db.SetTxContext(common.HexToHash(tx.Hash), txIdx)

	out.runs = make([]int64, 0, r.cfg.Runs)
	for run := 0; run < r.cfg.Runs; run++ {
		isLast := run == r.cfg.Runs-1

		var rec *state.AccessRecorder
		if r.cfg.RecordRW {
			rec = state.NewRecorder(r.cfg.StateConfig)
		}
		db.SetRecorder(rec)

		var ft *tracer.FrameTracer
		if rec != nil {
			ft = tracer.NewFrameTracer(rec)
		}
		var vmCfg vm.Config
		if ft != nil {
			vmCfg.Tracer = ft.Hooks()
		}

		evm := vm.NewEVM(blockCtx, db, r.chainConfig, vmCfg)
		evm.SetTxContext(core.NewEVMTxContext(msg))

		// GasPool 使用副本，避免多次 run 重复扣减区块 gas
		gpCopy := *gp

		snap := db.Snapshot()
		start := time.Now()
		result, execErr := core.ApplyMessage(evm, msg, &gpCopy)
		out.runs = append(out.runs, time.Since(start).Nanoseconds())

		if !isLast {
			// 回滚到执行前，供下一次 run
			db.RevertToSnapshot(snap)
			continue
		}
		// 正式执行：结果与 GasPool 保留（状态收尾由调用方处理）
		out.result, out.execErr = result, execErr
		*gp = gpCopy
		out.recorder = rec
	}
	return out
}

// fillExecOutcome 将 executeTx 的产出填充进 TxResult 的公共字段
// （耗时统计 / 执行结果判定 / 读写集提取），供串行与预执行共用。
func fillExecOutcome(res *output.TxResult, out execOutcome) {
	// 耗时统计
	res.ElapsedNs = median(out.runs)
	if len(out.runs) > 1 {
		res.Runs = out.runs
	}

	// 执行结果
	if out.execErr != nil {
		// core 级错误（nonce/余额不匹配等），TransitionDb 已内部回滚
		res.Status = 0
		res.Err = out.execErr.Error()
		if out.result != nil {
			res.GasUsed = out.result.UsedGas
		}
	} else if out.result != nil {
		if out.result.Failed() {
			res.Status = 0
			if out.result.Err != nil {
				res.Err = out.result.Err.Error()
			}
		} else {
			res.Status = 1
		}
		res.GasUsed = out.result.UsedGas
	}

	// 读写集
	if out.recorder != nil {
		out.recorder.SetRootResult(res.GasUsed, res.Status == 0, res.Err)
		out.recorder.Freeze()
		res.CallTree = out.recorder.CallTree()
		res.FlatReadKeys = out.recorder.FlatReadKeys()
		res.FlatWriteKeys = out.recorder.FlatWriteKeys()
		res.Stats = buildStats(out.recorder)
	}
}

// loadWitness 将 dataset witness 灌入 MemoryStateDB。
func loadWitness(db *state.MemoryStateDB, w *dataset.Witness) {
	if w == nil {
		return
	}
	for addrStr, aw := range w.Accounts {
		addr := common.HexToAddress(addrStr)
		balance := aw.Balance.ToBig()
		var code []byte
		if aw.Code != "" {
			if b, err := hex.DecodeString(strings.TrimPrefix(aw.Code, "0x")); err == nil {
				code = b
			}
		}
		storage := make(map[common.Hash]common.Hash, len(aw.Storage))
		for k, v := range aw.Storage {
			storage[common.HexToHash(k)] = common.HexToHash(v)
		}
		db.InitAccount(addr, uint256.MustFromBig(balance), uint64(aw.Nonce), code, storage)
	}
}

func buildStats(rec *state.AccessRecorder) output.TxStats {
	tree := rec.CallTree()
	stats := output.TxStats{
		FrameCount:  rec.FrameCount(),
		AccessCount: rec.EntryCount(),
	}
	if tree != nil {
		stats.MaxDepth = maxDepth(tree, 0)
		countRW(tree, &stats.ReadCount, &stats.WriteCount)
	}
	return stats
}

func maxDepth(f *state.CallFrame, d int) int {
	if f == nil {
		return d
	}
	deepest := d
	for _, c := range f.Children {
		if cd := maxDepth(c, d+1); cd > deepest {
			deepest = cd
		}
	}
	return deepest
}

func countRW(f *state.CallFrame, reads, writes *int) {
	if f == nil {
		return
	}
	for i := range f.Accesses {
		if f.Accesses[i].OpType == state.OpWrite {
			*writes++
		} else {
			*reads++
		}
	}
	for _, c := range f.Children {
		countRW(c, reads, writes)
	}
}

// buildCompare 构建与 dataset 的对比信息。
func buildCompare(blk *dataset.BlockData, txIdx int, res *output.TxResult) *output.CompareDiff {
	diff := &output.CompareDiff{}
	// canonical receipt
	if txIdx < len(blk.Canonical.Receipts) {
		cr := blk.Canonical.Receipts[txIdx]
		cStatus := uint64(cr.Status)
		cGas := uint64(cr.GasUsed)
		diff.CanonicalStatus = &cStatus
		diff.CanonicalGasUsed = &cGas
		localStatus := uint64(0)
		if res.Status == 1 {
			localStatus = 1
		}
		sm := cStatus == localStatus
		gm := cGas == res.GasUsed
		diff.StatusMatch = &sm
		diff.GasUsedMatch = &gm
	}
	// rwsets 对比
	if txIdx < len(blk.RwSets) {
		rs := blk.RwSets[txIdx]
		diff.ReadKeyDiff = keyDiff(rs.ReadKeys, res.FlatReadKeys)
		diff.WriteKeyDiff = keyDiff(rs.WriteKeys, res.FlatWriteKeys)
	}
	return diff
}

func keyDiff(canon, local []string) *output.KeyDiff {
	cset := make(map[string]struct{}, len(canon))
	for _, k := range canon {
		cset[k] = struct{}{}
	}
	lset := make(map[string]struct{}, len(local))
	for _, k := range local {
		lset[k] = struct{}{}
	}
	d := &output.KeyDiff{}
	for _, k := range canon {
		if _, ok := lset[k]; !ok {
			d.Missing = append(d.Missing, k)
		}
	}
	for _, k := range local {
		if _, ok := cset[k]; !ok {
			d.Extra = append(d.Extra, k)
		}
	}
	sort.Strings(d.Missing)
	sort.Strings(d.Extra)
	return d
}

// buildReceipt 模拟链上 MakeReceipt 的核心开销：GetLogs + CreateBloom。
// 返回构造的 receipt（含 bloom），用于计时 receipt 构建阶段。
func buildReceipt(blk *dataset.BlockData, txIdx int, logs []*types.Log, result *core.ExecutionResult, txHash string) *types.Receipt {
	blockHash := common.HexToHash(blk.Header.Hash)
	receipt := &types.Receipt{
		BlockHash:   blockHash,
		BlockNumber: new(big.Int).SetUint64(blk.BlockNumber()),
		TxHash:      common.HexToHash(txHash),
		Logs:        logs,
	}
	if result != nil {
		if result.Failed() {
			receipt.Status = types.ReceiptStatusFailed
		} else {
			receipt.Status = types.ReceiptStatusSuccessful
		}
		receipt.GasUsed = result.UsedGas
	}
	// 计算 bloom（链上 MakeReceipt 的大头）
	receipt.Bloom = types.CreateBloom(receipt)
	return receipt
}

func median(v []int64) int64 {
	if len(v) == 0 {
		return 0
	}
	s := make([]int64, len(v))
	copy(s, v)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

// bigMin 返回两个非 nil *big.Int 中较小的那个。
func bigMin(x, y *big.Int) *big.Int {
	if x.Cmp(y) < 0 {
		return x
	}
	return y
}
