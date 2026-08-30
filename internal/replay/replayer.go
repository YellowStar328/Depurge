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

	msg, err := tx.ToMessage()
	if err != nil {
		res.Status = 2
		res.Err = fmt.Sprintf("build message: %v", err)
		return res
	}
	// 对齐 TransactionToMessage 的 baseFee 语义：
	// GasPrice = min(GasTipCap + baseFee, GasFeeCap)
	if blockCtx.BaseFee != nil && msg.GasFeeCap != nil && msg.GasTipCap != nil {
		msg.GasPrice = bigMin(new(big.Int).Add(msg.GasTipCap, blockCtx.BaseFee), msg.GasFeeCap)
	}
	db.SetTxContext(common.HexToHash(tx.Hash), txIdx)

	runs := make([]int64, 0, r.cfg.Runs)
	var (
		finalResult *core.ExecutionResult
		finalErr    error
		recorder    *state.AccessRecorder
	)

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
		runs = append(runs, time.Since(start).Nanoseconds())

		if !isLast {
			// 回滚到执行前，供下一次 run
			db.RevertToSnapshot(snap)
			continue
		}
		// 正式提交：状态变更保留，供同区块后续交易依赖
		finalResult, finalErr = result, execErr
		*gp = gpCopy
		if result != nil && result.Err != nil {
			// 交易失败（revert 等）：EVM 内部已回滚到 tx 快照
		}
		db.Finalise(true)

		// MPT 树更新：口径由 cfg.MPTPerTx 决定。
		//   true  = 每笔交易 CommitMPT（pre-Byzantium 语义）
		//   false = 区块结束时统一一次（现代主网语义），此处跳过，由 replayBlock 统一处理
		if r.cfg.MPTPerTx {
			mptStart := time.Now()
			db.CommitMPT()
			res.MptNs = time.Since(mptStart).Nanoseconds()
		}

		// receipt 构建：对齐链上 MakeReceipt 的 GetLogs + CreateBloom
		recStart := time.Now()
		_ = buildReceipt(blk, txIdx, db.Logs(), finalResult, tx.Hash)
		res.ReceiptNs = time.Since(recStart).Nanoseconds()

		recorder = rec
	}

	// 耗时统计
	res.ElapsedNs = median(runs)
	if len(runs) > 1 {
		res.Runs = runs
	}

	// 执行结果
	if finalErr != nil {
		// core 级错误（nonce/余额不匹配等），TransitionDb 已内部回滚
		res.Status = 0
		res.Err = finalErr.Error()
		if finalResult != nil {
			res.GasUsed = finalResult.UsedGas
		}
	} else if finalResult != nil {
		if finalResult.Failed() {
			res.Status = 0
			if finalResult.Err != nil {
				res.Err = finalResult.Err.Error()
			}
		} else {
			res.Status = 1
		}
		res.GasUsed = finalResult.UsedGas
	}

	// 读写集
	if recorder != nil {
		recorder.SetRootResult(res.GasUsed, res.Status == 0, res.Err)
		recorder.Freeze()
		res.CallTree = recorder.CallTree()
		res.FlatReadKeys = recorder.FlatReadKeys()
		res.FlatWriteKeys = recorder.FlatWriteKeys()
		res.Stats = buildStats(recorder)
	}

	// 可选：与 dataset canonical / rwsets 对比
	if r.cfg.Compare {
		res.Compare = buildCompare(blk, txIdx, &res)
	}
	return res
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
