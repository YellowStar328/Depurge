package replay

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"

	"depurge/internal/dataset"
)

// BuildBlockContext 从 dataset 区块头构造 EVM BlockContext，
// 保证重放使用与链上一致的区块上下文（时间戳、区块号、gas 限制、
// baseFee、prevRandao、coinbase 等）。
func BuildBlockContext(h *dataset.BlockHeader, chainCfg *params.ChainConfig) vm.BlockContext {
	random := common.HexToHash(h.PrevRandao)

	// 构造一个最小 types.Header 用于 CalcBlobFee（仅需 Number/Time/ExcessBlobGas）
	hdr := &types.Header{
		Number:        new(big.Int).SetUint64(uint64(h.Number)),
		Time:          uint64(h.Timestamp),
		ExcessBlobGas: blobGasPtr(uint64(h.ExcessBlobGas)),
	}
	// blobBaseFee 仅在 Cancun 及以后有意义；Pre-Cancun 区块调用 CalcBlobFee 会 panic
	var blobBaseFee *big.Int
	if chainCfg.IsCancun(hdr.Number, hdr.Time) {
		blobBaseFee = eip4844.CalcBlobFee(chainCfg, hdr)
	}

	return vm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		// dataset 未提供历史区块哈希（hashWindow 数据缺失），
		// BLOCKHASH 指令返回零哈希（极少使用，不影响读写集采集）。
		GetHash: func(uint64) common.Hash { return common.Hash{} },

		Coinbase:    common.HexToAddress(h.Beneficiary),
		GasLimit:    uint64(h.GasLimit),
		BlockNumber: new(big.Int).SetUint64(uint64(h.Number)),
		Time:        uint64(h.Timestamp),
		Difficulty:  h.Difficulty.ToBig(),
		BaseFee:     h.BaseFeePerGas.ToBig(),
		BlobBaseFee: blobBaseFee,
		Random:      &random,
	}
}

// blobGasPtr 返回 v 的 *uint64 指针。
func blobGasPtr(v uint64) *uint64 { return &v }
