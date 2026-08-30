package dataset

import (
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
)

// ToMessage 将 dataset 的 TxData 转换为 EVM 可执行的 core.Message。
//
// dataset 直接提供了 from 字段，因此无需重建签名交易并验签，
// 直接构造 Message（跳过 r/s/v 恢复 sender 的步骤）。
// SkipAccountChecks 保持 false，以保留 nonce 与 EOA 检查（与链上语义一致）。
func (tx *TxData) ToMessage() (*core.Message, error) {
	if !common.IsHexAddress(tx.From) {
		return nil, fmt.Errorf("invalid from address %q", tx.From)
	}
	from := common.HexToAddress(tx.From)

	msg := &core.Message{
		From:       from,
		Nonce:      uint64(tx.Nonce),
		GasLimit:   uint64(tx.Gas),
		Value:      tx.Value.ToBig(),
		AccessList: tx.AccessList,
	}
	// 对齐 Geth TransactionToMessage 的 gas 字段语义：
	// Legacy/2930: GasPrice = GasFeeCap = GasTipCap = gasPrice
	// 1559/4844:  GasFeeCap = maxFee, GasTipCap = maxPriority, GasPrice 初始 = feeCap
	//            （effective gas price 由 replayer 结合 baseFee 计算）
	if tx.GasPrice != nil {
		gp := tx.GasPrice.ToBig()
		msg.GasPrice = gp
		msg.GasFeeCap = new(big.Int).Set(gp)
		msg.GasTipCap = new(big.Int).Set(gp)
	} else {
		if tx.MaxFeePerGas != nil {
			msg.GasFeeCap = tx.MaxFeePerGas.ToBig()
		}
		if tx.MaxPriorityFeePerGas != nil {
			msg.GasTipCap = tx.MaxPriorityFeePerGas.ToBig()
		}
		if msg.GasFeeCap != nil {
			msg.GasPrice = new(big.Int).Set(msg.GasFeeCap)
		}
	}
	if tx.BlobVersionedHashes != nil && tx.GasPrice == nil && tx.MaxFeePerGas != nil {
		// 仅 type-3 (blob) 交易填充 BlobHashes。
		// 实测 dataset 中部分 legacy 转账也带 blobVersionedHashes 字段（导出噪音），
		// 依据字段组合判定：blob tx 必为 maxFee 定价（无 gasPrice）。
		msg.BlobHashes = tx.BlobVersionedHashes
		// dataset 未提供独立的 blob fee cap 字段，沿用 gas fee cap 语义
		msg.BlobGasFeeCap = tx.MaxFeePerGas.ToBig()
	}
	// input
	if tx.Input != "" && tx.Input != "0x" {
		data, err := decodeHex(tx.Input)
		if err != nil {
			return nil, fmt.Errorf("invalid input hex: %w", err)
		}
		msg.Data = data
	}
	// to（空 = 合约创建）
	if tx.To != "" && tx.To != "0x" {
		if !common.IsHexAddress(tx.To) {
			return nil, fmt.Errorf("invalid to address %q", tx.To)
		}
		to := common.HexToAddress(tx.To)
		msg.To = &to
	}
	return msg, nil
}

func decodeHex(s string) ([]byte, error) {
	if len(s) < 2 || s[0] != '0' || (s[1] != 'x' && s[1] != 'X') {
		return nil, fmt.Errorf("missing 0x prefix")
	}
	return hex.DecodeString(s[2:])
}
