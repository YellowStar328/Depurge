package dataset

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// BlockHeader 对齐 dataset 区块 JSON 的 header 字段。
type BlockHeader struct {
	BaseFeePerGas *Big   `json:"baseFeePerGas"`
	Beneficiary   string `json:"beneficiary"` // coinbase，hex 地址字符串
	BlobGasUsed   U64    `json:"blobGasUsed"`
	Difficulty    *Big   `json:"difficulty"`
	ExcessBlobGas U64    `json:"excessBlobGas"`
	GasLimit      U64    `json:"gasLimit"`
	Hash          string `json:"hash"`
	Nonce         string `json:"nonce"`
	Number        U64    `json:"number"`
	ParentHash    string `json:"parentHash"`
	PrevRandao    string `json:"prevRandao"`
	Timestamp     U64    `json:"timestamp"`
}

// BlockData 是单个区块文件（解压后 JSON）的顶层结构。
type BlockData struct {
	Header       BlockHeader `json:"header"`
	Transactions []TxData    `json:"transactions"`
	Witness      Witness     `json:"witness"`
	Canonical    Canonical   `json:"canonical"`
	RwSets       []FlatRwSet `json:"rwsets"`
}

// BlockNumber 返回区块号。
func (b *BlockData) BlockNumber() uint64 { return uint64(b.Header.Number) }

// Witness 是区块级状态快照，锚定于 pre-first-user-tx。
type Witness struct {
	Accounts map[string]AccountWitness `json:"accounts"`
}

// AccountWitness 是单个账户在 witness 中的状态。
// 注意：实测 codeHash 多为空字符串（以 code 内联为准）；
// nonce 为空字符串或 hex 字符串。
type AccountWitness struct {
	Balance  *Big              `json:"balance"`
	Nonce    U64               `json:"nonce"`
	CodeHash string            `json:"codeHash"`
	Code     string            `json:"code"` // hex 字符串，内联合约字节码
	Storage  map[string]string `json:"storage"`
}

// Canonical 是链上参考执行结果。
type Canonical struct {
	Receipts []CanonicalReceipt `json:"receipts"`
}

// CanonicalReceipt 是链上单笔交易的 receipt 摘要。
type CanonicalReceipt struct {
	TxHash    string `json:"txHash"`
	TxIndex   int    `json:"txIndex"`
	Status    U64    `json:"status"`
	GasUsed   U64    `json:"gasUsed"`
	LogsCount U64    `json:"logsCount"`
}

// FlatRwSet 是 dataset 自带的扁平读写集（交易级聚合，无调用帧信息）。
type FlatRwSet struct {
	TxHash    string   `json:"txHash"`
	TxIndex   int      `json:"txIndex"`
	ReadKeys  []string `json:"readKeys"`
	WriteKeys []string `json:"writeKeys"`
}

// TxData 对齐 dataset 交易 JSON 字段（原始签名交易数据）。
// 字段是否有值即隐含交易类型：GasPrice → Legacy/2930，
// MaxFeePerGas → 1559/4844，BlobVersionedHashes → 4844。
type TxData struct {
	AccessList           types.AccessList `json:"accessList"`
	BlobVersionedHashes  []common.Hash    `json:"blobVersionedHashes"`
	BlockNumber          string           `json:"blockNumber"`
	From                 string           `json:"from"`
	Gas                  U64              `json:"gas"`
	GasPrice             *Big             `json:"gasPrice"`             // Legacy / 2930
	MaxFeePerGas         *Big             `json:"maxFeePerGas"`         // 1559 / 4844
	MaxPriorityFeePerGas *Big             `json:"maxPriorityFeePerGas"` // 1559 / 4844
	Hash                 string           `json:"hash"`
	Input                string           `json:"input"`
	Nonce                U64              `json:"nonce"`
	R                    string           `json:"r"`
	S                    string           `json:"s"`
	To                   string           `json:"to"` // 可为空（合约创建）
	V                    string           `json:"v"`
	Value                *Big             `json:"value"`
}
