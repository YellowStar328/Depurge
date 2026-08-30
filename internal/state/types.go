// Package state 提供 MemoryStateDB（go-ethereum vm.StateDB 的内存实现）
// 与 AccessRecorder（slot 级读写集采集器）。
package state

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

// OpType 操作类型。
type OpType int

const (
	OpRead OpType = iota
	OpWrite
)

func (o OpType) String() string {
	if o == OpWrite {
		return "write"
	}
	return "read"
}

// MarshalJSON 实现 JSON 序列化为字符串。
func (o OpType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", o.String())), nil
}

// AccessKind 访问对象类别。
type AccessKind int

const (
	KindStorage AccessKind = iota
	KindBalance
	KindNonce
	KindCode
)

func (k AccessKind) String() string {
	switch k {
	case KindBalance:
		return "balance"
	case KindNonce:
		return "nonce"
	case KindCode:
		return "code"
	default:
		return "storage"
	}
}

// MarshalJSON 实现 JSON 序列化为字符串。
func (k AccessKind) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", k.String())), nil
}

// AccessEntry slot 级单条访问记录（方案 D 默认粒度）。
// Value/OldValue：storage 时为槽值；balance/nonce 时将数值右对齐编码进 32 字节。
type AccessEntry struct {
	FrameID  string         `json:"frame_id"`
	Address  common.Address `json:"address"`
	Kind     AccessKind     `json:"kind"`
	Slot     common.Hash    `json:"slot,omitempty"`              // 仅 storage 有效
	Value    common.Hash    `json:"value"`                       // 读值或写入新值
	OldValue common.Hash    `json:"old_value,omitempty"`         // 仅 write 有效
	OpType   OpType         `json:"op_type"`
}

// CallFrame 调用树节点：一个 EVM 调用帧及其直接产生的读写集。
type CallFrame struct {
	FrameID  string         `json:"frame_id"`
	ParentID string         `json:"parent_id,omitempty"`
	Type     string         `json:"type"`     // CALL/CALLCODE/DELEGATECALL/STATICCALL/CREATE/CREATE2/ROOT
	Caller   common.Address `json:"caller"`   // 调用者
	Address  common.Address `json:"address"`  // 被调用合约地址
	Depth    int            `json:"depth"`    // EVM 调用深度（顶层=0）
	GasUsed  uint64         `json:"gas_used"` // 该帧 gas 消耗（OnExit 提供）
	Reverted bool           `json:"reverted"`
	Err      string         `json:"error,omitempty"`
	Children []*CallFrame   `json:"children,omitempty"`
	Accesses []AccessEntry  `json:"accesses,omitempty"` // 本帧直接产生的读写集
}

// FlatKey 生成与 dataset rwsets 对齐的扁平 key。
func FlatStorageKey(addr common.Address, slot common.Hash) string {
	return fmt.Sprintf("storage:%s:%s", addr.String(), slot.String())
}

// FlatBalanceKey 生成 balance 扁平 key。
func FlatBalanceKey(addr common.Address) string {
	return fmt.Sprintf("acct:%s:balance", addr.String())
}

// FlatNonceKey 生成 nonce 扁平 key。
func FlatNonceKey(addr common.Address) string {
	return fmt.Sprintf("acct:%s:nonce", addr.String())
}

// FlatCodeKey 生成 code 扁平 key。
func FlatCodeKey(addr common.Address) string {
	return fmt.Sprintf("acct:%s:code", addr.String())
}

// HashFromU256 将 uint256 值编码为 common.Hash（右对齐）。
func HashFromU256(v *uint256.Int) common.Hash {
	return common.Hash(v.Bytes32())
}

// HashFromBig 将 big.Int 编码为 common.Hash（右对齐）。
func HashFromBig(v *big.Int) common.Hash {
	if v == nil {
		return common.Hash{}
	}
	return common.Hash(new(uint256.Int).SetBytes(v.Bytes()).Bytes32())
}

// HashFromU64 将 uint64 编码为 common.Hash（右对齐）。
func HashFromU64(v uint64) common.Hash {
	return common.Hash(new(uint256.Int).SetUint64(v).Bytes32())
}
