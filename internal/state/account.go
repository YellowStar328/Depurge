package state

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
)

// emptyCodeHash 是空代码的 keccak256 哈希。
var emptyCodeHash = crypto.Keccak256Hash(nil)

// AccountState 内存账户状态。
// originStorage / originBalance / originNonce 表示本区块 witness（或上一笔交易
// Finalise 提交后）的已提交值；修改先写当前值，Finalise 时合并回 origin。
type AccountState struct {
	Balance *uint256.Int
	Nonce   uint64
	Code    []byte
	codeHash common.Hash // 惰性计算缓存；零值表示未计算

	originBalance *uint256.Int
	originNonce   uint64

	// storage
	originStorage map[common.Hash]common.Hash // 已提交值（GetCommittedState 读取）
	dirtyStorage  map[common.Hash]common.Hash // 本 tx 修改（GetState 优先读取）

	// MPT storage trie：真实 trie，Finalise 时把 dirty 槽增量 Update 进去，
	// Commit 后重算 storage root（对齐链上 MPT 树更新开销）。
	storageTrie *trie.Trie

	touched bool // 是否被本 tx touch（Finalise 时用于 EIP-158 空账户删除判断）

	created bool // 本 tx 内创建（journal 回滚时整体删除）
}

// NewAccountState 从 witness 初始化账户状态。
func NewAccountState(balance *uint256.Int, nonce uint64, code []byte, storage map[common.Hash]common.Hash, storageTrie *trie.Trie) *AccountState {
	if balance == nil {
		balance = new(uint256.Int)
	}
	if storage == nil {
		storage = make(map[common.Hash]common.Hash)
	}
	if code == nil {
		code = []byte{}
	}
	return &AccountState{
		Balance:       new(uint256.Int).Set(balance),
		originBalance: new(uint256.Int).Set(balance),
		Nonce:         nonce,
		originNonce:   nonce,
		Code:          code,
		originStorage: storage,
		dirtyStorage:  make(map[common.Hash]common.Hash),
		storageTrie:   storageTrie,
	}
}

// GetState 读取当前槽值（dirty 优先）。
func (a *AccountState) GetState(key common.Hash) common.Hash {
	if v, ok := a.dirtyStorage[key]; ok {
		return v
	}
	return a.originStorage[key]
}

// SetState 写入槽值（不落 journal，由 MemoryStateDB 统一 journal）。
func (a *AccountState) SetState(key, value common.Hash) {
	a.dirtyStorage[key] = value
	a.touched = true
}

// GetCommittedState 读取已提交槽值。
func (a *AccountState) GetCommittedState(key common.Hash) common.Hash {
	return a.originStorage[key]
}

// GetCodeHash 返回代码哈希；witness 的 codeHash 多为空，从 code 实时计算并缓存。
func (a *AccountState) GetCodeHash() common.Hash {
	if a.codeHash == (common.Hash{}) {
		a.codeHash = crypto.Keccak256Hash(a.Code)
	}
	return a.codeHash
}

// IsEmpty 报告账户是否为空（EIP-161: balance=nonce=code=0）。
func (a *AccountState) IsEmpty() bool {
	return a.Balance.IsZero() && a.Nonce == 0 && len(a.Code) == 0
}

// finalise 提交 dirty 状态到 origin（交易结束调用）。
// 若存在 storage trie，则把 dirty 槽增量 Update 进 trie（对齐链上
// state_object.updateRoot 的存储写入），Commit 的时机由 MemoryStateDB 统一控制。
func (a *AccountState) finalise() {
	if a.storageTrie != nil {
		for k, v := range a.dirtyStorage {
			// 对齐 geth updateStorage：value 去前导零，零值删除
			if v == (common.Hash{}) {
				_ = a.storageTrie.Update(k[:], nil)
			} else {
				_ = a.storageTrie.Update(k[:], common.TrimLeftZeroes(v[:]))
			}
		}
	}
	for k, v := range a.dirtyStorage {
		a.originStorage[k] = v
	}
	a.dirtyStorage = make(map[common.Hash]common.Hash)
	a.originBalance.Set(a.Balance)
	a.originNonce = a.Nonce
	a.touched = false
	a.created = false
}

// clone 深拷贝账户状态（预执行的独立状态快照）。
//
// Balance/originBalance/Code/originStorage/dirtyStorage 逐项复制，
// 保证克隆库上的任何修改（含 journal 回滚）都不会波及原库。
// storageTrie 共享引用：纯内存模式（预执行默认）下恒为 nil；
// 带 trie 模式下 trie 基于已 Commit 的 root 重建、写前不可变，共享只读安全。
func (a *AccountState) clone() *AccountState {
	c := &AccountState{
		Balance:       new(uint256.Int).Set(a.Balance),
		Nonce:         a.Nonce,
		Code:          append([]byte(nil), a.Code...),
		codeHash:      a.codeHash,
		originBalance: new(uint256.Int).Set(a.originBalance),
		originNonce:   a.originNonce,
		storageTrie:   a.storageTrie,
		touched:       a.touched,
		created:       a.created,
	}
	c.originStorage = make(map[common.Hash]common.Hash, len(a.originStorage))
	for k, v := range a.originStorage {
		c.originStorage[k] = v
	}
	c.dirtyStorage = make(map[common.Hash]common.Hash, len(a.dirtyStorage))
	for k, v := range a.dirtyStorage {
		c.dirtyStorage[k] = v
	}
	return c
}
