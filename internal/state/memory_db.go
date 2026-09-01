package state

import (
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	gethstate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/stateless"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// MemoryStateDB 实现 vm.StateDB 接口的内存状态数据库。
//
// 从 dataset witness 初始化（区块级），同区块内交易共享该实例：
// 交易成功后状态保留（影响后续交易），失败由 EVM 通过
// Snapshot/RevertToSnapshot 回滚。Finalise 在每笔交易结束后
// 将 dirty 状态提交为 committed（对齐 Geth statedb 语义）。
//
// 所有读写方法通过注入的 per-tx AccessRecorder 采集 slot 级读写集；
// recorder 为 nil 时跳过采集（纯性能基准模式）。
type MemoryStateDB struct {
	accounts map[common.Address]*AccountState

	journal []revertFunc // 当前 tx 的回滚日志

	refund           uint64
	logs             []*types.Log
	logSize          uint64
	selfdestructed   map[common.Address]struct{}
	transientStorage map[common.Address]map[common.Hash]common.Hash

	// EIP-2930 访问列表（柏林热/冷访问语义）
	accessAddrs map[common.Address]struct{}
	accessSlots map[common.Address]map[common.Hash]struct{}

	// tx 上下文（AddLog 填充）
	txCtxHash common.Hash
	txCtxIdx  int

	// per-tx 读写集采集器（nil = 不采集）
	recorder *AccessRecorder

	// MPT 树（真 trie）：stateTrie 为账户树，每账户 storageTrie 为存储树。
	// 用于在 Finalise 后计算真实的 state root / storage root（模拟链上 MPT 树更新开销）。
	// trieDB 为共享的节点数据库，trieDB 为 nil 时关闭 MPT（纯内存模式，root 恒为零）。
	trieDB    *triedb.Database
	stateTrie *trie.Trie

	// CoW epoch（原子访问：多 worker 并发 CloneCoW 同一父库时会写）：
	// 0 = CoW 禁用（深拷贝 Clone / 串行等常规路径，零额外开销）；
	// 非 0 = CloneCoW 活跃，写入口须先物化共享账户（协议见 cow.go）。
	epoch atomic.Uint64
}

type revertFunc func()

// NewMemoryStateDB 创建空的内存状态数据库（默认启用真 MPT 树）。
func NewMemoryStateDB() *MemoryStateDB {
	return NewMemoryStateDBWithTrie(true)
}

// NewMemoryStateDBWithTrie 创建内存状态数据库，withTrie 决定是否挂载真 MPT 树。
// withTrie=false 时走纯内存模式（GetStorageRoot 恒零，CommitMPT 为空操作），
// 用于纯性能基准对比。
func NewMemoryStateDBWithTrie(withTrie bool) *MemoryStateDB {
	s := &MemoryStateDB{
		accounts:         make(map[common.Address]*AccountState),
		selfdestructed:   make(map[common.Address]struct{}),
		transientStorage: make(map[common.Address]map[common.Hash]common.Hash),
		accessAddrs:      make(map[common.Address]struct{}),
		accessSlots:      make(map[common.Address]map[common.Hash]struct{}),
	}
	if withTrie {
		s.trieDB = triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil)
		s.stateTrie = trie.NewEmpty(s.trieDB)
	}
	return s
}

// Clone 深拷贝当前状态库，返回一份独立快照（预执行用）。
//
// 克隆语义：
//   - accounts 逐账户深拷贝（AccountState.clone），克隆库上的任何修改
//     （含 journal 回滚、Finalise）都不会波及原库；
//   - trieDB/stateTrie 共享引用：纯内存模式（NewMemoryStateDBWithTrie(false)，
//     预执行默认）下均为 nil；带 trie 模式下 trie 节点不可变、共享只读安全，
//     克隆库 CommitMPT 时写的是自己的 storageTrie/stateTrie 引用语义由
//     AccountState.clone 保持一致（storageTrie 共享，写前需注意，见其注释）；
//   - per-tx 运行态（journal/refund/logs/selfdestructed/transientStorage/
//     访问列表/recorder）一律重置为干净初始，克隆库相当于「刚初始化完」。
//
// 典型用法：区块开始时 loadWitness 灌入基础库 base，之后每笔交易
// db := base.Clone() 得到互不影响的独立状态执行。
func (s *MemoryStateDB) Clone() *MemoryStateDB {
	c := &MemoryStateDB{
		trieDB:    s.trieDB,
		stateTrie: s.stateTrie,
	}
	c.accounts = make(map[common.Address]*AccountState, len(s.accounts))
	for addr, acc := range s.accounts {
		c.accounts[addr] = acc.clone()
	}
	c.selfdestructed = make(map[common.Address]struct{})
	c.transientStorage = make(map[common.Address]map[common.Hash]common.Hash)
	c.accessAddrs = make(map[common.Address]struct{})
	c.accessSlots = make(map[common.Address]map[common.Hash]struct{})
	return c
}

// SetRecorder 注入本 tx 的采集器（每笔交易开始时调用；nil 表示不采集）。
func (s *MemoryStateDB) SetRecorder(r *AccessRecorder) { s.recorder = r }

// Recorder 返回当前采集器。
func (s *MemoryStateDB) Recorder() *AccessRecorder { return s.recorder }

// InitAccount 用 witness 数据初始化一个账户（区块开始时调用）。
// 启用 MPT 时，把 witness 的 storage 直接灌入一个新建的 storage trie 并 Commit，
// 得到该账户在 witness 锚点处的 storage root（对齐链上初始状态）。
func (s *MemoryStateDB) InitAccount(addr common.Address, balance *uint256.Int, nonce uint64, code []byte, storage map[common.Hash]common.Hash) {
	var st *trie.Trie
	if s.trieDB != nil {
		st = trie.NewEmpty(s.trieDB)
		for k, v := range storage {
			if v == (common.Hash{}) {
				continue
			}
			_ = st.Update(k[:], common.TrimLeftZeroes(v[:]))
		}
		// Commit 使该 storage trie 不可再写；后续 Finalise 会基于该 root 重建。
		root, _ := st.Commit(false)
		st, _ = trie.New(trie.StorageTrieID(types.EmptyRootHash, common.BytesToHash(addr[:]), root), s.trieDB)
	}
	s.adoptAccount(addr, NewAccountState(balance, nonce, code, storage, st))
}

// AccountCount 返回账户总数。
func (s *MemoryStateDB) AccountCount() int { return len(s.accounts) }

// ---- 内部工具 ----

func (s *MemoryStateDB) journalAppend(f revertFunc) {
	s.journal = append(s.journal, f)
}

// getAccount 返回账户（可能为 nil）。
func (s *MemoryStateDB) getAccount(addr common.Address) *AccountState {
	return s.accounts[addr]
}

// getOrCreateForWrite 返回账户，不存在则创建空账户。
// 创建时记录隐式读（balance），并 journal 回滚删除。
// 该语义对齐 dataset rwsets：对不存在账户的写访问（如 CreateAccount、
// 向 coinbase 转 tip）会先产生一条 balance 读。
func (s *MemoryStateDB) getOrCreateForWrite(addr common.Address) *AccountState {
	if _, ok := s.accounts[addr]; ok {
		// 账户级 CoW：epoch 不匹配（与其他库共享）则先物化再写；
		// epoch=0 的深拷贝/常规路径恒 no-op。
		return s.cowEnsureAccount(addr)
	}
	var st *trie.Trie
	if s.trieDB != nil {
		st = trie.NewEmpty(s.trieDB)
	}
	acc := NewAccountState(new(uint256.Int), 0, nil, nil, st)
	s.adoptAccount(addr, acc)
	s.recorder.RecordBalanceRead(addr, common.Hash{})
	s.journalAppend(func() { delete(s.accounts, addr) })
	return acc
}

// ---- 账户基础 ----

func (s *MemoryStateDB) CreateAccount(addr common.Address) {
	s.getOrCreateForWrite(addr)
}

func (s *MemoryStateDB) CreateContract(addr common.Address) {
	acc := s.getOrCreateForWrite(addr)
	created := acc.created
	s.journalAppend(func() { acc.created = created })
	acc.created = true
}

func (s *MemoryStateDB) SubBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	return s.addBalanceDelta(addr, new(uint256.Int).Neg(amount), reason)
}

func (s *MemoryStateDB) AddBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	return s.addBalanceDelta(addr, amount, reason)
}

func (s *MemoryStateDB) addBalanceDelta(addr common.Address, delta *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	acc := s.getOrCreateForWrite(addr)
	if delta == nil || delta.IsZero() {
		return *acc.Balance // 0 值变动只 touch 不写值
	}
	old := new(uint256.Int).Set(acc.Balance)
	newVal := new(uint256.Int).Add(acc.Balance, delta)
	journalOld := old
	s.journalAppend(func() { acc.Balance.Set(journalOld) })
	acc.Balance.Set(newVal)
	acc.touched = true
	s.recorder.RecordBalanceWrite(addr, HashFromU256(journalOld), HashFromU256(newVal))
	return *newVal
}

func (s *MemoryStateDB) GetBalance(addr common.Address) *uint256.Int {
	acc := s.getAccount(addr)
	if acc == nil {
		s.recorder.RecordBalanceRead(addr, common.Hash{})
		return new(uint256.Int)
	}
	s.recorder.RecordBalanceRead(addr, HashFromU256(acc.Balance))
	return acc.Balance
}

func (s *MemoryStateDB) GetNonce(addr common.Address) uint64 {
	acc := s.getAccount(addr)
	if acc == nil {
		s.recorder.RecordNonceRead(addr, common.Hash{})
		return 0
	}
	s.recorder.RecordNonceRead(addr, HashFromU64(acc.Nonce))
	return acc.Nonce
}

func (s *MemoryStateDB) SetNonce(addr common.Address, nonce uint64, reason tracing.NonceChangeReason) {
	acc := s.getOrCreateForWrite(addr)
	old := acc.Nonce
	s.journalAppend(func() { acc.Nonce = old })
	acc.Nonce = nonce
	acc.touched = true
	s.recorder.RecordNonceWrite(addr, HashFromU64(old), HashFromU64(nonce))
}

// ---- 代码 ----

func (s *MemoryStateDB) GetCodeHash(addr common.Address) common.Hash {
	acc := s.getAccount(addr)
	if acc == nil {
		return common.Hash{}
	}
	return acc.GetCodeHash()
}

func (s *MemoryStateDB) GetCode(addr common.Address) []byte {
	acc := s.getAccount(addr)
	if acc == nil {
		return nil
	}
	s.recorder.RecordCodeRead(addr)
	return acc.Code
}

func (s *MemoryStateDB) SetCode(addr common.Address, code []byte) []byte {
	acc := s.getOrCreateForWrite(addr)
	oldCode := acc.Code
	oldHash := acc.codeHash
	s.journalAppend(func() {
		acc.Code = oldCode
		acc.codeHash = oldHash
	})
	acc.Code = code
	acc.codeHash = crypto.Keccak256Hash(code) // 急算：读路径必须纯读（共享账户并发读）
	acc.touched = true
	return oldCode
}

func (s *MemoryStateDB) GetCodeSize(addr common.Address) int {
	acc := s.getAccount(addr)
	if acc == nil {
		return 0
	}
	return len(acc.Code)
}

func (s *MemoryStateDB) GetStorageRoot(addr common.Address) common.Hash {
	if s.trieDB == nil {
		return common.Hash{}
	}
	acc := s.getAccount(addr)
	if acc == nil || acc.storageTrie == nil {
		return types.EmptyRootHash
	}
	return acc.storageTrie.Hash()
}

// ---- 存储 ----

func (s *MemoryStateDB) GetState(addr common.Address, key common.Hash) common.Hash {
	acc := s.getAccount(addr)
	if acc == nil {
		s.recorder.RecordStorageRead(addr, key, common.Hash{})
		return common.Hash{}
	}
	val := acc.GetState(key)
	s.recorder.RecordStorageRead(addr, key, val)
	return val
}

func (s *MemoryStateDB) SetState(addr common.Address, key, value common.Hash) common.Hash {
	acc := s.getOrCreateForWrite(addr)
	old := acc.GetState(key)
	dirtyOld, dirtyOk := acc.dirtyStorage[key]
	s.journalAppend(func() {
		if dirtyOk {
			acc.dirtyStorage[key] = dirtyOld
		} else {
			delete(acc.dirtyStorage, key)
		}
	})
	acc.SetState(key, value)
	s.recorder.RecordStorageWrite(addr, key, old, value)
	return old
}

func (s *MemoryStateDB) GetCommittedState(addr common.Address, key common.Hash) common.Hash {
	acc := s.getAccount(addr)
	if acc == nil {
		return common.Hash{}
	}
	return acc.GetCommittedState(key)
}

// ---- transient storage (EIP-1153) ----

func (s *MemoryStateDB) GetTransientState(addr common.Address, key common.Hash) common.Hash {
	if m, ok := s.transientStorage[addr]; ok {
		return m[key]
	}
	return common.Hash{}
}

func (s *MemoryStateDB) SetTransientState(addr common.Address, key, value common.Hash) {
	m, ok := s.transientStorage[addr]
	if !ok {
		m = make(map[common.Hash]common.Hash)
		s.transientStorage[addr] = m
	}
	oldVal, had := m[key]
	s.journalAppend(func() {
		if had {
			m[key] = oldVal
		} else {
			delete(m, key)
		}
	})
	m[key] = value
}

// ---- refund ----

func (s *MemoryStateDB) AddRefund(gas uint64) {
	old := s.refund
	s.journalAppend(func() { s.refund = old })
	s.refund += gas
}

func (s *MemoryStateDB) SubRefund(gas uint64) {
	old := s.refund
	s.journalAppend(func() { s.refund = old })
	s.refund -= gas
}

func (s *MemoryStateDB) GetRefund() uint64 { return s.refund }

// ---- 自毁 ----

func (s *MemoryStateDB) SelfDestruct(addr common.Address) uint256.Int {
	if _, ok := s.selfdestructed[addr]; !ok {
		s.journalAppend(func() { delete(s.selfdestructed, addr) })
		s.selfdestructed[addr] = struct{}{}
	}
	acc := s.getAccount(addr)
	if acc == nil {
		return uint256.Int{}
	}
	return *acc.Balance
}

func (s *MemoryStateDB) HasSelfDestructed(addr common.Address) bool {
	_, ok := s.selfdestructed[addr]
	return ok
}

func (s *MemoryStateDB) SelfDestruct6780(addr common.Address) (uint256.Int, bool) {
	acc := s.getAccount(addr)
	if acc == nil {
		return uint256.Int{}, false
	}
	destructed := false
	if _, ok := s.selfdestructed[addr]; !ok {
		s.journalAppend(func() { delete(s.selfdestructed, addr) })
		s.selfdestructed[addr] = struct{}{}
	}
	// EIP-6780：仅本 tx 内创建的合约真正自毁
	if acc.created {
		destructed = true
	}
	return *acc.Balance, destructed
}

// ---- 账户存在性 ----

func (s *MemoryStateDB) Exist(addr common.Address) bool {
	_, ok := s.accounts[addr]
	return ok
}

func (s *MemoryStateDB) Empty(addr common.Address) bool {
	acc := s.getAccount(addr)
	if acc == nil {
		return true
	}
	s.recorder.RecordBalanceRead(addr, HashFromU256(acc.Balance))
	s.recorder.RecordNonceRead(addr, HashFromU64(acc.Nonce))
	return acc.IsEmpty()
}

// ---- 访问列表 (EIP-2930) ----

func (s *MemoryStateDB) AddressInAccessList(addr common.Address) bool {
	_, ok := s.accessAddrs[addr]
	return ok
}

func (s *MemoryStateDB) SlotInAccessList(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool) {
	_, addressOk = s.accessAddrs[addr]
	if slots, ok := s.accessSlots[addr]; ok {
		_, slotOk = slots[slot]
	}
	return addressOk, slotOk
}

func (s *MemoryStateDB) addAddressToAccessList(addr common.Address) {
	_, existed := s.accessAddrs[addr]
	s.journalAppend(func() {
		if existed {
			s.accessAddrs[addr] = struct{}{}
		} else {
			delete(s.accessAddrs, addr)
		}
	})
	s.accessAddrs[addr] = struct{}{}
}

func (s *MemoryStateDB) AddAddressToAccessList(addr common.Address) {
	s.addAddressToAccessList(addr)
}

func (s *MemoryStateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	s.addAddressToAccessList(addr)
	slots, ok := s.accessSlots[addr]
	if !ok {
		slots = make(map[common.Hash]struct{})
		s.accessSlots[addr] = slots
	}
	_, slotExisted := slots[slot]
	s.journalAppend(func() {
		if slotExisted {
			slots[slot] = struct{}{}
		} else {
			delete(slots, slot)
		}
	})
	slots[slot] = struct{}{}
}

// ---- 快照 / 回滚 ----

func (s *MemoryStateDB) Snapshot() int {
	return len(s.journal)
}

func (s *MemoryStateDB) RevertToSnapshot(revid int) {
	if revid > len(s.journal) {
		return
	}
	for i := len(s.journal) - 1; i >= revid; i-- {
		s.journal[i]()
	}
	s.journal = s.journal[:revid]
}

// ---- 日志 / 预镜像 ----

func (s *MemoryStateDB) AddLog(log *types.Log) {
	s.journalAppend(func() {
		if len(s.logs) > 0 {
			s.logs = s.logs[:len(s.logs)-1]
		}
		if s.logSize > 0 {
			s.logSize--
		}
	})
	log.TxHash = s.txCtxHash
	log.TxIndex = uint(s.txCtxIdx)
	log.Index = uint(s.logSize)
	s.logs = append(s.logs, log)
	s.logSize++
}

func (s *MemoryStateDB) Logs() []*types.Log { return s.logs }

func (s *MemoryStateDB) AddPreimage(common.Hash, []byte) {
	// 未启用预镜像记录，忽略
}

func (s *MemoryStateDB) PointCache() *utils.PointCache {
	// 仅 verkle (EIP-4762) 使用，主网场景安全返回 nil
	return nil
}

func (s *MemoryStateDB) Witness() *stateless.Witness {
	// 未启用 witness 收集（vm.Config.EnableWitnessCollection=false），返回 nil
	return nil
}

// AccessEvents 返回 verkle 访问事件（仅 EIP-4762 使用，主网场景返回 nil）。
func (s *MemoryStateDB) AccessEvents() *gethstate.AccessEvents {
	return nil
}

// ---- 交易上下文 ----

func (s *MemoryStateDB) SetTxContext(thash common.Hash, ti int) {
	s.txCtxHash = thash
	s.txCtxIdx = ti
}

// ---- Prepare / Finalise ----

func (s *MemoryStateDB) Prepare(rules params.Rules, sender, coinbase common.Address, dest *common.Address, precompiles []common.Address, txAccesses types.AccessList) {
	// 重建访问列表：清空后加入 tx 声明的 accesses + 热地址
	s.accessAddrs = make(map[common.Address]struct{})
	s.accessSlots = make(map[common.Address]map[common.Hash]struct{})

	for _, al := range txAccesses {
		s.accessAddrs[al.Address] = struct{}{}
		if len(al.StorageKeys) > 0 {
			slots, ok := s.accessSlots[al.Address]
			if !ok {
				slots = make(map[common.Hash]struct{})
				s.accessSlots[al.Address] = slots
			}
			for _, k := range al.StorageKeys {
				slots[k] = struct{}{}
			}
		}
	}
	// 热地址：sender、coinbase、dest、precompiles
	s.accessAddrs[sender] = struct{}{}
	s.accessAddrs[coinbase] = struct{}{}
	for _, p := range precompiles {
		s.accessAddrs[p] = struct{}{}
	}
	if dest != nil {
		s.accessAddrs[*dest] = struct{}{}
	}
}

// Finalise 在交易结束后提交 dirty 状态：
//   - 将 dirtyStorage 并入 originStorage（下一笔交易的 committed）
//   - EIP-158: 删除被 touch 后变空的账户
//   - EIP-6780: 仅本 tx 创建且自毁的合约被删除
//   - 清空 journal / refund / 访问列表 / transient storage / 日志
func (s *MemoryStateDB) Finalise(deleteEmptyObjects bool) {
	for addr, acc := range s.accounts {
		if acc.touched {
			// EIP-158：touched 且空的账户删除
			if deleteEmptyObjects && acc.IsEmpty() {
				delete(s.accounts, addr)
				continue
			}
			// finalise 会原地写账户（dirty → origin 合并）：先确保账户私有。
			// 正常路径下 touched 账户必经写入口已物化，此处为防御性兜底
			//（覆盖「克隆时账户已 touched」的边界）；epoch=0 时恒 no-op。
			acc = s.cowEnsureAccount(addr)
			// 槽级 CoW：仅当确有 dirty 槽要并入 origin 时才拷贝 origin map
			//（dirty 为空时 finalise 对 originStorage 无原地写，可免拷贝）。
			if len(acc.dirtyStorage) > 0 {
				s.cowEnsureOrigin(acc, cowCallerFinalise)
				cowStats.slotWrites[cowCallerFinalise].Add(uint64(len(acc.dirtyStorage)))
			}
			acc.finalise()
		}
	}
	// 自毁处理（EIP-6780：仅 tx 内创建的合约真正删除）
	for addr := range s.selfdestructed {
		if acc, ok := s.accounts[addr]; ok && acc.created {
			delete(s.accounts, addr)
		}
	}
	s.journal = nil
	s.refund = 0
	s.selfdestructed = make(map[common.Address]struct{})
	s.accessAddrs = make(map[common.Address]struct{})
	s.accessSlots = make(map[common.Address]map[common.Hash]struct{})
	s.transientStorage = make(map[common.Address]map[common.Hash]common.Hash)
	s.logs = nil
	s.logSize = 0
}

// CommitMPT 执行真实的 MPT 树更新，返回新的 state root。
//
// 对齐链上 IntermediateRoot/Commit 语义：
//  1. 对每个存在 storage trie 的账户，把其 dirty storage 提交进 storage trie，
//     Commit 得到 storage root，再用新 root 重建 trie 供下一笔交易继续增量更新；
//  2. 将账户（balance/nonce/codeHash/storageRoot）RLP 编码后 Update 进 state trie；
//  3. Commit state trie 得到 state root，并基于新 root 重建 state trie。
//
// 在纯内存模式（trieDB == nil）下为空操作，返回零哈希。
// 该方法应与 Finalise 配对：Finalise 合并 dirty 状态到 origin，
// CommitMPT 将这些变更落进 trie 并重算 root（这才是链上 MPT 树更新的耗时大头）。
func (s *MemoryStateDB) CommitMPT() common.Hash {
	if s.trieDB == nil || s.stateTrie == nil {
		return common.Hash{}
	}

	// 1. 每个账户的 storage trie：Commit 并重建
	for addr, acc := range s.accounts {
		if acc.storageTrie == nil {
			continue
		}
		// Commit 得到 storage root（含本 tx 已在 finalise 中 Update 的 dirty 槽）
		storageRoot, _ := acc.storageTrie.Commit(false)
		// 基于新 root 重建，供下一笔交易继续增量写
		acc.storageTrie, _ = trie.New(
			trie.StorageTrieID(types.EmptyRootHash, common.BytesToHash(addr[:]), storageRoot),
			s.trieDB,
		)
	}

	// 2. 账户树：把每个账户更新进 state trie
	for addr, acc := range s.accounts {
		storageRoot := types.EmptyRootHash
		if acc.storageTrie != nil {
			storageRoot = acc.storageTrie.Hash()
		}
		account := &types.StateAccount{
			Nonce:    acc.Nonce,
			Balance:  acc.Balance,
			Root:     storageRoot,
			CodeHash: acc.GetCodeHash().Bytes(),
		}
		encoded, err := rlp.EncodeToBytes(account)
		if err != nil {
			continue
		}
		_ = s.stateTrie.Update(addr.Bytes(), encoded)
	}

	// 3. state trie：Commit 得到 state root，并重建
	stateRoot, _ := s.stateTrie.Commit(false)
	s.stateTrie, _ = trie.New(trie.StateTrieID(stateRoot), s.trieDB)

	return stateRoot
}
