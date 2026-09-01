package state

import (
	"bytes"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

// AccountSnapshot 账户最终状态快照（导出用于区块末 MPT 构建与状态对比）。
type AccountSnapshot struct {
	Addr    common.Address
	Balance *uint256.Int
	Nonce   uint64
	Code    []byte
	Storage map[common.Hash]common.Hash
}

// MergeCommittedFrom 将成员库 src（已 Finalise）按其扁平写集 writeKeys
// 字段级合并进 master（s），实现 Vegeta 并行验证批次的"提交写入"。
//
// 语义（AccountState 字段未导出，合并逻辑必须落在 state 包内）：
//   - 仅支持纯内存模式（s 与 src 的 trieDB 均为 nil）；
//   - storage:addr:slot → master 账户 originStorage[slot] = src 当前值
//     （成员已 Finalise，origin 即最终值；批内成员写集两两无交集，无覆盖歧义）；
//   - acct:addr:balance → master 账户 Balance 原地 Set src 值，
//     除非 addr 在 additiveDeltas 中（如 coinbase 的 tip 累积——可交换加法，
//     冲突检测层已过滤该 key，批内多成员同时写时必须增量累加而非覆盖）。
//     增量 delta 由调用方在成员执行时预计算（= 成员最终值 − 成员克隆时
//     master 的值），不能在合并时用"src − 当前 master"重算——同批先合并的
//     成员已推进 master，会污染增量；
//   - acct:addr:nonce   → master 账户 Nonce += 1（可交换计数器累加，见下）；
//   - code 写不被 recorder 记录（SetCode 无记录），以 codeHash 不一致兜底拷贝
//     （覆盖新建合约与 metamorphic 场景）；
//   - src 有而 master 无的账户（写集涉及或兜底扫描）→ 整账户深拷贝（合约创建）；
//   - src 无而 master 有的账户（写集涉及）→ 删除（EIP-158 空账户删除 / 自毁，
//     成员 Finalise 已消化）；
//   - 合并直接落"已提交"值（origin），不经过 journal/dirty——master 在并行
//     阶段不被执行，串行兜底阶段直接在其上执行时 origin 即 committed 语义。
func (s *MemoryStateDB) MergeCommittedFrom(src *MemoryStateDB, writeKeys []string, additiveDeltas map[common.Address]*big.Int) error {
	if s.trieDB != nil || src.trieDB != nil {
		return fmt.Errorf("MergeCommittedFrom 仅支持纯内存模式（无 trie）")
	}

	type slotKey struct {
		addr common.Address
		slot common.Hash
	}
	var (
		involved     = make(map[common.Address]struct{}, len(writeKeys))
		balanceAddrs = make(map[common.Address]struct{})
		nonceAddrs   = make(map[common.Address]struct{})
		storageSlots = make(map[slotKey]struct{})
	)
	for _, k := range writeKeys {
		addr, kind, slot, ok := parseFlatKey(k)
		if !ok {
			return fmt.Errorf("unrecognized flat write key %q", k)
		}
		involved[addr] = struct{}{}
		switch kind {
		case "balance":
			balanceAddrs[addr] = struct{}{}
		case "nonce":
			nonceAddrs[addr] = struct{}{}
		case "storage":
			storageSlots[slotKey{addr, slot}] = struct{}{}
		}
	}

	// 成员新建但写集未覆盖的账户兜底：src 有而 master 无 → 整账户深拷贝。
	// 覆盖"零 endowment 的 CREATE"场景（SetCode 不入扁平写集，
	// 构造器又未写存储时该账户在写集中无任何 key）。
	// src = master 克隆 + 成员执行，故 src 有而 master 无的账户必为成员新建。
	for addr, sAcc := range src.accounts {
		if _, ok := s.accounts[addr]; !ok {
			s.adoptAccount(addr, sAcc.clone())
		}
	}

	// 账户级合并：新建 / 删除 / 字段拷贝
	for addr := range involved {
		sAcc, sOk := src.accounts[addr]
		mAcc, mOk := s.accounts[addr]
		switch {
		case sOk && !mOk:
			// 成员新建的账户：整账户深拷贝（Finalise 后 touched/created 已复位）
			s.adoptAccount(addr, sAcc.clone())
		case !sOk && mOk:
			// 成员删除的账户（Finalise 的 EIP-158 / 自毁删除）
			delete(s.accounts, addr)
		case sOk && mOk:
			// CoW 物化：master 可能仍有存活的 CloneCoW 子库观察着该账户
			//（depurge 并行阶段），原地改会污染子库快照；epoch 不匹配时
			// 先替换为私有副本再改。深拷贝路径（epoch=0）恒 no-op。
			mAcc = s.cowEnsureAccount(addr)
			if _, ok := balanceAddrs[addr]; ok {
				if delta, add := additiveDeltas[addr]; add {
					// 可交换累加（coinbase tip）：master += 调用方预计算的增量。
					// 批内多成员各自持有"克隆时基准 + 自身 tip"，增量累加与串行
					// 累积语义一致；delta 为负时做减法（防御路径）。
					// 注意 uint256.FromBig 第二返回值是 overflow（溢出=true），
					// 非 "ok"，故取 !overflow 判定有效。
					if delta.Sign() < 0 {
						neg := new(big.Int).Neg(delta)
						if d, overflow := uint256.FromBig(neg); !overflow && d != nil {
							mAcc.Balance.Sub(mAcc.Balance, d)
						}
					} else if d, overflow := uint256.FromBig(delta); !overflow && d != nil {
						mAcc.Balance.Add(mAcc.Balance, d)
					}
				} else {
					mAcc.Balance.Set(sAcc.Balance)
				}
			}
		if _, ok := nonceAddrs[addr]; ok {
			// nonce 是可交换计数器：每笔交易执行后 sender nonce 确定性 +1
			//（StateTransition 在 evm.Call 之前 SetNonce(+1)，即使交易 revert
			// 也不回滚；只有 preCheck 失败才不 +1，而那些交易不会进 merge）。
			// 并发合并用「+=1 累加」而非覆盖，保证同 sender 多笔交易并发执行
			// 时 nonce 正确递增——覆盖语义会因多成员各自从相同基准 clone +1
			// 后覆盖合并而丢失递增（serialized-order MISMATCH 的根因）。
			// 注意：EIP-7702 authority 的 SetNonce 是设到 auth.Nonce+1
			// （特定值，非 +1 递增）；Prague 之前的 dataset 不涉及。
			mAcc.Nonce += 1
		}
			// code 兜底：写集不含 code 写，以哈希不一致兜底
			if mAcc.GetCodeHash() != sAcc.GetCodeHash() {
				mAcc.Code = append([]byte(nil), sAcc.Code...)
				mAcc.codeHash = sAcc.GetCodeHash()
			}
		}
	}

	// 存储槽合并：master 账户 originStorage[slot] = src 最终值
	for sk := range storageSlots {
		sAcc, sOk := src.accounts[sk.addr]
		if !sOk {
			// 写了槽但成员库无该账户：异常路径，防御性跳过（保持 master 现状）
			continue
		}
		mAcc, mOk := s.accounts[sk.addr]
		if !mOk {
			// master 缺账户：整账户拷贝补齐（正常不会走到——账户级合并已处理）
			s.adoptAccount(sk.addr, sAcc.clone())
			continue
		}
		// CoW 物化（账户级 + 槽级）：originStorage 可能与存活子库共享，
		// 原地写单槽前必须整 map 私有化。深拷贝路径（epoch=0）恒 no-op。
		mAcc = s.cowEnsureAccount(sk.addr)
		s.cowEnsureOrigin(mAcc, cowCallerMerge)
		mAcc.originStorage[sk.slot] = sAcc.GetState(sk.slot)
		cowStats.slotWrites[cowCallerMerge].Add(1)
	}
	return nil
}

// ExportAccounts 导出全部账户的最终状态快照（按地址排序）。
// dirty 未合并的值防御性一并导出；正常调用前应已 Finalise。
// 用于区块末 MPT 构建（InitAccount 灌入带 trie 的新库）。
func (s *MemoryStateDB) ExportAccounts() []AccountSnapshot {
	out := make([]AccountSnapshot, 0, len(s.accounts))
	for addr, acc := range s.accounts {
		storage := make(map[common.Hash]common.Hash, len(acc.originStorage)+len(acc.dirtyStorage))
		for k, v := range acc.originStorage {
			storage[k] = v
		}
		for k, v := range acc.dirtyStorage {
			storage[k] = v
		}
		out = append(out, AccountSnapshot{
			Addr:    addr,
			Balance: new(uint256.Int).Set(acc.Balance),
			Nonce:   acc.Nonce,
			Code:    append([]byte(nil), acc.Code...),
			Storage: storage,
		})
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Addr[:], out[j].Addr[:]) < 0 })
	return out
}

// ExportFlatState 导出扁平 key→value 最终状态（与 recorder 读写 key 同口径），
// 用于 Vegeta 最终状态与串行基线的 diff 诊断。
// 值统一右对齐 32 字节 hex（与 AccessEntry 的 value 编码一致）；
// 空账户（EIP-161：balance=nonce=code=0 且无 storage）跳过——
// 与"账户不存在"语义等价，避免空账户残留制造假差异。
func (s *MemoryStateDB) ExportFlatState() map[string]string {
	out := make(map[string]string, len(s.accounts)*3)
	for addr, acc := range s.accounts {
		if acc.IsEmpty() && len(acc.originStorage) == 0 && len(acc.dirtyStorage) == 0 {
			continue
		}
		out[FlatBalanceKey(addr)] = HashFromU256(acc.Balance).String()
		out[FlatNonceKey(addr)] = HashFromU64(acc.Nonce).String()
		out[FlatCodeKey(addr)] = acc.GetCodeHash().String()
		for k, v := range acc.originStorage {
			out[FlatStorageKey(addr, k)] = v.String()
		}
		for k, v := range acc.dirtyStorage {
			out[FlatStorageKey(addr, k)] = v.String()
		}
	}
	return out
}

// DiffFlatStates 对比两份扁平状态，返回仅在 a / 仅在 b 出现的 key
// （含同 key 不同值）。两者均为空即状态一致。
func DiffFlatStates(a, b map[string]string) (onlyInA, onlyInB []string) {
	for k, va := range a {
		if vb, ok := b[k]; !ok || vb != va {
			onlyInA = append(onlyInA, k)
		}
	}
	for k, vb := range b {
		if va, ok := a[k]; !ok || va != vb {
			onlyInB = append(onlyInB, k)
		}
	}
	sort.Strings(onlyInA)
	sort.Strings(onlyInB)
	return onlyInA, onlyInB
}

// parseFlatKey 解析扁平 key（storage:0xADDR:0xSLOT / acct:0xADDR:balance|nonce|code）。
func parseFlatKey(k string) (addr common.Address, kind string, slot common.Hash, ok bool) {
	if strings.HasPrefix(k, "storage:") {
		rest := k[len("storage:"):]
		i := strings.IndexByte(rest, ':')
		if i <= 0 {
			return common.Address{}, "", common.Hash{}, false
		}
		addrStr, slotStr := rest[:i], rest[i+1:]
		if !common.IsHexAddress(addrStr) || len(slotStr) != 66 {
			return common.Address{}, "", common.Hash{}, false
		}
		return common.HexToAddress(addrStr), "storage", common.HexToHash(slotStr), true
	}
	if strings.HasPrefix(k, "acct:") {
		rest := k[len("acct:"):]
		i := strings.LastIndexByte(rest, ':')
		if i <= 0 {
			return common.Address{}, "", common.Hash{}, false
		}
		addrStr, kind := rest[:i], rest[i+1:]
		if !common.IsHexAddress(addrStr) {
			return common.Address{}, "", common.Hash{}, false
		}
		switch kind {
		case "balance", "nonce", "code":
			return common.HexToAddress(addrStr), kind, common.Hash{}, true
		}
	}
	return common.Address{}, "", common.Hash{}, false
}
