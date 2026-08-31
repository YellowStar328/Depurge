package llmsoundness

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// InstResult 是一次实例化的结果。
type InstResult struct {
	// Keys 是实例化出的具体 key（slot:<addr>:<slot>）。
	Keys map[string]struct{}
	// Unresolved 是无法实例化的声明条数。
	Unresolved int
	// UnresolvedDetail 按 "原因:field@account" 聚合。
	UnresolvedDetail map[string]int
}

func NewInstResult() *InstResult {
	return &InstResult{Keys: map[string]struct{}{}, UnresolvedDetail: map[string]int{}}
}

func (r *InstResult) addKey(k string) { r.Keys[k] = struct{}{} }

func (r *InstResult) addUnresolved(reason, account string) {
	r.Unresolved++
	r.UnresolvedDetail[reason+"@"+account]++
}

// Instantiate 把某个 selector 在 mode（"read"/"write"）下的 (account, field)
// 声明实例化成具体 key。sender 用于解析 msg.sender；args 是 ABI 解码出的
// 参数（按 ABI 顺序），用于解析 addr1/addr2。
//
// 对 mapping field，LLM 会按 key 层级「由外到内」对同一 field 输出多条记录
// （嵌套 mapping 每层一条）。这里按 mapping 深度把同 field 的记录分块，逐块
// 链式计算 slot，并在末端 value 为 struct 时展开全部成员 slot。
func (c *Contract) Instantiate(sel, mode string, sender common.Address, args []any) *InstResult {
	res := NewInstResult()
	var accs []Access
	switch mode {
	case "read":
		accs = c.Selectors[sel].Reads
	case "write":
		accs = c.Selectors[sel].Writes
	}

	// 按 field 分组（保留出现顺序），供嵌套 mapping 链式解析。
	byField := map[string][]Access{}
	var order []string
	for _, a := range accs {
		f := normalizeLabel(a.Field)
		if _, ok := byField[f]; !ok {
			order = append(order, f)
		}
		byField[f] = append(byField[f], a)
	}
	for _, f := range order {
		c.instantiateField(res, f, byField[f], sender, args)
	}
	return res
}

func (c *Contract) instantiateField(res *InstResult, field string, entries []Access, sender common.Address, args []any) {
	si, ok := c.labelIndex()[field]
	if !ok {
		for _, a := range entries {
			res.addUnresolved("unknown-field:"+a.Field, a.Account)
		}
		return
	}
	td, hasTd := c.Storage.Types[si.Type]
	if !hasTd {
		// 原始标量类型（t_bool / t_uintN / t_address / t_string_storage / t_bytesN
		// 等）在很多合约的 types 表里被省略。它们都落在 base slot，直接发 base。
		// 只有 mapping/struct/array 这类需要类型细节的才记 unknown-type。
		if !strings.HasPrefix(si.Type, "t_mapping") &&
			!strings.Contains(si.Type, "t_struct") &&
			!strings.Contains(si.Type, "t_array") {
			if slot, ok := parseSlot(si.Slot); ok {
				res.addKey(c.CanonicalKey(slot))
				return
			}
		}
		for _, a := range entries {
			res.addUnresolved("unknown-type:"+si.Type, a.Account)
		}
		return
	}

	if td.Encoding == "mapping" {
		keyTypes := c.mappingKeyChain(si.Type)
		depth := len(keyTypes)
		if depth == 0 {
			for _, a := range entries {
				res.addUnresolved("bad-mapping:"+a.Field, a.Account)
			}
			return
		}
		// 按深度把同 field 的记录切成 key 链。
		for i := 0; i < len(entries); i += depth {
			end := i + depth
			if end > len(entries) {
				end = len(entries)
			}
			chunk := entries[i:end]
			if len(chunk) < depth {
				for _, a := range chunk {
					res.addUnresolved("partial-chain:"+a.Field, a.Account)
				}
				continue
			}
			c.instantiateChain(res, si, keyTypes, chunk, sender, args)
		}
		return
	}

	// 非 mapping（inplace / bytes / dynamic_array）：slot 与 account 无关，
	// 整个 field 只解析一次。
	c.instantiateScalarField(res, si, td, entries)
}

// instantiateChain 处理一条嵌套 mapping 的 key 链（chunk 长度 == keyTypes 长度）。
func (c *Contract) instantiateChain(res *InstResult, si StorageVar, keyTypes []string, chunk []Access, sender common.Address, args []any) {
	slot, ok := parseSlot(si.Slot)
	if !ok {
		res.addUnresolved("bad-slot:"+chunk[0].Field, chunk[0].Account)
		return
	}
	cur := slot
	for i, a := range chunk {
		if keyTypes[i] != "t_address" {
			// 非地址 key（tokenId / poolId / bytes32 等）无法从 account 符号静态解析。
			res.addUnresolved("dynamic-key:"+a.Field, a.Account)
			return
		}
		keyAddr, ok := c.resolveAccountKey(a.Account, sender, args)
		if !ok {
			res.addUnresolved("dynamic-key:"+a.Field, a.Account)
			return
		}
		cur = mappingSlot(keyAddr.Bytes(), cur)
	}
	// cur 是末端 value 的 slot；按 value 类型展开（struct 展开成员）。
	c.emitValue(res, c.mappingValueType(si.Type), cur)
}

// instantiateScalarField 处理非 mapping field。
func (c *Contract) instantiateScalarField(res *InstResult, si StorageVar, td TypeDesc, entries []Access) {
	slot, ok := parseSlot(si.Slot)
	if !ok {
		for _, a := range entries {
			res.addUnresolved("bad-slot:"+a.Field, a.Account)
		}
		return
	}
	if td.Encoding == "inplace" {
		c.emitValue(res, si.Type, slot)
		return
	}
	// bytes / dynamic_array：发 base（长度/指针）slot。
	res.addKey(c.CanonicalKey(slot))
}

// emitValue 按 value 类型在 slot 处展开：struct 展开成员 word，其余发单个 slot。
func (c *Contract) emitValue(res *InstResult, valueType string, slot common.Hash) {
	if td, ok := c.Storage.Types[valueType]; ok && len(td.Members) > 0 {
		for _, s := range c.structSlots(td, slot, 0) {
			res.addKey(c.CanonicalKey(s))
		}
		return
	}
	res.addKey(c.CanonicalKey(slot))
}

// structSlots 返回 struct 占据的全部 word slot（base + 各成员 word 偏移，去重）。
// 成员若本身是 struct 则递归展开；数组/mapping 成员发其所在 word（长度/指针槽）。
func (c *Contract) structSlots(td TypeDesc, base common.Hash, depth int) []common.Hash {
	if depth > 8 {
		return []common.Hash{base}
	}
	seen := map[common.Hash]struct{}{}
	var out []common.Hash
	add := func(s common.Hash) {
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	for _, m := range td.Members {
		off, ok := parseSlot(m.Slot)
		if !ok {
			continue
		}
		ms := addSlot(base, off)
		add(ms)
		if mt, ok := c.Storage.Types[m.Type]; ok && len(mt.Members) > 0 {
			for _, s := range c.structSlots(mt, ms, depth+1) {
				add(s)
			}
		}
	}
	if len(out) == 0 {
		out = append(out, base)
	}
	return out
}

// mappingKeyChain 返回 mapping 类型每层的 key 类型（按嵌套顺序，外→内）。
func (c *Contract) mappingKeyChain(tid string) []string {
	var keys []string
	cur := tid
	for {
		td, ok := c.Storage.Types[cur]
		if !ok || td.Encoding != "mapping" {
			break
		}
		keys = append(keys, td.Key)
		cur = td.Value
	}
	return keys
}

// mappingValueType 返回 mapping 的末端 value 类型 id。
func (c *Contract) mappingValueType(tid string) string {
	cur := tid
	for {
		td, ok := c.Storage.Types[cur]
		if !ok || td.Encoding != "mapping" {
			return cur
		}
		cur = td.Value
	}
}

// CanonicalKey 把 slot 拼成与 dataset rwset 对齐的 key。
func (c *Contract) CanonicalKey(slot common.Hash) string {
	return "slot:" + strings.ToLower(c.Address.Hex()) + ":" + slot.Hex()
}

func (c *Contract) labelIndex() map[string]StorageVar {
	idx := make(map[string]StorageVar, len(c.Storage.Storage)*2)
	for _, v := range c.Storage.Storage {
		idx[v.Label] = v
		idx[normalizeLabel(v.Label)] = v
	}
	return idx
}

func normalizeLabel(s string) string { return strings.TrimPrefix(s, "_") }

// resolveAccountKey 把 account 符号解析成具体地址。
func (c *Contract) resolveAccountKey(account string, sender common.Address, args []any) (common.Address, bool) {
	switch account {
	case "msg.sender":
		return sender, true
	case "addr1":
		if a, ok := nthAddress(args, 0); ok {
			return a, true
		}
	case "addr2":
		if a, ok := nthAddress(args, 1); ok {
			return a, true
		}
	}
	return common.Address{}, false
}

func nthAddress(args []any, n int) (common.Address, bool) {
	idx := 0
	for _, a := range args {
		if addr, ok := a.(common.Address); ok {
			if idx == n {
				return addr, true
			}
			idx++
		}
	}
	return common.Address{}, false
}

func parseSlot(s string) (common.Hash, bool) {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return common.Hash{}, false
	}
	return common.BigToHash(v), true
}

// addSlot 返回 slot + off。
func addSlot(slot common.Hash, off common.Hash) common.Hash {
	v := new(big.Int).Add(slot.Big(), off.Big())
	return common.BigToHash(v)
}

// mappingSlot 按 Solidity mapping 布局算 key 槽：
// slot = keccak(pad(key) || pad(base_slot))。
func mappingSlot(key []byte, baseSlot common.Hash) common.Hash {
	var buf []byte
	if len(key) >= 32 {
		buf = append(buf, key[:32]...)
	} else {
		buf = make([]byte, 32)
		copy(buf[32-len(key):], key)
	}
	buf = append(buf, baseSlot.Bytes()...)
	return crypto.Keccak256Hash(buf)
}
