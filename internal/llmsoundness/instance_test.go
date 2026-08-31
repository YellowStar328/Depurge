package llmsoundness

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// testAddr 是合约地址（实例化 key 的前缀）。
var testAddr = common.HexToAddress("0x1111111111111111111111111111111111111111")

// pad32 把 b 左填 0 到 32 字节。
func pad32(b []byte) []byte {
	if len(b) >= 32 {
		return b[:32]
	}
	out := make([]byte, 32)
	copy(out[32-len(b):], b)
	return out
}

// expectMapping 计算 keccak(pad(key)||pad(base))。
func expectMapping(key []byte, base common.Hash) common.Hash {
	buf := append(pad32(key), base.Bytes()...)
	return crypto.Keccak256Hash(buf)
}

func hashFromInt(v int64) common.Hash { return common.BigToHash(big.NewInt(v)) }

// TestNestedMappingChain 锁定嵌套 mapping 的链式 slot 计算：
// allowed[from][msg.sender] = keccak(pad(sender)||keccak(pad(from)||pad(base)))。
func TestNestedMappingChain(t *testing.T) {
	from := common.HexToAddress("0xaaaa000000000000000000000000000000000001")
	sender := common.HexToAddress("0xbbbb000000000000000000000000000000000002")
	c := &Contract{
		Address: testAddr,
		Storage: StorageLayout{
			Storage: []StorageVar{{Label: "allowed", Slot: "5", Type: "t_mapping(t_address,t_mapping(t_address,t_uint256))"}},
			Types: map[string]TypeDesc{
				"t_mapping(t_address,t_mapping(t_address,t_uint256))": {Encoding: "mapping", Key: "t_address", Value: "t_mapping(t_address,t_uint256)"},
				"t_mapping(t_address,t_uint256)":                      {Encoding: "mapping", Key: "t_address", Value: "t_uint256"},
			},
		},
		Selectors: map[string]*FuncAnalysis{
			"0x23b872dd": {Reads: []Access{{Account: "addr1", Field: "allowed"}, {Account: "msg.sender", Field: "allowed"}}},
		},
	}
	res := c.Instantiate("0x23b872dd", "read", sender, []any{from, sender, big.NewInt(1)})
	if res.Unresolved != 0 {
		t.Fatalf("expected no unresolved, got %d (%v)", res.Unresolved, res.UnresolvedDetail)
	}
	inner := expectMapping(from.Bytes(), hashFromInt(5))
	want := expectMapping(sender.Bytes(), inner)
	wantKey := c.CanonicalKey(want)
	if _, ok := res.Keys[wantKey]; !ok {
		t.Fatalf("missing chained key %s; got %v", wantKey, res.Keys)
	}
	if len(res.Keys) != 1 {
		t.Fatalf("expected exactly 1 key, got %d: %v", len(res.Keys), res.Keys)
	}
}

// TestStructExpansion 锁定 mapping 命中 struct 时展开全部成员 word（去重）。
func TestStructExpansion(t *testing.T) {
	owner := common.HexToAddress("0xcccc000000000000000000000000000000000003")
	c := &Contract{
		Address: testAddr,
		Storage: StorageLayout{
			Storage: []StorageVar{{Label: "positions", Slot: "3", Type: "t_mapping(t_address,t_struct(Pos))"}},
			Types: map[string]TypeDesc{
				"t_mapping(t_address,t_struct(Pos))": {Encoding: "mapping", Key: "t_address", Value: "t_struct(Pos)"},
				"t_struct(Pos)": {Encoding: "inplace", Members: []StructMember{
					{Label: "a", Slot: "0", Type: "t_uint128"},
					{Label: "b", Slot: "1", Type: "t_uint128"},
					{Label: "c", Slot: "1", Type: "t_uint128"}, // 与 b 同 word，应去重
					{Label: "d", Slot: "2", Type: "t_uint256"},
				}},
			},
		},
		Selectors: map[string]*FuncAnalysis{
			"0xdeadbeef": {Reads: []Access{{Account: "addr1", Field: "positions"}}},
		},
	}
	res := c.Instantiate("0xdeadbeef", "read", common.Address{}, []any{owner})
	if res.Unresolved != 0 {
		t.Fatalf("expected no unresolved, got %d (%v)", res.Unresolved, res.UnresolvedDetail)
	}
	base := expectMapping(owner.Bytes(), hashFromInt(3))
	// 成员 word 偏移 {0,1,2} → 3 个 slot（b/c 同 word 去重）。
	for _, off := range []int64{0, 1, 2} {
		s := addSlot(base, hashFromInt(off))
		key := c.CanonicalKey(s)
		if _, ok := res.Keys[key]; !ok {
			t.Fatalf("missing struct member slot %s", key)
		}
	}
	if len(res.Keys) != 3 {
		t.Fatalf("expected 3 deduped member slots, got %d: %v", len(res.Keys), res.Keys)
	}
}

// TestPrimitiveFallback 锁定：原始标量类型（t_bool）不在 types 表时按 base slot 发。
func TestPrimitiveFallback(t *testing.T) {
	c := &Contract{
		Address: testAddr,
		Storage: StorageLayout{
			Storage: []StorageVar{{Label: "paused", Slot: "8", Type: "t_bool"}},
			Types:   map[string]TypeDesc{}, // t_bool 故意不放
		},
		Selectors: map[string]*FuncAnalysis{
			"0xcafebabe": {Reads: []Access{{Account: "global", Field: "paused"}}},
		},
	}
	res := c.Instantiate("0xcafebabe", "read", common.Address{}, nil)
	if res.Unresolved != 0 {
		t.Fatalf("expected no unresolved, got %d (%v)", res.Unresolved, res.UnresolvedDetail)
	}
	want := c.CanonicalKey(hashFromInt(8))
	if _, ok := res.Keys[want]; !ok {
		t.Fatalf("missing primitive base slot %s; got %v", want, res.Keys)
	}
}

// TestNonAddressKeyUnresolved 锁定：非地址 key（如 tokenId）保持 unresolved，不瞎猜。
func TestNonAddressKeyUnresolved(t *testing.T) {
	c := &Contract{
		Address: testAddr,
		Storage: StorageLayout{
			Storage: []StorageVar{{Label: "_owners", Slot: "4", Type: "t_mapping(t_uint256,t_address)"}},
			Types: map[string]TypeDesc{
				"t_mapping(t_uint256,t_address)": {Encoding: "mapping", Key: "t_uint256", Value: "t_address"},
			},
		},
		Selectors: map[string]*FuncAnalysis{
			"0x6352211e": {Reads: []Access{{Account: "global", Field: "_owners"}}},
		},
	}
	res := c.Instantiate("0x6352211e", "read", common.Address{}, []any{big.NewInt(42)})
	if res.Unresolved != 1 {
		t.Fatalf("expected 1 unresolved (dynamic uint key), got %d (%v)", res.Unresolved, res.UnresolvedDetail)
	}
	if len(res.Keys) != 0 {
		t.Fatalf("expected no keys for dynamic key, got %v", res.Keys)
	}
}
