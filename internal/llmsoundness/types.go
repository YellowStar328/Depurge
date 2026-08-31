// Package llmsoundness 用 dataset 自带的 canonical rwsets 作为 ground truth，
// 离线评测 LLM 静态分析（llm/mainnet_rw）产出的读写集保守度：
//   - 漏报率（recall）：canonical 实际访问的 key，有多少被 LLM 实例化后的 key 集覆盖。
//   - 多报率（precision）：LLM 声称的 key，有多少实际被访问。
package llmsoundness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// Contract 是一个被 LLM 分析的合约（llm/mainnet_rw/<addr>/ 目录）。
type Contract struct {
	Address common.Address
	Dir     string

	Meta    ContractMeta
	Storage StorageLayout
	ABI     abi.ABI
	// Selectors 是 selector -> 函数分析结果（reads/writes 的 (account, field) 对）。
	Selectors map[string]*FuncAnalysis
	// SelectorNames 是 selector -> 函数名（来自 funcs.json，用于诊断输出）。
	SelectorNames map[string]string
}

// ContractMeta 对应 meta.json。
type ContractMeta struct {
	Address        string `json:"address"`
	ContractName   string `json:"contract_name"`
	CompilerVer    string `json:"compiler_version"`
	Verified       bool   `json:"verified"`
	Proxy          string `json:"proxy"`
	Implementation string `json:"implementation"`
}

// StorageLayout 对应 storage.json。
type StorageLayout struct {
	Storage []StorageVar        `json:"storage"`
	Types   map[string]TypeDesc `json:"types"`
}

// StorageVar 是单个 storage 变量。
type StorageVar struct {
	Label    string `json:"label"`
	Slot     string `json:"slot"`
	Offset   int    `json:"offset"`
	Type     string `json:"type"`
	Contract string `json:"contract"`
}

// TypeDesc 对应 storage.json 里 types 的每一项。
type TypeDesc struct {
	Encoding      string         `json:"encoding"` // inplace / mapping / bytes / dynamic_array
	Label         string         `json:"label"`
	NumberOfBytes string         `json:"numberOfBytes"`
	Key           string         `json:"key,omitempty"`
	Value         string         `json:"value,omitempty"`
	Base          string         `json:"base,omitempty"`
	Members       []StructMember `json:"members,omitempty"` // struct 类型的成员列表
}

// StructMember 是 struct 类型的一个成员。Slot 是成员相对 struct 起始的
// word 偏移（十进制字符串），Offset 是 word 内字节偏移。
type StructMember struct {
	Label  string `json:"label"`
	Slot   string `json:"slot"`
	Offset int    `json:"offset"`
	Type   string `json:"type"`
}

// HasStorage 报告合约是否带 storage 布局（storage.json）。缺布局的合约
// 所有 field 都无法实例化，应单独归类，避免污染整体 recall/precision。
func (c *Contract) HasStorage() bool {
	return len(c.Storage.Storage) > 0 || len(c.Storage.Types) > 0
}

// FuncAnalysis 是单个 selector 的 LLM 分析结果（0x<selector>.json）。
type FuncAnalysis struct {
	Reads  []Access `json:"reads"`
	Writes []Access `json:"writes"`
}

// Access 是一条 (account, field) 访问声明。
type Access struct {
	Account string `json:"account"` // global / msg.sender / addr1 / addr2 / 其他符号
	Field   string `json:"field"`   // storage 变量名（label）
}

// LoadContracts 加载 llm/mainnet_rw 下所有合约目录。
func LoadContracts(llmDir string) (map[common.Address]*Contract, error) {
	entries, err := os.ReadDir(llmDir)
	if err != nil {
		return nil, err
	}
	contracts := make(map[common.Address]*Contract)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(llmDir, e.Name())
		metaPath := filepath.Join(dir, "meta.json")
		if _, err := os.Stat(metaPath); err != nil {
			continue
		}
		c, err := loadContract(dir)
		if err != nil {
			continue // 跳过无法解析的合约，不影响整体
		}
		contracts[c.Address] = c
	}
	return contracts, nil
}

func loadContract(dir string) (*Contract, error) {
	c := &Contract{Dir: dir, Selectors: map[string]*FuncAnalysis{}, SelectorNames: map[string]string{}}

	if raw, err := os.ReadFile(filepath.Join(dir, "meta.json")); err == nil {
		json.Unmarshal(raw, &c.Meta)
	}
	c.Address = common.HexToAddress(c.Meta.Address)

	if raw, err := os.ReadFile(filepath.Join(dir, "storage.json")); err == nil {
		json.Unmarshal(raw, &c.Storage)
	}

	if raw, err := os.ReadFile(filepath.Join(dir, "abi.json")); err == nil {
		if parsed, err := abi.JSON(strings.NewReader(string(raw))); err == nil {
			c.ABI = parsed
		}
	}

	// funcs.json -> selector name 映射
	if raw, err := os.ReadFile(filepath.Join(dir, "funcs.json")); err == nil {
		var funcs []struct {
			Selector string `json:"selector"`
			Name     string `json:"name"`
		}
		if json.Unmarshal(raw, &funcs) == nil {
			for _, f := range funcs {
				c.SelectorNames[strings.ToLower(f.Selector)] = f.Name
			}
		}
	}

	// 加载所有 0x<selector>.json
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "0x") || !strings.HasSuffix(name, ".json") {
			continue
		}
		sel := strings.ToLower(strings.TrimSuffix(name, ".json"))
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		var fa FuncAnalysis
		if json.Unmarshal(raw, &fa) != nil {
			continue
		}
		c.Selectors[sel] = &fa
	}
	return c, nil
}

// SortedContracts 按地址排序返回合约切片（确定性输出）。
func SortedContracts(m map[common.Address]*Contract) []*Contract {
	out := make([]*Contract, 0, len(m))
	for _, c := range m {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.Compare(out[i].Address.Hex(), out[j].Address.Hex()) < 0
	})
	return out
}
