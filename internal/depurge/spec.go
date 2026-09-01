// Package depurge 实现 depurge 并行重放算法的核心逻辑：
// step1 三臂保守读写集（spec_rw）选择与 LLM key 桥接（本文件），
// step2/3 key 队列事件驱动无屏障调度器（scheduler.go）。
//
// 本包为纯算法包：不依赖 EVM/dataset/state，可独立单测。
package depurge

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// Arm 是 step1 保守读写集的获取臂。
type Arm string

const (
	ArmA Arm = "A" // LLM 优先 + 回退：LLM 成功用 LLM 集，失败回退预执行集
	ArmB Arm = "B" // 并集：预执行集 ∪ LLM 集
	ArmC Arm = "C" // 纯预执行（baseline）
)

// ParseArm 校验并归一化臂参数（大小写不敏感）。
func ParseArm(s string) (Arm, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "A":
		return ArmA, nil
	case "B":
		return ArmB, nil
	case "C":
		return ArmC, nil
	}
	return "", fmt.Errorf("invalid spec arm %q (expect A|B|C)", s)
}

// LLMFailReason 是 LLM 静态分析失败的逐类枚举（step1 统计用）。
type LLMFailReason int

const (
	LLMOK          LLMFailReason = iota
	LLMNotContract               // 交易无目标合约（合约创建 / To 为空）
	LLMNoContract                // 目标合约无 LLM 分析
	LLMNoSelector                // selector 无分析结果（含无 calldata）
	LLMDecodeFail                // calldata 解码失败（无法实例化参数）
	LLMNoStorage                 // 合约无 storage 布局
	LLMUnresolved                // 含未解析动态键
	LLMEmpty                     // 实例化结果为空
)

// String 返回失败类别的稳定名称（用于报告与 map 键）。
func (r LLMFailReason) String() string {
	switch r {
	case LLMOK:
		return "ok"
	case LLMNotContract:
		return "not-contract"
	case LLMNoContract:
		return "no-contract"
	case LLMNoSelector:
		return "no-selector"
	case LLMDecodeFail:
		return "decode-fail"
	case LLMNoStorage:
		return "no-storage"
	case LLMUnresolved:
		return "unresolved"
	case LLMEmpty:
		return "empty"
	}
	return "unknown"
}

// AllLLMFailReasons 按报告顺序列出全部失败类别（不含 LLMOK）。
func AllLLMFailReasons() []LLMFailReason {
	return []LLMFailReason{
		LLMNotContract, LLMNoContract, LLMNoSelector, LLMDecodeFail,
		LLMNoStorage, LLMUnresolved, LLMEmpty,
	}
}

// BridgeLLMKey 把 LLM canonical key（slot:<小写地址>:<slot>）桥接为
// recorder 格式（storage:<EIP-55地址>:<slot>）。
//
// 三臂并集/超集判定前必须统一两种格式，否则判定静默失效。
// slot 经 common.HexToHash 归一化为完整 66 字符小写十六进制，
// 与 state.FlatStorageKey 的 common.Hash.String() 口径一致。
func BridgeLLMKey(k string) (string, bool) {
	parts := strings.Split(k, ":")
	if len(parts) != 3 || parts[0] != "slot" {
		return "", false
	}
	if !strings.HasPrefix(parts[2], "0x") {
		return "", false
	}
	addr := common.HexToAddress(parts[1])
	slot := common.HexToHash(parts[2])
	return "storage:" + addr.Hex() + ":" + slot.Hex(), true
}

// SelectSpec 按臂合并出单笔交易的最终 spec 集合（去重排序）：
//
//   - ArmA：llmOK 时用 LLM 集（+静态合成的 senderBal），否则回退预执行集；
//   - ArmB：预执行集 ∪（llmOK 时 LLM 集 +senderBal）；
//   - ArmC：纯预执行集。
//
// senderBal 是集成层为 LLM 臂静态合成的 sender balance key
// （acct:<From>:balance，gas 真实调度键）；为空则不合成。
// 本函数不做 nonce/coinbase 过滤，由集成层在调度前统一过滤。
func SelectSpec(arm Arm, preKeys, llmKeys []string, llmOK bool, senderBal string) []string {
	set := make(map[string]struct{}, len(preKeys)+len(llmKeys)+1)
	add := func(ks []string) {
		for _, k := range ks {
			set[k] = struct{}{}
		}
	}
	switch arm {
	case ArmA:
		if llmOK {
			add(llmKeys)
			if senderBal != "" {
				set[senderBal] = struct{}{}
			}
		} else {
			add(preKeys)
		}
	case ArmB:
		add(preKeys)
		if llmOK {
			add(llmKeys)
			if senderBal != "" {
				set[senderBal] = struct{}{}
			}
		}
	default: // ArmC
		add(preKeys)
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
