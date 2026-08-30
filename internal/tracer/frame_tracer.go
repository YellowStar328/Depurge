// Package tracer 提供基于 go-ethereum tracing.Hooks 的调用帧追踪器，
// 将每个 EVM 调用帧（CALL/DELEGATECALL/CREATE 等）与 AccessRecorder
// 中的读写集记录通过 frame_id 关联，形成树形调用结构。
package tracer

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/vm"

	"depurge/internal/state"
)

// FrameTracer 维护当前交易的 EVM 调用栈。
//
// per-tx 实例：每笔交易开始时创建，与 per-tx AccessRecorder 绑定。
// OnEnter 将新帧入栈（在 recorder 中创建 CallFrame 节点），
// OnExit 写入帧结果并弹栈。读写集记录通过 recorder 当前帧自动归属。
type FrameTracer struct {
	recorder *state.AccessRecorder
	hooks    *tracing.Hooks
}

// NewFrameTracer 创建 per-tx 帧追踪器。
func NewFrameTracer(recorder *state.AccessRecorder) *FrameTracer {
	ft := &FrameTracer{recorder: recorder}
	ft.hooks = &tracing.Hooks{
		OnEnter: ft.onEnter,
		OnExit:  ft.onExit,
	}
	return ft
}

// Hooks 返回注入 vm.Config.Tracer 的钩子集。
func (ft *FrameTracer) Hooks() *tracing.Hooks { return ft.hooks }

// onEnter 子调用入栈：在 recorder 中创建子 CallFrame。
func (ft *FrameTracer) onEnter(depth int, typ byte, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	ft.recorder.NewFrame(opcodeName(typ), from, to, depth)
}

// onExit 子调用出栈：写入帧结果（gas/revert/err）并弹栈。
func (ft *FrameTracer) onExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	ft.recorder.FinishFrame(depth, gasUsed, reverted, errStr)
}

// opcodeName 将 EVM opcode 字节转换为调用类型名称。
func opcodeName(typ byte) string {
	switch vm.OpCode(typ) {
	case vm.CALL:
		return "CALL"
	case vm.CALLCODE:
		return "CALLCODE"
	case vm.DELEGATECALL:
		return "DELEGATECALL"
	case vm.STATICCALL:
		return "STATICCALL"
	case vm.CREATE:
		return "CREATE"
	case vm.CREATE2:
		return "CREATE2"
	case vm.SELFDESTRUCT:
		return "SELFDESTRUCT"
	default:
		return "CALL"
	}
}
