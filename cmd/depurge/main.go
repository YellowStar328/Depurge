// depurge 是面向并发控制科研的以太坊主网交易离线重放沙盒。
//
// 从 dataset 目录读取区块（header + transactions + witness），
// 在基于 go-ethereum core/vm 的内存 EVM 中重放交易，采集执行耗时与
// slot 级读写集。支持三种算法（--replay-serial / --replay-vegeta / --replay-depurge），
// 结果统一写入根目录 run-summary.log：每次命令覆盖写，命令内多算法依次追加写。
package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/spf13/cobra"

	"depurge/internal/dataset"
	"depurge/internal/output"
	"depurge/internal/replay"
	"depurge/internal/state"
)

func main() {
	var (
		datasetDir     string
		outputDir      string
		blocks         string
		runs           int
		compare        bool
		noRecord       bool
		granularity    string
		collectBalance bool
		collectNonce   bool
		mptPerTx       bool

		// 算法选择开关
		replaySerial  bool
		replayVegeta  bool
		replayDepurge bool

		// vegeta 专属
		vParallelism    int
		vEdgeOrder      string
		vSerialOrder    string
		vFilterNonce    bool
		vFilterCoinbase bool

		// depurge 专属
		dArm    string
		dLLMDir string

		// 性能分析
		cpuProfile string
	)

	rootCmd := &cobra.Command{
		Use:   "depurge",
		Short: "以太坊主网交易离线重放沙盒（并发控制科研）",
	}

	replayCmd := &cobra.Command{
		Use:   "replay",
		Short: "重放 dataset 交易：串行（--replay-serial）、Vegeta 并行（--replay-vegeta）、Depurge 并行（--replay-depurge）",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cpuProfile != "" {
				pf, err := os.Create(cpuProfile)
				if err != nil {
					return fmt.Errorf("create cpu profile: %w", err)
				}
				defer pf.Close()
				if err := pprof.StartCPUProfile(pf); err != nil {
					return err
				}
				defer pprof.StopCPUProfile()
			}
			if datasetDir == "" {
				return fmt.Errorf("--dataset is required")
			}
			if !replaySerial && !replayVegeta && !replayDepurge {
				return fmt.Errorf("no algorithm selected: enable --replay-serial and/or --replay-vegeta and/or --replay-depurge")
			}
			loader, err := dataset.NewLoader(datasetDir)
			if err != nil {
				return err
			}

			// Runs 固定为 1：vegeta 前提（读写集是调度核心输入）。
			cfg := replay.DefaultConfig()
			cfg.Runs = 1
			cfg.RecordRW = !noRecord
			cfg.Compare = compare
			cfg.MPTPerTx = mptPerTx
			cfg.StateConfig = state.DefaultConfig()
			switch granularity {
			case "slot":
				cfg.StateConfig.Granularity = state.GranularitySlot
			case "account":
				cfg.StateConfig.Granularity = state.GranularityAccount
			default:
				return fmt.Errorf("invalid --rwset-granularity %q (expect slot|account)", granularity)
			}
			cfg.StateConfig.CollectBalance = collectBalance
			cfg.StateConfig.CollectNonce = collectNonce

			if outputDir == "" {
				outputDir = datasetDir + "/results"
			}
			writer, err := output.NewWriter(outputDir)
			if err != nil {
				return err
			}

			r := replay.NewReplayer(loader, cfg)

			// run-summary.log：每次命令覆盖写，命令内多算法依次追加写。
			f, err := os.OpenFile("run-summary.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				return fmt.Errorf("open run-summary.log: %w", err)
			}
			defer f.Close()

			// 1) 串行执行算法（之前 run-summary.log 的串行重放：EVM/MPT/receipt 耗时）
			if replaySerial {
				fmt.Printf("depurge serial: dataset=%s blocks=%s runs=%d\n", datasetDir, blocksOrAll(blocks), runs)
				if err := r.RunSerial(writer, f, blocks, runs); err != nil {
					return err
				}
			}

			// 2) Vegeta 并行算法（各阶段耗时 + state-diff，预执行不计入总耗时）
			if replayVegeta {
				vcfg := replay.VegetaConfig{
					Parallelism:    vParallelism,
					EdgeOrder:      vEdgeOrder,
					SerialOrder:    vSerialOrder,
					FilterNonce:    vFilterNonce,
					FilterCoinbase: vFilterCoinbase,
					StateConfig:    cfg.StateConfig,
				}
				fmt.Printf("depurge vegeta: dataset=%s blocks=%s runs=%d parallelism=%s edge-order=%s serial-order=%s filter-nonce=%v filter-coinbase=%v\n",
					datasetDir, blocksOrAll(blocks), runs, parallelismOrAuto(vParallelism),
					vEdgeOrder, vSerialOrder, vFilterNonce, vFilterCoinbase)
				if err := r.RunVegeta(vcfg, blocks, runs, f); err != nil {
					return err
				}
			}

			// 3) Depurge 并行算法（三臂 spec_rw + key 队列事件驱动调度 + state-diff）
			if replayDepurge {
				dcfg := replay.DepurgeConfig{
					Arm:            dArm,
					LLMDir:         dLLMDir,
					Parallelism:    vParallelism,
					FilterNonce:    vFilterNonce,
					FilterCoinbase: vFilterCoinbase,
					StateConfig:    cfg.StateConfig,
				}
				fmt.Printf("depurge depurge: dataset=%s blocks=%s runs=%d parallelism=%s spec-arm=%s filter-nonce=%v filter-coinbase=%v\n",
					datasetDir, blocksOrAll(blocks), runs, parallelismOrAuto(vParallelism),
					dArm, vFilterNonce, vFilterCoinbase)
				if err := r.RunDepurge(dcfg, blocks, runs, f); err != nil {
					return err
				}
			}
			return nil
		},
	}

	replayCmd.Flags().StringVar(&datasetDir, "dataset", "", "dataset 目录路径（必填）")
	replayCmd.Flags().StringVar(&outputDir, "output", "", "结果输出目录（默认 <dataset>/results）")
	replayCmd.Flags().StringVar(&blocks, "blocks", "", "区块范围过滤，如 24000000-24000005（默认全部）")
	replayCmd.Flags().BoolVar(&compare, "compare", false, "与 dataset 自带 canonical/rwsets 对比")
	replayCmd.Flags().BoolVar(&noRecord, "no-record", false, "跳过读写集采集（纯性能基准）")
	replayCmd.Flags().StringVar(&granularity, "rwset-granularity", "slot", "读写集粒度: slot|account")
	replayCmd.Flags().BoolVar(&collectBalance, "collect-balance", true, "采集 balance 读写")
	replayCmd.Flags().BoolVar(&collectNonce, "collect-nonce", true, "采集 nonce 读写")
	replayCmd.Flags().BoolVar(&mptPerTx, "mpt-per-tx", false, "MPT 提交口径：true=每笔交易提交；false=区块结束一次（默认）")

	replayCmd.Flags().BoolVar(&replaySerial, "replay-serial", true, "运行串行执行算法（EVM/MPT/receipt 耗时）")
	replayCmd.Flags().BoolVar(&replayVegeta, "replay-vegeta", true, "运行 Vegeta 并行算法（各阶段耗时 + state-diff）")
	replayCmd.Flags().BoolVar(&replayDepurge, "replay-depurge", false, "运行 Depurge 并行算法（三臂 spec_rw + key 队列事件驱动调度 + state-diff）")

	replayCmd.Flags().IntVar(&vParallelism, "parallelism", 0, "vegeta/depurge worker 数（<=0 = runtime.NumCPU()）")
	replayCmd.Flags().StringVar(&vEdgeOrder, "edge-order", "new", "vegeta DAG 冲突边定向：new=聚簇序；original=原始区块序")
	replayCmd.Flags().StringVar(&vSerialOrder, "serial-order", "block", "vegeta 串行兜底顺序：block=原始区块序；hash=交易哈希字典序")
	replayCmd.Flags().BoolVar(&vFilterNonce, "filter-nonce", true, "vegeta/depurge 聚簇/建边/验证时过滤 nonce 伪冲突 key")
	replayCmd.Flags().BoolVar(&vFilterCoinbase, "filter-coinbase", true, "vegeta/depurge 过滤 coinbase 的 balance tip 写 key")

	replayCmd.Flags().StringVar(&dArm, "spec-arm", "C", "depurge step1 读写集获取臂：A=LLM 优先+回退；B=并集；C=纯预执行")
	replayCmd.Flags().StringVar(&dLLMDir, "llm-dir", "llm/mainnet_rw", "depurge A/B 臂的 LLM 静态分析数据目录")
	replayCmd.Flags().IntVar(&runs, "runs", 1, "每区块整管线重复轮数，串行/vegeta/depurge 共用（>1 取平均，减少测量噪声）")
	replayCmd.Flags().StringVar(&cpuProfile, "cpuprofile", "", "写 CPU profile 到指定路径（性能分析用，可选）")

	rootCmd.AddCommand(replayCmd)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func blocksOrAll(b string) string {
	if b == "" {
		return "all"
	}
	return b
}

func parallelismOrAuto(n int) string {
	if n <= 0 {
		return fmt.Sprintf("auto (%d cpus)", runtime.NumCPU())
	}
	return fmt.Sprintf("%d", n)
}
