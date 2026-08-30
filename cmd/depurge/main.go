// depurge 是面向并发控制科研的以太坊主网交易离线重放沙盒。
//
// 从 dataset 目录读取区块（header + transactions + witness），
// 在基于 go-ethereum core/vm 的内存 EVM 中按原始顺序重放交易，
// 精确采集每笔交易的执行耗时与 slot 级读写集（含深层嵌套子调用），
// 以 JSONL 格式输出到 results/ 目录。
package main

import (
	"fmt"
	"os"

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
		runs           int
		blocks         string
		compare        bool
		noRecord       bool
		granularity    string
		collectBalance bool
		collectNonce   bool
		mptPerTx       bool
	)

	rootCmd := &cobra.Command{
		Use:   "depurge",
		Short: "以太坊主网交易离线重放沙盒（并发控制科研）",
	}

	replayCmd := &cobra.Command{
		Use:   "replay",
		Short: "重放 dataset 中的交易并采集耗时与读写集",
		RunE: func(cmd *cobra.Command, args []string) error {
			if datasetDir == "" {
				return fmt.Errorf("--dataset is required")
			}
			loader, err := dataset.NewLoader(datasetDir)
			if err != nil {
				return err
			}

			cfg := replay.DefaultConfig()
			cfg.Runs = runs
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

			// 输出目录：默认 dataset 下的 results/
			if outputDir == "" {
				outputDir = datasetDir + "/results"
			}
			writer, err := output.NewWriter(outputDir)
			if err != nil {
				return err
			}

			fmt.Printf("depurge replay: dataset=%s blocks=%s runs=%d record=%v granularity=%s\n",
				datasetDir, blocksOrAll(blocks), runs, !noRecord, granularity)

			r := replay.NewReplayer(loader, cfg)
			return r.Run(writer, blocks)
		},
	}

	replayCmd.Flags().StringVar(&datasetDir, "dataset", "", "dataset 目录路径（必填）")
	replayCmd.Flags().StringVar(&outputDir, "output", "", "结果输出目录（默认 <dataset>/results）")
	replayCmd.Flags().IntVar(&runs, "runs", 1, "每笔交易执行次数（>1 输出各次耗时与中位数）")
	replayCmd.Flags().StringVar(&blocks, "blocks", "", "区块范围过滤，如 24000000-24000005（默认全部）")
	replayCmd.Flags().BoolVar(&compare, "compare", false, "与 dataset 自带 canonical/rwsets 对比")
	replayCmd.Flags().BoolVar(&noRecord, "no-record", false, "跳过读写集采集（纯性能基准）")
	replayCmd.Flags().StringVar(&granularity, "rwset-granularity", "slot", "读写集粒度: slot|account")
	replayCmd.Flags().BoolVar(&collectBalance, "collect-balance", true, "采集 balance 读写")
	replayCmd.Flags().BoolVar(&collectNonce, "collect-nonce", true, "采集 nonce 读写")
	replayCmd.Flags().BoolVar(&mptPerTx, "mpt-per-tx", false, "MPT 提交口径：true=每笔交易提交（pre-Byzantium）；false=区块结束一次（现代主网，默认）")

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
