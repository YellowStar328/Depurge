package dataset

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Loader 加载一个 dataset 目录。
type Loader struct {
	Dir      string
	Manifest *Manifest
}

// NewLoader 读取 manifest.json 并返回 Loader。
func NewLoader(dir string) (*Loader, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return &Loader{Dir: dir, Manifest: &m}, nil
}

// BlockList 返回可加载的区块号列表（升序）。
// rangeFilter 形如 "24000000-24000005" 或 "24000000"，为空时加载全部。
func (l *Loader) BlockList(rangeFilter string) ([]uint64, error) {
	entries, err := os.ReadDir(filepath.Join(l.Dir, "blocks"))
	if err != nil {
		return nil, fmt.Errorf("read blocks dir: %w", err)
	}
	var nums []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".json.zst") {
			continue
		}
		var n uint64
		if _, err := fmt.Sscanf(strings.TrimSuffix(name, ".json.zst"), "%d", &n); err != nil {
			continue
		}
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	if rangeFilter == "" {
		return nums, nil
	}
	lo, hi, err := parseRange(rangeFilter)
	if err != nil {
		return nil, err
	}
	var filtered []uint64
	for _, n := range nums {
		if n >= lo && n <= hi {
			filtered = append(filtered, n)
		}
	}
	return filtered, nil
}

func parseRange(r string) (lo, hi uint64, err error) {
	parts := strings.SplitN(r, "-", 2)
	if len(parts) == 1 {
		if _, err := fmt.Sscanf(parts[0], "%d", &lo); err != nil {
			return 0, 0, fmt.Errorf("invalid block filter %q", r)
		}
		return lo, lo, nil
	}
	if _, err := fmt.Sscanf(parts[0], "%d", &lo); err != nil {
		return 0, 0, fmt.Errorf("invalid block filter %q", r)
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &hi); err != nil {
		return 0, 0, fmt.Errorf("invalid block filter %q", r)
	}
	return lo, hi, nil
}

// LoadBlock 解压并解析单个区块文件。
func (l *Loader) LoadBlock(num uint64) (*BlockData, error) {
	path := filepath.Join(l.Dir, "blocks", fmt.Sprintf("%d.json.zst", num))
	raw, err := readZstd(path)
	if err != nil {
		return nil, fmt.Errorf("read block %d: %w", num, err)
	}
	var blk BlockData
	if err := json.Unmarshal(raw, &blk); err != nil {
		return nil, fmt.Errorf("parse block %d: %w", num, err)
	}
	return &blk, nil
}

// ForEachBlock 流式逐区块加载，避免大 dataset 一次性载入内存。
func (l *Loader) ForEachBlock(rangeFilter string, fn func(*BlockData) error) error {
	nums, err := l.BlockList(rangeFilter)
	if err != nil {
		return err
	}
	for _, n := range nums {
		blk, err := l.LoadBlock(n)
		if err != nil {
			return err
		}
		if err := fn(blk); err != nil {
			return err
		}
	}
	return nil
}

var (
	zstdOnce   sync.Once
	zstdDecode *zstd.Decoder
)

// readZstd 解压一个 .zst 文件并返回全部内容。
// 使用 DecodeAll（并发安全，decoder 可复用）。
func readZstd(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	zstdOnce.Do(func() {
		zstdDecode, _ = zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
		)
	})
	return zstdDecode.DecodeAll(raw, nil)
}
