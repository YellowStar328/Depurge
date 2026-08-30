// Package dataset 提供对预制 dataset 目录（manifest + 压缩区块 JSON）的加载能力。
package dataset

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// U64 解析 JSON 中的 uint64，兼容多种表示：
//   - JSON number:        21000
//   - hex 字符串:         "0x12a05f200"
//   - 十进制字符串:       "21000"
//   - 空字符串 / null:    0
type U64 uint64

func (u *U64) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" {
		*u = 0
		return nil
	}
	if s == `""` {
		*u = 0
		return nil
	}
	if len(s) > 0 && s[0] != '"' {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return fmt.Errorf("U64: invalid number %q: %w", s, err)
		}
		*u = U64(v)
		return nil
	}
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return fmt.Errorf("U64: not a string: %w", err)
	}
	str = strings.TrimSpace(str)
	if str == "" {
		*u = 0
		return nil
	}
	v, err := parseHexOrDecUint(str)
	if err != nil {
		return fmt.Errorf("U64: invalid value %q: %w", str, err)
	}
	*u = U64(v)
	return nil
}

// Big 解析 JSON 中的大整数，兼容 hex 字符串 / 十进制字符串 / JSON number / 空串(0)。
type Big big.Int

// UnmarshalJSON implements json.Unmarshaler.
func (b *Big) UnmarshalJSON(data []byte) error {
	s := strings.TrimSpace(string(data))
	if s == "null" {
		*b = Big(*big.NewInt(0))
		return nil
	}
	if s == `""` {
		*b = Big(*big.NewInt(0))
		return nil
	}
	var v big.Int
	if len(s) > 0 && s[0] != '"' {
		// JSON number
		n, ok := new(big.Int).SetString(s, 10)
		if !ok {
			return fmt.Errorf("Big: invalid number %q", s)
		}
		v = *n
		*b = Big(v)
		return nil
	}
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return fmt.Errorf("Big: not a string: %w", err)
	}
	str = strings.TrimSpace(str)
	if str == "" {
		*b = Big(*big.NewInt(0))
		return nil
	}
	n, ok := parseHexOrDecBig(str)
	if !ok {
		return fmt.Errorf("Big: invalid value %q", str)
	}
	*b = Big(*n)
	return nil
}

// ToBig 返回底层 *big.Int 的拷贝。
func (b *Big) ToBig() *big.Int {
	if b == nil {
		return new(big.Int)
	}
	return new(big.Int).Set((*big.Int)(b))
}

func parseHexOrDecUint(s string) (uint64, error) {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		if len(s) == 2 {
			return 0, nil
		}
		return strconv.ParseUint(s[2:], 16, 64)
	}
	return strconv.ParseUint(s, 10, 64)
}

func parseHexOrDecBig(s string) (*big.Int, bool) {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		if len(s) == 2 {
			return new(big.Int), true
		}
		return new(big.Int).SetString(s[2:], 16)
	}
	return new(big.Int).SetString(s, 10)
}
