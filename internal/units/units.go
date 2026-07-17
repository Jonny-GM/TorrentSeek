// Package units parses and formats human-friendly byte sizes for
// configuration flags ("32MiB", "512k", "1048576").
package units

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseBytes parses a byte size: a plain integer, or an integer with a
// K/M/G/T suffix (optionally followed by "iB" or "B"). All suffixes are
// binary (K = 1024).
func ParseBytes(s string) (int64, error) {
	t := strings.TrimSpace(strings.ToLower(s))
	t = strings.TrimSuffix(t, "ib")
	t = strings.TrimSuffix(t, "b")
	shift := 0
	switch {
	case strings.HasSuffix(t, "k"):
		shift, t = 10, t[:len(t)-1]
	case strings.HasSuffix(t, "m"):
		shift, t = 20, t[:len(t)-1]
	case strings.HasSuffix(t, "g"):
		shift, t = 30, t[:len(t)-1]
	case strings.HasSuffix(t, "t"):
		shift, t = 40, t[:len(t)-1]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid byte size %q", s)
	}
	if shift > 0 && n > (1<<62)>>shift {
		return 0, fmt.Errorf("byte size %q overflows", s)
	}
	return n << shift, nil
}

// FormatBytes renders n compactly, using a binary suffix when exact.
func FormatBytes(n int64) string {
	for _, u := range []struct {
		shift int
		sfx   string
	}{{40, "TiB"}, {30, "GiB"}, {20, "MiB"}, {10, "KiB"}} {
		if n != 0 && n%(1<<u.shift) == 0 {
			return fmt.Sprintf("%d%s", n>>u.shift, u.sfx)
		}
	}
	return strconv.FormatInt(n, 10)
}
