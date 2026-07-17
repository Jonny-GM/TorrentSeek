package units

import "testing"

func TestParseBytes(t *testing.T) {
	good := map[string]int64{
		"0":      0,
		"1024":   1024,
		"32MiB":  32 << 20,
		"32m":    32 << 20,
		"32MB":   32 << 20,
		"512k":   512 << 10,
		"512KiB": 512 << 10,
		"1g":     1 << 30,
		"2TiB":   2 << 40,
		" 8 MiB": 8 << 20,
		"100B":   100,
	}
	for in, want := range good {
		got, err := ParseBytes(in)
		if err != nil || got != want {
			t.Errorf("ParseBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "MiB", "-1", "-5k", "1.5M", "10x", "9999999999999g"} {
		if _, err := ParseBytes(bad); err == nil {
			t.Errorf("ParseBytes(%q) should error", bad)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	cases := map[int64]string{
		0:         "0",
		100:       "100",
		1024:      "1KiB",
		32 << 20:  "32MiB",
		1 << 30:   "1GiB",
		3<<20 + 1: "3145729",
	}
	for in, want := range cases {
		if got := FormatBytes(in); got != want {
			t.Errorf("FormatBytes(%d) = %q, want %q", in, got, want)
		}
	}
}
