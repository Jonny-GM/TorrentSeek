package rencode

import (
	"bytes"
	"encoding/hex"
	"math"
	"reflect"
	"strings"
	"testing"
)

// Wire-compatibility vectors generated with the reference Python rencode
// module (rencode_orig 1.0.x, the pure-Python implementation Deluge
// bundles) — each hex string is the byte-exact output of
// rencode.dumps(value) for the value the case constructs. Encoding must
// reproduce them exactly; decoding them must reproduce the value.
var vectors = []struct {
	name string
	v    any
	hex  string
}{
	{"int 0", int64(0), "00"},
	{"int 1", int64(1), "01"},
	{"int 43 fixed max", int64(43), "2b"},
	{"int 44 first int8", int64(44), "3e2c"},
	{"int -1", int64(-1), "46"},
	{"int -32 neg fixed min", int64(-32), "65"},
	{"int -33 first int8", int64(-33), "3edf"},
	{"int 127 int8 max", int64(127), "3e7f"},
	{"int -128 int8 min", int64(-128), "3e80"},
	{"int 128 first int16", int64(128), "3f0080"},
	{"int -32768 int16 min", int64(-32768), "3f8000"},
	{"int 32767 int16 max", int64(32767), "3f7fff"},
	{"int 32768 first int32", int64(32768), "4000008000"},
	{"int int32 min", int64(-2147483648), "4080000000"},
	{"int int32 max", int64(2147483647), "407fffffff"},
	{"int first int64", int64(2147483648), "410000000080000000"},
	{"int int64 max", int64(math.MaxInt64), "417fffffffffffffff"},
	{"int int64 min", int64(math.MinInt64), "418000000000000000"},
	{"true", true, "43"},
	{"false", false, "44"},
	{"none", nil, "45"},
	{"empty string", "", "80"},
	{"string a", "a", "8161"},
	{"string hello", "hello", "8568656c6c6f"},
	{"string 63 bytes fixed max", strings.Repeat("x", 63), "bf" + strings.Repeat("78", 63)},
	{"string 64 bytes long form", strings.Repeat("x", 64), "36343a" + strings.Repeat("78", 64)},
	{"string 200 bytes long form", strings.Repeat("x", 200), "3230303a" + strings.Repeat("78", 200)},
	{"empty list", []any{}, "c0"},
	{"list 1 2 3", []any{int64(1), int64(2), int64(3)}, "c3010203"},
	{"nested list", []any{"a", []any{"b", nil}}, "c28161c2816245"},
	{"empty dict", map[any]any{}, "66"},
	{"dict key 42", map[any]any{"key": int64(42)}, "67836b65792a"},
	{"nested dict", map[any]any{"a": map[any]any{"b": []any{int64(1), "c"}}}, "678161678162c2018163"},
	{"float64 1.5", 1.5, "2c3ff8000000000000"},
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex in test vector: %v", err)
	}
	return b
}

func TestEncodeMatchesReference(t *testing.T) {
	for _, tc := range vectors {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Encode(tc.v)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if want := mustHex(t, tc.hex); !bytes.Equal(got, want) {
				t.Errorf("Encode(%v) = %x, reference produced %x", tc.v, got, want)
			}
		})
	}
}

func TestDecodeMatchesReference(t *testing.T) {
	for _, tc := range vectors {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Decode(mustHex(t, tc.hex))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !reflect.DeepEqual(got, tc.v) {
				t.Errorf("Decode(%s) = %#v, want %#v", tc.hex, got, tc.v)
			}
		})
	}
}

// The reference encodes floats as float32 by default; Deluge's daemon may
// therefore send float32 on the wire. Decoding must accept it (widened to
// float64).
func TestDecodeFloat32(t *testing.T) {
	got, err := Decode(mustHex(t, "423fc00000")) // Python: dumps(1.5), float_bits=32
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != 1.5 {
		t.Errorf("Decode(float32 1.5) = %v", got)
	}
}

// Long-form (>=64 element) lists and (>=25 pair) dicts use the
// terminator-based encoding. The vectors above only cover the fixed
// forms, so round-trip these through both directions explicitly, checking
// against reference bytes for the list.
func TestLongFormsMatchReference(t *testing.T) {
	// Python: dumps(list(range(70)))
	wantPrefix := "3b" // chrList
	list := make([]any, 70)
	for i := range list {
		list[i] = int64(i)
	}
	enc, err := Encode(list)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if hex.EncodeToString(enc[:1]) != wantPrefix || enc[len(enc)-1] != 0x7f {
		t.Fatalf("70-element list must use terminator form, got %x", enc)
	}
	back, err := Decode(enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(back, list) {
		t.Errorf("long list round trip mismatch")
	}

	dict := make(map[any]any, 30)
	for i := range 30 {
		dict[int64(i)] = int64(i)
	}
	encD, err := Encode(dict)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if encD[0] != chrDict || encD[len(encD)-1] != chrTerm {
		t.Fatalf("30-pair dict must use terminator form, got %x", encD)
	}
	backD, err := Decode(encD)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(backD, dict) {
		t.Errorf("long dict round trip mismatch")
	}
}

// A realistic RPC request tuple, byte-exact against the reference:
// Python: dumps(((1, b"daemon.login", (b"user", b"pass"),
// {b"client_version": b"2.2.0"}),))
func TestRPCRequestShape(t *testing.T) {
	req := []any{[]any{
		int64(1), "daemon.login",
		[]any{"user", "pass"},
		map[any]any{"client_version": "2.2.0"},
	}}
	want := "c1c4018c6461656d6f6e2e6c6f67696ec284757365728470617373678e636c69656e745f76657273696f6e85322e322e30"
	got, err := Encode(req)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if hex.EncodeToString(got) != want {
		t.Errorf("Encode(rpc request) = %x, reference produced %s", got, want)
	}
	back, err := Decode(mustHex(t, want))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(back, req) {
		t.Errorf("Decode(rpc request) = %#v", back)
	}
}

func TestEncodeGoTypeWidening(t *testing.T) {
	for _, v := range []any{int(7), int8(7), int16(7), int32(7), uint(7), uint8(7), uint16(7), uint32(7), uint64(7)} {
		enc, err := Encode(v)
		if err != nil {
			t.Fatalf("Encode(%T): %v", v, err)
		}
		if !bytes.Equal(enc, []byte{7}) {
			t.Errorf("Encode(%T 7) = %x, want 07", v, enc)
		}
	}
	if _, err := Encode(uint64(math.MaxUint64)); err == nil {
		t.Error("Encode(MaxUint64) must fail: exceeds int64")
	}
	enc, err := Encode([]byte("ab"))
	if err != nil || !bytes.Equal(enc, []byte{0x82, 'a', 'b'}) {
		t.Errorf("Encode([]byte) = %x, %v", enc, err)
	}
	if _, err := Encode(struct{}{}); err == nil {
		t.Error("Encode(struct{}{}) must fail with ErrUnsupportedType")
	}
}

func TestEncodeStringKeyedMap(t *testing.T) {
	enc, err := Encode(map[string]any{"key": int64(42)})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if hex.EncodeToString(enc) != "67836b65792a" {
		t.Errorf("Encode(map[string]any) = %x", enc)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	cases := map[string]string{
		"empty input":            "",
		"trailing bytes":         "0000",
		"truncated int16":        "3f00",
		"truncated string":       "85686565",
		"unknown after term use": "7f",
		"truncated list":         "c30102",
		"truncated long string":  "36343a78",
		"bad string length":      "34413a78",
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(mustHex(t, h)); err == nil {
				t.Errorf("Decode(%s) succeeded, want error", h)
			}
		})
	}
}

func TestDecodeASCIIBigInt(t *testing.T) {
	// Python: dumps(10**20) — exceeds int64, must error cleanly, not wrap.
	if _, err := Decode(mustHex(t, "3d313030303030303030303030303030303030303030307f")); err == nil {
		t.Error("Decode(10^20) succeeded, want range error")
	}
	// An in-range ASCII int decodes fine (the reference only emits this
	// form beyond int64, but accepting any in-range value is harmless).
	got, err := Decode(append(append([]byte{chrInt}, "12345"...), chrTerm))
	if err != nil || got != int64(12345) {
		t.Errorf("Decode(ascii 12345) = %v, %v", got, err)
	}
}
