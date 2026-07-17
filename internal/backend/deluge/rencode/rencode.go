// Package rencode implements the subset of the rencode serialization
// format that Deluge's daemon RPC round-trips: integers, floats,
// strings/bytes, lists, dicts, booleans, and None. It is a from-scratch
// implementation against the reference Python module's format (rencode
// 1.0.x, the version Deluge pins), written in-house because the existing
// Go implementation is GPL-2.0-licensed and TorrentSeek is MIT — see
// docs/spec/04-deluge-backend.md.
//
// Wire format (byte values, from the reference implementation):
//
//	0..43     positive int, value embedded in the type byte
//	44        float64, 8 bytes big-endian IEEE 754 follow
//	59        list, elements follow until terminator 127
//	60        dict, key/value pairs follow until terminator 127
//	61        big int as ASCII decimal, bytes follow until terminator 127
//	62..65    int8/int16/int32/int64, big-endian bytes follow
//	66        float32, 4 bytes big-endian IEEE 754 follow
//	67 68 69  true, false, none
//	70..101   negative int, -1-(b-70) embedded in the type byte
//	102..126  dict with 0..24 pairs, length embedded in the type byte
//	'0'..'9'  string longer than 63 bytes: ASCII length, ':', bytes
//	128..191  string with 0..63 bytes, length embedded in the type byte
//	192..255  list with 0..63 elements, length embedded in the type byte
//
// Decoded values map to Go as: int64, float64 (float32 widened), string,
// bool, nil, []any, and map[any]any. Lists and tuples are not
// distinguished (the reference decoder returns tuples for both; Deluge's
// RPC treats them interchangeably).
package rencode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
)

const (
	chrList    = 59
	chrDict    = 60
	chrInt     = 61
	chrInt1    = 62
	chrInt2    = 63
	chrInt4    = 64
	chrInt8    = 65
	chrFloat32 = 66
	chrFloat64 = 44
	chrTrue    = 67
	chrFalse   = 68
	chrNone    = 69
	chrTerm    = 127

	intPosFixedStart = 0
	intPosFixedCount = 44
	intNegFixedStart = 70
	intNegFixedCount = 32
	dictFixedStart   = 102
	dictFixedCount   = 25
	strFixedStart    = 128
	strFixedCount    = 64
	listFixedStart   = 192
	listFixedCount   = 64

	// maxIntASCII bounds the ASCII big-int form on decode, matching the
	// reference implementation's MAX_INT_LENGTH anti-DoS check.
	maxIntASCII = 64
)

// ErrUnsupportedType is returned by Encode for Go values outside the
// supported subset.
var ErrUnsupportedType = errors.New("rencode: unsupported type")

// Encode serializes v. Supported types: all Go signed/unsigned integer
// types (unsigned must fit int64), float32/float64, string, []byte, bool,
// nil, []any, map[string]any, and map[any]any.
func Encode(v any) ([]byte, error) {
	var b []byte
	if err := encodeValue(&b, v); err != nil {
		return nil, err
	}
	return b, nil
}

func encodeValue(b *[]byte, v any) error {
	switch x := v.(type) {
	case nil:
		*b = append(*b, chrNone)
	case bool:
		if x {
			*b = append(*b, chrTrue)
		} else {
			*b = append(*b, chrFalse)
		}
	case int:
		encodeInt(b, int64(x))
	case int8:
		encodeInt(b, int64(x))
	case int16:
		encodeInt(b, int64(x))
	case int32:
		encodeInt(b, int64(x))
	case int64:
		encodeInt(b, x)
	case uint:
		return encodeUint(b, uint64(x))
	case uint8:
		encodeInt(b, int64(x))
	case uint16:
		encodeInt(b, int64(x))
	case uint32:
		encodeInt(b, int64(x))
	case uint64:
		return encodeUint(b, x)
	case float32:
		*b = append(*b, chrFloat32)
		*b = binary.BigEndian.AppendUint32(*b, math.Float32bits(x))
	case float64:
		*b = append(*b, chrFloat64)
		*b = binary.BigEndian.AppendUint64(*b, math.Float64bits(x))
	case string:
		encodeString(b, []byte(x))
	case []byte:
		encodeString(b, x)
	case []any:
		return encodeList(b, x)
	case map[string]any:
		if len(x) >= dictFixedCount {
			*b = append(*b, chrDict)
			for k, val := range x {
				encodeString(b, []byte(k))
				if err := encodeValue(b, val); err != nil {
					return err
				}
			}
			*b = append(*b, chrTerm)
			return nil
		}
		*b = append(*b, byte(dictFixedStart+len(x)))
		for k, val := range x {
			encodeString(b, []byte(k))
			if err := encodeValue(b, val); err != nil {
				return err
			}
		}
	case map[any]any:
		if len(x) >= dictFixedCount {
			*b = append(*b, chrDict)
			for k, val := range x {
				if err := encodeValue(b, k); err != nil {
					return err
				}
				if err := encodeValue(b, val); err != nil {
					return err
				}
			}
			*b = append(*b, chrTerm)
			return nil
		}
		*b = append(*b, byte(dictFixedStart+len(x)))
		for k, val := range x {
			if err := encodeValue(b, k); err != nil {
				return err
			}
			if err := encodeValue(b, val); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%w: %T", ErrUnsupportedType, v)
	}
	return nil
}

func encodeUint(b *[]byte, x uint64) error {
	if x > math.MaxInt64 {
		return fmt.Errorf("%w: uint64 %d overflows int64", ErrUnsupportedType, x)
	}
	encodeInt(b, int64(x))
	return nil
}

func encodeInt(b *[]byte, x int64) {
	switch {
	case 0 <= x && x < intPosFixedCount:
		*b = append(*b, byte(intPosFixedStart+x))
	case -intNegFixedCount <= x && x < 0:
		*b = append(*b, byte(intNegFixedStart-1-x))
	case math.MinInt8 <= x && x <= math.MaxInt8:
		*b = append(*b, chrInt1, byte(x))
	case math.MinInt16 <= x && x <= math.MaxInt16:
		*b = append(*b, chrInt2)
		*b = binary.BigEndian.AppendUint16(*b, uint16(x))
	case math.MinInt32 <= x && x <= math.MaxInt32:
		*b = append(*b, chrInt4)
		*b = binary.BigEndian.AppendUint32(*b, uint32(x))
	default:
		*b = append(*b, chrInt8)
		*b = binary.BigEndian.AppendUint64(*b, uint64(x))
	}
}

func encodeString(b *[]byte, s []byte) {
	if len(s) < strFixedCount {
		*b = append(*b, byte(strFixedStart+len(s)))
		*b = append(*b, s...)
		return
	}
	*b = append(*b, strconv.Itoa(len(s))...)
	*b = append(*b, ':')
	*b = append(*b, s...)
}

func encodeList(b *[]byte, list []any) error {
	if len(list) < listFixedCount {
		*b = append(*b, byte(listFixedStart+len(list)))
		for _, v := range list {
			if err := encodeValue(b, v); err != nil {
				return err
			}
		}
		return nil
	}
	*b = append(*b, chrList)
	for _, v := range list {
		if err := encodeValue(b, v); err != nil {
			return err
		}
	}
	*b = append(*b, chrTerm)
	return nil
}

// Decode deserializes one rencode value from data, requiring the value to
// consume all of data (matching the reference loads()).
func Decode(data []byte) (any, error) {
	d := &decoder{buf: data}
	v, err := d.value()
	if err != nil {
		return nil, err
	}
	if d.pos != len(data) {
		return nil, fmt.Errorf("rencode: %d trailing bytes after value", len(data)-d.pos)
	}
	return v, nil
}

type decoder struct {
	buf []byte
	pos int
}

var errTruncated = errors.New("rencode: truncated input")

func (d *decoder) next() (byte, error) {
	if d.pos >= len(d.buf) {
		return 0, errTruncated
	}
	b := d.buf[d.pos]
	d.pos++
	return b, nil
}

func (d *decoder) take(n int) ([]byte, error) {
	if n < 0 || d.pos+n > len(d.buf) {
		return nil, errTruncated
	}
	s := d.buf[d.pos : d.pos+n]
	d.pos += n
	return s, nil
}

func (d *decoder) value() (any, error) {
	t, err := d.next()
	if err != nil {
		return nil, err
	}
	switch {
	case t >= intPosFixedStart && t < intPosFixedStart+intPosFixedCount:
		return int64(t - intPosFixedStart), nil
	case t >= intNegFixedStart && t < intNegFixedStart+intNegFixedCount:
		return int64(-1 - int(t-intNegFixedStart)), nil
	case t >= strFixedStart && t < strFixedStart+strFixedCount:
		s, err := d.take(int(t - strFixedStart))
		if err != nil {
			return nil, err
		}
		return string(s), nil
	case t >= listFixedStart: // listFixedStart+listFixedCount wraps past 255
		n := int(t - listFixedStart)
		list := make([]any, 0, n)
		for range n {
			v, err := d.value()
			if err != nil {
				return nil, err
			}
			list = append(list, v)
		}
		return list, nil
	case t >= dictFixedStart && t < dictFixedStart+dictFixedCount:
		return d.dictBody(int(t - dictFixedStart))
	case t >= '0' && t <= '9':
		return d.longString(t)
	}
	switch t {
	case chrNone:
		return nil, nil
	case chrTrue:
		return true, nil
	case chrFalse:
		return false, nil
	case chrInt1:
		s, err := d.take(1)
		if err != nil {
			return nil, err
		}
		return int64(int8(s[0])), nil
	case chrInt2:
		s, err := d.take(2)
		if err != nil {
			return nil, err
		}
		return int64(int16(binary.BigEndian.Uint16(s))), nil
	case chrInt4:
		s, err := d.take(4)
		if err != nil {
			return nil, err
		}
		return int64(int32(binary.BigEndian.Uint32(s))), nil
	case chrInt8:
		s, err := d.take(8)
		if err != nil {
			return nil, err
		}
		return int64(binary.BigEndian.Uint64(s)), nil
	case chrInt:
		return d.asciiInt()
	case chrFloat32:
		s, err := d.take(4)
		if err != nil {
			return nil, err
		}
		return float64(math.Float32frombits(binary.BigEndian.Uint32(s))), nil
	case chrFloat64:
		s, err := d.take(8)
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(binary.BigEndian.Uint64(s)), nil
	case chrList:
		var list []any
		for {
			if d.pos < len(d.buf) && d.buf[d.pos] == chrTerm {
				d.pos++
				return list, nil
			}
			v, err := d.value()
			if err != nil {
				return nil, err
			}
			list = append(list, v)
		}
	case chrDict:
		return d.dictBody(-1)
	}
	return nil, fmt.Errorf("rencode: unknown type byte 0x%02x", t)
}

// dictBody reads n key/value pairs, or until the terminator when n < 0.
func (d *decoder) dictBody(n int) (any, error) {
	dict := make(map[any]any)
	for i := 0; n < 0 || i < n; i++ {
		if n < 0 {
			if d.pos < len(d.buf) && d.buf[d.pos] == chrTerm {
				d.pos++
				return dict, nil
			}
		}
		k, err := d.value()
		if err != nil {
			return nil, err
		}
		v, err := d.value()
		if err != nil {
			return nil, err
		}
		key, err := dictKey(k)
		if err != nil {
			return nil, err
		}
		dict[key] = v
	}
	return dict, nil
}

// dictKey rejects unhashable decoded keys (lists/dicts) rather than
// panicking on map assignment. The reference implementation allows tuple
// keys; nothing Deluge's RPC sends uses them, so they are out of scope.
func dictKey(k any) (any, error) {
	switch k.(type) {
	case []any, map[any]any:
		return nil, fmt.Errorf("rencode: unsupported dict key type %T", k)
	}
	return k, nil
}

func (d *decoder) longString(first byte) (any, error) {
	lenDigits := []byte{first}
	for {
		c, err := d.next()
		if err != nil {
			return nil, err
		}
		if c == ':' {
			break
		}
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("rencode: bad byte 0x%02x in string length", c)
		}
		lenDigits = append(lenDigits, c)
		if len(lenDigits) > 10 {
			return nil, errors.New("rencode: string length too long")
		}
	}
	n, err := strconv.Atoi(string(lenDigits))
	if err != nil {
		return nil, fmt.Errorf("rencode: bad string length: %w", err)
	}
	s, err := d.take(n)
	if err != nil {
		return nil, err
	}
	return string(s), nil
}

func (d *decoder) asciiInt() (any, error) {
	start := d.pos
	for {
		c, err := d.next()
		if err != nil {
			return nil, err
		}
		if c == chrTerm {
			break
		}
		if d.pos-start > maxIntASCII {
			return nil, errors.New("rencode: integer too long")
		}
	}
	n, err := strconv.ParseInt(string(d.buf[start:d.pos-1]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("rencode: bad ascii integer: %w", err)
	}
	return n, nil
}
