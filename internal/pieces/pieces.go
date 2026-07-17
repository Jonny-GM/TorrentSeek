// Package pieces holds piece arithmetic shared by the scheduler, streamer,
// and backends: bitfields of completed pieces and the mapping between file
// byte ranges and torrent piece ranges.
package pieces

import "fmt"

// Range is a half-open range of piece indices [Begin, End).
type Range struct {
	Begin int
	End   int
}

// Empty reports whether the range covers no pieces.
func (r Range) Empty() bool { return r.End <= r.Begin }

// Len is the number of pieces in the range.
func (r Range) Len() int {
	if r.Empty() {
		return 0
	}
	return r.End - r.Begin
}

func (r Range) String() string { return fmt.Sprintf("[%d,%d)", r.Begin, r.End) }

// FromByteRange maps a byte range of a file onto the torrent pieces backing
// it. fileOffset is the file's byte offset within the torrent payload,
// pieceSize the torrent's piece length, and [off, off+length) the byte range
// within the file. A zero or negative length yields an empty Range.
func FromByteRange(fileOffset, pieceSize, off, length int64) Range {
	if length <= 0 || pieceSize <= 0 {
		return Range{}
	}
	start := fileOffset + off
	end := start + length // exclusive
	return Range{
		Begin: int(start / pieceSize),
		End:   int((end + pieceSize - 1) / pieceSize),
	}
}

// AvailableBytes returns how many bytes of the file at
// [fileOffset, fileOffset+fileLength) within the torrent payload are backed
// by completed pieces — the API's bytes_available. Partial overlap counts
// partially: a completed piece shared with a neighboring file contributes
// only the bytes inside this file.
func AvailableBytes(b Bitfield, fileOffset, pieceSize, fileLength int64) int64 {
	if fileLength <= 0 || pieceSize <= 0 {
		return 0
	}
	fileEnd := fileOffset + fileLength
	var total int64
	for i := FromByteRange(fileOffset, pieceSize, 0, fileLength); i.Begin < i.End; i.Begin++ {
		if !b.Has(i.Begin) {
			continue
		}
		pieceStart := int64(i.Begin) * pieceSize
		pieceEnd := pieceStart + pieceSize
		total += min(pieceEnd, fileEnd) - max(pieceStart, fileOffset)
	}
	return total
}

// Bitfield tracks piece completion, one bit per piece, most significant bit
// first within each byte (the BitTorrent wire convention).
type Bitfield struct {
	bits []byte
	n    int
}

// NewBitfield returns an all-zero bitfield for n pieces.
func NewBitfield(n int) Bitfield {
	return Bitfield{bits: make([]byte, (n+7)/8), n: n}
}

// FromBytes wraps a raw bitfield (e.g. decoded from a client's RPC) known to
// describe n pieces. The slice is copied.
func FromBytes(raw []byte, n int) (Bitfield, error) {
	if len(raw) != (n+7)/8 {
		return Bitfield{}, fmt.Errorf("bitfield length %d does not match %d pieces", len(raw), n)
	}
	b := NewBitfield(n)
	copy(b.bits, raw)
	return b, nil
}

// Len is the number of pieces the bitfield describes.
func (b Bitfield) Len() int { return b.n }

// Has reports whether piece i is complete. Out-of-range indices are false.
func (b Bitfield) Has(i int) bool {
	if i < 0 || i >= b.n {
		return false
	}
	return b.bits[i/8]&(0x80>>(i%8)) != 0
}

// Set marks piece i complete. Out-of-range indices panic: they always
// indicate a bookkeeping bug.
func (b Bitfield) Set(i int) {
	if i < 0 || i >= b.n {
		panic(fmt.Sprintf("pieces: Set(%d) out of range [0,%d)", i, b.n))
	}
	b.bits[i/8] |= 0x80 >> (i % 8)
}

// Count returns the number of complete pieces.
func (b Bitfield) Count() int {
	c := 0
	for i := 0; i < b.n; i++ {
		if b.Has(i) {
			c++
		}
	}
	return c
}

// HasRange reports whether every piece in r is complete. Empty ranges are
// trivially complete; ranges extending past the bitfield are not.
func (b Bitfield) HasRange(r Range) bool {
	if r.Empty() {
		return true
	}
	if r.Begin < 0 || r.End > b.n {
		return false
	}
	for i := r.Begin; i < r.End; i++ {
		if !b.Has(i) {
			return false
		}
	}
	return true
}

// FirstMissing returns the index of the first incomplete piece in r, or -1
// if r is fully complete. Pieces outside [0,Len) count as missing.
func (b Bitfield) FirstMissing(r Range) int {
	if r.Empty() {
		return -1
	}
	for i := r.Begin; i < r.End; i++ {
		if i < 0 || i >= b.n || !b.Has(i) {
			return i
		}
	}
	return -1
}

// Clone returns an independent copy.
func (b Bitfield) Clone() Bitfield {
	c := NewBitfield(b.n)
	copy(c.bits, b.bits)
	return c
}
