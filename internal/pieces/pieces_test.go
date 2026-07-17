package pieces

import "testing"

func TestFromByteRange(t *testing.T) {
	tests := []struct {
		name                  string
		fileOffset, pieceSize int64
		off, length           int64
		want                  Range
	}{
		{"whole first piece", 0, 16, 0, 16, Range{0, 1}},
		{"one byte", 0, 16, 0, 1, Range{0, 1}},
		{"crosses piece boundary", 0, 16, 15, 2, Range{0, 2}},
		{"exact second piece", 0, 16, 16, 16, Range{1, 2}},
		{"file offset shifts pieces", 40, 16, 0, 1, Range{2, 3}},
		{"offset plus range crosses", 40, 16, 7, 2, Range{2, 4}},
		{"zero length is empty", 0, 16, 8, 0, Range{}},
		{"negative length is empty", 0, 16, 8, -3, Range{}},
		{"large range", 0, 1 << 20, 0, 10 << 20, Range{0, 10}},
		{"unaligned tail", 0, 1 << 20, 0, 10<<20 + 1, Range{0, 11}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FromByteRange(tt.fileOffset, tt.pieceSize, tt.off, tt.length)
			if got != tt.want {
				t.Errorf("FromByteRange(%d, %d, %d, %d) = %v, want %v",
					tt.fileOffset, tt.pieceSize, tt.off, tt.length, got, tt.want)
			}
		})
	}
}

func TestAvailableBytes(t *testing.T) {
	// Torrent layout mirroring the fake's movie spec: 16-byte pieces,
	// file A at [0,40), file B at [40,240). Piece 2 = bytes [32,48) is
	// shared: 8 bytes of A, 8 bytes of B.
	b := NewBitfield(15)
	if got := AvailableBytes(b, 40, 16, 200); got != 0 {
		t.Errorf("empty bitfield: %d, want 0", got)
	}

	b.Set(2)
	if got := AvailableBytes(b, 0, 16, 40); got != 8 {
		t.Errorf("file A with shared piece 2: %d, want 8", got)
	}
	if got := AvailableBytes(b, 40, 16, 200); got != 8 {
		t.Errorf("file B with shared piece 2: %d, want 8", got)
	}

	b.Set(3)
	if got := AvailableBytes(b, 40, 16, 200); got != 24 {
		t.Errorf("file B with pieces 2,3: %d, want 24", got)
	}

	// Last piece (14) covers bytes [224,240): all 16 inside file B.
	b.Set(14)
	if got := AvailableBytes(b, 40, 16, 200); got != 40 {
		t.Errorf("file B with pieces 2,3,14: %d, want 40", got)
	}

	for i := 0; i < 15; i++ {
		if !b.Has(i) {
			b.Set(i)
		}
	}
	if got := AvailableBytes(b, 0, 16, 40); got != 40 {
		t.Errorf("complete file A: %d, want 40", got)
	}
	if got := AvailableBytes(b, 40, 16, 200); got != 200 {
		t.Errorf("complete file B: %d, want 200", got)
	}
	if got := AvailableBytes(b, 0, 16, 0); got != 0 {
		t.Errorf("zero-length file: %d, want 0", got)
	}
}

func TestBitfieldBasics(t *testing.T) {
	b := NewBitfield(10)
	if b.Len() != 10 || b.Count() != 0 {
		t.Fatalf("new bitfield: Len=%d Count=%d", b.Len(), b.Count())
	}
	b.Set(0)
	b.Set(7)
	b.Set(8)
	b.Set(9)
	for i, want := range []bool{true, false, false, false, false, false, false, true, true, true} {
		if b.Has(i) != want {
			t.Errorf("Has(%d) = %v, want %v", i, b.Has(i), want)
		}
	}
	if b.Count() != 4 {
		t.Errorf("Count = %d, want 4", b.Count())
	}
	if b.Has(-1) || b.Has(10) {
		t.Error("out-of-range Has should be false")
	}
}

func TestBitfieldSetPanicsOutOfRange(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Set out of range did not panic")
		}
	}()
	NewBitfield(3).Set(3)
}

func TestBitfieldRanges(t *testing.T) {
	b := NewBitfield(8)
	for _, i := range []int{2, 3, 4} {
		b.Set(i)
	}
	if !b.HasRange(Range{2, 5}) {
		t.Error("HasRange([2,5)) = false, want true")
	}
	if b.HasRange(Range{2, 6}) {
		t.Error("HasRange([2,6)) = true, want false")
	}
	if !b.HasRange(Range{}) {
		t.Error("empty range should be trivially complete")
	}
	if b.HasRange(Range{6, 9}) {
		t.Error("range past end should not be complete")
	}
	if got := b.FirstMissing(Range{2, 6}); got != 5 {
		t.Errorf("FirstMissing([2,6)) = %d, want 5", got)
	}
	if got := b.FirstMissing(Range{2, 5}); got != -1 {
		t.Errorf("FirstMissing([2,5)) = %d, want -1", got)
	}
	if got := b.FirstMissing(Range{6, 9}); got != 6 {
		t.Errorf("FirstMissing([6,9)) = %d, want 6", got)
	}
}

func TestFromBytes(t *testing.T) {
	// 10 pieces: 0b10000001 0b11000000 = pieces 0, 7, 8, 9.
	b, err := FromBytes([]byte{0x81, 0xC0}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if b.Count() != 4 || !b.Has(0) || !b.Has(7) || !b.Has(8) || !b.Has(9) {
		t.Errorf("decoded bitfield wrong: count=%d", b.Count())
	}
	if _, err := FromBytes([]byte{0x81}, 10); err == nil {
		t.Error("length mismatch should error")
	}
}

func TestClone(t *testing.T) {
	b := NewBitfield(4)
	b.Set(1)
	c := b.Clone()
	c.Set(2)
	if b.Has(2) {
		t.Error("mutating clone affected original")
	}
	if !c.Has(1) {
		t.Error("clone lost original bits")
	}
}
