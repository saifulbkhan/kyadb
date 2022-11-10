package storage

import "testing"

func checkRecordLength(t *testing.T, r *Record, want int) {
	got := len((*r))
	if got != want {
		t.Errorf("expected length %d, got %d", want, got)
	}
}

func checkRecordBytes(t *testing.T, r *Record, offset int, want []byte) {
	got := (*r)[offset : offset+len(want)]
	if string(got) != string(want) {
		t.Errorf("expected bytes %v, got %v", want, got)
	}
}

func TestNewRecord(t *testing.T) {
	numElements := uint16(5)
	r := NewRecord(numElements)

	checkRecordLength(t, r, 14)
	checkRecordBytes(t, r, 0, []byte{14, 0})
	checkRecordBytes(t, r, 2, []byte{12, 0})
	checkRecordBytes(t, r, 4, []byte{0, 0, 0, 0, 0, 0})
}

func TestRecord_Length(t *testing.T) {
	t.Run(
		"length zero", func(t *testing.T) {
			r := Record(make([]byte, 2))
			want := uint16(0)
			got := r.Length()
			if got != want {
				t.Errorf("expected length %d, got %d", want, got)
			}
		},
	)

	t.Run(
		"length non-zero", func(t *testing.T) {
			r := NewRecord(2)
			got := r.Length()
			want := uint16(8)
			if got != want {
				t.Errorf("expected length %d, got %d", want, got)
			}
		},
	)
}
