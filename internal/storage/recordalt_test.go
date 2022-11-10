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

func TestRecord_SetUint32(t *testing.T) {
	t.Run(
		"check two elements", func(t *testing.T) {
			r := NewRecord(2)
			r.SetUint32(0, 10)
			r.SetUint32(1, 20)

			checkRecordLength(t, r, 16)
			checkRecordBytes(t, r, 4, []byte{8, 0, 12, 0})
			checkRecordBytes(t, r, 8, []byte{10, 0, 0, 0})
			checkRecordBytes(t, r, 12, []byte{20, 0, 0, 0})
		},
	)

	t.Run(
		"check smallest uint32", func(t *testing.T) {
			r := NewRecord(1)
			r.SetUint32(0, 0)

			checkRecordLength(t, r, 10)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{0, 0, 0, 0})
		},
	)

	t.Run(
		"check largest uint32", func(t *testing.T) {
			r := NewRecord(1)
			r.SetUint32(0, 4294967295)

			checkRecordLength(t, r, 10)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{255, 255, 255, 255})
		},
	)
}

func TestRecord_SetUint64(t *testing.T) {
	t.Run(
		"check two elements", func(t *testing.T) {
			r := NewRecord(2)
			r.SetUint64(0, 10)
			r.SetUint64(1, 20)

			checkRecordLength(t, r, 24)
			checkRecordBytes(t, r, 4, []byte{8, 0, 16, 0})
			checkRecordBytes(t, r, 8, []byte{10, 0, 0, 0, 0, 0, 0, 0})
			checkRecordBytes(t, r, 16, []byte{20, 0, 0, 0, 0, 0, 0, 0})
		},
	)

	t.Run(
		"check smallest uint64", func(t *testing.T) {
			r := NewRecord(1)
			r.SetUint64(0, 0)

			checkRecordLength(t, r, 14)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{0, 0, 0, 0, 0, 0, 0, 0})
		},
	)

	t.Run(
		"check largest uint64", func(t *testing.T) {
			r := NewRecord(1)
			r.SetUint64(0, 18446744073709551615)

			checkRecordLength(t, r, 14)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{255, 255, 255, 255, 255, 255, 255, 255})
		},
	)
}

func TestRecord_SetInt32(t *testing.T) {
	t.Run(
		"check two elements", func(t *testing.T) {
			r := NewRecord(2)
			r.SetInt32(0, 10)
			r.SetInt32(1, -20)

			checkRecordLength(t, r, 16)
			checkRecordBytes(t, r, 4, []byte{8, 0, 12, 0})
			checkRecordBytes(t, r, 8, []byte{10, 0, 0, 0})
			checkRecordBytes(t, r, 12, []byte{236, 255, 255, 255})
		},
	)

	t.Run(
		"check smallest int32", func(t *testing.T) {
			r := NewRecord(1)
			r.SetInt32(0, -2147483648)

			checkRecordLength(t, r, 10)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{0, 0, 0, 128})
		},
	)

	t.Run(
		"check largest int32", func(t *testing.T) {
			r := NewRecord(1)
			r.SetInt32(0, 2147483647)

			checkRecordLength(t, r, 10)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{255, 255, 255, 127})
		},
	)
}

func TestRecord_SetInt64(t *testing.T) {
	t.Run(
		"check two elements", func(t *testing.T) {
			r := NewRecord(2)
			r.SetInt64(0, 10)
			r.SetInt64(1, -20)

			checkRecordLength(t, r, 24)
			checkRecordBytes(t, r, 4, []byte{8, 0, 16, 0})
			checkRecordBytes(t, r, 8, []byte{10, 0, 0, 0, 0, 0, 0, 0})
			checkRecordBytes(t, r, 16, []byte{236, 255, 255, 255, 255, 255, 255, 255})
		},
	)

	t.Run(
		"check smallest int64", func(t *testing.T) {
			r := NewRecord(1)
			r.SetInt64(0, -9223372036854775808)

			checkRecordLength(t, r, 14)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{0, 0, 0, 0, 0, 0, 0, 128})
		},
	)

	t.Run(
		"check largest int64", func(t *testing.T) {
			r := NewRecord(1)
			r.SetInt64(0, 9223372036854775807)

			checkRecordLength(t, r, 14)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{255, 255, 255, 255, 255, 255, 255, 127})
		},
	)
}

func TestRecord_SetFloat32(t *testing.T) {
	t.Run(
		"check two elements", func(t *testing.T) {
			r := NewRecord(2)
			r.SetFloat32(0, 10.5)
			r.SetFloat32(1, -20.5)

			checkRecordLength(t, r, 16)
			checkRecordBytes(t, r, 4, []byte{8, 0, 12, 0})
			checkRecordBytes(t, r, 8, []byte{0, 0, 40, 65})
			checkRecordBytes(t, r, 12, []byte{0, 0, 164, 193})
		},
	)

	t.Run(
		"check smallest float32", func(t *testing.T) {
			r := NewRecord(1)
			r.SetFloat32(0, -3.40282346638528859811704183484516925440e+38)

			checkRecordLength(t, r, 10)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{255, 255, 127, 255})
		},
	)

	t.Run(
		"check largest float32", func(t *testing.T) {
			r := NewRecord(1)
			r.SetFloat32(0, 3.40282346638528859811704183484516925440e+38)

			checkRecordLength(t, r, 10)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{255, 255, 127, 127})
		},
	)
}

func TestRecord_SetFloat64(t *testing.T) {
	t.Run(
		"check two elements", func(t *testing.T) {
			r := NewRecord(2)
			r.SetFloat64(0, 10.5)
			r.SetFloat64(1, -20.5)

			checkRecordLength(t, r, 24)
			checkRecordBytes(t, r, 4, []byte{8, 0, 16, 0})
			checkRecordBytes(t, r, 8, []byte{0, 0, 0, 0, 0, 0, 37, 64})
			checkRecordBytes(t, r, 16, []byte{0, 0, 0, 0, 0, 128, 52, 192})
		},
	)

	t.Run(
		"check smallest float64", func(t *testing.T) {
			r := NewRecord(1)
			r.SetFloat64(0, -1.797693134862315708145274237317043567981e+308)

			checkRecordLength(t, r, 14)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{255, 255, 255, 255, 255, 255, 239, 255})
		},
	)

	t.Run(
		"check largest float64", func(t *testing.T) {
			r := NewRecord(1)
			r.SetFloat64(0, 1.797693134862315708145274237317043567981e+308)

			checkRecordLength(t, r, 14)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{255, 255, 255, 255, 255, 255, 239, 127})
		},
	)
}
