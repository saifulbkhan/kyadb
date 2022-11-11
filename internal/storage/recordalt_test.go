package storage

import (
	"testing"
	"time"
)

func checkRecordLength(t *testing.T, r *Record, want int) {
	got := len(*r)
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

	t.Run(
		"check element update", func(t *testing.T) {
			r := NewRecord(1)
			r.SetUint32(0, 10)
			r.SetUint32(0, 20)

			checkRecordLength(t, r, 10)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{20, 0, 0, 0})
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

	t.Run(
		"check element update", func(t *testing.T) {
			r := NewRecord(1)
			r.SetUint64(0, 10)
			r.SetUint64(0, 20)

			checkRecordLength(t, r, 14)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{20, 0, 0, 0, 0, 0, 0, 0})
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

	t.Run(
		"check element update", func(t *testing.T) {
			r := NewRecord(1)
			r.SetInt32(0, 10)
			r.SetInt32(0, -20)

			checkRecordLength(t, r, 10)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{236, 255, 255, 255})
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

	t.Run(
		"check element update", func(t *testing.T) {
			r := NewRecord(1)
			r.SetInt64(0, 10)
			r.SetInt64(0, -20)

			checkRecordLength(t, r, 14)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{236, 255, 255, 255, 255, 255, 255, 255})
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

	t.Run(
		"check element update", func(t *testing.T) {
			r := NewRecord(1)
			r.SetFloat32(0, 10.5)
			r.SetFloat32(0, -20.5)

			checkRecordLength(t, r, 10)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{0, 0, 164, 193})
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

	t.Run(
		"check element update", func(t *testing.T) {
			r := NewRecord(1)
			r.SetFloat64(0, 10.5)
			r.SetFloat64(0, -20.5)

			checkRecordLength(t, r, 14)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{0, 0, 0, 0, 0, 128, 52, 192})
		},
	)
}

func TestRecord_SetBool(t *testing.T) {
	t.Run(
		"check two elements", func(t *testing.T) {
			r := NewRecord(2)
			r.SetBool(0, true)
			r.SetBool(1, false)

			checkRecordLength(t, r, 10)
			checkRecordBytes(t, r, 4, []byte{8, 0, 9, 0})
			checkRecordBytes(t, r, 8, []byte{1, 0})
		},
	)

	t.Run(
		"check element update", func(t *testing.T) {
			r := NewRecord(1)
			r.SetBool(0, true)
			r.SetBool(0, false)

			checkRecordLength(t, r, 7)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{0})
		},
	)
}

func TestRecord_SetTime(t *testing.T) {
	t.Run(
		"check two elements", func(t *testing.T) {
			r := NewRecord(2)
			r.SetTime(0, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
			r.SetTime(1, time.Date(2022, 10, 25, 3, 25, 0, 0, time.UTC))

			checkRecordLength(t, r, 24)
			checkRecordBytes(t, r, 4, []byte{8, 0, 16, 0})
			checkRecordBytes(t, r, 8, []byte{0, 0, 0, 0, 0, 0, 0, 0})
			checkRecordBytes(t, r, 16, []byte{0, 120, 231, 11, 253, 49, 33, 23})
		},
	)

	t.Run(
		"check element update", func(t *testing.T) {
			r := NewRecord(1)
			r.SetTime(0, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
			r.SetTime(0, time.Date(2022, 10, 25, 3, 25, 0, 0, time.UTC))

			checkRecordLength(t, r, 14)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{0, 120, 231, 11, 253, 49, 33, 23})
		},
	)
}

func TestRecord_SetString(t *testing.T) {
	t.Run(
		"check empty string", func(t *testing.T) {
			r := NewRecord(1)
			err := r.SetString(0, "")
			if err != nil {
				t.Error(err)
			}

			checkRecordLength(t, r, 8)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{0, 0})
		},
	)

	t.Run(
		"check two elements", func(t *testing.T) {
			r := NewRecord(2)
			err := r.SetString(0, "hello")
			if err != nil {
				t.Error(err)
			}
			err = r.SetString(1, "world")
			if err != nil {
				t.Error(err)
			}

			checkRecordLength(t, r, 22)
			checkRecordBytes(t, r, 4, []byte{8, 0, 15, 0})
			checkRecordBytes(t, r, 8, []byte{5, 0, 104, 101, 108, 108, 111})
			checkRecordBytes(t, r, 15, []byte{5, 0, 119, 111, 114, 108, 100})
		},
	)

	t.Run(
		"check string with null character", func(t *testing.T) {
			r := NewRecord(1)
			err := r.SetString(0, "hello\x00world")
			if err != nil {
				t.Error(err)
			}

			checkRecordLength(t, r, 19)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(
				t, r, 6, []byte{11, 0, 104, 101, 108, 108, 111, 0, 119, 111, 114, 108, 100},
			)
		},
	)

	t.Run(
		"check emoji", func(t *testing.T) {
			r := NewRecord(1)
			err := r.SetString(0, "😀")
			if err != nil {
				t.Error(err)
			}

			checkRecordLength(t, r, 12)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{4, 0, 240, 159, 152, 128})
		},
	)

	t.Run(
		"check element update", func(t *testing.T) {
			r := NewRecord(1)
			err := r.SetString(0, "hello")
			if err != nil {
				t.Error(err)
			}
			err = r.SetString(0, "world")
			if err != nil {
				t.Error(err)
			}
			checkRecordLength(t, r, 13)
			checkRecordBytes(t, r, 4, []byte{6, 0})
			checkRecordBytes(t, r, 6, []byte{5, 0, 119, 111, 114, 108, 100})
		},
	)

	t.Run(
		"check write overflows", func(t *testing.T) {
			r := NewRecord(1)
			err := r.SetString(0, "hello")
			if err != nil {
				t.Error(err)
			}
			err = r.SetString(0, "world!")
			if err == nil {
				t.Error("expected error when writing over a shorter string")
			}
		},
	)
}
