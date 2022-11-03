package storage

import (
	"math"
	"testing"
	"time"
)

func checkLength(t *testing.T, r Record, want int) {
	got := len(r)
	if got != want {
		t.Errorf("expected length %d, got %d", want, got)
	}
}

func checkBytes(t *testing.T, r Record, offset int, want []byte) {
	got := r[offset : offset+len(want)]
	if string(got) != string(want) {
		t.Errorf("expected bytes %v, got %v", want, got)
	}
}

func checkBytesOneOf(t *testing.T, r Record, offset int, want [][]byte) {
	got := r[offset : offset+len(want[0])]
	for _, b := range want {
		if string(got) == string(b) {
			return
		}
	}
	t.Errorf("expected bytes one of %v, got %v", want, got)
}

func TestRecord_SerializeInt(t *testing.T) {
	t.Parallel()

	t.Run(
		"check length with three appends", func(t *testing.T) {
			r := Record{}

			r.SerializeInt(1)
			checkLength(t, r, 4)

			r.SerializeInt(1)
			checkLength(t, r, 8)

			r.SerializeInt(1)
			checkLength(t, r, 12)
		},
	)

	t.Run(
		"check byte arrangement", func(t *testing.T) {
			r := Record{}

			r.SerializeInt(1)
			checkBytes(t, r, 0, []byte{0, 0, 0, 1})

			r.SerializeInt(int(math.Pow(2, 8)))
			checkBytes(t, r, 4, []byte{0, 0, 1, 0})

			r.SerializeInt(int(math.Pow(2, 16)))
			checkBytes(t, r, 8, []byte{0, 1, 0, 0})

			r.SerializeInt(math.MaxInt32)
			checkBytes(t, r, 12, []byte{127, 255, 255, 255})

			r.SerializeInt(-1)
			checkBytes(t, r, 16, []byte{255, 255, 255, 255})
		},
	)
}

func TestRecord_SerializeLong(t *testing.T) {
	t.Parallel()

	t.Run(
		"check length with three appends", func(t *testing.T) {
			r := Record{}

			r.SerializeLong(1)
			checkLength(t, r, 8)

			r.SerializeLong(1)
			checkLength(t, r, 16)

			r.SerializeLong(1)
			checkLength(t, r, 24)
		},
	)

	t.Run(
		"check byte arrangement", func(t *testing.T) {
			r := Record{}

			r.SerializeLong(1)
			checkBytes(t, r, 0, []byte{0, 0, 0, 0, 0, 0, 0, 1})

			r.SerializeLong(int64(math.Pow(2, 8)))
			checkBytes(t, r, 8, []byte{0, 0, 0, 0, 0, 0, 1, 0})

			r.SerializeLong(int64(math.Pow(2, 16)))
			checkBytes(t, r, 16, []byte{0, 0, 0, 0, 0, 1, 0, 0})

			r.SerializeLong(int64(math.Pow(2, 32)))
			checkBytes(t, r, 24, []byte{0, 0, 0, 1, 0, 0, 0, 0})

			r.SerializeLong(math.MaxInt64)
			checkBytes(t, r, 32, []byte{127, 255, 255, 255, 255, 255, 255, 255})

			r.SerializeLong(-1)
			checkBytes(t, r, 40, []byte{255, 255, 255, 255, 255, 255, 255, 255})
		},
	)
}

func TestRecord_SerializeFloat(t *testing.T) {
	t.Parallel()

	t.Run(
		"check length with three appends", func(t *testing.T) {
			r := Record{}

			r.SerializeFloat(1.0)
			checkLength(t, r, 4)

			r.SerializeFloat(0.0)
			checkLength(t, r, 8)

			r.SerializeFloat(-1.0)
			checkLength(t, r, 12)
		},
	)

	// TODO: check byte arrangement
}

func TestRecord_SerializeDouble(t *testing.T) {
	t.Parallel()

	t.Run(
		"check length with three appends", func(t *testing.T) {
			r := Record{}

			r.SerializeDouble(1.0)
			checkLength(t, r, 8)

			r.SerializeDouble(0.0)
			checkLength(t, r, 16)

			r.SerializeDouble(-1.0)
			checkLength(t, r, 24)
		},
	)

	// TODO: check byte arrangement
}

func TestRecord_SerializeBool(t *testing.T) {
	t.Parallel()

	t.Run(
		"check value of true", func(t *testing.T) {
			r := Record{}

			r.SerializeBool(true)
			checkBytes(t, r, 0, []byte{1})
		},
	)

	t.Run(
		"check value of false", func(t *testing.T) {
			r := Record{}

			r.SerializeBool(false)
			checkBytes(t, r, 0, []byte{0})
		},
	)
}

func TestRecord_SerializeString(t *testing.T) {
	t.Parallel()

	t.Run(
		"check empty string append", func(t *testing.T) {
			r := Record{}

			r.SerializeString("")
			checkLength(t, r, 4)
			checkBytes(t, r, 0, []byte{0, 0, 0, 0})
		},
	)

	t.Run(
		"check string append", func(t *testing.T) {
			r := Record{}

			r.SerializeString("hello")
			checkLength(t, r, 9)
			checkBytes(t, r, 0, []byte{0, 0, 0, 5, 104, 101, 108, 108, 111})
		},
	)

	t.Run(
		"check string append with null character", func(t *testing.T) {
			r := Record{}

			r.SerializeString("hello\x00world")
			checkLength(t, r, 15)
			checkBytes(
				t, r, 0, []byte{
					0, 0, 0, 11, 104, 101, 108, 108, 111, 0, 119, 111, 114, 108, 100,
				},
			)
		},
	)

	t.Run(
		"check string append with emoji", func(t *testing.T) {
			r := Record{}

			r.SerializeString("😎")
			checkLength(t, r, 8)
			checkBytes(t, r, 0, []byte{0, 0, 0, 4, 240, 159, 152, 142})
		},
	)
}

func TestRecord_SerializeTime(t *testing.T) {
	t.Parallel()

	t.Run(
		"check time append epoch", func(t *testing.T) {
			r := Record{}

			r.SerializeTime(
				time.Date(
					1970, 1, 1, 0, 0, 0, 0, time.UTC,
				),
			)
			checkLength(t, r, 8)
			checkBytes(t, r, 0, []byte{0, 0, 0, 0, 0, 0, 0, 0})
		},
	)

	t.Run(
		"check time append", func(t *testing.T) {
			r := Record{}

			r.SerializeTime(
				time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC),
			)
			checkLength(t, r, 8)
			checkBytes(t, r, 0, []byte{23, 33, 38, 205, 58, 198, 0, 0})
		},
	)
}

func TestRecord_SerializeArray(t *testing.T) {
	t.Parallel()

	t.Run(
		"check empty array append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeArray(Array{INTEGER, []any{}})
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 5)
			checkBytes(t, r, 0, []byte{0, 0, 0, 0, byte(INTEGER)})
		},
	)

	t.Run(
		"check integer array append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeArray(Array{INTEGER, []any{1, 2, 3}})
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 17)
			checkBytes(
				t, r, 0, []byte{
					0, 0, 0, 3,
					byte(INTEGER),
					0, 0, 0, 1,
					0, 0, 0, 2,
					0, 0, 0, 3,
				},
			)
		},
	)

	t.Run(
		"check long array append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeArray(Array{LONG, []any{int64(1), int64(2), int64(3)}})
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 29)
			checkBytes(
				t, r, 0, []byte{
					0, 0, 0, 3,
					byte(LONG),
					0, 0, 0, 0, 0, 0, 0, 1,
					0, 0, 0, 0, 0, 0, 0, 2,
					0, 0, 0, 0, 0, 0, 0, 3,
				},
			)
		},
	)

	t.Run(
		"check float array append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeArray(Array{FLOAT, []any{float32(1), float32(2), float32(3)}})
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 17)
			checkBytes(
				t, r, 0, []byte{
					0, 0, 0, 3,
					byte(FLOAT),
					63, 128, 0, 0,
					64, 0, 0, 0,
					64, 64, 0, 0,
				},
			)
		},
	)

	t.Run(
		"check double array append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeArray(Array{DOUBLE, []any{float64(1), float64(2), float64(3)}})
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 29)
			checkBytes(
				t, r, 0, []byte{
					0, 0, 0, 3,
					byte(DOUBLE),
					63, 240, 0, 0, 0, 0, 0, 0,
					64, 0, 0, 0, 0, 0, 0, 0,
					64, 8, 0, 0, 0, 0, 0, 0,
				},
			)
		},
	)

	t.Run(
		"check bool array append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeArray(Array{BOOLEAN, []any{true, false, true}})
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 8)
			checkBytes(t, r, 0, []byte{0, 0, 0, 3, byte(BOOLEAN), 1, 0, 1})
		},
	)

	t.Run(
		"check string array append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeArray(Array{STRING, []any{"hello", "world"}})
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 23)
			checkBytes(
				t, r, 0, []byte{
					0, 0, 0, 2,
					byte(STRING),
					0, 0, 0, 5,
					104, 101, 108, 108, 111,
					0, 0, 0, 5,
					119, 111, 114, 108, 100,
				},
			)
		},
	)

	t.Run(
		"check time array append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeArray(
				Array{
					TIME,
					[]any{
						time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
						time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC),
					},
				},
			)
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 21)
			checkBytes(
				t, r, 0, []byte{
					0, 0, 0, 2,
					byte(TIME),
					0, 0, 0, 0, 0, 0, 0, 0,
					23, 33, 38, 205, 58, 198, 0, 0,
				},
			)
		},
	)

	t.Run(
		"check array append with invalid type", func(t *testing.T) {
			r := Record{}

			err := r.SerializeArray(Array{STRING, []any{1, 2, 3}})
			if err == nil {
				t.Error("expected error when appending array with invalid type")
			}
		},
	)
}

func TestRecord_SerializeMap(t *testing.T) {
	t.Parallel()

	t.Run(
		"check int:int map append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeMap(
				Map{INTEGER, INTEGER, map[any]any{1: 2, 3: 4}},
			)
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 22)
			checkBytesOneOf(
				t, r, 0, [][]byte{
					{
						0, 0, 0, 2,
						byte(INTEGER), byte(INTEGER),
						0, 0, 0, 1,
						0, 0, 0, 2,
						0, 0, 0, 3,
						0, 0, 0, 4,
					},
					{
						0, 0, 0, 2,
						byte(INTEGER), byte(INTEGER),
						0, 0, 0, 3,
						0, 0, 0, 4,
						0, 0, 0, 1,
						0, 0, 0, 2,
					},
				},
			)
		},
	)

	t.Run(
		"check long:long map append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeMap(
				Map{
					LONG, LONG,
					map[any]any{
						int64(1): int64(2),
						int64(3): int64(4),
					},
				},
			)
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 38)
			checkBytesOneOf(
				t, r, 0, [][]byte{
					{
						0, 0, 0, 2,
						byte(LONG), byte(LONG),
						0, 0, 0, 0, 0, 0, 0, 1,
						0, 0, 0, 0, 0, 0, 0, 2,
						0, 0, 0, 0, 0, 0, 0, 3,
						0, 0, 0, 0, 0, 0, 0, 4,
					},
					{
						0, 0, 0, 2,
						byte(LONG), byte(LONG),
						0, 0, 0, 0, 0, 0, 0, 3,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 1,
						0, 0, 0, 0, 0, 0, 0, 2,
					},
				},
			)
		},
	)

	t.Run(
		"check float:float map append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeMap(
				Map{
					FLOAT, FLOAT,
					map[any]any{
						float32(1.0): float32(2.0),
						float32(3.0): float32(4.0),
					},
				},
			)
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 22)
			checkBytesOneOf(
				t, r, 0, [][]byte{
					{
						0, 0, 0, 2,
						byte(FLOAT), byte(FLOAT),
						63, 128, 0, 0,
						64, 0, 0, 0,
						64, 64, 0, 0,
						64, 128, 0, 0,
					},
					{
						0, 0, 0, 2,
						byte(FLOAT), byte(FLOAT),
						64, 64, 0, 0,
						64, 128, 0, 0,
						63, 128, 0, 0,
						64, 0, 0, 0,
					},
				},
			)
		},
	)

	t.Run(
		"check double:double  map append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeMap(Map{DOUBLE, DOUBLE, map[any]any{1.0: 2.0, 3.0: 4.0}})
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 38)
			checkBytesOneOf(
				t, r, 0, [][]byte{
					{
						0, 0, 0, 2,
						byte(DOUBLE), byte(DOUBLE),
						63, 240, 0, 0, 0, 0, 0, 0,
						64, 0, 0, 0, 0, 0, 0, 0,
						64, 8, 0, 0, 0, 0, 0, 0,
						64, 16, 0, 0, 0, 0, 0, 0,
					},
					{
						0, 0, 0, 2,
						byte(DOUBLE), byte(DOUBLE),
						64, 8, 0, 0, 0, 0, 0, 0,
						64, 16, 0, 0, 0, 0, 0, 0,
						63, 240, 0, 0, 0, 0, 0, 0,
						64, 0, 0, 0, 0, 0, 0, 0,
					},
				},
			)
		},
	)

	t.Run(
		"check bool:bool map append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeMap(
				Map{
					BOOLEAN, BOOLEAN,
					map[any]any{
						true:  false,
						false: true,
					},
				},
			)
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 10)
			checkBytesOneOf(
				t, r, 0, [][]byte{
					{
						0, 0, 0, 2,
						byte(BOOLEAN), byte(BOOLEAN),
						1, 0,
						0, 1,
					},
					{
						0, 0, 0, 2,
						byte(BOOLEAN), byte(BOOLEAN),
						0, 1,
						1, 0,
					},
				},
			)
		},
	)

	t.Run(
		"check string:string map append", func(t *testing.T) {
			r := Record{}

			err := r.SerializeMap(
				Map{
					STRING, STRING,
					map[any]any{
						"hello": "world",
						"foo":   "bar",
					},
				},
			)
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 38)
			checkBytesOneOf(
				t, r, 0, [][]byte{
					{
						0, 0, 0, 2,
						byte(STRING), byte(STRING),
						0, 0, 0, 5,
						104, 101, 108, 108, 111,
						0, 0, 0, 5,
						119, 111, 114, 108, 100,
						0, 0, 0, 3,
						102, 111, 111,
						0, 0, 0, 3,
						98, 97, 114,
					},
					{
						0, 0, 0, 2,
						byte(STRING), byte(STRING),
						0, 0, 0, 3,
						102, 111, 111,
						0, 0, 0, 3,
						98, 97, 114,
						0, 0, 0, 5,
						104, 101, 108, 108, 111,
						0, 0, 0, 5,
						119, 111, 114, 108, 100,
					},
				},
			)
		},
	)

	t.Run(
		"check int:string map append", func(t *testing.T) {
			r := Record{}
			err := r.SerializeMap(Map{INTEGER, STRING, map[any]any{1: "foo", 2: ""}})
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 25)
			checkBytesOneOf(
				t, r, 0, [][]byte{
					{
						0, 0, 0, 2,
						byte(INTEGER), byte(STRING),
						0, 0, 0, 1,
						0, 0, 0, 3,
						102, 111, 111,
						0, 0, 0, 2,
						0, 0, 0, 0,
					},
					{
						0, 0, 0, 2,
						byte(INTEGER), byte(STRING),
						0, 0, 0, 2,
						0, 0, 0, 0,
						0, 0, 0, 1,
						0, 0, 0, 3,
						102, 111, 111,
					},
				},
			)
		},
	)

	t.Run(
		"check string:array map append", func(t *testing.T) {
			r := Record{}
			err := r.SerializeMap(
				Map{
					STRING, ARRAY,
					map[any]any{
						"foo": Array{INTEGER, []any{1, 2, 3}},
						"bar": Array{INTEGER, []any{-1, -2}},
					},
				},
			)
			if err != nil {
				t.Error(err)
			}
			checkLength(t, r, 50)
			checkBytesOneOf(
				t, r, 0, [][]byte{
					{
						0, 0, 0, 2,
						byte(STRING), byte(ARRAY),

						0, 0, 0, 3,
						102, 111, 111,
						0, 0, 0, 3,
						byte(INTEGER),
						0, 0, 0, 1,
						0, 0, 0, 2,
						0, 0, 0, 3,

						0, 0, 0, 3,
						98, 97, 114,
						0, 0, 0, 2,
						byte(INTEGER),
						255, 255, 255, 255,
						255, 255, 255, 254,
					},
					{
						0, 0, 0, 2,
						byte(STRING), byte(ARRAY),

						0, 0, 0, 3,
						98, 97, 114,
						0, 0, 0, 2,
						byte(INTEGER),
						255, 255, 255, 255,
						255, 255, 255, 254,

						0, 0, 0, 3,
						102, 111, 111,
						0, 0, 0, 3,
						byte(INTEGER),
						0, 0, 0, 1,
						0, 0, 0, 2,
						0, 0, 0, 3,
					},
				},
			)
		},
	)

	t.Run(
		"check string:map map append", func(t *testing.T) {
			r := Record{}
			err := r.SerializeMap(Map{STRING, MAP, map[any]any{"foo": map[any]any{"bar": 1}}})
			if err == nil {
				t.Error("error expected when serializing maps as values in maps")
			}
		},
	)

	t.Run(
		"check invalid key type", func(t *testing.T) {
			r := Record{}
			err := r.SerializeMap(Map{INTEGER, STRING, map[any]any{1.0: "foo"}})
			if err == nil {
				t.Error("error expected when serializing map with invalid key type")
			}
		},
	)

	t.Run(
		"check invalid value type", func(t *testing.T) {
			r := Record{}
			err := r.SerializeMap(Map{INTEGER, STRING, map[any]any{1: 1}})
			if err == nil {
				t.Error("error expected when serializing map with invalid value type")
			}
		},
	)
}

func TestRecord_DeserializeInt(t *testing.T) {
	t.Parallel()

	t.Run(
		"check three reads", func(t *testing.T) {
			r := Record{}

			for i := 0; i < 3; i++ {
				r.SerializeInt(i * 10)
			}

			var offset RecordOffset
			for i := 0; i < 3; i++ {
				got, newOffset := r.DeserializeInt(offset)
				want := i * 10
				if got != want {
					t.Errorf("expected %d, got %d", want, got)
				}
				offset = newOffset
			}
		},
	)
}

func TestRecord_DeserializeLong(t *testing.T) {
	t.Parallel()

	t.Run(
		"check three reads", func(t *testing.T) {
			r := Record{}

			for i := 0; i < 3; i++ {
				r.SerializeLong(int64(i * 10))
			}

			var offset RecordOffset
			for i := 0; i < 3; i++ {
				got, newOffset := r.DeserializeLong(offset)
				want := int64(i) * 10
				if got != want {
					t.Errorf("expected %d, got %d", want, got)
				}
				offset = newOffset
			}
		},
	)
}

func TestRecord_DeserializeFloat(t *testing.T) {
	t.Parallel()

	t.Run(
		"check three reads", func(t *testing.T) {
			r := Record{}

			for i := 0; i < 3; i++ {
				r.SerializeFloat(float32(i) * 3.14)
			}

			var offset RecordOffset
			for i := 0; i < 3; i++ {
				got, newOffset := r.DeserializeFloat(offset)
				want := float32(i) * 3.14
				if got != want {
					t.Errorf("expected %f, got %f", want, got)
				}
				offset = newOffset
			}
		},
	)
}

func TestRecord_DeserializeDouble(t *testing.T) {
	t.Parallel()

	t.Run(
		"check three reads", func(t *testing.T) {
			r := Record{}

			for i := 0; i < 3; i++ {
				r.SerializeDouble(float64(i) * 3.14)
			}

			var offset RecordOffset
			for i := 0; i < 3; i++ {
				got, newOffset := r.DeserializeDouble(offset)
				want := float64(i) * 3.14
				if got != want {
					t.Errorf("expected %f, got %f", want, got)
				}
				offset = newOffset
			}
		},
	)
}

func TestRecord_DeserializeBool(t *testing.T) {
	t.Parallel()

	t.Run(
		"check three reads", func(t *testing.T) {
			r := Record{}

			for i := 0; i < 3; i++ {
				r.SerializeBool(i%2 == 0)
			}

			var offset RecordOffset
			for i := 0; i < 3; i++ {
				got, newOffset := r.DeserializeBool(offset)
				want := i%2 == 0
				if got != want {
					t.Errorf("expected %t, got %t", want, got)
				}
				offset = newOffset
			}
		},
	)
}

func TestRecord_DeserializeString(t *testing.T) {
	t.Parallel()

	t.Run(
		"check three reads", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			want := "concurrency"
			r.SerializeString(want)
			got, newOffset := r.DeserializeString(offset)
			offset = newOffset
			if got != want {
				t.Errorf("expected %s, got %s", want, got)
			}

			want = "is"
			r.SerializeString(want)
			got, newOffset = r.DeserializeString(offset)
			offset = newOffset
			if got != want {
				t.Errorf("expected %s, got %s", want, got)
			}

			want = "not parallelism"
			r.SerializeString(want)
			got, newOffset = r.DeserializeString(offset)
			offset = newOffset
			if got != want {
				t.Errorf("expected %s, got %s", want, got)
			}
		},
	)
}

func TestRecord_DeserializeTime(t *testing.T) {
	t.Parallel()

	t.Run(
		"check three reads", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			want := time.Now().AddDate(1, 0, 0)
			r.SerializeTime(want)
			got, newOffset := r.DeserializeTime(offset)
			offset = newOffset
			if got != want {
				t.Errorf("expected %s, got %s", want, got)
			}

			want = time.Now().AddDate(0, 1, 1)
			r.SerializeTime(want)
			got, newOffset = r.DeserializeTime(offset)
			offset = newOffset
			if got != want {
				t.Errorf("expected %s, got %s", want, got)
			}

			want = time.Now().AddDate(0, 0, -1)
			r.SerializeTime(want)
			got, newOffset = r.DeserializeTime(offset)
			offset = newOffset
			if got != want {
				t.Errorf("expected %s, got %s", want, got)
			}
		},
	)
}

func TestRecord_DeserializeArray(t *testing.T) {
	t.Parallel()

	t.Run(
		"check empty array read", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			err := r.SerializeArray(Array{INTEGER, []any{}})
			if err != nil {
				t.Errorf("expected no error while serialzing, got %s", err)
			}

			got, newOffset, err := r.DeserializeArray(offset)
			if err != nil {
				t.Errorf("expected no error while deserializing, got %s", err)
			}

			if len(got.Values) != 0 {
				t.Errorf("expected empty array, got %v", got)
			}
			if newOffset != offset+5 {
				t.Errorf(
					"expected offset (%d) to be incremented by 5, got %d", offset, newOffset,
				)
			}
			if got.ElementType != INTEGER {
				t.Errorf(
					"expected element type to be %b (integer), got %b", INTEGER, got.ElementType,
				)
			}
		},
	)

	t.Run(
		"check integer array read", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			err := r.SerializeArray(Array{INTEGER, []any{1, 2, 3}})
			if err != nil {
				t.Errorf("expected no error while serialzing, got %s", err)
			}

			got, newOffset, err := r.DeserializeArray(offset)
			if err != nil {
				t.Errorf("expected no error while deserializing, got %s", err)
			}

			if len(got.Values) != 3 {
				t.Errorf("expected array of length 3, got %v", got)
			}
			if newOffset != offset+17 {
				t.Errorf(
					"expected offset (%d) to be incremented by 14, got %d", offset, newOffset,
				)
			}
			if got.ElementType != INTEGER {
				t.Errorf(
					"expected element type to be %b (integer), got %b", INTEGER, got.ElementType,
				)
			}
		},
	)

	t.Run(
		"check long array read", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			err := r.SerializeArray(Array{LONG, []any{int64(1), int64(2), int64(3)}})
			if err != nil {
				t.Errorf("expected no error while serialzing, got %s", err)
			}

			got, newOffset, err := r.DeserializeArray(offset)
			if err != nil {
				t.Errorf("expected no error while deserializing, got %s", err)
			}

			if len(got.Values) != 3 {
				t.Errorf("expected array of length 3, got %v", got)
			}
			if newOffset != offset+29 {
				t.Errorf(
					"expected offset (%d) to be incremented by 22, got %d", offset, newOffset,
				)
			}
			if got.ElementType != LONG {
				t.Errorf(
					"expected element type to be %b (long), got %b", LONG, got.ElementType,
				)
			}
		},
	)

	t.Run(
		"check float array read", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			err := r.SerializeArray(Array{FLOAT, []any{float32(1.0), float32(2.0), float32(3.0)}})
			if err != nil {
				t.Errorf("expected no error while serialzing, got %s", err)
			}

			got, newOffset, err := r.DeserializeArray(offset)
			if err != nil {
				t.Errorf("expected no error while deserializing, got %s", err)
			}

			if len(got.Values) != 3 {
				t.Errorf("expected array of length 3, got %v", got)
			}
			if newOffset != offset+17 {
				t.Errorf(
					"expected offset (%d) to be incremented by 14, got %d", offset, newOffset,
				)
			}
			if got.ElementType != FLOAT {
				t.Errorf(
					"expected element type to be %b (float), got %b", FLOAT, got.ElementType,
				)
			}
		},
	)

	t.Run(
		"check double array read", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			err := r.SerializeArray(Array{DOUBLE, []any{1.0, 2.0, 3.0}})
			if err != nil {
				t.Errorf("expected no error while serialzing, got %s", err)
			}

			got, newOffset, err := r.DeserializeArray(offset)
			if err != nil {
				t.Errorf("expected no error while deserializing, got %s", err)
			}

			if len(got.Values) != 3 {
				t.Errorf("expected array of length 3, got %v", got)
			}
			if newOffset != offset+29 {
				t.Errorf(
					"expected offset (%d) to be incremented by 22, got %d", offset, newOffset,
				)
			}
			if got.ElementType != DOUBLE {
				t.Errorf(
					"expected element type to be %b (double), got %b", DOUBLE, got.ElementType,
				)
			}
		},
	)

	t.Run(
		"check string array read", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			err := r.SerializeArray(Array{STRING, []any{"hello", "world"}})
			if err != nil {
				t.Errorf("expected no error while serialzing, got %s", err)
			}

			got, newOffset, err := r.DeserializeArray(offset)
			if err != nil {
				t.Errorf("expected no error while deserializing, got %s", err)
			}

			if len(got.Values) != 2 {
				t.Errorf("expected array of length 2, got %v", got)
			}
			if newOffset != offset+5+2*(4+5) {
				t.Errorf(
					"expected offset (%d) to be incremented by 5 + (2 * (4 + 5)), got %d",
					offset,
					newOffset,
				)
			}
			if got.ElementType != STRING {
				t.Errorf(
					"expected element type to be %b (string), got %b", STRING, got.ElementType,
				)
			}
			if got.Values[0] != "hello" {
				t.Errorf("expected first element to be 'hello', got %s", got.Values[0])
			}
			if got.Values[1] != "world" {
				t.Errorf("expected second element to be 'world', got %s", got.Values[1])
			}
		},
	)

	t.Run(
		"check time array read", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			err := r.SerializeArray(Array{TIME, []any{time.Now(), time.Now().AddDate(0, 0, 1)}})
			if err != nil {
				t.Errorf("expected no error while serialzing, got %s", err)
			}

			got, newOffset, err := r.DeserializeArray(offset)
			if err != nil {
				t.Errorf("expected no error while deserializing, got %s", err)
			}

			if len(got.Values) != 2 {
				t.Errorf("expected array of length 2, got %v", got)
			}
			if newOffset != offset+5+2*8 {
				t.Errorf(
					"expected offset (%d) to be incremented by 5 + (2 * 8), got %d",
					offset,
					newOffset,
				)
			}
			if got.ElementType != TIME {
				t.Errorf(
					"expected element type to be %b (time), got %b", TIME, got.ElementType,
				)
			}
		},
	)
}

func TestRecord_DeserializeMap(t *testing.T) {
	t.Parallel()

	t.Run(
		"check empty map read", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			err := r.SerializeMap(Map{INTEGER, STRING, map[any]any{}})
			if err != nil {
				t.Errorf("expected no error while serialzing, got %s", err)
			}

			got, gotOffset, err := r.DeserializeMap(offset)
			if err != nil {
				t.Errorf("expected no error while deserializing, got %s", err)
			}

			if len(got.data) != 0 {
				t.Errorf("expected map with 0 elements, got %v", got)
			}

			wantOffset := offset + 6
			if gotOffset != wantOffset {
				t.Errorf(
					"expected offset (%d) to be incremented by %d, got %d",
					offset,
					wantOffset,
					gotOffset,
				)
			}

			if got.KeyType != INTEGER {
				t.Errorf(
					"expected key type to be %b (int), got %b", INTEGER, got.KeyType,
				)
			}
			if got.ValueType != STRING {
				t.Errorf(
					"expected value type to be %b (string), got %b", STRING, got.ValueType,
				)
			}
		},
	)

	t.Run(
		"check int:string map read", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			err := r.SerializeMap(
				Map{
					INTEGER, STRING,
					map[any]any{
						1: "hello",
						2: "world",
					},
				},
			)
			if err != nil {
				t.Errorf("expected no error while serialzing, got %s", err)
			}

			got, gotOffset, err := r.DeserializeMap(offset)
			if err != nil {
				t.Errorf("expected no error while deserializing, got %s", err)
			}

			if len(got.data) != 2 {
				t.Errorf("expected map with 2 elements, got %v", got)
			}

			wantOffset := offset + 6 + 2*(4+4+5)
			if gotOffset != wantOffset {
				t.Errorf(
					"expected offset (%d) to be incremented by %d, got %d",
					offset,
					wantOffset,
					gotOffset,
				)
			}

			if got.KeyType != INTEGER {
				t.Errorf(
					"expected key type to be %b (int), got %b", INTEGER, got.KeyType,
				)
			}
			if got.ValueType != STRING {
				t.Errorf(
					"expected value type to be %b (string), got %b", STRING, got.ValueType,
				)
			}

			hello, ok := got.data[1]
			if !ok {
				t.Errorf("expected key 1 to be present in map, got %v", got)
			}
			if hello != "hello" {
				t.Errorf("expected first element to be 'hello', got %s", hello)
			}

			world, ok := got.data[2]
			if !ok {
				t.Errorf("expected key 2 to be present in map, got %v", got)
			}
			if world != "world" {
				t.Errorf("expected second element to be 'world', got %s", world)
			}
		},
	)

	t.Run(
		"check string:array map read", func(t *testing.T) {
			r := Record{}
			var offset RecordOffset

			err := r.SerializeMap(
				Map{
					STRING, ARRAY,
					map[any]any{
						"hello": Array{STRING, []any{"world", "foo", "bar"}},
						"world": Array{STRING, []any{"hello", "foo", "bar"}},
					},
				},
			)
			if err != nil {
				t.Errorf("expected no error while serialzing, got %s", err)
			}

			got, gotOffset, err := r.DeserializeMap(offset)
			if err != nil {
				t.Errorf("expected no error while deserializing, got %s", err)
			}

			if len(got.data) != 2 {
				t.Errorf("expected map with 2 elements, got %v", got)
			}

			wantOffset := offset + 6 + 2*(4+5+4+1+4+5+2*(4+3))
			if gotOffset != wantOffset {
				t.Errorf(
					"expected offset (%d) to be incremented by %d, got %d",
					offset,
					wantOffset,
					gotOffset,
				)
			}

			if got.KeyType != STRING {
				t.Errorf(
					"expected key type to be %b (string), got %b", STRING, got.KeyType,
				)
			}
			if got.ValueType != ARRAY {
				t.Errorf(
					"expected value type to be %b (array), got %b", ARRAY, got.ValueType,
				)
			}

			world, ok := got.data["hello"]
			if !ok {
				t.Errorf("expected key 'hello' to be present in map, got %v", got)
			}
			wantArray := []any{"world", "foo", "bar"}
			for i, v := range world.(Array).Values {
				if v != wantArray[i] {
					t.Errorf("expected element at index %d to be %s, got %s", i, wantArray[i], v)
				}
			}

			hello, ok := got.data["world"]
			if !ok {
				t.Errorf("expected key 'world' to be present in map, got %v", got)
			}
			wantArray = []any{"hello", "foo", "bar"}
			for i, v := range hello.(Array).Values {
				if v != wantArray[i] {
					t.Errorf("expected element at index %d to be %s, got %s", i, wantArray[i], v)
				}
			}
		},
	)
}
