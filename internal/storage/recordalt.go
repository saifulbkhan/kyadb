package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

const (
	INTEGER ElementType = 'i'
	LONG    ElementType = 'l'
	FLOAT   ElementType = 'f'
	DOUBLE  ElementType = 'd'
	BOOLEAN ElementType = 'b'
	STRING  ElementType = 's'
	TIME    ElementType = 't'
	ARRAY   ElementType = 'a'
	MAP     ElementType = 'm'
)

type Record []byte
type RecordOffset uint16
type ElementType byte

type Array struct {
	ElementType ElementType
	Values      []any
}

type Map struct {
	KeyType   ElementType
	ValueType ElementType
	data      map[any]any
}

type WriteOverflowError struct {
	availableBytes uint16
	requiredBytes  uint16
	data           any
}

func (e *WriteOverflowError) Error() string {
	return fmt.Sprintf("not enough space to write %v bytes for %v", e.requiredBytes, e.data)
}

func (r *Record) setLength(length uint16) {
	binary.LittleEndian.PutUint16((*r)[0:2], length)
}

func (r *Record) setHeaderLength(headerLength uint16) {
	binary.LittleEndian.PutUint16((*r)[2:4], headerLength)
}

func (r *Record) offset(position uint16) uint16 {
	return binary.LittleEndian.Uint16((*r)[4+2*position : 6+2*position])
}

func (r *Record) setOffset(position uint16, offset uint16) {
	binary.LittleEndian.PutUint16((*r)[4+2*position:6+2*position], offset)
}

// NewRecord takes in the number of elements that will be stored in a record and returns a record
// initialized with the appropriate length, header length and offsets for element positions. All
// offsets are initialized to 0, meaning that the values for those element positions are null by
// default.
func NewRecord(numElements uint16) *Record {
	headerLength := 2 + 2*numElements
	length := 2 + headerLength
	r := Record(make([]byte, length))
	binary.LittleEndian.PutUint16(r[0:2], length)
	binary.LittleEndian.PutUint16(r[2:4], headerLength)
	return &r
}

// Length returns the length of the record in bytes.
func (r *Record) Length() uint16 {
	return binary.LittleEndian.Uint16((*r)[0:2])
}

// SetUint32 saves the given uint32 value at the given element position in the record.
func (r *Record) SetUint32(position uint16, value uint32) {
	offset := r.offset(position)
	if offset == 0 {
		offset = r.Length()
		r.setLength(offset + 4)
		r.setOffset(position, offset)
		*r = append(*r, byte(value), byte(value>>8), byte(value>>16), byte(value>>24))
	} else {
		(*r)[offset] = byte(value)
		(*r)[offset+1] = byte(value >> 8)
		(*r)[offset+2] = byte(value >> 16)
		(*r)[offset+3] = byte(value >> 24)
	}
}

// SetUint64 saves the given uint64 value at the given element position in the record.
func (r *Record) SetUint64(position uint16, value uint64) {
	offset := r.offset(position)
	if offset == 0 {
		offset = r.Length()
		r.setLength(offset + 8)
		r.setOffset(position, offset)
		*r = append(
			*r,
			byte(value),
			byte(value>>8),
			byte(value>>16),
			byte(value>>24),
			byte(value>>32),
			byte(value>>40),
			byte(value>>48),
			byte(value>>56),
		)
	} else {
		(*r)[offset] = byte(value)
		(*r)[offset+1] = byte(value >> 8)
		(*r)[offset+2] = byte(value >> 16)
		(*r)[offset+3] = byte(value >> 24)
		(*r)[offset+4] = byte(value >> 32)
		(*r)[offset+5] = byte(value >> 40)
		(*r)[offset+6] = byte(value >> 48)
		(*r)[offset+7] = byte(value >> 56)
	}
}

// SetInt32 saves the given int32 value at the given element position in the record.
func (r *Record) SetInt32(position uint16, value int32) {
	r.SetUint32(position, uint32(value))
}

// SetInt64 saves the given int64 value at the given element position in the record.
func (r *Record) SetInt64(position uint16, value int64) {
	r.SetUint64(position, uint64(value))
}

// SetFloat32 saves the given float32 value at the given element position in the record.
func (r *Record) SetFloat32(position uint16, value float32) {
	r.SetUint32(position, math.Float32bits(value))
}

// SetFloat64 saves the given float64 value at the given element position in the record.
func (r *Record) SetFloat64(position uint16, value float64) {
	r.SetUint64(position, math.Float64bits(value))
}

// SetBool saves the given bool value at the given element position in the record.
func (r *Record) SetBool(position uint16, value bool) {
	offset := r.offset(position)
	if offset == 0 {
		offset = r.Length()
		r.setLength(offset + 1)
		r.setOffset(position, offset)
		if value {
			*r = append(*r, 1)
		} else {
			*r = append(*r, 0)
		}
	} else {
		if value {
			(*r)[offset] = 1
		} else {
			(*r)[offset] = 0
		}
	}
}

// SetTime saves the given time value at the given element position in the record.
func (r *Record) SetTime(position uint16, value time.Time) {
	r.SetUint64(position, uint64(value.UnixNano()))
}

// SetString saves the given string value at the given element position in the record. If a string
// value is already stored at the given element position and the incoming value is smaller or equal
// to the length of the existing string, the existing string is overwritten with the new value. If
// the incoming value is larger than the length of the existing string, a WriteOverflowError is
// returned.
func (r *Record) SetString(position uint16, value string) error {
	offset := r.offset(position)
	if offset == 0 {
		offset = r.Length()
		r.setLength(offset + 2 + uint16(len(value)))
		r.setOffset(position, offset)
		*r = append(*r, byte(len(value)), byte(len(value)>>8))
		*r = append(*r, value...)
	} else {
		currentLength := binary.LittleEndian.Uint16((*r)[offset : offset+2])
		requiredLength := uint16(len(value))
		if currentLength < requiredLength {
			return &WriteOverflowError{
				currentLength, requiredLength, value,
			}
		}
		copy((*r)[offset+2:offset+2+requiredLength], value)
	}
	return nil
}
