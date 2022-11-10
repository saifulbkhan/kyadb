package storage

import (
	"encoding/binary"
	"math"
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
	}
	*r = append(*r, byte(value), byte(value>>8), byte(value>>16), byte(value>>24))
}

// SetUint64 saves the given uint64 value at the given element position in the record.
func (r *Record) SetUint64(position uint16, value uint64) {
	offset := r.offset(position)
	if offset == 0 {
		offset = r.Length()
		r.setLength(offset + 8)
		r.setOffset(position, offset)
	}
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
