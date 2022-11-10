package storage

import "encoding/binary"

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
