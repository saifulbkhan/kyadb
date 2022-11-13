package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

const (
	NULL    ElementType = '\x00'
	UINT32  ElementType = 'u'
	UINT64  ElementType = 'v'
	INT32   ElementType = 'i'
	INT64   ElementType = 'l'
	FLOAT32 ElementType = 'f'
	FLOAT64 ElementType = 'd'
	BOOL    ElementType = 'b'
	STRING  ElementType = 's'
	TIME    ElementType = 't'
	ARRAY   ElementType = 'a'
	MAP     ElementType = 'm'
)

type Record []byte
type ElementPosition uint16
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

// WriteOverflowError is returned when there is not enough space in the record to write the given
// data.
type WriteOverflowError struct {
	availableBytes uint16
	requiredBytes  uint16
	data           any
}

// TypeMismatchError is returned when the type of the user-provided value does not match the type
// of the element expected at a position.
type TypeMismatchError struct {
	expected ElementType
	actual   ElementType
}

// UnrecognizedTypeError is returned when the type of the user-provided value is not recognized.
type UnrecognizedTypeError struct {
	value any
}

func (e *WriteOverflowError) Error() string {
	return fmt.Sprintf("not enough space to write %v bytes for %v", e.requiredBytes, e.data)
}

func (e *TypeMismatchError) Error() string {
	expectedTypeName, err := nameForElementType(e.expected)
	if err != nil {
		return err.Error()
	}
	actualTypeName, err := nameForElementType(e.actual)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("expected type '%s', got '%s'", expectedTypeName, actualTypeName)
}

func (e *UnrecognizedTypeError) Error() string {
	return fmt.Sprintf("unrecognized type %T", e.value)
}

func nameForElementType(elemType ElementType) (string, error) {
	var elemTypeName string
	var err error
	switch elemType {
	case NULL:
		elemTypeName = "null"
	case UINT32:
		elemTypeName = "uint32"
	case UINT64:
		elemTypeName = "uint64"
	case INT32:
		elemTypeName = "int32"
	case INT64:
		elemTypeName = "int64"
	case FLOAT32:
		elemTypeName = "float32"
	case FLOAT64:
		elemTypeName = "float64"
	case BOOL:
		elemTypeName = "bool"
	case STRING:
		elemTypeName = "string"
	case TIME:
		elemTypeName = "time"
	case ARRAY:
		elemTypeName = "array"
	case MAP:
		elemTypeName = "map"
	default:
		err = &UnrecognizedTypeError{elemType}
	}
	return elemTypeName, err
}

func bytesNeededForString(str string) uint16 {
	return uint16(len(str)) + 2
}

func bytesNeededForPrimitive(value any) (uint16, error) {
	var bytesNeeded uint16
	var err error
	switch value.(type) {
	case bool:
		bytesNeeded = 1
	case uint, uint32, int, int32, float32:
		bytesNeeded = 4
	case uint64, int64, float64, time.Time:
		bytesNeeded = 8
	case string:
		bytesNeeded = bytesNeededForString(value.(string))
	default:
		err = fmt.Errorf("unsupported primitive type %T", value)
	}
	return bytesNeeded, err
}

func bytesNeededForArray(arr Array) (uint16, error) {
	var bytesNeeded uint16
	var err error
	for _, value := range arr.Values {
		bytesNeededForElement, err := bytesNeededForPrimitive(value)
		if err != nil {
			break
		}
		bytesNeeded += bytesNeededForElement
	}
	return bytesNeeded + 3, err
}

func (r *Record) setLength(length uint16) {
	binary.LittleEndian.PutUint16((*r)[0:2], length)
}

func (r *Record) setHeaderLength(headerLength uint16) {
	binary.LittleEndian.PutUint16((*r)[2:4], headerLength)
}

func (r *Record) offsetForPosition(position ElementPosition) uint16 {
	return binary.LittleEndian.Uint16((*r)[4+2*position : 6+2*position])
}

func (r *Record) setOffset(position ElementPosition, offset uint16) {
	binary.LittleEndian.PutUint16((*r)[4+2*position:6+2*position], offset)
}

func (r *Record) writeUint32(offset uint16, value uint32) {
	(*r)[offset] = byte(value)
	(*r)[offset+1] = byte(value >> 8)
	(*r)[offset+2] = byte(value >> 16)
	(*r)[offset+3] = byte(value >> 24)
}

func (r *Record) writeUint64(offset uint16, value uint64) {
	(*r)[offset] = byte(value)
	(*r)[offset+1] = byte(value >> 8)
	(*r)[offset+2] = byte(value >> 16)
	(*r)[offset+3] = byte(value >> 24)
	(*r)[offset+4] = byte(value >> 32)
	(*r)[offset+5] = byte(value >> 40)
	(*r)[offset+6] = byte(value >> 48)
	(*r)[offset+7] = byte(value >> 56)
}

func (r *Record) writeByte(offset uint16, value bool) {
	if value {
		(*r)[offset] = 1
	} else {
		(*r)[offset] = 0
	}
}

func (r *Record) writeString(offset uint16, value string) {
	strLen := uint16(len(value))
	(*r)[offset] = byte(strLen)
	(*r)[offset+1] = byte(strLen >> 8)
	copy((*r)[offset+2:offset+2+strLen], value)
}

func (r *Record) writePrimitive(offset uint16, value any, expectedType ElementType) (
	uint16, error,
) {
	checkElementType := func(actualType ElementType) error {
		if expectedType != actualType {
			return &TypeMismatchError{expectedType, actualType}
		}
		return nil
	}

	var offsetAfterWrite uint16
	var err error
	switch value.(type) {
	case uint:
		err = checkElementType(UINT32)
		if err != nil {
			return offset, err
		}
		r.writeUint32(offset, uint32(value.(uint)))
		offsetAfterWrite = offset + 4
	case uint32:
		err = checkElementType(UINT32)
		if err != nil {
			return offset, err
		}
		r.writeUint32(offset, value.(uint32))
		offsetAfterWrite = offset + 4
	case uint64:
		err = checkElementType(UINT64)
		if err != nil {
			return offset, err
		}
		r.writeUint64(offset, value.(uint64))
		offsetAfterWrite = offset + 8
	case int:
		err = checkElementType(INT32)
		if err != nil {
			return offset, err
		}
		r.writeUint32(offset, uint32(value.(int)))
		offsetAfterWrite = offset + 4
	case int32:
		err = checkElementType(INT32)
		if err != nil {
			return offset, err
		}
		r.writeUint32(offset, uint32(value.(int32)))
		offsetAfterWrite = offset + 4
	case int64:
		err = checkElementType(INT64)
		if err != nil {
			return offset, err
		}
		r.writeUint64(offset, uint64(value.(int64)))
		offsetAfterWrite = offset + 8
	case float32:
		err = checkElementType(FLOAT32)
		if err != nil {
			return offset, err
		}
		r.writeUint32(offset, math.Float32bits(value.(float32)))
		offsetAfterWrite = offset + 4
	case float64:
		err = checkElementType(FLOAT64)
		if err != nil {
			return offset, err
		}
		r.writeUint64(offset, math.Float64bits(value.(float64)))
		offsetAfterWrite = offset + 8
	case bool:
		err = checkElementType(BOOL)
		if err != nil {
			return offset, err
		}
		r.writeByte(offset, value.(bool))
		offsetAfterWrite = offset + 1
	case string:
		err = checkElementType(STRING)
		if err != nil {
			return offset, err
		}
		r.writeString(offset, value.(string))
		offsetAfterWrite = offset + bytesNeededForString(value.(string))
	case time.Time:
		err = checkElementType(TIME)
		if err != nil {
			return offset, err
		}
		r.writeUint64(offset, uint64(value.(time.Time).UnixNano()))
		offsetAfterWrite = offset + 8
	default:
		err = fmt.Errorf("unsupported primitive type %T", value)
	}
	return offsetAfterWrite, err
}

func (r *Record) writeArray(offset uint16, arr Array) (uint16, error) {
	(*r)[offset] = byte(len(arr.Values))
	offset++
	(*r)[offset] = byte(len(arr.Values) >> 8)
	offset++
	(*r)[offset] = byte(arr.ElementType)
	offset++
	for _, value := range arr.Values {
		var err error
		offset, err = r.writePrimitive(offset, value, arr.ElementType)
		if err != nil {
			return offset, err
		}
	}
	return offset, nil
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
func (r *Record) SetUint32(position ElementPosition, value uint32) {
	offset := r.offsetForPosition(position)
	if offset == 0 {
		offset = r.Length()
		r.setLength(offset + 4)
		r.setOffset(position, offset)
		*r = append(*r, make([]byte, 4)...)
	}
	r.writeUint32(offset, value)
}

// SetUint64 saves the given uint64 value at the given element position in the record.
func (r *Record) SetUint64(position ElementPosition, value uint64) {
	offset := r.offsetForPosition(position)
	if offset == 0 {
		offset = r.Length()
		r.setLength(offset + 8)
		r.setOffset(position, offset)
		*r = append(*r, make([]byte, 8)...)
	}
	r.writeUint64(offset, value)
}

// SetInt32 saves the given int32 value at the given element position in the record.
func (r *Record) SetInt32(position ElementPosition, value int32) {
	r.SetUint32(position, uint32(value))
}

// SetInt64 saves the given int64 value at the given element position in the record.
func (r *Record) SetInt64(position ElementPosition, value int64) {
	r.SetUint64(position, uint64(value))
}

// SetFloat32 saves the given float32 value at the given element position in the record.
func (r *Record) SetFloat32(position ElementPosition, value float32) {
	r.SetUint32(position, math.Float32bits(value))
}

// SetFloat64 saves the given float64 value at the given element position in the record.
func (r *Record) SetFloat64(position ElementPosition, value float64) {
	r.SetUint64(position, math.Float64bits(value))
}

// SetBool saves the given bool value at the given element position in the record.
func (r *Record) SetBool(position ElementPosition, value bool) {
	offset := r.offsetForPosition(position)
	if offset == 0 {
		offset = r.Length()
		r.setLength(offset + 1)
		r.setOffset(position, offset)
		*r = append(*r, byte(0))
	}
	r.writeByte(offset, value)
}

// SetTime saves the given time value at the given element position in the record.
func (r *Record) SetTime(position ElementPosition, value time.Time) {
	r.SetUint64(position, uint64(value.UnixNano()))
}

// SetString saves the given string value at the given element position in the record. If a string
// value is already stored at the given element position and the incoming value is smaller or equal
// to the length of the existing string, the existing string is overwritten with the new value. If
// the incoming value is larger than the length of the existing string, a WriteOverflowError is
// returned.
func (r *Record) SetString(position ElementPosition, value string) error {
	offset := r.offsetForPosition(position)
	if offset == 0 {
		offset = r.Length()
		numBytes := bytesNeededForString(value)
		*r = append(*r, make([]byte, numBytes)...)
		r.writeString(offset, value)
		r.setOffset(position, offset)
		r.setLength(offset + numBytes)
	} else {
		currentLength := binary.LittleEndian.Uint16((*r)[offset : offset+2])
		requiredLength := uint16(len(value))
		if currentLength < requiredLength {
			return &WriteOverflowError{
				currentLength, requiredLength, value,
			}
		}
		r.writeString(offset, value)
	}
	return nil
}

// SetArray saves the given array value at the given element position in the record. If an array
// value is already stored at the given element position and the incoming value is smaller or equal
// to the length of the existing array, the existing array is overwritten with the new value. If
// the incoming value is larger than the length of the existing array, a WriteOverflowError is
// returned.
func (r *Record) SetArray(position ElementPosition, data Array) error {
	offset := r.offsetForPosition(position)
	if offset == 0 {
		offset = r.Length()
		numBytes, err := bytesNeededForArray(data)
		if err != nil {
			return err
		}
		*r = append(*r, make([]byte, numBytes)...)
		_, err = r.writeArray(offset, data)
		if err != nil {
			return err
		}
		r.setOffset(position, offset)
		r.setLength(offset + numBytes)
	} else {
		currentLength := binary.LittleEndian.Uint16((*r)[offset : offset+2])
		requiredLength := uint16(len(data.Values))
		if currentLength < requiredLength {
			return &WriteOverflowError{
				currentLength, requiredLength, data,
			}
		}
		_, err := r.writeArray(offset, data)
		if err != nil {
			return err
		}
	}
	return nil
}
