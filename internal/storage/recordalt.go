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
	Data      map[any]any
}

// WriteOverflowError is returned when there is not enough space in the record to write the given
// Data.
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

// InvalidElementTypeError is returned when user tries to create an Array with an unsupported
// element type.
type InvalidElementTypeError struct {
	elemType ElementType
}

// InvalidKeyTypeError is returned when user tries to create a Map with an unsupported key type.
type InvalidKeyTypeError struct {
	keyType ElementType
}

// InvalidValueTypeError is returned when user tries to create a Map with an unsupported value type.
type InvalidValueTypeError struct {
	valueType ElementType
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

func (e *InvalidElementTypeError) Error() string {
	elemTypeName, err := nameForElementType(e.elemType)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("invalid array element type '%s'", elemTypeName)
}

func (e *InvalidKeyTypeError) Error() string {
	keyTypeName, err := nameForElementType(e.keyType)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("invalid map key type '%s'", keyTypeName)
}

func (e *InvalidValueTypeError) Error() string {
	valueTypeName, err := nameForElementType(e.valueType)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("invalid map value type '%s'", valueTypeName)
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

func bytesNeededForArray(a Array) (uint16, error) {
	var bytesNeeded uint16
	var err error
	for _, value := range a.Values {
		bytesNeededForElement, err := bytesNeededForPrimitive(value)
		if err != nil {
			break
		}
		bytesNeeded += bytesNeededForElement
	}
	return bytesNeeded + 3, err
}

func bytesNeededForMap(m Map) (uint16, error) {
	var bytesNeeded uint16
	var err error
	for key, value := range m.Data {
		bytesNeededForKey, err := bytesNeededForPrimitive(key)
		if err != nil {
			break
		}
		var bytesNeededForValue uint16
		if m.ValueType == ARRAY {
			bytesNeededForValue, err = bytesNeededForArray(value.(Array))
		} else {
			bytesNeededForValue, err = bytesNeededForPrimitive(value)
		}
		if err != nil {
			break
		}
		bytesNeeded += bytesNeededForKey + bytesNeededForValue
	}
	return bytesNeeded + 4, err
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
		if err = checkElementType(UINT32); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint32(offset, uint32(value.(uint)))
		offsetAfterWrite = offset + 4
	case uint32:
		if err = checkElementType(UINT32); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint32(offset, value.(uint32))
		offsetAfterWrite = offset + 4
	case uint64:
		if err = checkElementType(UINT64); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint64(offset, value.(uint64))
		offsetAfterWrite = offset + 8
	case int:
		if err = checkElementType(INT32); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint32(offset, uint32(value.(int)))
		offsetAfterWrite = offset + 4
	case int32:
		if err = checkElementType(INT32); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint32(offset, uint32(value.(int32)))
		offsetAfterWrite = offset + 4
	case int64:
		if err = checkElementType(INT64); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint64(offset, uint64(value.(int64)))
		offsetAfterWrite = offset + 8
	case float32:
		if err = checkElementType(FLOAT32); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint32(offset, math.Float32bits(value.(float32)))
		offsetAfterWrite = offset + 4
	case float64:
		if err = checkElementType(FLOAT64); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint64(offset, math.Float64bits(value.(float64)))
		offsetAfterWrite = offset + 8
	case bool:
		if err = checkElementType(BOOLEAN); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeByte(offset, value.(bool))
		offsetAfterWrite = offset + 1
	case string:
		if err = checkElementType(STRING); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeString(offset, value.(string))
		offsetAfterWrite = offset + bytesNeededForString(value.(string))
	case time.Time:
		if err = checkElementType(TIME); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint64(offset, uint64(value.(time.Time).UnixNano()))
		offsetAfterWrite = offset + 8
	default:
		err = fmt.Errorf("unsupported primitive type %T", value)
	}
	return offsetAfterWrite, err
}

func (r *Record) writeArray(offset uint16, a Array) (uint16, error) {
	newOffset := offset
	(*r)[newOffset] = byte(len(a.Values))
	newOffset++
	(*r)[newOffset] = byte(len(a.Values) >> 8)
	newOffset++
	(*r)[newOffset] = byte(a.ElementType)
	newOffset++
	for _, value := range a.Values {
		var err error
		newOffset, err = r.writePrimitive(newOffset, value, a.ElementType)
		if err != nil {
			return offset, err
		}
	}
	return newOffset, nil
}

func (r *Record) writeMap(offset uint16, m Map) (uint16, error) {
	newOffset := offset
	(*r)[newOffset] = byte(len(m.Data))
	newOffset++
	(*r)[newOffset] = byte(len(m.Data) >> 8)
	newOffset++
	(*r)[newOffset] = byte(m.KeyType)
	newOffset++
	(*r)[newOffset] = byte(m.ValueType)
	newOffset++
	for key, value := range m.Data {
		var err error
		newOffset, err = r.writePrimitive(newOffset, key, m.KeyType)
		if err != nil {
			return offset, err
		}
		if m.ValueType == ARRAY {
			newOffset, err = r.writeArray(newOffset, value.(Array))
		} else {
			newOffset, err = r.writePrimitive(newOffset, value, m.ValueType)
		}
		if err != nil {
			return offset, err
		}
	}
	return newOffset, nil
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

// SetString saves the given string value at the given element position in the record.
//
// If a string value is already stored at the given element position and the incoming value is
// smaller or equal to the length of the existing string, the existing string is overwritten with
// the new value. If the incoming value is larger than the length of the existing string, a
// WriteOverflowError is returned.
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

// SetArray saves the given Array value at the given element position in the record. Arrays cannot
// have other arrays and maps as elements.
//
// If an Array value is already stored at the given element position and the incoming value is
// smaller or equal to the length of the existing Array, the existing Array is overwritten with the
// new value. If the incoming value is larger than the length of the existing array, a
// WriteOverflowError is returned.
//
// If the type of incoming Array element type does not match the existing Array element type,
// a TypeMismatchError is returned.
func (r *Record) SetArray(position ElementPosition, a Array) error {
	if a.ElementType == ARRAY {
		return &InvalidElementTypeError{a.ElementType}
	}
	if a.ElementType == MAP {
		return &InvalidElementTypeError{a.ElementType}
	}
	if a.Values == nil {
		return nil
	}

	offset := r.offsetForPosition(position)
	if offset == 0 {
		offset = r.Length()
		numBytes, err := bytesNeededForArray(a)
		if err != nil {
			return err
		}
		*r = append(*r, make([]byte, numBytes)...)
		_, err = r.writeArray(offset, a)
		if err != nil {
			return err
		}
		r.setOffset(position, offset)
		r.setLength(offset + numBytes)
	} else {
		currentElementType := ElementType((*r)[offset+2])
		if currentElementType != a.ElementType {
			return &TypeMismatchError{currentElementType, a.ElementType}
		}
		currentLength := binary.LittleEndian.Uint16((*r)[offset : offset+2])
		requiredLength := uint16(len(a.Values))
		if currentLength < requiredLength {
			return &WriteOverflowError{
				currentLength, requiredLength, a,
			}
		}
		_, err := r.writeArray(offset, a)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetMap saves the given Map value at the given element position in the record. Maps cannot have
// arrays and other maps as keys. Maps cannot have other maps as values. Maps can have arrays as
// values.
//
// If a Map value is already stored at the given element position and the incoming value is smaller
// or equal to the length of the existing Map, the existing Map is overwritten with the new value.
// If the incoming value is larger than the length of the existing Map, a WriteOverflowError is
// returned.
//
// If the type of incoming Map key and value types do not match the existing Map key and value
// types, a TypeMismatchError is returned.
func (r *Record) SetMap(position ElementPosition, m Map) error {
	if m.KeyType == ARRAY {
		return &InvalidKeyTypeError{m.KeyType}
	}
	if m.KeyType == MAP {
		return &InvalidKeyTypeError{m.KeyType}
	}
	if m.ValueType == MAP {
		return &InvalidValueTypeError{m.ValueType}
	}
	if m.Data == nil {
		return nil
	}

	offset := r.offsetForPosition(position)
	if offset == 0 {
		offset = r.Length()
		numBytes, err := bytesNeededForMap(m)
		if err != nil {
			return err
		}
		*r = append(*r, make([]byte, numBytes)...)
		_, err = r.writeMap(offset, m)
		if err != nil {
			return err
		}
		r.setOffset(position, offset)
		r.setLength(offset + numBytes)
	} else {
		currentKeyType := ElementType((*r)[offset+2])
		if currentKeyType != m.KeyType {
			err := &TypeMismatchError{currentKeyType, m.KeyType}
			return fmt.Errorf("key type mismatch: %w", err)
		}
		currentValueType := ElementType((*r)[offset+3])
		if currentValueType != m.ValueType {
			err := &TypeMismatchError{currentValueType, m.ValueType}
			return fmt.Errorf("value type mismatch: %w", err)
		}
		currentLength := binary.LittleEndian.Uint16((*r)[offset : offset+2])
		requiredLength := uint16(len(m.Data))
		if currentLength < requiredLength {
			return &WriteOverflowError{
				currentLength, requiredLength, m,
			}
		}
		_, err := r.writeMap(offset, m)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetUint32 returns the uint32 value stored at the given element position in the record.
func (r *Record) GetUint32(position ElementPosition) uint32 {
	offset := r.offsetForPosition(position)
	return binary.LittleEndian.Uint32((*r)[offset : offset+4])
}

// GetUint64 returns the uint64 value stored at the given element position in the record.
func (r *Record) GetUint64(position ElementPosition) uint64 {
	offset := r.offsetForPosition(position)
	return binary.LittleEndian.Uint64((*r)[offset : offset+8])
}

// GetInt32 returns the int32 value stored at the given element position in the record.
func (r *Record) GetInt32(position ElementPosition) int32 {
	offset := r.offsetForPosition(position)
	return int32(binary.LittleEndian.Uint32((*r)[offset : offset+4]))
}

// GetInt64 returns the int64 value stored at the given element position in the record.
func (r *Record) GetInt64(position ElementPosition) int64 {
	offset := r.offsetForPosition(position)
	return int64(binary.LittleEndian.Uint64((*r)[offset : offset+8]))
}

// GetFloat32 returns the float32 value stored at the given element position in the record.
func (r *Record) GetFloat32(position ElementPosition) float32 {
	offset := r.offsetForPosition(position)
	return math.Float32frombits(binary.LittleEndian.Uint32((*r)[offset : offset+4]))
}

// GetFloat64 returns the float64 value stored at the given element position in the record.
func (r *Record) GetFloat64(position ElementPosition) float64 {
	offset := r.offsetForPosition(position)
	return math.Float64frombits(binary.LittleEndian.Uint64((*r)[offset : offset+8]))
}

// GetBool returns the bool value stored at the given element position in the record.
func (r *Record) GetBool(position ElementPosition) bool {
	offset := r.offsetForPosition(position)
	return (*r)[offset] != 0
}

// GetTime returns the Timestamp value stored at the given element position in the record.
func (r *Record) GetTime(position ElementPosition) time.Time {
	offset := r.offsetForPosition(position)
	nanoseconds := binary.LittleEndian.Uint64((*r)[offset : offset+8])
	return time.Unix(0, int64(nanoseconds))
}
