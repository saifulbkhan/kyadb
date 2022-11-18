package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

const (
	NullType    ElementType = '\x00'
	Uint32Type  ElementType = 'u'
	Uint64Type  ElementType = 'v'
	Int32Type   ElementType = 'i'
	Int64Type   ElementType = 'l'
	Float32Type ElementType = 'f'
	Float64Type ElementType = 'd'
	BoolType    ElementType = 'b'
	StringType  ElementType = 's'
	TimeType    ElementType = 't'
	ArrayType   ElementType = 'a'
	MapType     ElementType = 'm'
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
	case NullType:
		elemTypeName = "null"
	case Uint32Type:
		elemTypeName = "uint32"
	case Uint64Type:
		elemTypeName = "uint64"
	case Int32Type:
		elemTypeName = "int32"
	case Int64Type:
		elemTypeName = "int64"
	case Float32Type:
		elemTypeName = "float32"
	case Float64Type:
		elemTypeName = "float64"
	case BoolType:
		elemTypeName = "bool"
	case StringType:
		elemTypeName = "string"
	case TimeType:
		elemTypeName = "time"
	case ArrayType:
		elemTypeName = "array"
	case MapType:
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
		if m.ValueType == ArrayType {
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

func (r *Record) writeBool(offset uint16, value bool) {
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
		if err = checkElementType(Uint32Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint32(offset, uint32(value.(uint)))
		offsetAfterWrite = offset + 4
	case uint32:
		if err = checkElementType(Uint32Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint32(offset, value.(uint32))
		offsetAfterWrite = offset + 4
	case uint64:
		if err = checkElementType(Uint64Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint64(offset, value.(uint64))
		offsetAfterWrite = offset + 8
	case int:
		if err = checkElementType(Int32Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint32(offset, uint32(value.(int)))
		offsetAfterWrite = offset + 4
	case int32:
		if err = checkElementType(Int32Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint32(offset, uint32(value.(int32)))
		offsetAfterWrite = offset + 4
	case int64:
		if err = checkElementType(Int64Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint64(offset, uint64(value.(int64)))
		offsetAfterWrite = offset + 8
	case float32:
		if err = checkElementType(Float32Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint32(offset, math.Float32bits(value.(float32)))
		offsetAfterWrite = offset + 4
	case float64:
		if err = checkElementType(Float64Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeUint64(offset, math.Float64bits(value.(float64)))
		offsetAfterWrite = offset + 8
	case bool:
		if err = checkElementType(BoolType); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeBool(offset, value.(bool))
		offsetAfterWrite = offset + 1
	case string:
		if err = checkElementType(StringType); err != nil {
			offsetAfterWrite = offset
			break
		}
		r.writeString(offset, value.(string))
		offsetAfterWrite = offset + bytesNeededForString(value.(string))
	case time.Time:
		if err = checkElementType(TimeType); err != nil {
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
		if m.ValueType == ArrayType {
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

func (r *Record) readUint32(offset uint16) uint32 {
	return binary.LittleEndian.Uint32((*r)[offset : offset+4])
}

func (r *Record) readUint64(offset uint16) uint64 {
	return binary.LittleEndian.Uint64((*r)[offset : offset+8])
}

func (r *Record) readBool(offset uint16) bool {
	return (*r)[offset] == 1
}

func (r *Record) readString(offset uint16) (string, uint16) {
	strLen := binary.LittleEndian.Uint16((*r)[offset : offset+2])
	return string((*r)[offset+2 : offset+2+strLen]), strLen
}

func (r *Record) readPrimitive(offset uint16, expectedType ElementType) (any, uint16, error) {
	var value any
	var offsetAfterRead uint16
	var err error
	switch expectedType {
	case Uint32Type:
		value = r.readUint32(offset)
		offsetAfterRead = offset + 4
	case Uint64Type:
		value = r.readUint64(offset)
		offsetAfterRead = offset + 8
	case Int32Type:
		value = int32(r.readUint32(offset))
		offsetAfterRead = offset + 4
	case Int64Type:
		value = int64(r.readUint64(offset))
		offsetAfterRead = offset + 8
	case Float32Type:
		value = math.Float32frombits(r.readUint32(offset))
		offsetAfterRead = offset + 4
	case Float64Type:
		value = math.Float64frombits(r.readUint64(offset))
		offsetAfterRead = offset + 8
	case BoolType:
		value = r.readBool(offset)
		offsetAfterRead = offset + 1
	case StringType:
		strValue, strLen := r.readString(offset)
		value = strValue
		offsetAfterRead = offset + strLen + 2
	case TimeType:
		value = time.Unix(0, int64(r.readUint64(offset)))
		offsetAfterRead = offset + 8
	default:
		err = fmt.Errorf("unsupported primitive type %v", expectedType)
	}
	return value, offsetAfterRead, err
}

func (r *Record) readArray(offset uint16) (Array, uint16, error) {
	arrayLen := binary.LittleEndian.Uint16((*r)[offset : offset+2])
	offset += 2
	elementType := ElementType((*r)[offset])
	offset++
	a := Array{Values: make([]any, arrayLen), ElementType: elementType}
	for i := uint16(0); i < arrayLen; i++ {
		var err error
		a.Values[i], offset, err = r.readPrimitive(offset, elementType)
		if err != nil {
			return a, offset, err
		}
	}
	return a, offset, nil
}

func (r *Record) readMap(offset uint16) (Map, uint16, error) {
	mapLen := binary.LittleEndian.Uint16((*r)[offset : offset+2])
	offset += 2
	keyType := ElementType((*r)[offset])
	offset++
	valueType := ElementType((*r)[offset])
	offset++
	m := Map{Data: make(map[any]any), KeyType: keyType, ValueType: valueType}
	for i := uint16(0); i < mapLen; i++ {
		var key any
		var err error
		key, offset, err = r.readPrimitive(offset, keyType)
		if err != nil {
			return m, offset, err
		}
		if valueType == ArrayType {
			m.Data[key], offset, err = r.readArray(offset)
		} else {
			m.Data[key], offset, err = r.readPrimitive(offset, valueType)
		}
		if err != nil {
			return m, offset, err
		}
	}
	return m, offset, nil
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
	r.writeBool(offset, value)
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
	if a.ElementType == ArrayType {
		return &InvalidElementTypeError{a.ElementType}
	}
	if a.ElementType == MapType {
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
	if m.KeyType == ArrayType {
		return &InvalidKeyTypeError{m.KeyType}
	}
	if m.KeyType == MapType {
		return &InvalidKeyTypeError{m.KeyType}
	}
	if m.ValueType == MapType {
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
func (r *Record) GetUint32(position ElementPosition) (isNull bool, value uint32) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value = binary.LittleEndian.Uint32((*r)[offset : offset+4])
	}
	return isNull, value
}

// GetUint64 returns the uint64 value stored at the given element position in the record.
func (r *Record) GetUint64(position ElementPosition) (isNull bool, value uint64) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value = binary.LittleEndian.Uint64((*r)[offset : offset+8])
	}
	return isNull, value
}

// GetInt32 returns the int32 value stored at the given element position in the record.
func (r *Record) GetInt32(position ElementPosition) (isNull bool, value int32) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value = int32(binary.LittleEndian.Uint32((*r)[offset : offset+4]))
	}
	return isNull, value
}

// GetInt64 returns the int64 value stored at the given element position in the record.
func (r *Record) GetInt64(position ElementPosition) (isNull bool, value int64) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value = int64(binary.LittleEndian.Uint64((*r)[offset : offset+8]))
	}
	return isNull, value
}

// GetFloat32 returns the float32 value stored at the given element position in the record.
func (r *Record) GetFloat32(position ElementPosition) (isNull bool, value float32) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value = math.Float32frombits(binary.LittleEndian.Uint32((*r)[offset : offset+4]))
	}
	return isNull, value
}

// GetFloat64 returns the float64 value stored at the given element position in the record.
func (r *Record) GetFloat64(position ElementPosition) (isNull bool, value float64) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value = math.Float64frombits(binary.LittleEndian.Uint64((*r)[offset : offset+8]))
	}
	return isNull, value
}

// GetBool returns the bool value stored at the given element position in the record.
func (r *Record) GetBool(position ElementPosition) (isNull bool, value bool) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value = (*r)[offset] != 0
	}
	return isNull, value
}

// GetTime returns the Timestamp value stored at the given element position in the record.
func (r *Record) GetTime(position ElementPosition) (isNull bool, value time.Time) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value = time.Unix(0, int64(binary.LittleEndian.Uint64((*r)[offset:offset+8])))
	}
	return isNull, value
}

// GetString returns the string value stored at the given element position in the record.
func (r *Record) GetString(position ElementPosition) (isNull bool, value string) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value, _ = r.readString(offset)
	}
	return isNull, value
}

// GetArray returns the Array value stored at the given element position in the record.
func (r *Record) GetArray(position ElementPosition) (isNull bool, value Array, err error) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value, _, err = r.readArray(offset)
	}
	return isNull, value, err
}

// GetMap returns the Map value stored at the given element position in the record.
func (r *Record) GetMap(position ElementPosition) (isNull bool, value Map, err error) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value, _, err = r.readMap(offset)
	}
	return isNull, value, err
}
