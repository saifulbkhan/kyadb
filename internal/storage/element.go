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

type Bytes = []byte
type ElementType = byte

type Array struct {
	ElementType ElementType
	Values      []any
}

type Map struct {
	KeyType   ElementType
	ValueType ElementType
	Data      map[any]any
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

func (e *TypeMismatchError) Error() string {
	expectedTypeName, err := NameForElementType(e.expected)
	if err != nil {
		return err.Error()
	}
	actualTypeName, err := NameForElementType(e.actual)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("expected type '%s', got '%s'", expectedTypeName, actualTypeName)
}

func (e *UnrecognizedTypeError) Error() string {
	return fmt.Sprintf("unrecognized type %T", e.value)
}

func NameForElementType(elemType ElementType) (string, error) {
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

func ElementTypeForValue(value any) (ElementType, error) {
	var elemType ElementType
	var err error
	switch value.(type) {
	case bool:
		elemType = BoolType
	case uint32:
		elemType = Uint32Type
	case uint64:
		elemType = Uint64Type
	case int32:
		elemType = Int32Type
	case int64:
		elemType = Int64Type
	case float32:
		elemType = Float32Type
	case float64:
		elemType = Float64Type
	case string:
		elemType = StringType
	case time.Time:
		elemType = TimeType
	case Array:
		elemType = ArrayType
	case Map:
		elemType = MapType
	default:
		err = fmt.Errorf("unsupported type %T", value)
	}
	return elemType, err
}

func IsPrimitiveElementType(elemType ElementType) bool {
	return elemType != NullType && elemType != ArrayType && elemType != MapType
}

func BytesNeededForString(str string) uint16 {
	return uint16(len(str)) + 2
}

func BytesNeededForPrimitive(value any) (uint16, error) {
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
		bytesNeeded = BytesNeededForString(value.(string))
	default:
		err = fmt.Errorf("unsupported primitive type %T", value)
	}
	return bytesNeeded, err
}

func BytesNeededForArray(a Array) (uint16, error) {
	var bytesNeeded uint16
	var err error
	for _, value := range a.Values {
		bytesNeededForElement, err := BytesNeededForPrimitive(value)
		if err != nil {
			break
		}
		bytesNeeded += bytesNeededForElement
	}
	return bytesNeeded + 3, err
}

func BytesNeededForMap(m Map) (uint16, error) {
	var bytesNeeded uint16
	var err error
	for key, value := range m.Data {
		bytesNeededForKey, err := BytesNeededForPrimitive(key)
		if err != nil {
			break
		}
		var bytesNeededForValue uint16
		if m.ValueType == ArrayType {
			bytesNeededForValue, err = BytesNeededForArray(value.(Array))
		} else {
			bytesNeededForValue, err = BytesNeededForPrimitive(value)
		}
		if err != nil {
			break
		}
		bytesNeeded += bytesNeededForKey + bytesNeededForValue
	}
	return bytesNeeded + 4, err
}

func WriteUint16(b *Bytes, offset uint16, value uint16) {
	(*b)[offset] = byte(value)
	(*b)[offset+1] = byte(value >> 8)
}

func WriteUint32(b *Bytes, offset uint16, value uint32) {
	(*b)[offset] = byte(value)
	(*b)[offset+1] = byte(value >> 8)
	(*b)[offset+2] = byte(value >> 16)
	(*b)[offset+3] = byte(value >> 24)
}

func WriteUint64(b *Bytes, offset uint16, value uint64) {
	(*b)[offset] = byte(value)
	(*b)[offset+1] = byte(value >> 8)
	(*b)[offset+2] = byte(value >> 16)
	(*b)[offset+3] = byte(value >> 24)
	(*b)[offset+4] = byte(value >> 32)
	(*b)[offset+5] = byte(value >> 40)
	(*b)[offset+6] = byte(value >> 48)
	(*b)[offset+7] = byte(value >> 56)
}

func WriteBool(b *Bytes, offset uint16, value bool) {
	if value {
		(*b)[offset] = 1
	} else {
		(*b)[offset] = 0
	}
}

func WriteString(b *Bytes, offset uint16, value string) {
	strLen := uint16(len(value))
	(*b)[offset] = byte(strLen)
	(*b)[offset+1] = byte(strLen >> 8)
	copy((*b)[offset+2:offset+2+strLen], value)
}

func WritePrimitive(b *Bytes, offset uint16, value any, expectedType ElementType) (uint16, error) {
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
		WriteUint32(b, offset, uint32(value.(uint)))
		offsetAfterWrite = offset + 4
	case uint32:
		if err = checkElementType(Uint32Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		WriteUint32(b, offset, value.(uint32))
		offsetAfterWrite = offset + 4
	case uint64:
		if err = checkElementType(Uint64Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		WriteUint64(b, offset, value.(uint64))
		offsetAfterWrite = offset + 8
	case int:
		if err = checkElementType(Int32Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		WriteUint32(b, offset, uint32(value.(int)))
		offsetAfterWrite = offset + 4
	case int32:
		if err = checkElementType(Int32Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		WriteUint32(b, offset, uint32(value.(int32)))
		offsetAfterWrite = offset + 4
	case int64:
		if err = checkElementType(Int64Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		WriteUint64(b, offset, uint64(value.(int64)))
		offsetAfterWrite = offset + 8
	case float32:
		if err = checkElementType(Float32Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		WriteUint32(b, offset, math.Float32bits(value.(float32)))
		offsetAfterWrite = offset + 4
	case float64:
		if err = checkElementType(Float64Type); err != nil {
			offsetAfterWrite = offset
			break
		}
		WriteUint64(b, offset, math.Float64bits(value.(float64)))
		offsetAfterWrite = offset + 8
	case bool:
		if err = checkElementType(BoolType); err != nil {
			offsetAfterWrite = offset
			break
		}
		WriteBool(b, offset, value.(bool))
		offsetAfterWrite = offset + 1
	case string:
		if err = checkElementType(StringType); err != nil {
			offsetAfterWrite = offset
			break
		}
		WriteString(b, offset, value.(string))
		offsetAfterWrite = offset + BytesNeededForString(value.(string))
	case time.Time:
		if err = checkElementType(TimeType); err != nil {
			offsetAfterWrite = offset
			break
		}
		WriteUint64(b, offset, uint64(value.(time.Time).UnixNano()))
		offsetAfterWrite = offset + 8
	default:
		err = fmt.Errorf("unsupported primitive type %T", value)
	}
	return offsetAfterWrite, err
}

func WriteArray(b *Bytes, offset uint16, a Array) (uint16, error) {
	newOffset := offset
	(*b)[newOffset] = byte(len(a.Values))
	newOffset++
	(*b)[newOffset] = byte(len(a.Values) >> 8)
	newOffset++
	(*b)[newOffset] = a.ElementType
	newOffset++
	for _, value := range a.Values {
		var err error
		newOffset, err = WritePrimitive(b, newOffset, value, a.ElementType)
		if err != nil {
			return offset, err
		}
	}
	return newOffset, nil
}

func WriteMap(b *Bytes, offset uint16, m Map) (uint16, error) {
	newOffset := offset
	(*b)[newOffset] = byte(len(m.Data))
	newOffset++
	(*b)[newOffset] = byte(len(m.Data) >> 8)
	newOffset++
	(*b)[newOffset] = m.KeyType
	newOffset++
	(*b)[newOffset] = m.ValueType
	newOffset++
	for key, value := range m.Data {
		var err error
		newOffset, err = WritePrimitive(b, newOffset, key, m.KeyType)
		if err != nil {
			return offset, err
		}
		if m.ValueType == ArrayType {
			newOffset, err = WriteArray(b, newOffset, value.(Array))
		} else {
			newOffset, err = WritePrimitive(b, newOffset, value, m.ValueType)
		}
		if err != nil {
			return offset, err
		}
	}
	return newOffset, nil
}

func ReadUint16(b *Bytes, offset uint16) uint16 {
	return binary.LittleEndian.Uint16((*b)[offset : offset+2])
}

func ReadUint32(b *Bytes, offset uint16) uint32 {
	return binary.LittleEndian.Uint32((*b)[offset : offset+4])
}

func ReadUint64(b *Bytes, offset uint16) uint64 {
	return binary.LittleEndian.Uint64((*b)[offset : offset+8])
}

func ReadBool(b *Bytes, offset uint16) bool {
	return (*b)[offset] == 1
}

func ReadString(b *Bytes, offset uint16) (string, uint16) {
	strLen := binary.LittleEndian.Uint16((*b)[offset : offset+2])
	return string((*b)[offset+2 : offset+2+strLen]), strLen
}

func ReadPrimitive(b *Bytes, offset uint16, expectedType ElementType) (any, uint16, error) {
	var value any
	var offsetAfterRead uint16
	var err error
	switch expectedType {
	case Uint32Type:
		value = ReadUint32(b, offset)
		offsetAfterRead = offset + 4
	case Uint64Type:
		value = ReadUint64(b, offset)
		offsetAfterRead = offset + 8
	case Int32Type:
		value = int32(ReadUint32(b, offset))
		offsetAfterRead = offset + 4
	case Int64Type:
		value = int64(ReadUint64(b, offset))
		offsetAfterRead = offset + 8
	case Float32Type:
		value = math.Float32frombits(ReadUint32(b, offset))
		offsetAfterRead = offset + 4
	case Float64Type:
		value = math.Float64frombits(ReadUint64(b, offset))
		offsetAfterRead = offset + 8
	case BoolType:
		value = ReadBool(b, offset)
		offsetAfterRead = offset + 1
	case StringType:
		strValue, strLen := ReadString(b, offset)
		value = strValue
		offsetAfterRead = offset + strLen + 2
	case TimeType:
		value = time.Unix(0, int64(ReadUint64(b, offset)))
		offsetAfterRead = offset + 8
	default:
		err = fmt.Errorf("unsupported primitive type %v", expectedType)
	}
	return value, offsetAfterRead, err
}

func ReadArray(b *Bytes, offset uint16) (Array, uint16, error) {
	arrayLen := binary.LittleEndian.Uint16((*b)[offset : offset+2])
	offset += 2
	elementType := (*b)[offset]
	offset++
	a := Array{Values: make([]any, arrayLen), ElementType: elementType}
	for i := uint16(0); i < arrayLen; i++ {
		var err error
		a.Values[i], offset, err = ReadPrimitive(b, offset, elementType)
		if err != nil {
			return a, offset, err
		}
	}
	return a, offset, nil
}

func ReadMap(b *Bytes, offset uint16) (Map, uint16, error) {
	mapLen := binary.LittleEndian.Uint16((*b)[offset : offset+2])
	offset += 2
	keyType := (*b)[offset]
	offset++
	valueType := (*b)[offset]
	offset++
	m := Map{Data: make(map[any]any), KeyType: keyType, ValueType: valueType}
	for i := uint16(0); i < mapLen; i++ {
		var key any
		var err error
		key, offset, err = ReadPrimitive(b, offset, keyType)
		if err != nil {
			return m, offset, err
		}
		if valueType == ArrayType {
			m.Data[key], offset, err = ReadArray(b, offset)
		} else {
			m.Data[key], offset, err = ReadPrimitive(b, offset, valueType)
		}
		if err != nil {
			return m, offset, err
		}
	}
	return m, offset, nil
}
