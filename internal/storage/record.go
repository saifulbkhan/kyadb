package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"kyadb/internal/structs/element"
)

type Record element.Bytes
type ElementPosition = uint16

// WriteOverflowError is returned when there is not enough space in the record to write the given
// Data.
type WriteOverflowError struct {
	availableBytes uint16
	requiredBytes  uint16
	data           any
}

// InvalidElementTypeError is returned when user tries to create an Array with an unsupported
// element type.
type InvalidElementTypeError struct {
	elemType element.Type
}

// InvalidKeyTypeError is returned when user tries to create a Map with an unsupported key type.
type InvalidKeyTypeError struct {
	keyType element.Type
}

// InvalidValueTypeError is returned when user tries to create a Map with an unsupported value type.
type InvalidValueTypeError struct {
	valueType element.Type
}

func (e *WriteOverflowError) Error() string {
	return fmt.Sprintf("not enough space to write %v bytes for %v", e.requiredBytes, e.data)
}

func (e *InvalidElementTypeError) Error() string {
	elemTypeName, err := element.NameForType(e.elemType)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("invalid array element type '%s'", elemTypeName)
}

func (e *InvalidKeyTypeError) Error() string {
	keyTypeName, err := element.NameForType(e.keyType)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("invalid map key type '%s'", keyTypeName)
}

func (e *InvalidValueTypeError) Error() string {
	valueTypeName, err := element.NameForType(e.valueType)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("invalid map value type '%s'", valueTypeName)
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
	element.WriteUint32((*element.Bytes)(r), offset, value)
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
	element.WriteUint64((*element.Bytes)(r), offset, value)
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
	element.WriteBool((*element.Bytes)(r), offset, value)
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
		numBytes := element.BytesNeededForString(value)
		*r = append(*r, make([]byte, numBytes)...)
		element.WriteString((*element.Bytes)(r), offset, value)
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
		element.WriteString((*element.Bytes)(r), offset, value)
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
func (r *Record) SetArray(position ElementPosition, a element.Array) error {
	if a.ElementType == element.ArrayType {
		return &InvalidElementTypeError{a.ElementType}
	}
	if a.ElementType == element.MapType {
		return &InvalidElementTypeError{a.ElementType}
	}
	if a.Values == nil {
		return nil
	}

	offset := r.offsetForPosition(position)
	if offset == 0 {
		offset = r.Length()
		numBytes, err := element.BytesNeededForArray(a)
		if err != nil {
			return err
		}
		*r = append(*r, make([]byte, numBytes)...)
		_, err = element.WriteArray((*element.Bytes)(r), offset, a)
		if err != nil {
			return err
		}
		r.setOffset(position, offset)
		r.setLength(offset + numBytes)
	} else {
		currentElementType := (*r)[offset+2]
		if currentElementType != a.ElementType {
			return &element.TypeMismatchError{Expected: currentElementType, Actual: a.ElementType}
		}
		currentLength := binary.LittleEndian.Uint16((*r)[offset : offset+2])
		requiredLength := uint16(len(a.Values))
		if currentLength < requiredLength {
			return &WriteOverflowError{
				currentLength, requiredLength, a,
			}
		}
		_, err := element.WriteArray((*element.Bytes)(r), offset, a)
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
func (r *Record) SetMap(position ElementPosition, m element.Map) error {
	if m.KeyType == element.ArrayType {
		return &InvalidKeyTypeError{m.KeyType}
	}
	if m.KeyType == element.MapType {
		return &InvalidKeyTypeError{m.KeyType}
	}
	if m.ValueType == element.MapType {
		return &InvalidValueTypeError{m.ValueType}
	}
	if m.Data == nil {
		return nil
	}

	offset := r.offsetForPosition(position)
	if offset == 0 {
		offset = r.Length()
		numBytes, err := element.BytesNeededForMap(m)
		if err != nil {
			return err
		}
		*r = append(*r, make([]byte, numBytes)...)
		_, err = element.WriteMap((*element.Bytes)(r), offset, m)
		if err != nil {
			return err
		}
		r.setOffset(position, offset)
		r.setLength(offset + numBytes)
	} else {
		currentKeyType := (*r)[offset+2]
		if currentKeyType != m.KeyType {
			err := &element.TypeMismatchError{Expected: currentKeyType, Actual: m.KeyType}
			return fmt.Errorf("key type mismatch: %w", err)
		}
		currentValueType := (*r)[offset+3]
		if currentValueType != m.ValueType {
			err := &element.TypeMismatchError{Expected: currentValueType, Actual: m.ValueType}
			return fmt.Errorf("value type mismatch: %w", err)
		}
		currentLength := binary.LittleEndian.Uint16((*r)[offset : offset+2])
		requiredLength := uint16(len(m.Data))
		if currentLength < requiredLength {
			return &WriteOverflowError{
				currentLength, requiredLength, m,
			}
		}
		_, err := element.WriteMap((*element.Bytes)(r), offset, m)
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
		value, _ = element.ReadString((*element.Bytes)(r), offset)
	}
	return isNull, value
}

// GetArray returns the Array value stored at the given element position in the record.
func (r *Record) GetArray(position ElementPosition) (isNull bool, value element.Array, err error) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value, _, err = element.ReadArray((*element.Bytes)(r), offset)
	}
	return isNull, value, err
}

// GetMap returns the Map value stored at the given element position in the record.
func (r *Record) GetMap(position ElementPosition) (isNull bool, value element.Map, err error) {
	offset := r.offsetForPosition(position)
	isNull = offset == 0
	if !isNull {
		value, _, err = element.ReadMap((*element.Bytes)(r), offset)
	}
	return isNull, value, err
}
