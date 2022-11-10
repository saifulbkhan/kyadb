package storage

import (
	"errors"
	"fmt"
	"math"
	"time"
)

//
// const (
// 	INTEGER ElementType = 'i'
// 	LONG    ElementType = 'l'
// 	FLOAT   ElementType = 'f'
// 	DOUBLE  ElementType = 'd'
// 	BOOLEAN ElementType = 'b'
// 	STRING  ElementType = 's'
// 	TIME    ElementType = 't'
// 	ARRAY   ElementType = 'a'
// 	MAP     ElementType = 'm'
// )
//
// type Record []byte
// type RecordOffset int16
// type ElementType byte
//
// type Array struct {
// 	ElementType ElementType
// 	Values      []any
// }
//
// type Map struct {
// 	KeyType   ElementType
// 	ValueType ElementType
// 	data      map[any]any
// }

// -------------------------------------------------------------------------------------------------
// QUESTION - how do you update a record? You can't just append to the end of the record, you need
// to update the offsets of all the other records. So you need to be able to update the record in
// place. This is a problem for the string, map and array types, because they have variable length.
// -------------------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------------------
// Algorithm for storing null values:
// 1. Use a function to set a null at a given offset (also takes in an element type):
//    - This function will set the bit for the latest element in bitmap to 1.
//    - This function however will also store a default value for the element at given byte offset,
//      only if the element type is not a variable-length element like string, array, or map.
// 2. Use a function to check if a value for a given column ID is null
//    - This function will just check if the bit at the given column ID (number) is set to 1 in the
//      bitmap.
// -------------------------------------------------------------------------------------------------

func ElementTypeOfValue(val any) (ElementType, error) {
	var elementType ElementType
	var err error
	switch val.(type) {
	case int:
		elementType = INTEGER
	case int64:
		elementType = LONG
	case float32:
		elementType = FLOAT
	case float64:
		elementType = DOUBLE
	case bool:
		elementType = BOOLEAN
	case string:
		elementType = STRING
	case time.Time:
		elementType = TIME
	case Array:
		elementType = ARRAY
	case Map:
		elementType = MAP
	default:
		err = fmt.Errorf("unsupported type %T", val)
	}
	return elementType, err
}

func typeNameOfValue(val any) (string, error) {
	var valType string
	var err error
	switch val.(type) {
	case int:
		valType = "int"
	case int64:
		valType = "int64"
	case float32:
		valType = "float32"
	case float64:
		valType = "float64"
	case bool:
		valType = "bool"
	case string:
		valType = "string"
	case time.Time:
		valType = "time.Time"
	case Array:
		valType = "array"
	default:
		err = fmt.Errorf("unsupported type %T", val)
	}
	return valType, err
}

func (r *Record) SerializeInt(val int) {
	*r = append(*r, byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
}

func (r *Record) SerializeLong(val int64) {
	*r = append(
		*r,
		byte(val>>56),
		byte(val>>48),
		byte(val>>40),
		byte(val>>32),
		byte(val>>24),
		byte(val>>16),
		byte(val>>8),
		byte(val),
	)
}

func (r *Record) SerializeFloat(val float32) {
	r.SerializeInt(int(math.Float32bits(val)))
}

func (r *Record) SerializeDouble(val float64) {
	r.SerializeLong(int64(math.Float64bits(val)))
}

func (r *Record) SerializeBool(val bool) {
	if val {
		*r = append(*r, byte(1))
	} else {
		*r = append(*r, byte(0))
	}
}

func (r *Record) SerializeString(str string) {
	r.SerializeInt(len(str))
	for _, val := range []byte(str) {
		*r = append(*r, val)
	}
}

func (r *Record) SerializeTime(val time.Time) {
	r.SerializeLong(val.UnixNano())
}

func (r *Record) serializeAny(val any, expectedType ElementType) error {
	checkValType := func(actualType ElementType) error {
		if actualType != expectedType {
			typeName, err := typeNameOfValue(val)
			if err != nil {
				return err
			}
			return fmt.Errorf("expected type '%s', got '%T'", typeName, val)
		}
		return nil
	}

	switch val := val.(type) {
	case int:
		err := checkValType(INTEGER)
		if err != nil {
			return err
		}
		r.SerializeInt(val)
	case int64:
		err := checkValType(LONG)
		if err != nil {
			return err
		}
		r.SerializeLong(val)
	case float32:
		err := checkValType(FLOAT)
		if err != nil {
			return err
		}
		r.SerializeFloat(val)
	case float64:
		err := checkValType(DOUBLE)
		if err != nil {
			return err
		}
		r.SerializeDouble(val)
	case bool:
		err := checkValType(BOOLEAN)
		if err != nil {
			return err
		}
		r.SerializeBool(val)
	case string:
		err := checkValType(STRING)
		if err != nil {
			return err
		}
		r.SerializeString(val)
	case time.Time:
		err := checkValType(TIME)
		if err != nil {
			return err
		}
		r.SerializeTime(val)
	case Array:
		if len(val.Values) > 0 {
			err := r.SerializeArray(val)
			if err != nil {
				return err
			}
		}
	case []any:
		return errors.New("regular arrays are not supported, wrap them in Array struct")
	default:
		return errors.New(fmt.Sprintf("unsupported type: %T", val))
	}
	return nil
}

func (r *Record) SerializeArray(data Array) error {
	if data.ElementType == ARRAY {
		return errors.New("arrays cannot be elements of an array")
	}
	if data.ElementType == MAP {
		return errors.New("maps cannot be elements of an array")
	}

	r.SerializeInt(len(data.Values))
	*r = append(*r, byte(data.ElementType))
	if len(data.Values) > 0 {
		for _, elem := range data.Values {
			err := r.serializeAny(elem, data.ElementType)
			if err != nil {
				return fmt.Errorf("error serializing array: %w", err)
			}
		}
	}

	return nil
}

func (r *Record) SerializeMap(m Map) error {
	if m.KeyType == ARRAY {
		return errors.New("arrays cannot be keys of a map")
	}
	if m.KeyType == MAP {
		return errors.New("maps cannot be keys of a map")
	}
	if m.ValueType == MAP {
		return errors.New("maps cannot be values of a map")
	}

	r.SerializeInt(len(m.data))
	*r = append(*r, byte(m.KeyType), byte(m.ValueType))
	if len(m.data) > 0 {
		for key, value := range m.data {
			err := r.serializeAny(key, m.KeyType)
			if err != nil {
				return fmt.Errorf("error serializing key '%v' in map: %w", key, err)
			}

			err = r.serializeAny(value, m.ValueType)
			if err != nil {
				return fmt.Errorf("error serializing value '%v' in map: %w", value, err)
			}
		}
	}

	return nil
}

func (r *Record) DeserializeInt(offset RecordOffset) (int, RecordOffset) {
	return int((*r)[offset])<<24 |
		int((*r)[offset+1])<<16 |
		int((*r)[offset+2])<<8 |
		int((*r)[offset+3]), offset + 4
}

func (r *Record) DeserializeLong(offset RecordOffset) (int64, RecordOffset) {
	return int64((*r)[offset])<<56 |
		int64((*r)[offset+1])<<48 |
		int64((*r)[offset+2])<<40 |
		int64((*r)[offset+3])<<32 |
		int64((*r)[offset+4])<<24 |
		int64((*r)[offset+5])<<16 |
		int64((*r)[offset+6])<<8 |
		int64((*r)[offset+7]), offset + 8
}

func (r *Record) DeserializeFloat(offset RecordOffset) (float32, RecordOffset) {
	val, newOffset := r.DeserializeInt(offset)
	return math.Float32frombits(uint32(val)), newOffset
}

func (r *Record) DeserializeDouble(offset RecordOffset) (float64, RecordOffset) {
	val, newOffset := r.DeserializeLong(offset)
	return math.Float64frombits(uint64(val)), newOffset
}

func (r *Record) DeserializeBool(offset RecordOffset) (bool, RecordOffset) {
	return (*r)[offset] != 0, offset + 1
}

func (r *Record) DeserializeString(offset RecordOffset) (string, RecordOffset) {
	length, newOffset := r.DeserializeInt(offset)
	return string((*r)[newOffset : int(newOffset)+length]), newOffset + RecordOffset(length)
}

func (r *Record) DeserializeTime(offset RecordOffset) (time.Time, RecordOffset) {
	val, newOffset := r.DeserializeLong(offset)
	return time.Unix(0, val), newOffset
}

func (r *Record) deserializeAny(offset RecordOffset, elementType ElementType) (
	any, RecordOffset, error,
) {
	var val any
	var newOffset RecordOffset
	var err error

	switch elementType {
	case INTEGER:
		val, newOffset = r.DeserializeInt(offset)
	case LONG:
		val, newOffset = r.DeserializeLong(offset)
	case FLOAT:
		val, newOffset = r.DeserializeFloat(offset)
	case DOUBLE:
		val, newOffset = r.DeserializeDouble(offset)
	case BOOLEAN:
		val, newOffset = r.DeserializeBool(offset)
	case STRING:
		val, newOffset = r.DeserializeString(offset)
	case TIME:
		val, newOffset = r.DeserializeTime(offset)
	default:
		err = errors.New(
			fmt.Sprintf("invalid deserialization attempt for type: %T", elementType),
		)
	}
	return val, newOffset, err
}

func (r *Record) DeserializeArray(offset RecordOffset) (Array, RecordOffset, error) {
	length, newOffset := r.DeserializeInt(offset)
	elementType := ElementType((*r)[newOffset])
	newOffset++

	if elementType == ARRAY {
		return Array{
			elementType,
			[]any{},
		}, offset, errors.New("arrays cannot be elements of an array")
	}
	if elementType == MAP {
		return Array{
			elementType,
			[]any{},
		}, offset, errors.New("maps cannot be elements of an array")
	}

	result := make([]any, length)
	for i := 0; i < length; i++ {
		var val any
		var err error
		val, newOffset, err = r.deserializeAny(newOffset, elementType)
		if err != nil {
			return Array{elementType, []any{}}, offset, err
		}
		result[i] = val
	}

	return Array{elementType, result}, newOffset, nil
}

func (r *Record) DeserializeMap(offset RecordOffset) (Map, RecordOffset, error) {
	length, newOffset := r.DeserializeInt(offset)
	keyType, newOffset := ElementType((*r)[newOffset]), newOffset+1
	valueType, newOffset := ElementType((*r)[newOffset]), newOffset+1

	result := make(map[any]any, length)
	for i := 0; i < length; i++ {
		var key any
		var err error
		key, newOffset, err = r.deserializeAny(newOffset, keyType)
		if err != nil {
			err = fmt.Errorf("error deserializing key in map: %w", err)
			return Map{keyType, valueType, map[any]any{}}, offset, err
		}

		var value any
		if valueType == ARRAY {
			value, newOffset, err = r.DeserializeArray(newOffset)
			if err != nil {
				err = fmt.Errorf("error deserializing array value in map: %w", err)
				return Map{keyType, valueType, map[any]any{}}, offset, err
			}
		} else {
			value, newOffset, err = r.deserializeAny(newOffset, valueType)
			if err != nil {
				err = fmt.Errorf("error deserializing value in map: %w", err)
				return Map{keyType, valueType, map[any]any{}}, offset, err
			}
		}
		result[key] = value
	}

	return Map{keyType, valueType, result}, newOffset, nil
}
