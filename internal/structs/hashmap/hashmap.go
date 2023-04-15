package hashmap

import (
	"hash"
	"time"

	"kyadb/internal/structs/element"
)

type Hashable interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr | ~float32 | ~float64 | ~string | ~bool | time.Time
}

type HashMap[K Hashable, V any] interface {
	Set(key K, value V) error
	Has(key K) bool
	Get(key K) (V, error)
	Pop(key K) (V, error)
	Length() uint64
	AtIndex(uint64) (K, V)
	Clear()
}

func hashMod[K Hashable](key K, hash64 hash.Hash64, numSlots uint64) (uint64, error) {
	numBytesNeeded, err := element.BytesNeededForPrimitive(key)
	if err != nil {
		return 0, err
	}
	b := make([]byte, numBytesNeeded)
	_, err = element.WritePrimitive(&b, 0, key, element.AnyType)
	if err != nil {
		return 0, err
	}

	_, err = hash64.Write(b)
	if err != nil {
		return 0, err
	}
	hashed := hash64.Sum64() % numSlots
	hash64.Reset()

	return hashed, nil
}
