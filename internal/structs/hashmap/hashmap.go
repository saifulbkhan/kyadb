package hashmap

import (
	"time"
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
