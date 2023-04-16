package hashmap

import (
	"fmt"
	"hash"
	"hash/maphash"

	"kyadb/internal/structs/set"
)

type LPHashMapElement[K Hashable, V any] struct {
	key   K
	value V
}

type LPHashMap[K Hashable, V any] struct {
	numSlots   uint64
	numItems   uint64
	elements   []*LPHashMapElement[K, V]
	tombstones *set.Set[uint64]
	hash64     hash.Hash64
}

type KeyNotFoundError struct {
	key any
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("key not found in hashmap: %v", e.key)
}

type IndexOutOfBoundsError struct {
	index uint64
}

func (e *IndexOutOfBoundsError) Error() string {
	return fmt.Sprintf("index out of bounds: %d", e.index)
}

func NewLPHashMap[K Hashable, V any](numSlots uint64) *LPHashMap[K, V] {
	hash64 := maphash.Hash{}
	hash64.SetSeed(maphash.MakeSeed())
	return &LPHashMap[K, V]{
		numSlots:   numSlots,
		elements:   make([]*LPHashMapElement[K, V], numSlots),
		tombstones: set.NewSet[uint64](),
		hash64:     &hash64,
	}
}

// doubleSlots doubles the number of slots in the hash map and rehashes all the elements.
func (h *LPHashMap[K, V]) doubleSlots() error {
	currentElements := h.elements
	h.numSlots = h.numSlots * 2
	h.elements = make([]*LPHashMapElement[K, V], h.numSlots)
	h.numItems = 0
	for _, element := range currentElements {
		if element == nil {
			continue
		}
		err := h.Set(element.key, element.value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *LPHashMap[K, V]) Set(key K, value V) error {
	slot, err := hashMod(key, h.hash64, h.numSlots)
	if err != nil {
		return err
	}

	// Check if a different key already exists at this slot
	rightSlot := slot
	for h.elements[rightSlot] != nil {
		nextSlot := (rightSlot + 1) % h.numSlots
		if h.elements[rightSlot].key == key {
			break
		} else if h.tombstones.Has(rightSlot) {
			h.tombstones.Remove(rightSlot)
			break
		} else if nextSlot == slot {
			// If we have looped back to the original slot, then double the number of slots
			err := h.doubleSlots()
			if err != nil {
				return err
			}
			return h.Set(key, value)
		}
		rightSlot = nextSlot
	}

	h.elements[rightSlot] = &(LPHashMapElement[K, V]{key: key, value: value})
	h.numItems++

	return nil
}

func (h *LPHashMap[K, V]) Has(key K) bool {
	slot, err := hashMod(key, h.hash64, h.numSlots)
	if err != nil {
		return false
	}

	rightSlot := slot
	for h.elements[rightSlot] != nil {
		nextSlot := (rightSlot + 1) % h.numSlots
		if h.elements[rightSlot].key == key {
			return !(h.tombstones.Has(rightSlot))
		} else if nextSlot == slot {
			return false
		}
		rightSlot = nextSlot
	}
	return false
}

func (h *LPHashMap[K, V]) getValueAndSlot(key K) (V, uint64, error) {
	var zero V
	slot, err := hashMod(key, h.hash64, h.numSlots)
	if err != nil {
		return zero, 0, err
	}

	rightSlot := slot
	for h.elements[rightSlot] != nil {
		nextSlot := (rightSlot + 1) % h.numSlots
		if h.elements[rightSlot].key == key {
			if h.tombstones.Has(rightSlot) {
				// Key was once there, but has now been deleted
				return zero, 0, &KeyNotFoundError{key: key}
			}
			return h.elements[rightSlot].value, rightSlot, nil
		} else if nextSlot == slot {
			return zero, 0, &KeyNotFoundError{key: key}
		}
		rightSlot = nextSlot
	}
	return zero, 0, &KeyNotFoundError{key: key}
}

func (h *LPHashMap[K, V]) Get(key K) (V, error) {
	value, _, err := h.getValueAndSlot(key)
	return value, err
}

func (h *LPHashMap[K, V]) Pop(key K) (V, error) {
	value, slot, err := h.getValueAndSlot(key)
	if err != nil {
		return value, err
	}
	h.tombstones.Add(slot)
	h.numItems--
	return value, nil
}

func (h *LPHashMap[K, V]) Length() uint64 {
	return h.numItems
}

func (h *LPHashMap[K, V]) AtIndex(index uint64) (K, V, error) {
	var key K
	var value V
	if index >= h.numItems {
		return key, value, &IndexOutOfBoundsError{index: index}
	}

	keyValuePairFound := false
	for _, element := range h.elements {
		if element == nil {
			continue
		}
		if index == 0 {
			key = element.key
			value = element.value
			keyValuePairFound = true
			break
		}
		index--
	}

	if !keyValuePairFound {
		return key, value, &IndexOutOfBoundsError{index: index}
	}
	return key, value, nil
}

func (h *LPHashMap[K, V]) Clear() {
	h.numItems = 0
	h.elements = make([]*LPHashMapElement[K, V], h.numSlots)
	h.tombstones.Clear()
}
