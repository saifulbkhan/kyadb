package storage

import (
	"encoding/binary"
	"fmt"
)

/*
 * First two bytes of the page store the number of slots in the page.
 * The next two bytes store an offset to the free space on the page.
 * After that is an array of slot entries. Each slot entry is 8 bytes and stores the byte offset or
 * the forwarded database address of a record.
 * The slot array is followed by the free space on the page.
 * The records are stored in reverse order on the page. The first record is stored at the end of
 * the page. The next record is stored before the first record and so on.
 */

const PageSize = 8 * 1024

// Page represents a page of data in a file.
type Page [PageSize]byte

// PageAddress is a unique identifier for a page in a file.
type PageAddress struct {
	FileID  uint16
	PageNum uint32
}

// RecordAddress represents the database address of a record. It is a combination of a record's page
// address and the slot number that stores the slotEntry for the record.
type RecordAddress struct {
	PageAddress
	SlotNum uint16
}

// slotEntry can store the offset of a record within a page or the forwarded address of the
// record within a file. The first 2 bytes represent the file number and the next 4 bytes represent
// the page number. The last 4 bytes represent the slot number or the record's offset within the
// page.
type slotEntry uint64

// PageFullError is returned when an operation cannot be completed because the page is full.
type PageFullError struct {
	available uint16
	needed    uint16
}

// RecordDeletedError is returned when a record is not present at the expected slot number.
type RecordDeletedError struct {
	SlotNum uint16
}

func (e *PageFullError) Error() string {
	return fmt.Sprintf(
		"operation cannot be completed, page full: available=%d, needed=%d", e.available, e.needed,
	)
}

func (e *RecordDeletedError) Error() string {
	return fmt.Sprintf("record at slot=%d has been deleted", e.SlotNum)
}

// recordAddressToSlotEntry casts a RecordAddress to a slotEntry.
func recordAddressToSlotEntry(addr RecordAddress) slotEntry {
	return slotEntry(addr.FileID)<<48 |
		slotEntry(addr.PageNum)<<16 |
		slotEntry(addr.SlotNum)
}

// offsetToSlotEntry casts a slotEntry to a RecordAddress.
func slotEntryToRecordAddress(offset slotEntry) RecordAddress {
	return RecordAddress{
		PageAddress: PageAddress{
			FileID:  uint16(offset >> 48),
			PageNum: uint32(offset >> 16),
		},
		SlotNum: uint16(offset),
	}
}

// isForwardedAddress returns true if a slotEntry represents a forwarded address. If the first two bytes
// are max uint16, then the slot entry is a not a forwarded address. We use max uint16 because
// the file ID is stored as uint16, but no file ID is ever max uint16.
func (s slotEntry) isForwardedAddress() bool {
	return s>>48 == 0xffff
}

// setNumSlots sets the number of slots in the page.
func (p *Page) setNumSlots(numSlots uint16) {
	binary.LittleEndian.PutUint16(p[:2], numSlots)
}

// getNumSlots returns the number of slots in the page.
func (p *Page) getNumSlots() uint16 {
	return binary.LittleEndian.Uint16(p[:2])
}

// setFreeOffset sets the offset to the free space on the page.
func (p *Page) setFreeOffset(offset uint16) {
	binary.LittleEndian.PutUint16(p[2:4], offset)
}

// getFreeOffset returns the offset to the free space on the page.
func (p *Page) getFreeOffset() uint16 {
	return binary.LittleEndian.Uint16(p[2:4])
}

// addSlot adds a slot entry to the page.
func (p *Page) addSlot(slot slotEntry) {
	numSlots := p.getNumSlots()
	binary.LittleEndian.PutUint64(p[4+8*numSlots:], uint64(slot))
	p.setNumSlots(numSlots + 1)
}

// setSlot sets the slot entry at the given slot number.
func (p *Page) setSlot(slotNum uint16, slot slotEntry) {
	binary.LittleEndian.PutUint64(p[4+8*slotNum:], uint64(slot))
}

// getSlot returns the slot entry at the given number.
func (p *Page) getSlot(slotNum uint16) slotEntry {
	return slotEntry(binary.LittleEndian.Uint64(p[4+8*slotNum:]))
}

// NewPage returns a new page.
func NewPage() *Page {
	p := &Page{}
	p.setNumSlots(0)
	p.setFreeOffset(PageSize)
	return p
}

// AddRecord adds a record to the page. It returns the slot number of the record.
func (p *Page) AddRecord(record *Record) (uint16, error) {
	// Get the free offset.
	offset := p.getFreeOffset()

	// Get the number of slots.
	numSlots := p.getNumSlots()

	// Calculate the new free offset.
	newOffset := offset - record.Length()

	// Check if the page has enough space for the record.
	headerLength := 4 + 8*numSlots
	newHeaderEnd := headerLength + 8
	if newOffset < newHeaderEnd {
		return 0, &PageFullError{
			available: offset - newHeaderEnd,
			needed:    record.Length(),
		}
	}

	// Write the record to the page.
	copy(p[newOffset:offset], *record)

	// Add the slot entry.
	p.addSlot(slotEntry(newOffset))

	// Update the free offset.
	p.setFreeOffset(newOffset)

	// Return the slot number (zero-indexed), which is now the same as the original number of slots.
	return numSlots, nil
}

// GetRecord returns the record at the given slot number.
//
// If the record is no longer on the page but has been moved to another page then the second return
// value is set to a non-nil address value instead.
//
// If the record has been deleted then RecordDeletedError is returned.
func (p *Page) GetRecord(slotNum uint16) (*Record, *RecordAddress, error) {
	// Get the slot entry value.
	entry := p.getSlot(slotNum)

	// If the slot entry is 0 (tombstone), then the record has been deleted.
	if entry == 0 {
		return nil, nil, &RecordDeletedError{slotNum}
	}

	// If the slot entry is a forwarded address then return the forwarded address.
	if entry.isForwardedAddress() {
		addr := slotEntryToRecordAddress(entry)
		return nil, &addr, nil
	}

	// Otherwise return the record at the entry.
	recordLength := binary.LittleEndian.Uint16(p[entry : entry+2])
	record := Record(p[entry : uint16(entry)+recordLength])
	return &record, nil, nil
}

// SetForwardedAddress sets the slot entry to a forwarded address.
func (p *Page) SetForwardedAddress(slotNum uint16, addr RecordAddress) {
	p.setSlot(slotNum, recordAddressToSlotEntry(addr))
}

// UpdateRecord updates a record at the given slot number.
//
// If the record is no longer on the page but has been moved to another page then the first return
// value is set to a non-nil address value instead. This new address should be used to update the
// record.
//
// If the record has been deleted then RecordDeletedError is returned.
//
// If the updated record is too large to fit on the page then PageFullError is returned. When
// PageFullError is returned the record is not updated. It is the caller's responsibility to add the
// updated record to a new page and update the record's slot entry to its new address on this page
// using the SetForwardedAddress method.
func (p *Page) UpdateRecord(slotNum uint16, record *Record) (*RecordAddress, error) {
	// Get the slot entry value.
	entry := p.getSlot(slotNum)

	// If the slot entry is 0 (tombstone), then the record has been deleted.
	if entry == 0 {
		return nil, &RecordDeletedError{slotNum}
	}

	// If the slot entry is a forwarded address then return the forwarded address.
	if entry.isForwardedAddress() {
		addr := slotEntryToRecordAddress(entry)
		return &addr, nil
	}

	// Otherwise update the record at the entry.
	recordLength := binary.LittleEndian.Uint16(p[entry : entry+2])
	if recordLength < record.Length() {
		// If the new record is larger than the existing one, then we need to move the record to a
		// new location on the page and update the slot entry.
		offset := p.getFreeOffset()
		newOffset := offset - record.Length()

		// Check if the page has enough space for the record.
		numSlots := p.getNumSlots()
		headerLength := 4 + 8*numSlots
		newHeaderEnd := headerLength + 8
		if newOffset < newHeaderEnd {
			return nil, &PageFullError{
				available: offset - newHeaderEnd,
				needed:    record.Length(),
			}
		}

		copy(p[newOffset:], *record)
		p.setSlot(slotNum, slotEntry(newOffset))
		p.setFreeOffset(newOffset)
	}
	copy(p[entry:uint16(entry)+recordLength], *record)
	return nil, nil
}

func (p *Page) DeleteRecord(slotNum uint16) {
	p.setSlot(slotNum, 0)
}
