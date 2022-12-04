package storage

import (
	"encoding/binary"
	"fmt"
)

const (
	PageSize        = 8 * 1024
	MaxPagesPerFile = 256 * 1024
	MaxFileSize     = PageSize * MaxPagesPerFile // 2GB
	MaxNumFiles     = 1024
)

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

func (e *PageFullError) Error() string {
	return fmt.Sprintf(
		"operation cannot be completed, page full: available=%d, needed=%d", e.available, e.needed,
	)
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

// isForwarded returns true if a slotEntry represents a forwarded address. If the first two bytes
// are max uint16, then the slot entry is a not a forwarded address. We use max uint16 because
// the file ID is stored as uint16, but no file ID is ever max uint16.
func (s slotEntry) isForwarded() bool {
	return s>>48 == 0xffff
}

/*
 * First two bytes of the page store the number of slots in the page.
 * The next two bytes store an offset to the free space on the page.
 * After that is an array of slot entries. Each slot entry is 8 bytes and stores the byte offset or
 * the forwarded database address of a record.
 * The slot array is followed by the free space on the page.
 * The records are stored in reverse order on the page. The first record is stored at the end of
 * the page. The next record is stored before the first record and so on.
 */

// setNumSlots sets the number of slots in the page.
func (p *Page) setNumSlots(numSlots uint16) {
	binary.BigEndian.PutUint16(p[:2], numSlots)
}

// getNumSlots returns the number of slots in the page.
func (p *Page) getNumSlots() uint16 {
	return binary.BigEndian.Uint16(p[:2])
}

// setFreeOffset sets the offset to the free space on the page.
func (p *Page) setFreeOffset(offset uint16) {
	binary.BigEndian.PutUint16(p[2:4], offset)
}

// getFreeOffset returns the offset to the free space on the page.
func (p *Page) getFreeOffset() uint16 {
	return binary.BigEndian.Uint16(p[2:4])
}

// addSlot adds a slot entry to the page.
func (p *Page) addSlot(slot slotEntry) {
	numSlots := p.getNumSlots()
	binary.BigEndian.PutUint64(p[4+8*numSlots:], uint64(slot))
	p.setNumSlots(numSlots + 1)
}

// setSlot sets the slot entry at the given slot number.
func (p *Page) setSlot(slotNum uint16, slot slotEntry) {
	binary.BigEndian.PutUint64(p[4+8*slotNum:], uint64(slot))
}

// getSlot returns the slot entry at the given number.
func (p *Page) getSlot(slotNum uint16) slotEntry {
	return slotEntry(binary.BigEndian.Uint64(p[4+8*slotNum:]))
}
