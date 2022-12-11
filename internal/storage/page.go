package storage

import "fmt"

const PageSize = 8 * 1024

// Page represents a page of data in a file.
type Page [PageSize]byte

// PageAddress is a unique identifier for a page in a file.
type PageAddress struct {
	FileID  uint16
	PageNum uint32
}

// PageFullError is returned when an operation cannot be completed because the page is full.
type PageFullError struct {
	Available uint16
	Needed    uint16
}

func (e *PageFullError) Error() string {
	return fmt.Sprintf(
		"operation cannot be completed, page full: available=%d, needed=%d",
		e.Available, e.Needed,
	)
}
