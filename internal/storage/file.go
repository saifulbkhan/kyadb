package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

/*
 * Files represent files in a database. Each file is a collection of pages.
 * The first 8 bytes are the file's header. The first 4 bytes in the header are the file's ID. The
 * next 4 bytes are the number of pages in the file.
 * This is followed by the pages containing records.
 * A separate file will be maintained per table which will store the free space capacity of each
 * page.
 */

const (
	BaseStoragePath = "lib/kyadb/data" // TODO: make this configurable
	MaxPagesPerFile = 256 * 1024
	MaxFileSize     = 8 + PageSize*MaxPagesPerFile // ~2GB
)

// We need the following functions:
// 1. Create a new file.
// 2. Open an existing file for read/write.
// 3. Append a page to a file.
// 4. Write a page to a file (pwrite).
// 5. Make all writes to a file durable (fsync).
// 6. Read one or more pages from a file (pread).
// 7. Delete (clear) a page from a file.
// 8. Get the number of pages in a file.
// 9. Get the file ID of a file.
// 10. Delete a file from disk.

// NewFile creates a new database file on disk, with the given table name and file ID.
func NewFile(tableName string, fileID uint16) (*os.File, error) {
	// TODO: data should not be in user's home directory, fine for MVP
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	dbFilePath := fmt.Sprintf("%s/.var/%s/%s/%d", home, BaseStoragePath, tableName, fileID)
	if err := os.MkdirAll(filepath.Dir(dbFilePath), 0744); err != nil {
		return nil, err
	}
	return os.OpenFile(dbFilePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)

	// TODO: Write the file header.
}
