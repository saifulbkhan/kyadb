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
	VarDir          = ".var"           // TODO: make this configurable
	BaseStoragePath = "lib/kyadb/base" // TODO: make this configurable
	MaxPagesPerFile = 256 * 1024
	MaxFileSize     = 8 + PageSize*MaxPagesPerFile // ~2GB
	defaultFilePerm = 0644
)

// dbFilePath returns the path to the database file on disk. It may return an error if the directory
// path cannot be determined.
func dbFilePath(tableName string, fileID uint32) (string, error) {
	// TODO: data should not be in user's home directory, fine for MVP
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dbFilePath := fmt.Sprintf("%s/%s/%s/%s/%d", home, VarDir, BaseStoragePath, tableName, fileID)
	return dbFilePath, nil
}

// writeHeader writes the file header to the given file.
func writeHeader(file *os.File, fileID uint32, numPages uint32, sync bool) error {
	var header Bytes = make([]byte, 8)
	WriteUint32(&header, 0, fileID)
	WriteUint32(&header, 4, numPages)
	if _, err := file.WriteAt(header, 0); err != nil {
		return err
	}
	if sync {
		if err := file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// writeNumPages writes the number of pages in the file to the file header.
func writeNumPages(file *os.File, numPages uint32, sync bool) error {
	var b Bytes = make([]byte, 4)
	WriteUint32(&b, 0, numPages)
	if _, err := file.WriteAt(b, 4); err != nil {
		return err
	}
	if sync {
		if err := file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// NewFile creates a new database file on disk, with the given table name and file ID.
func NewFile(tableName string, fileID uint32) (*os.File, error) {
	dbFilePath, err := dbFilePath(tableName, fileID)
	if err != nil {
		return nil, err
	}
	parentDir := filepath.Dir(dbFilePath)
	if err := os.MkdirAll(parentDir, 0744); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(dbFilePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, defaultFilePerm)
	if err != nil {
		return nil, err
	}

	err = writeHeader(file, fileID, 0, true)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// OpenFile opens an existing database file on disk, with the given table name and file ID.
func OpenFile(tableName string, fileID uint32) (*os.File, error) {
	dbFilePath, err := dbFilePath(tableName, fileID)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(dbFilePath, os.O_RDWR, defaultFilePerm)
}

// DeleteFile deletes the database file on disk, with the given table name and file ID.
func DeleteFile(tableName string, fileID uint32) error {
	dbFilePath, err := dbFilePath(tableName, fileID)
	if err != nil {
		return err
	}
	return os.Remove(dbFilePath)
}

// We need the following functions:
// 7. Get the file ID of a file.
// 6. Get the number of pages in a file.
// 5. Delete (clear) a page from a file.
// 4. Read one or more pages from a file (pread).
// 3. Make all writes to a file durable (fsync).
// 2. Write a page to a file (pwrite).
// 1. Append a page to a file.
