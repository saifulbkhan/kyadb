package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

/*
 * Files represent files in a database. Each file is a collection of pages.
 * The first 6 bytes are the file's header. The first 2 bytes in the header are the file's ID. The
 * next 4 bytes are the number of pages in the file.
 * This is followed by the pages containing records.
 * A separate file will be maintained per table which will store the free space capacity of each
 * page.
 */

const (
	VarDir          = ".var"           // TODO: make this configurable
	BaseStoragePath = "lib/kyadb/base" // TODO: make this configurable
	MaxPagesPerFile = 256 * 1024
	defaultFilePerm = 0644
)

type DatabaseFile struct {
	file     *os.File
	FileId   uint16
	NumPages uint32
}

type FileFullError struct{}

func (e *FileFullError) Error() string {
	return fmt.Sprintf("file is full, maximum number of pages allowed: %d", MaxPagesPerFile)
}

// dbFilePath returns the path to the database file on disk. It may return an error if the directory
// path cannot be determined.
func dbFilePath(tableName string, fileID uint16) (string, error) {
	// TODO: data should not be in user's home directory, fine for MVP
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dbFilePath := fmt.Sprintf("%s/%s/%s/%s/%d", home, VarDir, BaseStoragePath, tableName, fileID)
	return dbFilePath, nil
}

// loadNumPages reads the number of pages in the file from the file header.
func (dbFile *DatabaseFile) loadNumPages() error {
	var b = make([]byte, 4)
	if _, err := dbFile.file.ReadAt(b, 2); err != nil {
		return err
	}
	dbFile.NumPages = ReadUint32(&b, 0)
	return nil
}

// NewFile creates a new database file on disk, with the given table name and file ID.
func NewFile(tableName string, fileID uint16) (*DatabaseFile, error) {
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

	dbFile := &DatabaseFile{file, fileID, 0}
	err = dbFile.MakeDurable()
	if err != nil {
		return nil, err
	}

	return dbFile, nil
}

// OpenFile opens an existing database file on disk, with the given table name and file ID.
func OpenFile(tableName string, fileID uint16) (*DatabaseFile, error) {
	dbFilePath, err := dbFilePath(tableName, fileID)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(dbFilePath, os.O_RDWR, defaultFilePerm)
	if err != nil {
		return nil, err
	}
	dbFile := &DatabaseFile{file, fileID, 0}
	if err = dbFile.loadNumPages(); err != nil {
		return nil, err
	}
	return dbFile, err
}

// DeleteFile deletes the database file on disk, with the given table name and file ID.
func DeleteFile(tableName string, fileID uint16) error {
	dbFilePath, err := dbFilePath(tableName, fileID)
	if err != nil {
		return err
	}
	return os.Remove(dbFilePath)
}

// MakeDurable commits the current contents of the file to stable storage.
func (dbFile *DatabaseFile) MakeDurable() error {
	var header = make([]byte, 6)
	WriteUint16(&header, 0, dbFile.FileId)
	WriteUint32(&header, 2, dbFile.NumPages)
	if _, err := dbFile.file.WriteAt(header, 0); err != nil {
		return err
	}
	if err := dbFile.file.Sync(); err != nil {
		return err
	}
	return nil
}

// AppendPage adds a new page to the end of the file. It returns the page number of the newly added
// page and pointer to an error, if any.
func (dbFile *DatabaseFile) AppendPage(page *Page) (uint32, error) {
	if dbFile.NumPages == MaxPagesPerFile {
		return 0xffff, &FileFullError{}
	}

	offset := 6 + dbFile.NumPages*PageSize
	if _, err := dbFile.file.WriteAt(page[:], int64(offset)); err != nil {
		return 0xffff, err
	}
	dbFile.NumPages++
	return dbFile.NumPages - 1, nil
}

// WritePage writes a page to the given page number in the file. It returns a pointer to an error,
// if any.
func (dbFile *DatabaseFile) WritePage(page *Page, pageNum uint32) error {
	offset := 6 + pageNum*PageSize
	if _, err := dbFile.file.WriteAt(page[:], int64(offset)); err != nil {
		return err
	}
	return nil
}

// We need the following functions:
// 7. Get the file ID of a file.
// 6. Get the number of pages in a file.
// 5. Delete (clear) a page from a file.
// 4. Read one or more pages from a file (pread).
// 3. Make all writes to a file durable (fsync).
