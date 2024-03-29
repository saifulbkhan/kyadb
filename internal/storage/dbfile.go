package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"kyadb/internal/structs/element"
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
	VarDir          = ".var"      // TODO: make this configurable
	BaseDataPath    = "lib/kyadb" // TODO: make this configurable
	DBDataDir       = "db"        // TODO: make this configurable
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
func dbFilePath(fileID uint16) (string, error) {
	// TODO: data should not be in user's home directory, fine for MVP
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dbFilePath := fmt.Sprintf("%s/%s/%s/%s/%d", home, VarDir, BaseDataPath, DBDataDir, fileID)
	return dbFilePath, nil
}

// loadNumPages reads the number of pages in the file from the file header.
func (dbFile *DatabaseFile) loadNumPages() error {
	var b = make([]byte, 4)
	if _, err := dbFile.file.ReadAt(b, 2); err != nil {
		return err
	}
	dbFile.NumPages = element.ReadUint32(&b, 0)
	return nil
}

// NewDatabaseFile creates a new database file on disk, with the given table name and file ID.
func NewDatabaseFile(fileID uint16) (*DatabaseFile, error) {
	dbFilePath, err := dbFilePath(fileID)
	if err != nil {
		return nil, err
	}
	parentDir := filepath.Dir(dbFilePath)
	if err := os.MkdirAll(parentDir, 0744); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(
		dbFilePath, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_DIRECT, defaultFilePerm,
	)
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

// OpenDatabaseFile opens an existing database file on disk, with the given table name and file ID.
func OpenDatabaseFile(fileID uint16) (*DatabaseFile, error) {
	dbFilePath, err := dbFilePath(fileID)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(dbFilePath, os.O_RDWR|syscall.O_DIRECT, defaultFilePerm)
	if err != nil {
		return nil, err
	}
	dbFile := &DatabaseFile{file, fileID, 0}
	if err = dbFile.loadNumPages(); err != nil {
		return nil, err
	}
	return dbFile, err
}

// DeleteDatabaseFile deletes the database file on disk, with the given table name and file ID.
func DeleteDatabaseFile(fileID uint16) error {
	dbFilePath, err := dbFilePath(fileID)
	if err != nil {
		return err
	}
	return os.Remove(dbFilePath)
}

// MakeDurable commits the current contents of the file to stable storage.
func (dbFile *DatabaseFile) MakeDurable() error {
	var header = make([]byte, 6)
	element.WriteUint16(&header, 0, dbFile.FileId)
	element.WriteUint32(&header, 2, dbFile.NumPages)
	if _, err := dbFile.file.WriteAt(header, 0); err != nil {
		return err
	}
	if err := dbFile.file.Sync(); err != nil {
		return err
	}
	return nil
}

// AppendPages adds new pages to the end of the file. It returns an array of page numbers of the
// newly added pages. An error will be returned on the first failure. In case of failure, the
// returned array will contain the page numbers of the pages that were successfully added.
func (dbFile *DatabaseFile) AppendPages(pages *[]Page) ([]uint32, error) {
	var pageNumbers []uint32
	if dbFile.NumPages == MaxPagesPerFile {
		return pageNumbers, &FileFullError{}
	}

	offset := 6 + dbFile.NumPages*PageSize
	for i, page := range *pages {
		if _, err := dbFile.file.WriteAt(page[:], int64(offset)); err != nil {
			return pageNumbers, err
		}
		pageNumbers = append(pageNumbers, dbFile.NumPages+uint32(i))
		offset += PageSize
	}
	dbFile.NumPages++
	return pageNumbers, nil
}

// WritePages writes pages starting from the given page number in the file. It returns a number of
// pages successfully written to the file and a pointer to an error, if any.
func (dbFile *DatabaseFile) WritePages(pages *[]Page, pageNum uint32) (uint32, error) {
	offset := 6 + pageNum*PageSize
	var numWritten uint32
	for _, page := range *pages {
		if _, err := dbFile.file.WriteAt(page[:], int64(offset)); err != nil {
			return numWritten, err
		}
		offset += PageSize
		numWritten++
	}
	return numWritten, nil
}

// ReadPages reads a range of pages from the file. It returns a pointer to an error, if any.
func (dbFile *DatabaseFile) ReadPages(pageNum uint32, numPages uint32) (*[]Page, error) {
	offset := 6 + pageNum*PageSize
	pages := make([]Page, numPages)
	for i := 0; i < int(numPages); i++ {
		if _, err := dbFile.file.ReadAt(pages[i][:], int64(offset)); err != nil {
			return nil, err
		}
		offset += PageSize
	}
	return &pages, nil
}
