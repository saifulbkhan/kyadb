package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestNewFile(t *testing.T) {
	t.Run(
		"check basic file creation", func(t *testing.T) {
			dbFile, err := NewFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					t.Error(err)
				}
				err = os.Remove(file.Name())
				if err != nil {
					t.Error(err)
				}
			}(dbFile.file)

			home, err := os.UserHomeDir()
			if err != nil {
				t.Error(err)
			}
			want := fmt.Sprintf("%s/.var/lib/kyadb/base/test/1", home)
			got := dbFile.file.Name()
			if got != want {
				t.Errorf("got %s, want %s", got, want)
			}

			stat, err := dbFile.file.Stat()
			if err != nil {
				t.Error(err)
			}
			wantSize := int64(6)
			gotSize := stat.Size()
			if gotSize != wantSize {
				t.Errorf("got %d, want %d", gotSize, wantSize)
			}
		},
	)
}

func TestOpenFile(t *testing.T) {
	t.Run(
		"check basic file opening", func(t *testing.T) {
			dbFile, err := NewFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			wantName := dbFile.file.Name()
			stat, err := dbFile.file.Stat()
			if err != nil {
				t.Error(err)
			}
			wantSize := stat.Size()

			err = dbFile.file.Close()
			if err != nil {
				t.Error(err)
			}

			dbFile, err = OpenFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					t.Error(err)
				}
				err = os.Remove(file.Name())
				if err != nil {
					t.Error(err)
				}
			}(dbFile.file)

			gotName := dbFile.file.Name()
			if gotName != wantName {
				t.Errorf("got %s, want %s", gotName, wantName)
			}

			stat, err = dbFile.file.Stat()
			if err != nil {
				t.Error(err)
			}
			gotSize := stat.Size()
			if gotSize != wantSize {
				t.Errorf("got %d, want %d", gotSize, wantSize)
			}
		},
	)
}

func TestDeleteFile(t *testing.T) {
	t.Run(
		"check basic file deletion", func(t *testing.T) {
			dbFile, err := NewFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			err = dbFile.file.Close()
			if err != nil {
				t.Error(err)
			}

			err = DeleteFile("test", 1)
			if err != nil {
				t.Error(err)
			}

			home, err := os.UserHomeDir()
			if err != nil {
				t.Error(err)
			}
			filePath := fmt.Sprintf("%s/.var/lib/kyadb/base/test/1", home)
			if _, err := os.Stat(filePath); !os.IsNotExist(err) {
				t.Errorf("DB file %s still exists", filePath)
			}
		},
	)
}

func TestDatabaseFile_AppendPages(t *testing.T) {
	t.Run(
		"check basic page appending", func(t *testing.T) {
			dbFile, err := NewFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					t.Error(err)
				}
				err = os.Remove(file.Name())
				if err != nil {
					t.Error(err)
				}
			}(dbFile.file)

			pages := []Page{*NewTablePage()}
			_, err = dbFile.AppendPages(&pages)
			if err != nil {
				t.Error(err)
			}

			stat, err := dbFile.file.Stat()
			if err != nil {
				t.Error(err)
			}
			wantSize := int64(6 + PageSize)
			gotSize := stat.Size()
			if gotSize != wantSize {
				t.Errorf("got %d, want %d", gotSize, wantSize)
			}
		},
	)

	t.Run(
		"check page appending with offset", func(t *testing.T) {
			dbFile, err := NewFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					t.Error(err)
				}
				err = os.Remove(file.Name())
				if err != nil {
					t.Error(err)
				}
			}(dbFile.file)

			_, err = dbFile.AppendPages(&[]Page{*NewTablePage(), *NewTablePage()})
			if err != nil {
				t.Error(err)
			}

			stat, err := dbFile.file.Stat()
			if err != nil {
				t.Error(err)
			}
			wantSize := int64(6 + 2*PageSize)
			gotSize := stat.Size()
			if gotSize != wantSize {
				t.Errorf("got %d, want %d", gotSize, wantSize)
			}
		},
	)
}

func TestDatabaseFile_WritePage(t *testing.T) {
	t.Run(
		"check basic page writing", func(t *testing.T) {
			dbFile, err := NewFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					t.Error(err)
				}
				err = os.Remove(file.Name())
				if err != nil {
					t.Error(err)
				}
			}(dbFile.file)

			_, err = dbFile.AppendPages(&[]Page{*NewTablePage()})
			if err != nil {
				t.Error(err)
			}

			stat, err := dbFile.file.Stat()
			if err != nil {
				t.Error(err)
			}
			wantSize := int64(6 + PageSize)
			gotSize := stat.Size()
			if gotSize != wantSize {
				t.Errorf("got %d, want %d", gotSize, wantSize)
			}
		},
	)
}

func TestDatabaseFile_ReadPages(t *testing.T) {
	t.Run(
		"check basic page reading", func(t *testing.T) {
			dbFile, err := NewFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					t.Error(err)
				}
				err = os.Remove(file.Name())
				if err != nil {
					t.Error(err)
				}
			}(dbFile.file)

			_, err = dbFile.AppendPages(&[]Page{*NewTablePage(), *NewTablePage()})
			if err != nil {
				t.Error(err)
			}
			err = dbFile.MakeDurable()
			if err != nil {
				t.Error(err)
			}

			pages, err := dbFile.ReadPages(0, 2)
			if err != nil {
				t.Error(err)
			}

			if len(*pages) != 2 {
				t.Errorf("got %d, want %d", len(*pages), 2)
			}
		},
	)

	t.Run(
		"check page reading beyond file size", func(t *testing.T) {
			dbFile, err := NewFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					t.Error(err)
				}
				err = os.Remove(file.Name())
				if err != nil {
					t.Error(err)
				}
			}(dbFile.file)

			_, err = dbFile.AppendPages(&[]Page{*NewTablePage(), *NewTablePage()})
			if err != nil {
				t.Error(err)
			}
			err = dbFile.MakeDurable()
			if err != nil {
				t.Error(err)
			}

			_, err = dbFile.ReadPages(0, 3)
			if err == nil {
				t.Error("expected error when reading more pages than written")
			}
		},
	)
}

func BenchmarkDatabaseFile_ReadPages(b *testing.B) {
	dbFile, err := NewFile("test", 1)
	if err != nil {
		b.Error(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			b.Error(err)
		}
		err = os.Remove(file.Name())
		if err != nil {
			b.Error(err)
		}
	}(dbFile.file)

	var pages []Page
	for i := 0; i < MaxPagesPerFile; i++ {
		pages = append(pages, *NewTablePage())
	}
	_, err = dbFile.AppendPages(&pages)
	if err != nil {
		b.Error(err)
	}
	err = dbFile.MakeDurable()
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := dbFile.ReadPages(0, MaxPagesPerFile)
		if err != nil {
			b.Error(err)
		}
	}
}
