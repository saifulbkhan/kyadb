package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestNewFile(t *testing.T) {
	t.Run(
		"check basic file creation", func(t *testing.T) {
			file, err := NewFile("test", 1)
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
			}(file)

			home, err := os.UserHomeDir()
			if err != nil {
				t.Error(err)
			}
			want := fmt.Sprintf("%s/.var/lib/kyadb/base/test/1", home)
			got := file.Name()
			if got != want {
				t.Errorf("got %s, want %s", got, want)
			}

			stat, err := file.Stat()
			if err != nil {
				t.Error(err)
			}
			wantSize := int64(8)
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
			file, err := NewFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			wantName := file.Name()
			stat, err := file.Stat()
			if err != nil {
				t.Error(err)
			}
			wantSize := stat.Size()

			err = file.Close()
			if err != nil {
				t.Error(err)
			}

			file, err = OpenFile("test", 1)
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
			}(file)

			gotName := file.Name()
			if gotName != wantName {
				t.Errorf("got %s, want %s", gotName, wantName)
			}

			stat, err = file.Stat()
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
			file, err := NewFile("test", 1)
			if err != nil {
				t.Error(err)
			}
			err = file.Close()
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
				t.Errorf("file %s still exists", filePath)
			}
		},
	)
}
