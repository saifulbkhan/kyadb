package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestFile_NewFile(t *testing.T) {
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
			}(file)

			home, err := os.UserHomeDir()
			if err != nil {
				t.Error(err)
			}
			want := fmt.Sprintf("%s/.var/lib/kyadb/data/test/1", home)
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
			if got != want {
				t.Errorf("got %d, want %d", gotSize, wantSize)
			}

			err = os.Remove(file.Name())
			if err != nil {
				t.Error(err)
			}
		},
	)
}
