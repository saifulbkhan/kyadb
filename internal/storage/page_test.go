package storage

import (
	"testing"
)

func TestPage_AddRecord(t *testing.T) {
	t.Run(
		"check addition of three records", func(t *testing.T) {
			r1 := NewRecord(2)
			err := r1.SetString(0, "hello")
			if err != nil {
				return
			}
			err = r1.SetMap(1, Map{StringType, Int32Type, map[any]any{"a": 1, "b": 2}})
			if err != nil {
				t.Error(err)
			}

			r2 := NewRecord(2)
			r2.SetInt32(0, 123)
			err = r2.SetArray(1, Array{StringType, []any{"foo", "bar", "hello", "world"}})
			if err != nil {
				t.Error(err)
			}

			r3 := NewRecord(4)
			r3.SetUint32(0, 2048)
			r3.SetInt32(1, -2048)
			r3.SetBool(2, true)
			err = r3.SetString(3, "hello world")
			if err != nil {
				t.Error(err)
			}

			page := NewPage()
			slot, err := page.AddRecord(r1)
			if err != nil {
				t.Error(err)
			}
			expectedSlot := uint16(0)
			if slot != expectedSlot {
				t.Errorf("expected slot %v, got %v", expectedSlot, slot)
			}
			slot, err = page.AddRecord(r2)
			if err != nil {
				t.Error(err)
			}
			expectedSlot = uint16(1)
			if slot != expectedSlot {
				t.Errorf("expected slot %v, got %v", expectedSlot, slot)
			}
			slot, err = page.AddRecord(r3)
			if err != nil {
				t.Error(err)
			}
			expectedSlot = uint16(2)
			if slot != expectedSlot {
				t.Errorf("expected slot %v, got %v", expectedSlot, slot)
			}
		},
	)

	t.Run(
		"check page full error", func(t *testing.T) {
			page := NewPage()

			// The following record is 24 bytes long.
			r := NewRecord(1)
			err := r.SetString(0, "this is a record")

			// The record along with its slot each take (24 + 8) bytes. Therefore, we can only add
			// abs((PageSize - 4) / (24 + 8)) = 255 records to the page.
			for i := 0; i < 255; i++ {
				_, err := page.AddRecord(r)
				if err != nil {
					t.Error(err)
				}
			}

			// Any new record should result in an error.
			_, err = page.AddRecord(r)
			if err == nil {
				t.Error("expected page full error")
			}
		},
	)
}

func TestPage_GetRecord(t *testing.T) {
	t.Run(
		"check retrieval of three records", func(t *testing.T) {
			r1 := NewRecord(2)
			err := r1.SetString(0, "hello")
			if err != nil {
				return
			}
			err = r1.SetMap(1, Map{StringType, Int32Type, map[any]any{"a": 1, "b": 2}})
			if err != nil {
				t.Error(err)
			}

			r2 := NewRecord(2)
			r2.SetInt32(0, 123)
			err = r2.SetArray(1, Array{StringType, []any{"foo", "bar", "hello", "world"}})
			if err != nil {
				t.Error(err)
			}

			r3 := NewRecord(4)
			r3.SetUint32(0, 2048)
			r3.SetInt32(1, -2048)
			r3.SetBool(2, true)
			err = r3.SetString(3, "hello world")
			if err != nil {
				t.Error(err)
			}

			page := NewPage()
			_, err = page.AddRecord(r1)
			if err != nil {
				t.Error(err)
			}
			_, err = page.AddRecord(r2)
			if err != nil {
				t.Error(err)
			}
			_, err = page.AddRecord(r3)
			if err != nil {
				t.Error(err)
			}

			// Check that the records are retrieved in the same order as they were added.
			_, got, _ := page.GetRecord(0)
			if err != nil {
				t.Error(err)
			}
			if got == r1 {
				t.Errorf("expected record %v, got %v", r1, got)
			}
			_, got, _ = page.GetRecord(1)
			if err != nil {
				t.Error(err)
			}
			if got == r2 {
				t.Errorf("expected record %v, got %v", r2, got)
			}
			_, got, _ = page.GetRecord(2)
			if err != nil {
				t.Error(err)
			}
			if got == r3 {
				t.Errorf("expected record %v, got %v", r3, got)
			}
		},
	)
}
