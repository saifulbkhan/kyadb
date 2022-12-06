package storage

import (
	"reflect"
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
			if err != nil {
				t.Error(err)
			}

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
			got, _, err := page.GetRecord(0)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, r1) {
				t.Errorf("expected record %v, got %v", r1, got)
			}
			got, _, err = page.GetRecord(1)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, r2) {
				t.Errorf("expected record %v, got %v", r2, got)
			}
			got, _, err = page.GetRecord(2)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, r3) {
				t.Errorf("expected record %v, got %v", r3, got)
			}
		},
	)
}

func TestPage_SetForwardedAddress(t *testing.T) {
	t.Run(
		"check setting of forwarded address", func(t *testing.T) {
			record := NewRecord(4)
			record.SetUint32(0, 2048)

			page := NewPage()
			slotNum, err := page.AddRecord(record)
			if err != nil {
				return
			}
			want := RecordAddress{PageAddress: PageAddress{FileID: 0, PageNum: 1}, SlotNum: 2}
			page.SetForwardedAddress(slotNum, want)
			got := slotEntryToRecordAddress(page.getSlot(slotNum))
			if got != want {
				t.Errorf("expected forwarded address %v, got %v", want, got)
			}
		},
	)
}

func TestPage_UpdateRecord(t *testing.T) {
	t.Run(
		"check updating of record", func(t *testing.T) {
			r1 := NewRecord(2)
			r1.SetUint32(0, 1024)
			r1.SetUint32(1, 2048)

			r2 := NewRecord(2)
			err := r2.SetString(0, "hello")
			if err != nil {
				t.Error(err)
			}
			err = r2.SetString(1, "foo")
			if err != nil {
				t.Error(err)
			}

			r3 := NewRecord(2)
			err = r3.SetArray(0, Array{Int32Type, []any{1, 2, 3}})
			if err != nil {
				t.Error(err)
			}
			err = r3.SetMap(
				1,
				Map{StringType, Int32Type, map[any]any{"a": 1, "b": 2}},
			)
			if err != nil {
				t.Error(err)
			}

			page := NewPage()
			slotNum1, err := page.AddRecord(r1)
			if err != nil {
				t.Error(err)
			}
			slotNum2, err := page.AddRecord(r2)
			if err != nil {
				t.Error(err)
			}
			slotNum3, err := page.AddRecord(r3)
			if err != nil {
				t.Error(err)
			}

			r1.SetUint32(1, 4096)
			_, err = page.UpdateRecord(slotNum1, r1)
			if err != nil {
				t.Error(err)
			}
			got, _, err := page.GetRecord(slotNum1)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, r1) {
				t.Errorf("expected r1 %v, got %v", r1, got)
			}

			err = r2.SetString(1, "bar")
			if err != nil {
				t.Error(err)
			}
			_, err = page.UpdateRecord(slotNum2, r2)
			if err != nil {
				t.Error(err)
			}
			got, _, err = page.GetRecord(slotNum2)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, r2) {
				t.Errorf("expected r2 %v, got %v", r2, got)
			}

			err = r3.SetArray(0, Array{Int32Type, []any{4, 5, 6}})
			if err != nil {
				t.Error(err)
			}
			err = r3.SetMap(
				1,
				Map{StringType, Int32Type, map[any]any{"c": 3, "d": 4}},
			)
			if err != nil {
				t.Error(err)
			}
			_, err = page.UpdateRecord(slotNum3, r3)
			if err != nil {
				t.Error(err)
			}
			got, _, err = page.GetRecord(slotNum3)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, r3) {
				t.Errorf("expected r3 %v, got %v", r3, got)
			}
		},
	)

	t.Run(
		"check updating of record larger than existing record", func(t *testing.T) {
			r1 := NewRecord(2)
			r1.SetUint32(0, 1024)
			r1.SetUint32(1, 2048)

			r2 := NewRecord(2)
			err := r2.SetString(0, "hello")
			if err != nil {
				t.Error(err)
			}
			err = r2.SetString(1, "foo")
			if err != nil {
				t.Error(err)
			}

			r3 := NewRecord(2)
			err = r3.SetArray(0, Array{Int32Type, []any{1, 2, 3}})
			if err != nil {
				t.Error(err)
			}
			err = r3.SetMap(
				1,
				Map{StringType, Int32Type, map[any]any{"a": 1, "b": 2}},
			)
			if err != nil {
				t.Error(err)
			}

			page := NewPage()
			_, err = page.AddRecord(r1)
			if err != nil {
				t.Error(err)
			}
			slotNum2, err := page.AddRecord(r2)
			if err != nil {
				t.Error(err)
			}
			_, err = page.AddRecord(r3)
			if err != nil {
				t.Error(err)
			}
			originalOffset := page.getSlot(slotNum2)

			r2 = NewRecord(2)
			err = r2.SetString(0, "hello")
			if err != nil {
				t.Error(err)
			}
			err = r2.SetString(1, "world")
			if err != nil {
				t.Error(err)
			}
			_, err = page.UpdateRecord(slotNum2, r2)
			if err != nil {
				t.Error(err)
			}
			got, _, err := page.GetRecord(slotNum2)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, r2) {
				t.Errorf("expected record %v, got %v", r2, got)
			}

			newOffset := page.getSlot(slotNum2)
			if newOffset >= originalOffset {
				t.Errorf(
					"expected new offset %v to be less than original offset %v",
					newOffset,
					originalOffset,
				)
			}
		},
	)

	t.Run(
		"check page full error", func(t *testing.T) {
			page := NewPage()

			// The following record is 24 bytes long.
			r := NewRecord(1)
			err := r.SetString(0, "this is a record")
			if err != nil {
				t.Error(err)
			}

			// The record along with its slot each take (24 + 8) bytes. Therefore, we can only add
			// abs((PageSize - 4) / (24 + 8)) = 255 records to the page.
			for i := 0; i < 255; i++ {
				_, err := page.AddRecord(r)
				if err != nil {
					t.Error(err)
				}
			}

			// Updating any existing record with a record of smaller size should not cause an error.
			err = r.SetString(0, "this is a")
			if err != nil {
				return
			}
			_, err = page.UpdateRecord(0, r)
			if err != nil {
				t.Error(err)
			}

			// Updating any existing record with a record of larger size should cause an error.
			err = r.SetString(0, "this is a record that is a bit too long to fit")
			if err != nil {
				return
			}
			_, err = page.UpdateRecord(0, r)
			if err == nil {
				t.Error("expected error, got nil")
			}
		},
	)
}

func TestPage_DeleteRecord(t *testing.T) {
	t.Run(
		"check deletion of records", func(t *testing.T) {
			page := NewPage()

			r1 := NewRecord(2)
			r1.SetUint32(0, 1024)
			r1.SetUint32(1, 2048)

			r2 := NewRecord(2)
			err := r2.SetString(0, "hello")
			if err != nil {
				t.Error(err)
			}
			err = r2.SetString(1, "foo")
			if err != nil {
				t.Error(err)
			}

			r3 := NewRecord(2)
			err = r3.SetArray(0, Array{Int32Type, []any{1, 2, 3}})
			if err != nil {
				t.Error(err)
			}
			err = r3.SetMap(
				1,
				Map{StringType, Int32Type, map[any]any{"a": 1, "b": 2}},
			)
			if err != nil {
				t.Error(err)
			}

			_, err = page.AddRecord(r1)
			if err != nil {
				t.Error(err)
			}
			slotNum2, err := page.AddRecord(r2)
			if err != nil {
				t.Error(err)
			}
			_, err = page.AddRecord(r3)
			if err != nil {
				t.Error(err)
			}

			page.DeleteRecord(slotNum2)
			_, _, err = page.GetRecord(slotNum2)
			if err == nil {
				t.Error("expected error, got nil")
			}
			_, err = page.UpdateRecord(slotNum2, r2)
			if err == nil {
				t.Error("expected error, got nil")
			}
		},
	)
}
