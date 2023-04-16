package hashmap

import (
	"testing"
)

func TestLPHashMap_Set(t *testing.T) {
	t.Run(
		"should set key-value pairs and expand automatically", func(t *testing.T) {
			hm := NewLPHashMap[uint64, string](3)
			err := hm.Set(1, "1")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(2, "2")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(3, "3")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}

			for _, item := range hm.elements {
				if item.key != 1 && item.key != 2 && item.key != 3 {
					t.Errorf("unexpected key: %v", item.key)
				}
				if item.value != "1" && item.value != "2" && item.value != "3" {
					t.Errorf("unexpected value: %v", item.value)
				}
			}
			wantNumSlots := 3
			gotNumSlots := len(hm.elements)
			if gotNumSlots != wantNumSlots {
				t.Errorf(
					"unexpected number of slots in hashmap, want: %d, got: %d",
					wantNumSlots,
					gotNumSlots,
				)
			}

			err = hm.Set(4, "4")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			wantNumSlots = 3 * 2
			gotNumSlots = len(hm.elements)
			if gotNumSlots != wantNumSlots {
				t.Errorf(
					"unexpected number of slots in hashmap, want: %d, got: %d",
					wantNumSlots,
					gotNumSlots,
				)
			}
		},
	)

	t.Run(
		"should reuse deleted slots", func(t *testing.T) {
			hm := NewLPHashMap[int, string](3)
			err := hm.Set(1, "1")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(2, "2")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(3, "3")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}

			_, err = hm.Pop(2)
			if err != nil {
				t.Errorf("error popping key-value pair: %v", err)
			}

			err = hm.Set(4, "4")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			wantNumSlots := 3
			gotNumSlots := len(hm.elements)
			if gotNumSlots != wantNumSlots {
				t.Errorf(
					"unexpected number of slots in hashmap, want: %d, got: %d",
					wantNumSlots,
					gotNumSlots,
				)
			}

			found4 := false
			for _, item := range hm.elements {
				if item != nil && item.key == 4 {
					found4 = true
				}
			}
			if !found4 {
				t.Errorf("expected to find key 4 in hashmap")
			}
		},
	)
}

func TestLPHashMap_Has(t *testing.T) {
	t.Run(
		"should return true if key exists", func(t *testing.T) {
			hm := NewLPHashMap[int, string](3)
			err := hm.Set(1, "1")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(2, "2")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(3, "3")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}

			hasKey := hm.Has(2)
			if !hasKey {
				t.Errorf("expected to find key 2 in hashmap")
			}
		},
	)

	t.Run(
		"should return false if key does not exist", func(t *testing.T) {
			hm := NewLPHashMap[int, string](3)
			err := hm.Set(1, "1")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(2, "2")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(3, "3")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}

			hasKey := hm.Has(4)
			if hasKey {
				t.Errorf("expected not to find key 4 in hashmap")
			}
		},
	)
}

func TestLPHashMap_Get(t *testing.T) {
	t.Run(
		"should return value if key exists", func(t *testing.T) {
			hm := NewLPHashMap[int, string](3)
			err := hm.Set(1, "1")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(2, "2")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(3, "3")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}

			val, err := hm.Get(2)
			if err != nil {
				t.Errorf("error getting value: %v", err)
			}
			if val != "2" {
				t.Errorf("unexpected value: %v", val)
			}
		},
	)

	t.Run(
		"should return an error if key does not exist", func(t *testing.T) {
			hm := NewLPHashMap[int, string](3)
			err := hm.Set(1, "1")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(2, "2")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(3, "3")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}

			_, err = hm.Get(4)
			if err == nil {
				t.Errorf("expected to get an error")
			}
		},
	)
}

func TestLPHashMap_Pop(t *testing.T) {
	t.Run(
		"should return value if key exists", func(t *testing.T) {
			hm := NewLPHashMap[int, string](3)
			err := hm.Set(1, "1")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(2, "2")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(3, "3")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}

			val, err := hm.Pop(2)
			if err != nil {
				t.Errorf("error popping value: %v", err)
			}
			want := "2"
			got := val
			if got != want {
				t.Errorf("unexpected value: got %v, want %v", got, want)
			}
		},
	)

	t.Run(
		"should return an error if key does not exist", func(t *testing.T) {
			hm := NewLPHashMap[int, string](3)
			err := hm.Set(1, "1")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(2, "2")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(3, "3")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}

			_, err = hm.Pop(4)
			if err == nil {
				t.Errorf("expected to get an error")
			}
		},
	)
}

func TestLPHashMap_Length(t *testing.T) {
	t.Run(
		"should return the number of elements in the hashmap", func(t *testing.T) {
			hm := NewLPHashMap[int, string](3)
			err := hm.Set(1, "1")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(2, "2")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			err = hm.Set(3, "3")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}

			want := uint64(3)
			got := hm.Length()
			if got != want {
				t.Errorf("unexpected length: got %v, want %v", got, want)
			}

			err = hm.Set(4, "4")
			if err != nil {
				t.Errorf("error setting key-value pair: %v", err)
			}
			want = 4
			got = hm.Length()
			if got != want {
				t.Errorf("unexpected length: got %v, want %v", got, want)
			}
		},
	)
}

func TestLPHashMap_AtIndex(t *testing.T) {
	hm := NewLPHashMap[int, string](3)
	err := hm.Set(1, "1")
	if err != nil {
		t.Errorf("error setting key-value pair: %v", err)
	}
	err = hm.Set(2, "2")
	if err != nil {
		t.Errorf("error setting key-value pair: %v", err)
	}
	err = hm.Set(3, "3")
	if err != nil {
		t.Errorf("error setting key-value pair: %v", err)
	}
	err = hm.Set(4, "4")
	if err != nil {
		t.Errorf("error setting key-value pair: %v", err)
	}
	err = hm.Set(5, "5")
	if err != nil {
		t.Errorf("error setting key-value pair: %v", err)
	}

	for i := uint64(0); i < hm.Length(); i++ {
		key, val, err := hm.AtIndex(i)
		if err != nil {
			t.Errorf("error getting value: %v", err)
		}
		if key != 1 && key != 2 && key != 3 && key != 4 && key != 5 {
			t.Errorf("unexpected key: %v", key)
		}
		if val != "1" && val != "2" && val != "3" && val != "4" && val != "5" {
			t.Errorf("unexpected value: %v", val)
		}
	}

	_, _, err = hm.AtIndex(hm.Length() + 1)
	if err == nil {
		t.Errorf("expected to get an error")
	}
}

func TestLPHashMap_Clear(t *testing.T) {
	hm := NewLPHashMap[int, string](3)
	err := hm.Set(1, "1")
	if err != nil {
		t.Errorf("error setting key-value pair: %v", err)
	}
	err = hm.Set(2, "2")
	if err != nil {
		t.Errorf("error setting key-value pair: %v", err)
	}
	err = hm.Set(3, "3")
	if err != nil {
		t.Errorf("error setting key-value pair: %v", err)
	}

	hm.Clear()

	if hm.Length() != 0 {
		t.Errorf("expected length to be 0")
	}
	for i := range hm.elements {
		if hm.elements[i] != nil {
			t.Errorf("expected element to be nil")
		}
	}
}
