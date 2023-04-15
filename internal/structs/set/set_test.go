package set

import "testing"

func TestNewSet(t *testing.T) {
	s := NewSet[int]()
	if s == nil {
		t.Error("NewSet returned nil")
	}
}

func TestSet_Add(t *testing.T) {
	s := NewSet[int]()
	s.Add(1)
	s.Add(2)
	s.Add(3)

	for item := range *s {
		if item != 1 && item != 2 && item != 3 {
			t.Errorf("Set contains unexpected item %d", item)
		}
	}
}

func TestSet_Remove(t *testing.T) {
	s := NewSet[int]()
	s.Add(1)
	s.Add(2)
	s.Add(3)

	s.Remove(2)

	for item := range *s {
		if item == 2 {
			t.Errorf("Set contains deleted item %d", item)
		}
	}
}

func TestSet_Has(t *testing.T) {
	s := NewSet[int]()
	s.Add(1)
	s.Add(2)
	s.Add(3)

	if !s.Has(1) {
		t.Errorf("Set does not contain item %d", 1)
	}
	if !s.Has(2) {
		t.Errorf("Set does not contain item %d", 2)
	}
	if !s.Has(3) {
		t.Errorf("Set does not contain item %d", 3)
	}
}

func TestSet_Length(t *testing.T) {
	s := NewSet[int]()
	s.Add(1)
	s.Add(2)
	s.Add(3)

	if s.Length() != 3 {
		t.Errorf("Set has unexpected length %d", s.Length())
	}
}

func TestSet_Clear(t *testing.T) {
	s := NewSet[int]()
	s.Add(1)
	s.Add(2)
	s.Add(3)

	s.Clear()

	if s.Length() != 0 {
		t.Errorf("Set has unexpected length %d", s.Length())
	}
}

func TestSet_Intersection(t *testing.T) {
	s := NewSet[int]()
	s.Add(1)
	s.Add(2)
	s.Add(3)

	other := NewSet[int]()
	other.Add(2)
	other.Add(3)
	other.Add(4)

	intersection := s.Intersection(other)

	if intersection.Length() != 2 {
		t.Errorf("Intersection has unexpected length %d", intersection.Length())
	}

	if !intersection.Has(2) {
		t.Errorf("Intersection does not contain item %d", 2)
	}
	if !intersection.Has(3) {
		t.Errorf("Intersection does not contain item %d", 3)
	}
}

func TestSet_Union(t *testing.T) {
	s := NewSet[int]()
	s.Add(1)
	s.Add(2)
	s.Add(3)

	other := NewSet[int]()
	other.Add(2)
	other.Add(3)
	other.Add(4)

	union := s.Union(other)

	if union.Length() != 4 {
		t.Errorf("Union has unexpected length %d", union.Length())
	}

	if !union.Has(1) {
		t.Errorf("Union does not contain item %d", 1)
	}
	if !union.Has(2) {
		t.Errorf("Union does not contain item %d", 2)
	}
	if !union.Has(3) {
		t.Errorf("Union does not contain item %d", 3)
	}
	if !union.Has(4) {
		t.Errorf("Union does not contain item %d", 4)
	}
}

func TestSet_Difference(t *testing.T) {
	s := NewSet[int]()
	s.Add(1)
	s.Add(2)
	s.Add(3)

	other := NewSet[int]()
	other.Add(2)
	other.Add(3)
	other.Add(4)

	difference := s.Difference(other)

	if difference.Length() != 1 {
		t.Errorf("Difference has unexpected length %d", difference.Length())
	}

	if !difference.Has(1) {
		t.Errorf("Difference does not contain item %d", 1)
	}
}
