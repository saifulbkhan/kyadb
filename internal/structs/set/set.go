package set

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() *Set[T] {
	return &Set[T]{}
}

func (s *Set[T]) Add(item T) {
	(*s)[item] = struct{}{}
}

func (s *Set[T]) Remove(item T) {
	delete(*s, item)
}

func (s *Set[T]) Has(item T) bool {
	_, ok := (*s)[item]
	return ok
}

func (s *Set[T]) Length() uint64 {
	return uint64(len(*s))
}

func (s *Set[T]) Clear() {
	*s = Set[T]{}
}

func (s *Set[T]) Intersection(other *Set[T]) *Set[T] {
	intersection := NewSet[T]()
	for item := range *s {
		if other.Has(item) {
			intersection.Add(item)
		}
	}
	return intersection
}

func (s *Set[T]) Union(other *Set[T]) *Set[T] {
	union := NewSet[T]()
	for item := range *s {
		union.Add(item)
	}
	for item := range *other {
		union.Add(item)
	}
	return union
}

func (s *Set[T]) Difference(other *Set[T]) *Set[T] {
	difference := NewSet[T]()
	for item := range *s {
		if !other.Has(item) {
			difference.Add(item)
		}
	}
	return difference
}
