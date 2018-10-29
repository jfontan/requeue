package requeue

import (
	"sort"
	"sync"
)

type Set struct {
	l map[string]struct{}
	m sync.RWMutex
}

func NewSet() *Set {
	return &Set{
		l: make(map[string]struct{}),
	}
}

func (s *Set) Add(name string) {
	s.m.Lock()
	defer s.m.Unlock()

	s.l[name] = struct{}{}
}

func (s *Set) Contains(name string) bool {
	s.m.RLock()
	defer s.m.RUnlock()

	_, ok := s.l[name]
	return ok
}

func (s *Set) List() []string {
	l := make([]string, len(s.l))

	s.m.RLock()

	var i int
	for k := range s.l {
		l[i] = k
		i++
	}

	s.m.RUnlock()

	sort.Strings(l)
	return l
}
