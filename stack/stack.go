package stack

import "container/list"

func New() *stack {
	return &stack{new(list.List)}
}

func (s *stack) IsEmpty() bool {
	return s.l.Len() == 0
}

func (s *stack) Pop() interface{} {
	if e := s.l.Front(); e != nil {
		s.l.Remove(e)
		return e.Value
	}
	return nil
}

func (s *stack) Peek() interface{} {
	if e := s.l.Front(); e != nil {
		return e.Value
	}
	return nil
}

func (s *stack) Push(v interface{}) {
	s.l.PushFront(v)
}
