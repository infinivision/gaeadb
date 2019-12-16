package stack

import "container/list"

type Stack interface {
	IsEmpty() bool
	Push(interface{})
	Pop() interface{}
	Peek() interface{}
}

type stack struct {
	l *list.List
}
