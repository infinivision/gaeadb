package mvcc

import (
	"gaeadb/prefix"
	"gaeadb/suffix"
)

type MVCC interface {
	Close() error

	Del([]byte, uint64, suffix.Writer) error
	Get([]byte, uint64) (uint64, uint64, error)
	Set([]byte, uint64, uint64, suffix.Writer) error

	NewForwardIterator([]byte, uint64) (Iterator, error)
	NewBackwardIterator([]byte, uint64) (Iterator, error)
}

type Iterator interface {
	Close() error
	Next() error
	Valid() bool
	Key() []byte
	Value() uint64
	Timestamp() uint64
}

type forwardIterator struct {
	ts  uint64
	itr prefix.Iterator
}

type backwardIterator struct {
	ts  uint64
	itr prefix.Iterator
}

type mvcc struct {
	t prefix.Tree
}
