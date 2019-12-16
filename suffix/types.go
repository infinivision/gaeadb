package suffix

import (
	"bytes"
	"gaeadb/cache"
)

const (
	HeaderSize        = 4
	ElementHeaderSize = 10
)

type Iterator interface {
	Next()
	Valid() bool
	Key() []byte
	Value() uint64
}

type Writer interface {
	NewSuffix(byte, byte, uint64, uint64) error
	ChgPrefix(uint64, []uint16, []uint64) error
	NewPrefix(uint64, uint64, uint16, []uint16, []uint64) error
}

type element struct {
	off  uint64
	suff []byte
}

type forwardIterator struct {
	prefix []byte
	es     []*element
}

type backwardIterator struct {
	prefix []byte
	es     []*element
}

type suffix struct {
	free int
	w    Writer
	pg   cache.Page
	es   []*element
}

type Elements []*element

func (xs Elements) Len() int           { return len(xs) }
func (xs Elements) Swap(i, j int)      { xs[i], xs[j] = xs[j], xs[i] }
func (xs Elements) Less(i, j int) bool { return bytes.Compare(xs[i].suff, xs[j].suff) < 0 }
