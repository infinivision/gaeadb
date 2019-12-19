package suffix

import (
	"github.com/infinivision/gaeadb/cache"
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
