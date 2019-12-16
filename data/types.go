package data

import (
	"os"
	"sync"
)

const (
	HeaderSize = 2
)

const (
	Magic = "gaeadb"
)

type Data interface {
	Close() error
	Flush() error
	Del(uint64) error
	Read(uint64) ([]byte, error)
	Write(uint64, []byte) error
	Alloc([]byte) (uint64, error)
}

type file struct {
	size uint64
	fp   *os.File
}

type data struct {
	sync.Mutex
	dir  string
	size uint64
	fs   []*file
}
