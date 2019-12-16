package disk

import (
	"os"
)

const (
	InitDiskSize = 257 // 256 + 1
)

type Block interface {
	Buffer() []byte
	BlockNumber() int64
}

type Disk interface {
	Close() error
	Flush() error
	Blocks() int64
	Write(Block) error
	Read(int64, []byte) (Block, error)
}

type block struct {
	bn     int64 // block number
	buffer []byte
}

type disk struct {
	cnt int64 // block count
	fp  *os.File
}

func (a *block) Buffer() []byte {
	return a.buffer
}

func (a *block) BlockNumber() int64 {
	return a.bn
}
