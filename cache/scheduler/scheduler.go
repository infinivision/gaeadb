package scheduler

import (
	"gaeadb/constant"
	"gaeadb/disk"
)

func New(d disk.Disk) *scheduler {
	return &scheduler{d}
}

func (s *scheduler) Close() error {
	return s.d.Close()
}

func (s *scheduler) Flush() error {
	return s.d.Flush()
}

func (s *scheduler) Write(b disk.Block) error {
	return s.d.Write(b)
}

func (s *scheduler) Read(bn int64) (disk.Block, error) {
	return s.d.Read(bn, make([]byte, constant.BlockSize))
}
