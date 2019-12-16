package scheduler

import "github.com/infinivision/gaeadb/disk"

type Scheduler interface {
	Close() error
	Flush() error
	Write(disk.Block) error
	Read(int64) (disk.Block, error)
}

type scheduler struct {
	d disk.Disk
}
