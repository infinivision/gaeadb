package scheduler

import (
	"time"

	"github.com/infinivision/gaeadb/cache"
	"github.com/infinivision/gaeadb/data"
	"github.com/infinivision/gaeadb/scheduler/manager"
	"github.com/infinivision/gaeadb/wal"
)

const (
	Cycle = 1
)

const (
	MinCacheSize = 2000
)

const (
	CkptSize = 1024 * 1024
)

const (
	C = iota // commit
	D        // done
	S        // start
)

type Scheduler interface {
	Run()
	Stop()
	Start() uint64
	Done(uint64) error
	Commit(uint64, map[string]uint64, map[string][]byte) (uint64, error)
}

type result struct {
	err error
	ts  uint64
}

type message struct {
	t   int
	ts  uint64
	rch chan *result
	rmp map[string]uint64
	wmp map[string][]byte
}

type element struct {
	k  string
	ts uint64
}

type checkpoint struct {
	s  bool // start check point
	t  time.Time
	d  data.Data
	w  wal.Writer
	c  cache.Cache
	mp map[uint64]struct{}
	mq map[uint64]struct{} // backup
}

type scheduler struct {
	ts  uint64
	mts uint64 // min ts
	xs  []*element
	cp  *checkpoint
	ch  chan struct{}
	mch chan *message
	mgr manager.Manager
	mp  map[string]*element
}
