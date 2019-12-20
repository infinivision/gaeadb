package cache

import (
	"container/list"
	"sync"

	"github.com/infinivision/gaeadb/cache/scheduler"
	"github.com/infinivision/gaeadb/disk"
)

const (
	Cycle = 30
)

const (
	I = iota
	H
	C
	F
	E
)

const (
	FreeMultiples = 10
	ColdMultiples = 100
	MinCacheSize  = 2000
)

type Page interface {
	Sync()
	Buffer() []byte
	PageNumber() int64
}

type Cache interface {
	Run()
	Stop()
	Flush()
	Release(Page)
	Get(int64) (Page, error)
}

type page struct {
	t       int   // type
	n       int32 // refer
	cp      *cache
	b       disk.Block
	h, c, f *list.Element
}

type cache struct {
	n          int
	mp         *sync.Map
	hq, cq, fq *list.List
	pch        chan *page
	ch         chan struct{}
	sched      scheduler.Scheduler
}

func (pg *page) Sync() {
	pg.cp.sched.Write(pg.b)
}

func (pg *page) Buffer() []byte {
	return pg.b.Buffer()
}

func (pg *page) PageNumber() int64 {
	return pg.b.BlockNumber()
}
