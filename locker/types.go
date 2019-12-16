package locker

import (
	"container/list"
	"sync"
	"sync/atomic"
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
	ColdMultiples = 200
	MinCacheSize  = 2000
)

type Locker interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

type Table interface {
	Run()
	Stop()
	Get(uint64) Locker
}

type locker struct {
	t       int   // type
	n       int32 // refer
	k       uint64
	lkr     sync.RWMutex
	h, c, f *list.Element
}

type table struct {
	n          int
	mp         *sync.Map
	hq, cq, fq *list.List
	lch        chan *locker
	ch         chan struct{}
}

func (l *locker) Lock() {
	l.lkr.Lock()
}

func (l *locker) RLock() {
	l.lkr.RLock()
}

func (l *locker) Unlock() {
	l.lkr.Unlock()
	atomic.AddInt32(&l.n, -1)
}

func (l *locker) RUnlock() {
	l.lkr.RUnlock()
	atomic.AddInt32(&l.n, -1)
}
