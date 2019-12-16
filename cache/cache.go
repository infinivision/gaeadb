package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/infinivision/gaeadb/cache/scheduler"
	"github.com/infinivision/gaeadb/disk"
	"github.com/nnsgmsone/damrey/logger"
)

func New(limit int, d disk.Disk, log logger.Log) *cache {
	if limit < MinCacheSize {
		limit = MinCacheSize
	}
	return &cache{
		mp:    new(sync.Map),
		cq:    new(list.List),
		hq:    new(list.List),
		fq:    new(list.List),
		sched: scheduler.New(d),
		ch:    make(chan struct{}),
		n:     limit / FreeMultiples,
		pch:   make(chan *page, 1024),
	}
}

func (c *cache) Run() {
	cnt := 0
	freeSize := c.n * FreeMultiples
	ticker := time.NewTicker(Cycle * time.Second)
	for {
		select {
		case <-c.ch:
			c.ch <- struct{}{}
			return
		case pg := <-c.pch:
			if pg.t == I {
				if cnt = cnt + 1; cnt%freeSize == 0 {
					c.gc()
				}
			}
			c.get(pg)
		case <-ticker.C:
			c.gc()
		}
	}
}

func (c *cache) Stop() {
	c.ch <- struct{}{}
	<-c.ch
	c.sched.Close()
}

func (c *cache) Flush() {
	c.sched.Flush()
}

func (c *cache) Release(pg Page) {
	atomic.AddInt32(&pg.(*page).n, -1)
}

func (c *cache) Get(pn int64) (Page, error) {
	switch pn {
	case -1:
		b, err := c.sched.Read(pn)
		if err != nil {
			return nil, err
		}
		pg := &page{b: b, n: 1, cp: c}
		c.mp.Store(pg.PageNumber(), pg)
		c.pch <- pg
		return pg, nil
	default:
		for {
			if v, ok := c.mp.Load(pn); ok {
				pg := v.(*page)
				if add(&pg.n) > 0 {
					c.pch <- pg
					return pg, nil
				}
				for _, ok := c.mp.Load(pn); ok; _, ok = c.mp.Load(pn) { // wait for delete
				}
			}
			b, err := c.sched.Read(pn)
			if err != nil {
				return nil, err
			}
			pg := &page{b: b, n: 1, cp: c}
			if _, ok := c.mp.LoadOrStore(pn, pg); !ok {
				return pg, nil
			}
		}
	}
}

func (c *cache) get(pg *page) {
	switch pg.t {
	case E:
		return
	case I:
		c.set(pg)
		return
	case H:
		isBack := pg.h.Next() == nil
		c.hq.MoveToFront(pg.h)
		if isBack {
			c.reduce()
		}
		return
	case F:
		c.fq.Remove(pg.f)
		pg.f = nil
		c.set(pg)
		return
	}
	switch {
	case pg.h == nil:
		c.cq.MoveToFront(pg.c)
		pg.h = c.hq.PushFront(pg)
	default:
		pg.t = H
		c.cq.Remove(pg.c)
		pg.c = nil
		c.hq.MoveToFront(pg.h)
		c.exchange()
		c.reduce()
	}
}

func (c *cache) set(pg *page) {
	switch {
	case c.hq.Len() < c.n:
		pg.t = H
		pg.h = c.hq.PushFront(pg)
	case c.cq.Len() < c.n/ColdMultiples:
		pg.t = C
		pg.c = c.cq.PushFront(pg)
	default:
		c.release()
		pg.t = C
		pg.c = c.cq.PushFront(pg)
	}
}
func (c *cache) release() {
	if e := c.cq.Back(); e != nil {
		pg := e.Value.(*page)
		pg.c = nil
		c.cq.Remove(e)
		if pg.h != nil {
			c.hq.Remove(pg.h)
			pg.h = nil
		}
		pg.t = F
		pg.f = c.fq.PushFront(pg)
	}
}

func (c *cache) reduce() {
	for e := c.hq.Back(); e != nil; e = c.hq.Back() {
		pg := e.Value.(*page)
		if pg.t == H {
			return
		}
		pg.h = nil
		c.hq.Remove(e)
	}
}

func (c *cache) exchange() {
	if e := c.hq.Back(); e != nil {
		pg := e.Value.(*page)
		if pg.t != H {
			return
		}
		c.hq.Remove(e)
		pg.h = nil
		pg.t = C
		pg.c = c.cq.PushFront(pg)
	}
}

func (c *cache) gc() {
	var cnt int

	if n := c.fq.Len(); n < cnt {
		cnt = n
	} else {
		cnt = n / 2
	}
	prev := c.fq.Back()
	for e := prev; e != nil; e = prev {
		if cnt == 0 {
			break
		}
		cnt--
		pg := e.Value.(*page)
		if n := del(&pg.n); n >= 0 {
			continue
		}
		prev = e.Prev()
		pg.t = E
		c.fq.Remove(e)
		c.mp.Delete(pg.b.BlockNumber())
	}
}

func add(x *int32) int32 {
	for {
		curr := atomic.LoadInt32(x)
		if curr == -1 {
			return -1
		}
		next := curr + 1
		if atomic.CompareAndSwapInt32(x, curr, next) {
			return next
		}
	}
}

func del(x *int32) int32 {
	var curr int32

	if curr = atomic.LoadInt32(x); curr != 0 {
		return 0
	}
	if atomic.CompareAndSwapInt32(x, curr, -1) {
		return -1
	}
	return 0
}
