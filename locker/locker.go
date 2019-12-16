package locker

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

func New() *table {
	return &table{
		mp:  new(sync.Map),
		cq:  new(list.List),
		hq:  new(list.List),
		fq:  new(list.List),
		ch:  make(chan struct{}),
		lch: make(chan *locker, 1024),
		n:   MinCacheSize / FreeMultiples,
	}
}

func (t *table) Run() {
	cnt := 0
	freeSize := t.n * FreeMultiples
	ticker := time.NewTicker(Cycle * time.Second)
	for {
		select {
		case <-t.ch:
			t.ch <- struct{}{}
			return
		case l := <-t.lch:
			if l.t == I {
				if cnt = cnt + 1; cnt%freeSize == 0 {
					t.gc()
				}
			}
			t.get(l)
		case <-ticker.C:
			t.gc()
		}
	}
}

func (t *table) Stop() {
	t.ch <- struct{}{}
	<-t.ch
}

func (t *table) Get(k uint64) Locker {
	for {
		if v, ok := t.mp.Load(k); ok {
			l := v.(*locker)
			if add(&l.n) > 0 {
				t.lch <- l
				return l
			}
			for _, ok := t.mp.Load(k); ok; _, ok = t.mp.Load(k) { // wait for delete
			}
		}
		l := &locker{n: 1, k: k}
		if _, ok := t.mp.LoadOrStore(k, l); !ok {
			return l
		}
	}
}

func (t *table) get(l *locker) {
	switch l.t {
	case E:
		return
	case I:
		t.set(l)
		return
	case H:
		isBack := l.h.Next() == nil
		t.hq.MoveToFront(l.h)
		if isBack {
			t.reduce()
		}
		return
	case F:
		t.fq.Remove(l.f)
		l.f = nil
		t.set(l)
		return
	}
	switch {
	case l.h == nil:
		t.cq.MoveToFront(l.c)
		l.h = t.hq.PushFront(l)
	default:
		l.t = H
		t.cq.Remove(l.c)
		l.c = nil
		t.hq.MoveToFront(l.h)
		t.exchange()
		t.reduce()
	}
}

func (t *table) set(l *locker) {
	switch {
	case t.hq.Len() < t.n:
		l.t = H
		l.h = t.hq.PushFront(l)
	case t.cq.Len() < t.n/ColdMultiples:
		l.t = C
		l.c = t.cq.PushFront(l)
	default:
		t.release()
		l.t = C
		l.c = t.cq.PushFront(l)
	}
}
func (t *table) release() {
	if e := t.cq.Back(); e != nil {
		l := e.Value.(*locker)
		l.c = nil
		t.cq.Remove(e)
		if l.h != nil {
			t.hq.Remove(l.h)
			l.h = nil
		}
		l.t = F
		l.f = t.fq.PushFront(l)
	}
}

func (t *table) reduce() {
	for e := t.hq.Back(); e != nil; e = t.hq.Back() {
		l := e.Value.(*locker)
		if l.t == H {
			return
		}
		l.h = nil
		t.hq.Remove(e)
	}
}

func (t *table) exchange() {
	if e := t.hq.Back(); e != nil {
		l := e.Value.(*locker)
		if l.t != H {
			return
		}
		t.hq.Remove(e)
		l.h = nil
		l.t = C
		l.c = t.cq.PushFront(l)
	}
}

func (t *table) gc() {
	var cnt int

	if n := t.fq.Len(); n < cnt {
		cnt = n
	} else {
		cnt = n / 2
	}
	prev := t.fq.Back()
	for e := prev; e != nil; e = prev {
		if cnt == 0 {
			break
		}
		cnt--
		l := e.Value.(*locker)
		if n := del(&l.n); n >= 0 {
			continue
		}
		prev = e.Prev()
		l.t = E
		t.fq.Remove(e)
		t.mp.Delete(l.k)
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
