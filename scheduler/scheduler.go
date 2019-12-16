package scheduler

import (
	"encoding/binary"
	"gaeadb/cache"
	"gaeadb/constant"
	"gaeadb/data"
	"gaeadb/errmsg"
	"gaeadb/scheduler/manager"
	"gaeadb/wal"
	"sort"
	"sync/atomic"
	"time"
)

func New(ts uint64, d data.Data, c cache.Cache, w wal.Writer) *scheduler {
	return &scheduler{
		ts:  ts,
		mts: ts,
		xs:  []*element{},
		mgr: manager.New(),
		ch:  make(chan struct{}),
		mp:  make(map[string]*element),
		mch: make(chan *message, 1024),
		cp: &checkpoint{
			c:  c,
			d:  d,
			w:  w,
			s:  true,
			t:  time.Now(),
			mp: make(map[uint64]struct{}),
			mq: make(map[uint64]struct{}),
		},
	}
}

func (s *scheduler) Run() {
	ticker := time.NewTicker(Cycle * time.Second)
	for {
		select {
		case <-s.ch:
			s.ch <- struct{}{}
			return
		case m := <-s.mch:
			s.process(m)
		case <-ticker.C:
			s.gc()
		}
	}
}

func (s *scheduler) Stop() {
	s.ch <- struct{}{}
	<-s.ch
}

func (s *scheduler) Start() uint64 {
	ts := atomic.LoadUint64(&s.ts)
	s.mch <- &message{t: S, ts: ts}
	return ts
}

func (s *scheduler) Done(ts uint64) error {
	rch := make(chan *result)
	s.mch <- &message{t: D, ts: ts, rch: rch}
	r := <-rch
	return r.err
}

func (s *scheduler) Commit(ts uint64, rmp map[string]uint64, wmp map[string][]byte) (uint64, error) {
	rch := make(chan *result)
	s.mch <- &message{C, ts, rch, rmp, wmp}
	r := <-rch
	return r.ts, r.err
}

func (s *scheduler) process(m *message) {
	switch m.t {
	case S:
		s.mgr.Add(m.ts)
	case D:
		err := s.cp.endCKPT(m.ts)
		m.rch <- &result{err: err}
	case C:
		var err error

		for k, rts := range m.rmp {
			if e, ok := s.mp[k]; ok && e.ts > rts {
				err = errmsg.TransactionConflict
				m.rch <- &result{err: err}
				return
			}
		}
		ts := atomic.AddUint64(&s.ts, 1)
		for k, _ := range m.wmp {
			if e, ok := s.mp[k]; ok {
				e.ts = ts
				copy(s.xs[e.i+1:], s.xs[e.i:])
				e.i, s.xs = push(e, s.xs)
			} else {
				e = &element{k: k, ts: ts}
				s.mp[k] = e
				e.i, s.xs = push(e, s.xs)
			}
		}
		if s.mgr.Del(m.ts) {
			s.mts = m.ts
		}
		switch {
		case s.cp.s:
			s.cp.mp[ts] = struct{}{}
			if len(s.cp.mp) > CkptSize || time.Now().Sub(s.cp.t) > constant.CheckPointCycle {
				err = s.cp.startCKPT()
			}
		default:
			s.cp.mq[ts] = struct{}{}
		}
		m.rch <- &result{err, ts}
	}
}

func (s *scheduler) gc() {
	for len(s.xs) > 0 && s.xs[0].ts < s.mts {
		delete(s.mp, s.xs[0].k)
		s.xs = s.xs[1:]
	}
}

func (c *checkpoint) endCKPT(t uint64) error {
	if _, ok := c.mq[t]; ok {
		delete(c.mq, t)
		return nil
	}
	if delete(c.mp, t); !c.s && len(c.mp) == 0 {
		c.s = true
		c.t = time.Now()
		c.mp, c.mq = c.mq, c.mp
		c.c.Flush()
		if err := c.d.Flush(); err != nil {
			return err
		}
		if err := c.w.Append([]byte{wal.EC}); err != nil {
			return err
		}
		c.w.EndCKPT()
	}
	return nil
}

func (c *checkpoint) startCKPT() error {
	c.s = false
	log := make([]byte, 1+4+8*len(c.mp))
	log[0] = wal.SC
	binary.LittleEndian.PutUint32(log[1:], uint32(len(c.mp)))
	i := 5
	for t, _ := range c.mp {
		binary.LittleEndian.PutUint64(log[i:], t)
		i += 8
	}
	c.w.StartCKPT()
	return c.w.Append(log)
}

func push(x *element, xs []*element) (int, []*element) {
	i := sort.Search(len(xs), func(i int) bool { return xs[i].ts >= x.ts })
	xs = append(xs, &element{})
	copy(xs[i+1:], xs[i:])
	xs[i] = x
	return i, xs
}
