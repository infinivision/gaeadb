package manager

import (
	"sort"
)

func New() *manager {
	return &manager{
		xs: []*element{},
		mp: make(map[uint64]*element),
	}
}

func (m *manager) Run() {
}

func (m *manager) Stop() {
	m.ch <- struct{}{}
	<-m.ch
}

func (m *manager) Add(ts uint64) {
	if e, ok := m.mp[ts]; ok {
		e.n++
	} else {
		e := &element{n: 1, ts: ts}
		m.mp[ts] = e
		m.xs = push(e, m.xs)
	}
}

func (m *manager) Del(ts uint64) bool {
	if e, ok := m.mp[ts]; ok {
		if e.n = e.n - 1; e.n == 0 {
			r := m.xs[0].ts == ts
			delete(m.mp, ts)
			if r {
				m.xs = m.xs[1:]
				for len(m.xs) > 0 {
					if _, ok := m.mp[m.xs[0].ts]; !ok {
						m.xs = m.xs[1:]
					} else {
						break
					}
				}
			}
			return r
		}
	}
	return false
}

func push(x *element, xs []*element) []*element {
	i := sort.Search(len(xs), func(i int) bool { return xs[i].ts >= x.ts })
	xs = append(xs, &element{})
	copy(xs[i+1:], xs[i:])
	xs[i] = x
	return xs
}
