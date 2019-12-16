package manager

type Manager interface {
	Add(uint64)
	Del(uint64) bool
}

type element struct {
	n  int // reference count
	ts uint64
}

type manager struct {
	xs []*element
	ch chan struct{}
	mp map[uint64]*element
}
