package transaction

import (
	"encoding/binary"

	"github.com/infinivision/gaeadb/data"
	"github.com/infinivision/gaeadb/mvcc"
	"github.com/infinivision/gaeadb/scheduler"
	"github.com/infinivision/gaeadb/wal"
	"github.com/nnsgmsone/damrey/logger"
)

type Transaction interface {
	Commit() error
	Rollback() error
	Del([]byte) error
	Set([]byte, []byte) error
	Get([]byte) ([]byte, error)
	NewForwardIterator([]byte) (Iterator, error)
	NewBackwardIterator([]byte) (Iterator, error)
}

type Iterator interface {
	Close() error
	Next() error
	Valid() bool
	Key() []byte
	Value() ([]byte, error)
}

type kvList struct {
	ks  [][]byte
	mp  map[string][]byte
	omp map[string]uint64
}

type forwardIterator struct {
	kv  *kvList
	tx  *transaction
	itr mvcc.Iterator
}

type backwardIterator struct {
	kv  *kvList
	tx  *transaction
	itr mvcc.Iterator
}

type transaction struct {
	s    int  // transaction size
	ro   bool // read only
	n    int32
	rts  uint64 // read timestamp
	wts  uint64 // write timestamp
	d    data.Data
	m    mvcc.MVCC
	w    wal.Writer
	log  logger.Log
	rmp  map[string]uint64 // read cache
	wmp  map[string][]byte // write cache
	schd scheduler.Scheduler
}

type walWriter struct {
	ts uint64
	w  wal.Writer
}

func (w *walWriter) NewSuffix(end, start byte, pn, val uint64) error {
	log := make([]byte, 3+8+16)
	log[0] = wal.NS
	log[1] = start
	log[2] = end
	binary.LittleEndian.PutUint64(log[3:], w.ts)
	binary.LittleEndian.PutUint64(log[11:], pn)
	binary.LittleEndian.PutUint64(log[19:], val)
	return w.w.Append(log)
}

func (w *walWriter) ChgPrefix(pn uint64, os []uint16, vs []uint64) error {
	log := make([]byte, 9+8+4+len(os)*2+len(vs)*8)
	log[0] = wal.CP
	binary.LittleEndian.PutUint64(log[1:], w.ts)
	binary.LittleEndian.PutUint64(log[9:], pn)
	i := 17
	binary.LittleEndian.PutUint16(log[i:], uint16(len(os)))
	i += 2
	binary.LittleEndian.PutUint16(log[i:], uint16(len(vs)))
	i += 2
	for _, o := range os {
		binary.LittleEndian.PutUint16(log[i:], o)
		i += 2
	}
	for _, v := range vs {
		binary.LittleEndian.PutUint64(log[i:], v)
		i += 8
	}
	return w.w.Append(log)
}

func (w *walWriter) NewPrefix(par, pn uint64, o uint16, os []uint16, vs []uint64) error {
	log := make([]byte, 9+16+2+4+len(os)*2+len(vs)*8)
	log[0] = wal.NP
	binary.LittleEndian.PutUint64(log[1:], w.ts)
	binary.LittleEndian.PutUint64(log[9:], par)
	binary.LittleEndian.PutUint64(log[17:], pn)
	binary.LittleEndian.PutUint16(log[25:], o)
	i := 27
	binary.LittleEndian.PutUint16(log[i:], uint16(len(os)))
	i += 2
	binary.LittleEndian.PutUint16(log[i:], uint16(len(vs)))
	i += 2
	for _, o := range os {
		binary.LittleEndian.PutUint16(log[i:], o)
		i += 2
	}
	for _, v := range vs {
		binary.LittleEndian.PutUint64(log[i:], v)
		i += 8
	}
	return w.w.Append(log)
}
