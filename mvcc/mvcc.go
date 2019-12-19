package mvcc

import (
	"encoding/binary"

	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/errmsg"
	"github.com/infinivision/gaeadb/prefix"
	"github.com/infinivision/gaeadb/suffix"
)

func New(t prefix.Tree) *mvcc {
	return &mvcc{t}
}

func (m *mvcc) Close() error {
	return m.t.Close()
}

func (m *mvcc) Get(k []byte, ts uint64) (uint64, uint64, error) {
	itr, err := m.t.NewBackwardIterator(k)
	if err != nil {
		return 0, 0, err
	}
	defer itr.Close()
	for itr.Valid() {
		if len(itr.Key()) == len(k)+8 {
			if v, rts := itr.Value(), binary.BigEndian.Uint64(itr.Key()[len(k):]); rts <= ts && v != constant.Cancel {
				return v, rts, nil
			}
		}
		if err := itr.Next(); err != nil {
			return 0, 0, err
		}
	}
	return 0, 0, errmsg.NotExist
}

func (m *mvcc) Del(k []byte, ts uint64, w suffix.Writer) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, ts)
	return m.t.Del(append(k, buf...), w)
}

func (m *mvcc) Set(k []byte, v uint64, ts uint64, w suffix.Writer) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, ts)
	return m.t.Set(append(k, buf...), v, w)
}

func (m *mvcc) NewForwardIterator(pref []byte, ts uint64) (Iterator, error) {
	fItr, err := m.t.NewForwardIterator(pref)
	if err != nil {
		return nil, err
	}
	itr := &forwardIterator{ts: ts, itr: fItr, e: new(entry)}
	if err := itr.seek(); err != nil {
		itr.Close()
		return nil, err
	}
	return itr, nil
}

func (m *mvcc) NewBackwardIterator(pref []byte, ts uint64) (Iterator, error) {
	bItr, err := m.t.NewBackwardIterator(pref)
	if err != nil {
		return nil, err
	}
	itr := &backwardIterator{ts: ts, itr: bItr, e: new(entry)}
	if err := itr.seek(); err != nil {
		itr.Close()
		return nil, err
	}
	return itr, nil
}
