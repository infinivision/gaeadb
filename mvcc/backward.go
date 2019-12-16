package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/errmsg"
)

func (itr *backwardIterator) Close() error {
	return itr.itr.Close()
}

func (itr *backwardIterator) Next() error {
	if len(itr.ks) > 0 {
		itr.ks = itr.ks[1:]
		itr.vs = itr.vs[1:]
		itr.tss = itr.tss[1:]
		if len(itr.ks) == 0 {
			return itr.filter()
		}
		return nil
	}
	itr.itr.Next()
	for itr.itr.Valid() {
		if binary.BigEndian.Uint64(itr.itr.Key()[len(itr.itr.Key())-8:]) <= itr.ts && itr.itr.Value() != constant.Cancel {
			return itr.filter()
		}
		if err := itr.itr.Next(); err != nil {
			return err
		}
	}
	return errmsg.ScanEnd
}

func (itr *backwardIterator) Valid() bool {
	if len(itr.ks) > 0 {
		return true
	}
	return itr.itr.Valid()
}

func (itr *backwardIterator) Key() []byte {
	if len(itr.ks) > 0 {
		return itr.ks[0]
	}
	return itr.itr.Key()[:len(itr.itr.Key())-8]
}

func (itr *backwardIterator) Value() uint64 {
	if len(itr.vs) > 0 {
		return itr.vs[0]
	}
	return itr.itr.Value()
}

func (itr *backwardIterator) Timestamp() uint64 {
	if len(itr.tss) > 0 {
		return itr.tss[0]
	}
	return binary.BigEndian.Uint64(itr.itr.Key()[len(itr.itr.Key())-8:])
}

func (itr *backwardIterator) seek() error {
	for itr.itr.Valid() {
		if binary.BigEndian.Uint64(itr.itr.Key()[len(itr.itr.Key())-8:]) <= itr.ts && itr.itr.Value() != constant.Cancel {
			return itr.filter()
		}
		if err := itr.itr.Next(); err != nil {
			return err
		}
	}
	return errmsg.ScanEnd
}

func (itr *backwardIterator) filter() error {
	if !itr.itr.Valid() {
		return errmsg.ScanEnd
	}
	k := itr.Key()
	v := itr.Value()
	ts := itr.Timestamp()
	for {
		if err := itr.itr.Next(); err != nil {
			return err
		}
		if !itr.itr.Valid() {
			itr.ks = append(itr.ks, k)
			itr.vs = append(itr.vs, v)
			itr.tss = append(itr.tss, ts)
			return nil
		}
		if bytes.Compare(k, itr.Key()) != 0 {
			itr.ks = append(itr.ks, k)
			itr.vs = append(itr.vs, v)
			itr.tss = append(itr.tss, ts)
			return nil
		}
	}
}
