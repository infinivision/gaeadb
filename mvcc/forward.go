package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/errmsg"
)

func (itr *forwardIterator) Close() error {
	return itr.itr.Close()
}

func (itr *forwardIterator) Next() error {
	if itr.s {
		itr.s = false
		return itr.seek()
	}
	itr.itr.Next()
	for itr.itr.Valid() {
		if ts := binary.BigEndian.Uint64(itr.itr.Key()[len(itr.itr.Key())-8:]); ts <= itr.ts && itr.itr.Value() != constant.Cancel {
			itr.s = true
			return itr.filter(itr.itr.Key()[:len(itr.itr.Key())-8], ts)
		}
		if err := itr.itr.Next(); err != nil {
			return err
		}
	}
	return errmsg.ScanEnd
}

func (itr *forwardIterator) Valid() bool {
	if itr.s {
		return true
	}
	return itr.itr.Valid()
}

func (itr *forwardIterator) Key() []byte {
	return itr.e.k
}

func (itr *forwardIterator) Value() uint64 {
	return itr.e.v
}

func (itr *forwardIterator) Timestamp() uint64 {
	return itr.e.ts
}

func (itr *forwardIterator) seek() error {
	for itr.itr.Valid() {
		if ts := binary.BigEndian.Uint64(itr.itr.Key()[len(itr.itr.Key())-8:]); ts <= itr.ts && itr.itr.Value() != constant.Cancel {
			itr.s = true
			return itr.filter(itr.itr.Key()[:len(itr.itr.Key())-8], ts)
		}
		if err := itr.itr.Next(); err != nil {
			return err
		}
	}
	return errmsg.ScanEnd
}

func (itr *forwardIterator) filter(k []byte, ts uint64) error {
	if !itr.itr.Valid() {
		return errmsg.ScanEnd
	}
	itr.e.k = k
	itr.e.ts = ts
	itr.e.v = itr.itr.Value()
	for {
		if err := itr.itr.Next(); err != nil {
			return err
		}
		if !itr.itr.Valid() {
			return nil
		}
		if bytes.Compare(k, itr.itr.Key()[:len(itr.itr.Key())-8]) != 0 {
			return nil
		}
		if itr.Value() != constant.Cancel {
			itr.e.v = itr.itr.Value()
			itr.e.ts = binary.BigEndian.Uint64(itr.itr.Key()[len(itr.itr.Key())-8:])
		}
	}
}
