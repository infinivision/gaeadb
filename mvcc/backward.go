package mvcc

import (
	"encoding/binary"
	"gaeadb/constant"
	"gaeadb/errmsg"
)

func (itr *backwardIterator) Close() error {
	return itr.itr.Close()
}

func (itr *backwardIterator) Next() error {
	itr.itr.Next()
	for itr.itr.Valid() {
		if binary.BigEndian.Uint64(itr.itr.Key()[len(itr.itr.Key())-8:]) <= itr.ts && itr.itr.Value() != constant.Cancel {
			return nil
		}
		if err := itr.itr.Next(); err != nil {
			return err
		}
	}
	return errmsg.ScanEnd
}

func (itr *backwardIterator) Valid() bool {
	return itr.itr.Valid()
}

func (itr *backwardIterator) Key() []byte {
	return itr.itr.Key()[:len(itr.itr.Key())-8]
}

func (itr *backwardIterator) Value() uint64 {
	return itr.itr.Value()
}

func (itr *backwardIterator) Timestamp() uint64 {
	return binary.BigEndian.Uint64(itr.itr.Key()[len(itr.itr.Key())-8:])
}

func (itr *backwardIterator) seek() error {
	for itr.itr.Valid() {
		if binary.BigEndian.Uint64(itr.itr.Key()[len(itr.itr.Key())-8:]) <= itr.ts && itr.itr.Value() != constant.Cancel {
			return nil
		}
		if err := itr.itr.Next(); err != nil {
			return err
		}
	}
	return errmsg.ScanEnd
}
