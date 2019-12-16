package mvcc

import (
	"encoding/binary"
	"gaeadb/constant"
	"gaeadb/errmsg"
)

func (itr *forwardIterator) Close() error {
	return itr.itr.Close()
}

func (itr *forwardIterator) Next() error {
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

func (itr *forwardIterator) Valid() bool {
	return itr.itr.Valid()
}

func (itr *forwardIterator) Key() []byte {
	return itr.itr.Key()[:len(itr.itr.Key())-8]
}

func (itr *forwardIterator) Value() uint64 {
	return itr.itr.Value()
}

func (itr *forwardIterator) Timestamp() uint64 {
	return binary.BigEndian.Uint64(itr.itr.Key()[len(itr.itr.Key())-8:])
}

func (itr *forwardIterator) seek() error {
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
