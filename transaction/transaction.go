package transaction

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/infinivision/gaeadb/cache"
	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/data"
	"github.com/infinivision/gaeadb/errmsg"
	"github.com/infinivision/gaeadb/mvcc"
	"github.com/infinivision/gaeadb/scheduler"
	"github.com/infinivision/gaeadb/wal"
	"github.com/nnsgmsone/damrey/logger"
)

func New(ro bool, d data.Data, m mvcc.MVCC, w wal.Writer, log logger.Log, schd scheduler.Scheduler) *transaction {
	return &transaction{
		s:    13, // timestamp size + one byte + key's number
		d:    d,
		m:    m,
		w:    w,
		ro:   ro,
		log:  log,
		schd: schd,
		rts:  schd.Start(),
		rmp:  make(map[string]uint64),
		wmp:  make(map[string][]byte),
	}
}

func (tx *transaction) Rollback() error {
	if del(&tx.n) >= 0 {
		return nil
	}
	return nil
}

func (tx *transaction) Commit() error {
	var err error
	var os []uint64
	var ks []string

	switch {
	case tx.ro:
		return errmsg.ReadOnlyTransaction
	case del(&tx.n) >= 0:
		return nil
	}
	tx.wts, err = tx.schd.Commit(tx.rts, tx.rmp, tx.wmp)
	if err != nil {
		return err
	}
	cnt := 0
	log := make([]byte, tx.s)
	{ // commit
		log[0] = wal.ST
		binary.LittleEndian.PutUint64(log[1:], tx.wts)
		binary.LittleEndian.PutUint32(log[9:], uint32(len(tx.wmp)))
		i := 13
		for k, v := range tx.wmp {
			binary.LittleEndian.PutUint16(log[i:], uint16(len(k)))
			i += 2
			copy(log[i:], []byte(k))
			i += len(k)
			binary.LittleEndian.PutUint16(log[i:], uint16(len(v)))
			i += 2
			if len(v) > 0 {
				cnt++
				copy(log[i:], v)
				i += len(v)
			}
		}
		if err = tx.w.Append(log); err != nil {
			tx.log.Fatalf("transaction start failed: %v\n", err)
		}
	}
	{
		if len(log) < 9+4+cnt*8 {
			log = make([]byte, 9+4+cnt*8)
		} else {
			log = log[:9+4+cnt*8]
		}
		log[0] = wal.WD
		binary.LittleEndian.PutUint64(log[1:], tx.wts)
		binary.LittleEndian.PutUint32(log[9:], uint32(cnt))
		i := 13
		for k, v := range tx.wmp {
			switch {
			case v == nil:
				continue
			case len(v) == 0:
				ks = append(ks, k)
				continue
			}
			if o, err := tx.d.Alloc(v); err != nil {
				tx.log.Fatalf("transaction alloc space for data failed: %v\n", err)
			} else {
				os = append(os, o)
				binary.LittleEndian.PutUint64(log[i:], o)
				i += 8
			}
			ks = append(ks, k)
		}
		if err = tx.w.Append(log); err != nil {
			tx.log.Fatalf("transaction append record failed: %v\n", err)
		}
	}
	w := &walWriter{
		w:  tx.w,
		ts: tx.wts,
		mp: make(map[int64]cache.Page),
	}
	for _, k := range ks {
		switch {
		case tx.wmp[k] == nil:
			if err := tx.m.Set([]byte(k), constant.Delete, tx.wts, w); err != nil {
				tx.log.Fatalf("transaction del '%s' failed: %v\n", k, err)
			}
		case len(tx.wmp[k]) == 0:
			if err := tx.m.Set([]byte(k), constant.Empty, tx.wts, w); err != nil {
				tx.log.Fatalf("transaction set '%s' failed: %v\n", k, err)
			}
		default:
			if err := tx.d.Write(os[0], tx.wmp[k]); err != nil {
				tx.log.Fatalf("transaction write data of '%s' failed: %v\n", k, err)
			}
			if err := tx.m.Set([]byte(k), os[0], tx.wts, w); err != nil {
				tx.log.Fatalf("transaction set '%s' failed: %v\n", k, err)
			}
			os = os[1:]
		}
	}
	{
		log = log[:9]
		log[0] = wal.CT
		binary.LittleEndian.PutUint64(log[1:], tx.wts)
		if err = tx.w.Append(log); err != nil {
			tx.log.Fatalf("transaction commit failed: %v\n", err)
		}
	}
	{
		for _, pg := range w.mp {
			if pg.s {
				pg.pg.Sync()
			}
		}
	}
	if err := tx.schd.Done(tx.wts); err != nil {
		tx.log.Fatalf("transaction done failed: %v\n", err)
	}
	return nil
}

func (tx *transaction) Del(k []byte) error {
	switch {
	case tx.ro:
		return errmsg.ReadOnlyTransaction
	case len(k) == 0:
		return errmsg.KeyIsEmpty
	case len(k) > constant.MaxKeySize:
		return errmsg.KeyTooLong
	}
	if tx.s += 4 + len(k); tx.s > constant.MaxTransactionSize {
		return errmsg.OutOfSpace
	}
	tx.wmp[string(k)] = nil
	return nil
}

func (tx *transaction) Set(k, v []byte) error {
	switch {
	case tx.ro:
		return errmsg.ReadOnlyTransaction
	case len(k) == 0:
		return errmsg.KeyIsEmpty
	case len(k) > constant.MaxKeySize:
		return errmsg.KeyTooLong
	case len(v) > constant.MaxValueSize:
		return errmsg.ValTooLong
	}
	if tx.s += 4 + len(k) + len(v); tx.s > constant.MaxTransactionSize {
		return errmsg.OutOfSpace
	}
	tx.wmp[string(k)] = v
	return nil
}

func (tx *transaction) Get(k []byte) ([]byte, error) {
	if len(k) == 0 {
		return nil, errmsg.KeyIsEmpty
	}
	if !tx.ro {
		if v, ok := tx.wmp[string(k)]; ok {
			if v == nil {
				return nil, errmsg.NotExist
			}
			return v, nil
		}
	}
	o, ts, err := tx.m.Get(k, tx.rts)
	if err != nil {
		return nil, err
	}
	switch {
	case o != constant.Empty:
		if v, err := tx.d.Read(o); err != nil {
			return nil, err
		} else {
			if !tx.ro {
				tx.rmp[string(k)] = ts
			}
			return v, nil
		}
	default:
		if !tx.ro {
			tx.rmp[string(k)] = ts
		}
		return []byte{}, nil
	}
}

func (tx *transaction) NewForwardIterator(pref []byte) (Iterator, error) {
	if itr, err := tx.m.NewForwardIterator(pref, tx.rts); err != nil {
		return nil, err
	} else {
		fitr := &forwardIterator{
			tx:  tx,
			itr: itr,
			kv: &kvList{
				mp:  make(map[string][]byte),
				omp: make(map[string]uint64),
			},
		}
		return fitr, fitr.seek()
	}
}

func (tx *transaction) NewBackwardIterator(pref []byte) (Iterator, error) {
	if itr, err := tx.m.NewBackwardIterator(pref, tx.rts); err != nil {
		return nil, err
	} else {
		bitr := &forwardIterator{
			tx:  tx,
			itr: itr,
			kv: &kvList{
				mp:  make(map[string][]byte),
				omp: make(map[string]uint64),
			},
		}
		return bitr, bitr.seek()
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
