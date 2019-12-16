package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync/atomic"
	"syscall"

	"github.com/infinivision/gaeadb/sum"
)

func (w *walWriter) Close() error {
	return w.fp.close()
}

func (w *walWriter) EndCKPT() error {
	for w.n < w.m {
		if err := os.Truncate(fileName(w.n, w.dir), 0); err != nil {
			return err
		}
		w.n++
	}
	return nil
}

func (w *walWriter) StartCKPT() error {
	w.m = w.idx - 1
	return nil
}

func (w *walWriter) Append(record []byte) error {
	f, o, err := w.alloc(record)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(f.buf[o:], sum.Sum(crc32.New(crc32.MakeTable(crc32.Castagnoli)), record))
	binary.LittleEndian.PutUint32(f.buf[o+SumSize:], uint32(len(record)))
	copy(f.buf[o+HeaderSize:], record)
	defer func() {
		w.RLock()
		fp := w.fp
		w.RUnlock()
		cnt := add(&f.cnt, -1)
		if f != fp && cnt == 0 {
			f.close()
		}
	}()
	return f.flush()
}

func (w *walWriter) alloc(record []byte) (*file, int32, error) {
	m := int32(len(record) + HeaderSize)
	w.Lock()
	defer w.Unlock()
	for {
		if o, err := w.fp.alloc(m); err != nil {
			fp, err := newFile(fileName(w.idx, w.dir), w.flag)
			if err != nil {
				return nil, -1, err
			}
			w.idx++
			if atomic.LoadInt32(&w.fp.cnt) == 0 {
				w.fp.close()
			}
			w.fp = fp
		} else {
			add(&w.fp.cnt, 1)
			return w.fp, o, nil
		}
	}
}

func add(x *int32, y int32) int32 {
	for {
		curr := atomic.LoadInt32(x)
		next := curr + y
		if atomic.CompareAndSwapInt32(x, curr, next) {
			return next
		}
	}
}

func newWriter(dir string, flag int) (*walWriter, error) {
	w := &walWriter{
		n:    0,
		m:    0,
		dir:  dir,
		flag: flag,
	}
	for i := 0; ; i++ {
		fp, err := openFile(fileName(i, dir), flag)
		switch {
		case err == nil:
			if w.fp != nil {
				w.fp.close()
			}
			w.fp = fp
		case err == syscall.ENOENT:
			if w.fp == nil {
				fp, err := newFile(fileName(i, dir), flag)
				if err != nil {
					return nil, err
				}
				i++
				w.fp = fp
			} else {
				w.fp.getSize()
			}
			w.idx = i
			return w, nil
		default:
			return nil, err
		}
	}
}
