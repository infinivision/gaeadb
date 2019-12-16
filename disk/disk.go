package disk

import (
	"encoding/binary"
	"gaeadb/constant"
	"gaeadb/errmsg"
	"math"
	"os"
	"sync/atomic"
)

func New(path string) (*disk, error) {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, err
	}
	st, err := fp.Stat()
	if err != nil {
		fp.Close()
		return nil, err
	}
	d := &disk{fp: fp, cnt: st.Size() / constant.BlockSize}
	if d.cnt < InitDiskSize {
		d.cnt = 0
		if err := d.init(); err != nil {
			fp.Close()
			return nil, err
		}
	}
	return d, nil
}

func (d *disk) Close() error {
	return d.fp.Close()
}

func (d *disk) Flush() error {
	return d.fp.Sync()
}

func (d *disk) Blocks() int64 {
	return atomic.LoadInt64(&d.cnt)
}

func (d *disk) Read(bn int64, buf []byte) (Block, error) {
	switch {
	case bn < 0:
		if cnt := d.alloc(); cnt == -1 {
			return nil, errmsg.OutOfSpace
		} else {
			return &block{cnt - 1, buf}, nil
		}
	case bn > atomic.LoadInt64(&d.cnt):
		return nil, errmsg.OutOfSpace
	default:
		n, err := d.fp.ReadAt(buf, bn*constant.BlockSize)
		switch {
		case err != nil:
			return nil, err
		case n != constant.BlockSize:
			return nil, errmsg.ReadFailed
		}
		return &block{bn, buf}, nil
	}
}

func (d *disk) Write(b Block) error {
	n, err := d.fp.WriteAt(b.Buffer(), b.BlockNumber()*constant.BlockSize)
	switch {
	case err != nil:
		return err
	case n != constant.BlockSize:
		return errmsg.WriteFailed
	}
	return nil
}

func (d *disk) alloc() int64 {
	for {
		curr := atomic.LoadInt64(&d.cnt)
		if curr == math.MaxInt64 {
			return -1
		}
		next := curr + 1
		if atomic.CompareAndSwapInt64(&d.cnt, curr, next) {
			return next
		}
	}
}

func (d *disk) init() error {
	var bs []Block

	root, err := d.read(-1)
	if err != nil {
		return err
	}
	buf := root.Buffer()
	for i := 0; i < 256; i++ {
		b, err := d.read(-1)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(buf[i*8:], uint64(b.BlockNumber())|constant.PN<<constant.TypeOff)
		for j := 0; j < 256; j++ {
			binary.LittleEndian.PutUint64(b.Buffer()[j*8:], constant.ES<<constant.TypeOff)
		}
		bs = append(bs, b)
	}
	for _, b := range bs {
		if err := d.Write(b); err != nil {
			return err
		}
	}
	return d.Write(root)
}

func (d *disk) read(bn int64) (Block, error) {
	return d.Read(bn, make([]byte, constant.BlockSize))
}
