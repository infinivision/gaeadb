package data

import (
	"encoding/binary"
	"gaeadb/constant"
	"gaeadb/errmsg"
	"os"
)

func (f *file) close() error {
	return f.fp.Close()
}

func (f *file) flush() error {
	return f.fp.Sync()
}

func (f *file) write(o uint64, data []byte) error {
	return write(f.fp, int64(o), data)
}

func (f *file) read(o int64) ([]byte, error) {
	if int64(f.size)-o < HeaderSize {
		return nil, errmsg.NotExist
	}
	h, err := read(f.fp, o, HeaderSize)
	if err != nil {
		return nil, err
	}
	return read(f.fp, o+HeaderSize, int(binary.LittleEndian.Uint16(h)))
}

func (f *file) alloc(size uint64) (uint64, error) {
	curr := f.size
	if curr+size > constant.MaxDataFileSize {
		return 0, errmsg.OutOfSpace
	}
	f.size += size
	return curr, nil
}

func write(fp *os.File, o int64, buf []byte) error {
	n, err := fp.WriteAt(buf, o)
	switch {
	case err != nil:
		return err
	case n != len(buf):
		return errmsg.WriteFailed
	}
	return nil
}

func read(fp *os.File, o int64, n int) ([]byte, error) {
	buf := make([]byte, n)
	m, err := fp.ReadAt(buf, o)
	switch {
	case err != nil:
		return nil, err
	case n != m:
		return nil, errmsg.ReadFailed
	}
	return buf, nil
}
