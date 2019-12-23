package wal

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/errmsg"
	"github.com/infinivision/gaeadb/sum"
	"golang.org/x/sys/unix"
)

func (f *file) close() error {
	return unix.Munmap(f.buf)
}

func (f *file) flush() error {
	return unix.Msync(f.buf, unix.MS_SYNC)
}

func (f *file) alloc(size int32) (int32, error) {
	curr := f.size
	if curr+size > constant.MaxTransactionSize {
		return 0, errmsg.OutOfSpace
	}
	f.size += size
	return curr, nil
}

func (f *file) getSize() {
	buf := append([]byte{}, f.buf...)
	o := 0
	for len(buf) > HeaderSize {
		n := int(binary.LittleEndian.Uint32(buf[o+SumSize:]))
		if len(buf[o+HeaderSize:]) < n {
			f.size = int32(o)
			return
		}
		if sum.Sum(crc32.New(crc32.MakeTable(crc32.Castagnoli)), buf[o+HeaderSize:o+HeaderSize+n]) != binary.LittleEndian.Uint32(buf[o:]) {
			f.size = int32(o)
			return
		}
		switch buf[o+HeaderSize] {
		case EM:
			f.size = int32(o)
			return
		case EC:
			o += 1
		case SC:
			if len(buf[o+1:]) < 4 {
				f.size = int32(o)
				return
			}
			n := int(binary.LittleEndian.Uint32(buf[o+1:]))
			if len(buf[o+5:]) < n*8 { // incomplete record
				f.size = int32(o)
				return
			}
			o += 5 + n*8
		case AT:
			if len(buf[o+1:]) < 8 {
				f.size = int32(o)
				return
			}
			o += 9
		case CT:
			if len(buf[o+1:]) < 8 {
				f.size = int32(o)
				return
			}
			o += 9
		case ST:
			if len(buf[o+1:]) < 8 {
				f.size = int32(o)
				return
			}
			n := int(binary.LittleEndian.Uint32(buf[o+9:]))
			j := 13
			for i := 0; i < n; i++ {
				if len(buf[o+j:]) < 2 {
					f.size = int32(o)
					return
				}
				kn := int(binary.LittleEndian.Uint16(buf[o+j:]))
				j += 2
				if len(buf[o+j:]) < kn {
					f.size = int32(o)
					return
				}
				j += kn
				if len(buf[o+j:]) < 2 {
					f.size = int32(o)
					return
				}
				vn := int(binary.LittleEndian.Uint16(buf[o:]))
				j += 2
				if len(buf[o+j:]) < vn {
					f.size = int32(o)
					return
				}
				j += vn
			}
			o += j
		case WD:
			if len(buf[o+1:]) < 8 {
				f.size = int32(o)
				return
			}
			if len(buf[o+9:]) < 8 {
				f.size = int32(o)
				return
			}
			n := int(binary.LittleEndian.Uint32(buf[9:]))
			if len(buf[o+13:]) < n*8 {
				f.size = int32(o)
				return
			}
			o += 13 + n*8
		case NP:
			if len(buf[o+1:]) < 30 {
				f.size = int32(o)
				return
			}
			o += 31
		case CP:
			if len(buf[o+1:]) < 20 {
				f.size = int32(o)
				return
			}
			on := int(binary.LittleEndian.Uint16(buf[o+17:]))
			vn := int(binary.LittleEndian.Uint16(buf[o+19:]))
			o += 21 + on*2 + vn*8
		case NS:
			if len(buf[o+1:]) < 26 {
				f.size = int32(o)
				return
			}
			o += 27
		}
	}
	f.size = int32(o)
}

func newFile(path string, flag int) (*file, error) {
	fd, err := unix.Open(path, unix.O_CREAT|flag, 0664)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	if err := unix.Ftruncate(fd, constant.MaxTransactionSize); err != nil {
		return nil, err
	}
	buf, err := unix.Mmap(fd, 0, constant.MaxTransactionSize, unix.PROT_WRITE|unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &file{size: 0, buf: buf}, nil
}

func openFile(path string, flag int) (*file, error) {
	fd, err := unix.Open(path, flag, 0664)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	buf, err := unix.Mmap(fd, 0, constant.MaxTransactionSize, unix.PROT_WRITE|unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &file{size: 0, buf: buf}, nil
}
