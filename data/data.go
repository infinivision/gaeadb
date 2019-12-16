package data

import (
	"encoding/binary"
	"fmt"
	"gaeadb/errmsg"
	"os"
)

func New(dir string) (*data, error) {
	var size uint64

	d := &data{dir: dir}
	for i := 0; ; i++ {
		fp, err := openFile(d.fileName(i))
		switch {
		case err == nil:
			size = fp.size
			d.size += fp.size
			d.fs = append(d.fs, fp)
		case os.IsNotExist(err):
			if len(d.fs) == 0 {
				fp, err := newFile(d.fileName(len(d.fs)))
				if err != nil {
					return nil, err
				}
				if err := fp.write(0, []byte(Magic)); err != nil {
					fp.close()
					return nil, err
				}
				d.fs = append(d.fs, fp)
				fp.size += uint64(len(Magic))
			} else {
				d.size -= size
			}
			return d, nil
		default:
			d.Close()
			return nil, err
		}
	}
}

func (d *data) Close() error {
	for _, fp := range d.fs {
		fp.close()
	}
	return nil
}

func (d *data) Flush() error {
	for _, fp := range d.fs {
		fp.flush()
	}
	return nil
}

func (d *data) Del(o uint64) error {
	return nil
}

func (d *data) Read(o uint64) ([]byte, error) {
	for i, j := 0, len(d.fs); i < j; i++ {
		if o < d.fs[i].size {
			return d.fs[i].read(int64(o))
		}
		o -= d.fs[i].size
	}
	return nil, errmsg.NotExist
}

func (d *data) Write(o uint64, data []byte) error {
	data = append(header(data), data...)
	for i, j := 0, len(d.fs); i < j; i++ {
		if o < d.fs[i].size {
			return d.fs[i].write(o, data)
		}
		o -= d.fs[i].size
	}
	return errmsg.OutOfSpace
}

func (d *data) Alloc(data []byte) (uint64, error) {
	m := uint64(len(data) + HeaderSize)
	d.Lock()
	defer d.Unlock()
	for {
		size := d.size
		n := len(d.fs) - 1
		if o, err := d.fs[n].alloc(m); err != nil {
			fp, err := newFile(d.fileName(n + 1))
			if err != nil {
				return 0, err
			}
			d.size += d.fs[n].size
			d.fs = append(d.fs, fp)
		} else {
			return size + o, nil
		}
	}
}

func (d *data) fileName(idx int) string {
	return fmt.Sprintf("%s%c%v.DAT", d.dir, os.PathSeparator, idx)
}

func header(data []byte) []byte {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint16(buf, uint16(len(data)))
	return buf
}

func newFile(path string) (*file, error) {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, err
	}
	return &file{fp: fp, size: 0}, nil
}

func openFile(path string) (*file, error) {
	fp, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	st, err := fp.Stat()
	if err != nil {
		fp.Close()
		return nil, err
	}
	return &file{fp: fp, size: uint64(st.Size())}, nil
}
