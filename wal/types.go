package wal

import "sync"

const (
	EM byte = iota // empty entry
	EC             // end check point
	SC             // start check point
	AT             // abort transaction
	CT             // commmit transaction
	ST             // start transaction
	WD             // write data
	NP             // new prefix
	CP             // change prefix
	NS             // new suffix
)

const (
	SumSize    = 4
	RecordSize = 4
	HeaderSize = SumSize + RecordSize
)

type Writer interface {
	Close() error
	EndCKPT() error
	StartCKPT() error
	Append([]byte) error
}

type endCKPT struct {
}

type startCKPT struct {
	ts []uint64
}

type endTransaction struct {
	ts uint64
}

type startTransaction struct {
	ts uint64
	mp map[string][]byte
}

type writeData struct {
	ts uint64
	os []uint64
}

// ts.pn[off] = val, ts.pn[os] = vs
type newPrefix struct {
	ts  uint64
	pn  uint64
	val uint64
	off uint16
	os  []uint16
	vs  []uint64
}

// ts.pn[start, end] = val
type newSuffix struct {
	end   byte
	start byte
	ts    uint64
	pn    uint64
	val   uint64
}

// ts.pn[os] = vs
type chgPrefix struct {
	ts uint64
	pn uint64
	os []uint16
	vs []uint64
}

type record struct {
	rc interface{}
}

type file struct {
	cnt  int32 // reference count
	size int32 // file size
	buf  []byte
}

type walWriter struct {
	sync.RWMutex
	n    int // index of truncate
	m    int // index of check point
	idx  int
	flag int
	fp   *file
	dir  string
}

type recoverWriter struct {
}

func (r *recoverWriter) NewSuffix(_, _ byte, _, _ uint64) error {
	return nil
}

func (r *recoverWriter) ChgPrefix(_ uint64, _ []uint16, _ []uint64) error {
	return nil
}

func (r *recoverWriter) NewPrefix(_, _ uint64, _ uint16, _ []uint16, _ []uint64) error {
	return nil
}
