package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"

	"github.com/infinivision/gaeadb/cache"
	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/data"
	"github.com/infinivision/gaeadb/mvcc"
	"github.com/infinivision/gaeadb/sum"
	"golang.org/x/sys/unix"
)

func Recover(dir string, d data.Data, m mvcc.MVCC, c cache.Cache) (uint64, error) {
	h, l, err := headAndLast(dir)
	if err != nil {
		return 0, err
	}
	if h < 0 {
		return 0, nil
	}
	ok, last, err := findCKPT(dir, h, l)
	if err != nil {
		return 0, err
	}
	switch {
	case !ok:
		return recoverFromStart(dir, h, l, d, m, c)
	default:
		return recoverFromCKPT(dir, h, last, d, m, c)
	}
	return 0, nil
}

func recoverFromCKPT(dir string, head, last int, d data.Data, m mvcc.MVCC, c cache.Cache) (uint64, error) {
	rs, err := loads(head, last, dir)
	if err != nil {
		return 0, err
	}
	ts, mp, mr, mq, rs := getTimestamp(rs, true)
	for i, j := 0, len(rs); i < j; i++ {
		switch r := rs[i].rc.(type) {
		case startTransaction:
			if _, ok := mr[r.ts]; ok {
				break
			}
			if _, ok := mp[r.ts]; ok { // redo
				os := mq[r.ts].os
				for k, v := range r.mp {
					switch {
					case v == nil:
						if err := m.Del([]byte(k), r.ts, &recoverWriter{}); err != nil {
							return 0, err
						}
					default:
						if err := d.Write(os[0], v); err != nil {
							return 0, err
						}
						if err := m.Set([]byte(k), os[0], r.ts, &recoverWriter{}); err != nil {
							return 0, err
						}
						os = os[1:]
					}
				}
			} else { // undo
				if wd, ok := mq[r.ts]; ok {
					for _, o := range wd.os {
						if err := d.Del(o); err != nil {
							return 0, err
						}
					}
				}
				for k, _ := range r.mp {
					if err := m.Set([]byte(k), constant.Cancel, r.ts, &recoverWriter{}); err != nil {
						return 0, err
					}
				}
			}
		}
	}
	c.Flush()
	return ts, nil
}

func recoverFromStart(dir string, head, last int, d data.Data, m mvcc.MVCC, c cache.Cache) (uint64, error) {
	rs, err := loads(head, last, dir)
	if err != nil {
		return 0, err
	}
	ts, mp, _, mq, rs := getTimestamp(rs, false)
	for i, j := 0, len(rs); i < j; i++ {
		switch r := rs[i].rc.(type) {
		case startTransaction:
			if _, ok := mp[r.ts]; ok { // redo
				os := mq[r.ts].os
				for k, v := range r.mp {
					switch {
					case v == nil:
						if err := m.Del([]byte(k), r.ts, &recoverWriter{}); err != nil {
							return 0, err
						}
					default:
						if err := d.Write(os[0], v); err != nil {
							return 0, err
						}
						if err := m.Set([]byte(k), os[0], r.ts, &recoverWriter{}); err != nil {
							return 0, err
						}
						os = os[1:]
					}
				}
			} else { // undo
				if wd, ok := mq[r.ts]; ok {
					for _, o := range wd.os {
						if err := d.Del(o); err != nil {
							return 0, err
						}
					}
				}
				for k, _ := range r.mp {
					if err := m.Set([]byte(k), constant.Cancel, r.ts, &recoverWriter{}); err != nil {
						return 0, err
					}
				}
			}
		}
	}
	c.Flush()
	return ts, nil
}

func headAndLast(dir string) (int, int, error) {
	h := -1
	for i := 0; ; i++ {
		st, err := os.Stat(fileName(i, dir))
		switch {
		case err == nil:
			if h == -1 && st.Size() != 0 {
				h = i
			}
		case os.IsNotExist(err):
			return h, i - 1, nil
		default:
			return -1, -1, err
		}
	}
}

func findCKPT(dir string, head, last int) (bool, int, error) {
	for last >= head {
		rs, err := load(fileName(last, dir))
		if err != nil {
			return false, -1, err
		}
		for i := len(rs) - 1; i >= 0; i-- {
			switch rs[i].rc.(type) {
			case endCKPT:
				return true, last, nil
			}
		}
		last--
	}
	return false, head, nil
}

func loads(head, last int, dir string) ([]*record, error) {
	var rs []*record

	for head <= last {
		s, err := load(fileName(head, dir))
		if err != nil {
			return nil, err
		}
		rs = append(rs, s...)
		head++
	}
	return rs, nil
}

func load(path string) ([]*record, error) {
	var rs []*record

	fp, err := openFile(path, unix.O_RDWR)
	if err != nil {
		return nil, err
	}
	defer fp.close()
	buf := append([]byte{}, fp.buf...)
	for len(buf) > HeaderSize {
		n := int(binary.LittleEndian.Uint32(buf[SumSize:]))
		if len(buf[HeaderSize:]) < n {
			return rs, nil
		}
		if sum.Sum(crc32.New(crc32.MakeTable(crc32.Castagnoli)), buf[HeaderSize:HeaderSize+n]) != binary.LittleEndian.Uint32(buf) {
			return rs, nil
		}
		buf = buf[HeaderSize:]
		switch buf[0] {
		case EM:
			return rs, nil
		case EC:
			rs = append(rs, &record{endCKPT{}})
			buf = buf[1:]
		case SC:
			if len(buf[1:]) < 4 { // incomplete record
				return rs, nil
			}
			n := int(binary.LittleEndian.Uint32(buf[1:]))
			if len(buf[5:]) < n*8 { // incomplete record
				return rs, nil
			}
			o := 5
			sc := startCKPT{}
			for i := 0; i < n; i++ {
				sc.ts = append(sc.ts, binary.LittleEndian.Uint64(buf[o:]))
				o += 8
			}
			rs = append(rs, &record{sc})
			buf = buf[o:]
		case AT:
			if len(buf[1:]) < 8 { // incomplete record
				return rs, nil
			}
			rs = append(rs, &record{endTransaction{binary.LittleEndian.Uint64(buf[1:])}})
			buf = buf[9:]
		case CT:
			if len(buf[1:]) < 8 { // incomplete record
				return rs, nil
			}
			rs = append(rs, &record{endTransaction{binary.LittleEndian.Uint64(buf[1:])}})
			buf = buf[9:]
		case ST:
			if len(buf[1:]) < 8 { // incomplete record
				return rs, nil
			}
			st := startTransaction{}
			st.mp = make(map[string][]byte)
			st.ts = binary.LittleEndian.Uint64(buf[1:])
			n := int(binary.LittleEndian.Uint32(buf[9:]))
			o := 13
			for i := 0; i < n; i++ {
				if len(buf[o:]) < 2 {
					return rs, nil
				}
				kn := int(binary.LittleEndian.Uint16(buf[o:]))
				o += 2
				if len(buf[o:]) < kn {
					return rs, nil
				}
				k := buf[o : o+kn]
				o += kn
				if len(buf[o:]) < 2 {
					return rs, nil
				}
				vn := int(binary.LittleEndian.Uint16(buf[o:]))
				o += 2
				if len(buf[o:]) < vn {
					return rs, nil
				}
				if vn > 0 {
					v := buf[o : o+vn]
					st.mp[string(k)] = v
				} else {
					st.mp[string(k)] = nil
				}
				o += vn
			}
			rs = append(rs, &record{st})
			buf = buf[o:]
		case WD:
			if len(buf[1:]) < 8 { // incomplete record
				return rs, nil
			}
			if len(buf[9:]) < 4 { // incomplete record
				return rs, nil
			}
			n := int(binary.LittleEndian.Uint32(buf[9:]))
			if len(buf[13:]) < n*8 { // incomplete record
				return rs, nil
			}
			wd := writeData{}
			wd.ts = binary.LittleEndian.Uint64(buf[1:])
			o := 13
			for i := 0; i < n; i++ {
				wd.os = append(wd.os, binary.LittleEndian.Uint64(buf[o:]))
				o += 8
			}
			rs = append(rs, &record{wd})
			buf = buf[o:]
		case NP:
			if len(buf[1:]) < 30 { // incomplete record
				return rs, nil
			}
			on := int(binary.LittleEndian.Uint16(buf[27:]))
			vn := int(binary.LittleEndian.Uint16(buf[29:]))
			if len(buf[31:]) < on*2+vn*8 { // incomplete record
				return rs, nil
			}
			np := newPrefix{}
			np.ts = binary.LittleEndian.Uint64(buf[1:])
			np.pn = binary.LittleEndian.Uint64(buf[9:])
			np.val = binary.LittleEndian.Uint64(buf[17:])
			np.off = binary.LittleEndian.Uint16(buf[25:])
			o := 31
			for i := 0; i < on; i++ {
				np.os = append(np.os, binary.LittleEndian.Uint16(buf[o:]))
				o += 2
			}
			for i := 0; i < vn; i++ {
				np.vs = append(np.vs, binary.LittleEndian.Uint64(buf[o:]))
				o += 8
			}
			rs = append(rs, &record{np})
			buf = buf[o:]
		case CP:
			if len(buf[1:]) < 20 { // incomplete record
				return rs, nil
			}
			on := int(binary.LittleEndian.Uint16(buf[17:]))
			vn := int(binary.LittleEndian.Uint16(buf[19:]))
			if len(buf[21:]) < on*2+vn*8 { // incomplete record
				return rs, nil
			}
			cp := chgPrefix{}
			cp.ts = binary.LittleEndian.Uint64(buf[3:])
			cp.pn = binary.LittleEndian.Uint64(buf[11:])
			o := 21
			for i := 0; i < on; i++ {
				cp.os = append(cp.os, binary.LittleEndian.Uint16(buf[o:]))
				o += 2
			}
			for i := 0; i < vn; i++ {
				cp.vs = append(cp.vs, binary.LittleEndian.Uint64(buf[o:]))
				o += 8
			}
			rs = append(rs, &record{cp})
			buf = buf[o:]
		case NS:
			if len(buf[1:]) < 26 { // incomplete record
				return rs, nil
			}
			ns := newSuffix{}
			ns.start, ns.end = buf[1], buf[2]
			ns.ts = binary.LittleEndian.Uint64(buf[3:])
			ns.pn = binary.LittleEndian.Uint64(buf[11:])
			ns.val = binary.LittleEndian.Uint64(buf[19:])
			rs = append(rs, &record{ns})
			buf = buf[27:]
		}
	}
	return rs, nil
}

func recoverMetadata(rs []*record, c cache.Cache) error {
	for i, j := 0, len(rs); i < j; i++ {
		switch r := rs[i].rc.(type) {
		case newPrefix:
			par, err := c.Get(int64(r.pn))
			if err != nil {
				return err
			}
			pg, err := c.Get(int64(r.val))
			if err != nil {
				c.Release(par)
				return err
			}
			defer c.Release(pg)
			defer c.Release(par)
			binary.LittleEndian.PutUint64(par.Buffer()[r.off:], r.val|constant.PN<<constant.TypeOff)
			for i, j := 0, len(r.os); i < j; i++ {
				binary.LittleEndian.PutUint64(pg.Buffer()[r.os[i]*8:], r.vs[i])
			}
			pg.Sync()
			par.Sync()
		case newSuffix:
			pg, err := c.Get(int64(r.pn))
			if err != nil {
				return err
			}
			defer c.Release(pg)
			for i := int(r.start); i <= int(r.end); i++ {
				binary.LittleEndian.PutUint64(pg.Buffer()[i*8:], r.val)
			}
			pg.Sync()
		case chgPrefix:
			pg, err := c.Get(int64(r.pn))
			if err != nil {
				return err
			}
			defer c.Release(pg)
			for i, j := 0, len(r.os); i < j; i++ {
				binary.LittleEndian.PutUint64(pg.Buffer()[r.os[i]*8:], r.vs[i])
			}
			pg.Sync()
		}
	}
	return nil
}

func getTimestamp(rs []*record, isCKPT bool) (uint64, map[uint64]struct{}, map[uint64]struct{}, map[uint64]*writeData, []*record) {
	ts := uint64(0)
	mp := make(map[uint64]struct{})
	mq := make(map[uint64]*writeData)
	for i := len(rs) - 1; i >= 0; i-- {
		switch r := rs[i].rc.(type) {
		case writeData:
			mq[r.ts] = &r
		case startCKPT:
			if isCKPT {
				mr := make(map[uint64]struct{})
				for _, t := range r.ts {
					mr[t] = struct{}{}
				}
				return ts, mp, mr, mq, rs[i+1:]
			}
		case endTransaction:
			if r.ts > ts {
				ts = r.ts
			}
			mp[r.ts] = struct{}{}
		}
	}
	return ts, mp, nil, mq, rs
}
