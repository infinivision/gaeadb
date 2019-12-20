package transaction

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/errmsg"
)

func (itr *forwardIterator) Close() error {
	return itr.itr.Close()
}

func (itr *forwardIterator) Next() error {
	delete(itr.kv.mp, string(itr.kv.ks[0]))
	delete(itr.kv.omp, string(itr.kv.ks[0]))
	if itr.kv.ks = itr.kv.ks[1:]; len(itr.kv.ks) == 0 {
		err := itr.itr.Next()
		if err != nil {
			return err
		}
		return itr.seek()
	}
	return nil
}

func (itr *forwardIterator) Valid() bool {
	if len(itr.kv.ks) > 0 {
		return true
	}
	return false
}

func (itr *forwardIterator) Key() []byte {
	return itr.kv.ks[0]
}

func (itr *forwardIterator) Value() ([]byte, error) {
	k := string(itr.kv.ks[0])
	switch o := itr.kv.omp[k]; o {
	case constant.Empty:
		return []byte{}, nil
	case constant.Delete:
		return nil, errmsg.NotExist
	case constant.Cache:
		if v, ok := itr.tx.wmp[k]; ok {
			if v == nil {
				return nil, errmsg.NotExist
			}
			return v, nil
		}
	default:
		if v, ok := itr.kv.mp[k]; ok {
			return v, nil
		} else {
			return nil, errmsg.ReadFailed
		}
	}
	return itr.kv.mp[string(itr.kv.ks[0])], nil
}

func (itr *forwardIterator) seek() error {
	for itr.itr.Valid() {
		key := string(itr.itr.Key())
		if !itr.tx.ro {
			for k, _ := range itr.tx.wmp {
				if bytes.Compare([]byte(k), []byte(key)) < 0 {
					itr.kv.omp[k] = constant.Cache
					itr.kv.ks = LtPush([]byte(k), itr.kv.ks)
				}
			}
			if _, ok := itr.tx.wmp[key]; !ok {
				itr.tx.rmp[key] = itr.itr.Timestamp()
			}
		}
		itr.kv.omp[key] = itr.itr.Value()
		itr.kv.ks = append(itr.kv.ks, []byte(key))
		if len(itr.kv.ks) > constant.PreLoad {
			itr.fill()
			return nil
		}
		err := itr.itr.Next()
		switch {
		case err == errmsg.ScanEnd:
			itr.fill()
			return nil
		case err != nil:
			return err
		}
	}
	return errmsg.ScanEnd
}

func (itr *forwardIterator) fill() {
	min, max := itr.kv.omp[string(itr.kv.ks[0])], itr.kv.omp[string(itr.kv.ks[0])]
	for _, k := range itr.kv.ks {
		if o := itr.kv.omp[string(k)]; o > constant.Cache {
			switch {
			case o < min:
				min = o
			case o > max:
				max = o
			}
		}
	}
	buf, err := itr.tx.d.Load(min, int(max-min)+32) // preload
	switch {
	case err == nil:
		for _, k := range itr.kv.ks {
			if o := itr.kv.omp[string(k)]; o > constant.Cache {
				if int(o-min)+2 < len(buf) {
					n := int(binary.LittleEndian.Uint16(buf[o-min:]))
					if len(buf[o-min+2:]) >= n {
						itr.kv.mp[string(k)] = buf[int(o-min)+2 : int(o-min)+2+n]
						continue
					}
				}
				if v, err := itr.tx.d.Read(o); err == nil {
					itr.kv.mp[string(k)] = v
				}
			}
		}
	case err != nil:
		itr.tx.log.Errorf("forwardIterator -  failed to preLoad: %v\n", err)
		for _, k := range itr.kv.ks {
			if o := itr.kv.omp[string(k)]; o > constant.Cache {
				if v, err := itr.tx.d.Read(o); err == nil {
					itr.kv.mp[string(k)] = v
				}
			}
		}
	}
}

func LtPush(x []byte, xs [][]byte) [][]byte {
	i := sort.Search(len(xs), func(i int) bool { return bytes.Compare(xs[i], x) >= 0 })
	xs = append(xs, []byte{})
	copy(xs[i+1:], xs[i:])
	xs[i] = x
	return xs
}
