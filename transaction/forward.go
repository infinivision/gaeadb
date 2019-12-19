package transaction

import (
	"bytes"
	"sort"

	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/errmsg"
)

func (itr *forwardIterator) Close() error {
	return itr.itr.Close()
}

func (itr *forwardIterator) Next() error {
	if len(itr.ks) > 0 {
		itr.ks = itr.ks[1:]
		return nil
	}
	if err := itr.itr.Next(); err != nil {
		return err
	}
	if !itr.tx.ro {
		key := string(itr.itr.Key())
		for k, _ := range itr.tx.wmp {
			if bytes.Compare([]byte(k), []byte(key)) < 0 {
				itr.ks = LtPush([]byte(k), itr.ks)
			}
		}
		if _, ok := itr.tx.wmp[key]; !ok {
			itr.tx.rmp[key] = itr.itr.Timestamp()
		}
	}
	return nil
}

func (itr *forwardIterator) Valid() bool {
	if len(itr.ks) > 0 {
		return true
	}
	return itr.itr.Valid()
}

func (itr *forwardIterator) Key() []byte {
	if len(itr.ks) > 0 {
		return itr.ks[0]
	}
	return itr.itr.Key()
}

func (itr *forwardIterator) Value() ([]byte, error) {
	var k []byte

	switch {
	case len(itr.ks) > 0:
		k = itr.ks[0]
	default:
		k = itr.itr.Key()
	}
	if !itr.tx.ro {
		if v, ok := itr.tx.wmp[string(k)]; ok {
			if v == nil {
				return nil, errmsg.NotExist
			}
			return v, nil
		}
	}
	switch v := itr.itr.Value(); v {
	case constant.Empty:
		return []byte{}, nil
	case constant.Delete:
		return nil, errmsg.NotExist
	default:
		return itr.tx.d.Read(v)
	}
}

func LtPush(x []byte, xs [][]byte) [][]byte {
	i := sort.Search(len(xs), func(i int) bool { return bytes.Compare(xs[i], x) >= 0 })
	xs = append(xs, []byte{})
	copy(xs[i+1:], xs[i:])
	xs[i] = x
	return xs
}
