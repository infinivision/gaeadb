package transaction

import (
	"bytes"
	"gaeadb/constant"
	"gaeadb/errmsg"
	"sort"
)

func (itr *backwardIterator) Close() error {
	return itr.itr.Close()
}

func (itr *backwardIterator) Next() error {
	if len(itr.ks) > 0 {
		itr.ks = itr.ks[1:]
		return nil
	}
	if err := itr.itr.Next(); err != nil {
		return err
	}
	key := string(itr.itr.Key())
	for k, _ := range itr.tx.wmp {
		if bytes.Compare([]byte(k), []byte(key)) > 0 {
			itr.ks = GtPush([]byte(k), itr.ks)
		}
	}
	if _, ok := itr.tx.wmp[key]; !ok {
		itr.tx.rmp[key] = itr.itr.Timestamp()
	}
	return nil
}

func (itr *backwardIterator) Valid() bool {
	if len(itr.ks) > 0 {
		return true
	}
	return itr.itr.Valid()
}

func (itr *backwardIterator) Key() []byte {
	if len(itr.ks) > 0 {
		return itr.ks[0]
	}
	return itr.itr.Key()
}

func (itr *backwardIterator) Value() ([]byte, error) {
	var k []byte

	switch {
	case len(itr.ks) > 0:
		k = itr.ks[0]
	default:
		k = itr.itr.Key()
	}
	if v, ok := itr.tx.wmp[string(k)]; ok {
		if v == nil {
			return nil, errmsg.NotExist
		}
		return v, nil
	}
	if v := itr.itr.Value(); v == constant.Delete {
		return nil, errmsg.NotExist
	} else {
		return itr.tx.d.Read(v)
	}
}

func GtPush(x []byte, xs [][]byte) [][]byte {
	i := sort.Search(len(xs), func(i int) bool { return bytes.Compare(xs[i], x) <= 0 })
	xs = append(xs, []byte{})
	copy(xs[i+1:], xs[i:])
	xs[i] = x
	return xs

}
