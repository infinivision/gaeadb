package suffix

import "bytes"

func (itr *backwardIterator) Next() {
	for len(itr.es) > 0 {
		itr.es = itr.es[:len(itr.es)-1]
		if len(itr.es) == 0 {
			return
		}
		if bytes.HasPrefix(itr.es[len(itr.es)-1].suff, itr.prefix) {
			return
		}
	}
}

func (itr *backwardIterator) Valid() bool {
	return len(itr.es) != 0
}

func (itr *backwardIterator) Key() []byte {
	return itr.es[len(itr.es)-1].suff
}

func (itr *backwardIterator) Value() uint64 {
	return itr.es[len(itr.es)-1].off
}
