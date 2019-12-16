package suffix

import (
	"bytes"
)

func (itr *forwardIterator) Next() {
	for len(itr.es) > 0 {
		itr.es = itr.es[1:]
		if len(itr.es) == 0 {
			return
		}
		if bytes.HasPrefix(itr.es[0].suff, itr.prefix) {
			return
		}
	}
}

func (itr *forwardIterator) Valid() bool {
	return len(itr.es) != 0
}

func (itr *forwardIterator) Key() []byte {
	return itr.es[0].suff
}

func (itr *forwardIterator) Value() uint64 {
	return itr.es[0].off
}
