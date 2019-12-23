package prefix

import (
	"encoding/binary"

	"github.com/infinivision/gaeadb/cache"
	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/suffix"
)

func (itr *backwardIterator) Close() error {
	for {
		if itr.s.IsEmpty() {
			return nil
		}
		e := itr.s.Pop().(*backwardElement)
		switch e.typ {
		case R:
		case E:
			e.rsrc.le.RUnlock()
			itr.t.c.Release(e.rsrc.pg)
		case P:
			itr.t.c.Release(e.rsrc.pg)
		}
	}
	return nil
}

func (itr *backwardIterator) Next() error {
	for i := 0; ; i = 1 {
		if itr.s.IsEmpty() {
			return nil
		}
		e := itr.s.Peek().(*backwardElement)
		switch e.typ {
		case C:
			if i == 0 {
				itr.s.Pop()
			} else {
				itr.v = e.val
				itr.k = e.pref
				return nil
			}
		case S:
			if i == 0 {
				e.itr.Next()
			}
			if e.itr.Valid() {
				itr.v = e.itr.Value()
				itr.k = append(e.pref, e.itr.Key()...)
				return nil
			}
			itr.s.Pop()
		case R:
			if e.cnt == -1 {
				itr.s.Pop()
				continue
			}
			if err := itr.down(byte(e.cnt), nil, e); err != nil {
				itr.Close()
				return err
			}
		case E:
			itr.s.Pop()
			e.rsrc.le.RUnlock()
			itr.t.c.Release(e.rsrc.pg)
		case P:
			if e.cnt == -1 {
				itr.s.Pop()
				itr.t.c.Release(e.rsrc.pg)
				continue
			}
			if err := itr.down(byte(e.cnt), e.rsrc.pg, e); err != nil {
				itr.Close()
				return err
			}
		}
	}
}

func (itr *backwardIterator) Valid() bool {
	return !itr.s.IsEmpty()
}

func (itr *backwardIterator) Key() []byte {
	if itr.s.IsEmpty() {
		return nil
	}
	return itr.k
}

func (itr *backwardIterator) Value() uint64 {
	if itr.s.IsEmpty() {
		return 0
	}
	return itr.v
}

func (itr *backwardIterator) seek() error {
	e := itr.s.Peek().(*backwardElement)
	switch e.typ {
	case S:
		itr.v = e.itr.Value()
		itr.k = append(e.pref, e.itr.Key()...)
		return nil
	case C:
		itr.v = e.val
		itr.k = e.pref
		return nil
	default:
		return itr.Next()
	}
}

func (itr *backwardIterator) down(k byte, par cache.Page, e *backwardElement) error {
	if e.typ == R {
		e.cnt--
		root, err := itr.t.c.Get(constant.RootPage)
		if err != nil {
			return err
		}
		le := itr.t.t.Get(uint64(constant.RootPage) | uint64(k)<<constant.TypeOff)
		le.RLock()
		pn, _ := branch(k, root.Buffer())
		pg, err := itr.t.c.Get(pn)
		if err != nil {
			defer itr.t.c.Release(root)
			defer le.RUnlock()
			return err
		}
		itr.s.Push(&backwardElement{
			typ:  E,
			rsrc: &resource{le: le, pg: root},
		})
		if v := binary.LittleEndian.Uint64(root.Buffer()[2048+int(k)*8:]); v != constant.Cancel {
			itr.s.Push(&backwardElement{
				typ:  C,
				val:  v,
				pref: []byte{k},
			})
		}
		itr.s.Push(&backwardElement{
			typ:  P,
			cnt:  255,
			pref: []byte{k},
			rsrc: &resource{pg: pg},
		})
		return nil
	}
	pn, typ := branch(k, par.Buffer())
	pg, err := itr.t.c.Get(pn)
	if err != nil {
		return err
	}
	switch typ {
	case constant.PN:
		e.cnt--
		if v := binary.LittleEndian.Uint64(par.Buffer()[2048+int(k)*8:]); v != constant.Cancel {
			itr.s.Push(&backwardElement{
				typ:  C,
				val:  v,
				pref: append(e.pref, k),
			})
		}
		itr.s.Push(&backwardElement{
			typ:  P,
			cnt:  255,
			pref: append(e.pref, k),
			rsrc: &resource{pg: pg},
		})
	case constant.MS:
		var vs []uint64
		var ks [][]byte

		e.cnt = int(pg.Buffer()[2]) - 1
		for i, j := e.cnt+1, int(k); i <= j; i++ {
			if v := binary.LittleEndian.Uint64(par.Buffer()[2048+i*8:]); v != constant.Cancel {
				vs = append(vs, v)
				ks = append(ks, append(e.pref, byte(i)))
			}
		}
		if bItr := suffix.NewBackwardIterator(ks, vs, nil, pg); bItr.Valid() {
			itr.s.Push(&backwardElement{
				typ:  S,
				itr:  bItr,
				pref: e.pref,
			})
		}
		itr.t.c.Release(pg)
	case constant.SN:
		e.cnt--
		if v := binary.LittleEndian.Uint64(par.Buffer()[2048+int(k)*8:]); v != constant.Cancel {
			itr.s.Push(&backwardElement{
				typ:  C,
				val:  v,
				pref: append(e.pref, k),
			})
		}
		if bItr := suffix.NewBackwardIterator(nil, nil, nil, pg); bItr.Valid() {
			itr.s.Push(&backwardElement{
				typ:  S,
				itr:  bItr,
				pref: append(e.pref, k),
			})
		}
		itr.t.c.Release(pg)
	default:
		e.cnt--
		itr.t.c.Release(pg)
		if v := binary.LittleEndian.Uint64(par.Buffer()[2048+int(k)*8:]); v != constant.Cancel {
			itr.s.Push(&backwardElement{
				typ:  C,
				val:  v,
				pref: append(e.pref, k),
			})
		}
	}
	return nil
}
