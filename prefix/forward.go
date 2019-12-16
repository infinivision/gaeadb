package prefix

import (
	"encoding/binary"

	"github.com/infinivision/gaeadb/cache"
	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/suffix"
)

func (itr *forwardIterator) Close() error {
	for {
		if itr.s.IsEmpty() {
			return nil
		}
		e := itr.s.Pop().(*forwardElement)
		switch e.typ {
		case R:
		case C:
		case E:
			e.rsrc.le.RUnlock()
			itr.t.c.Release(e.rsrc.pg)
		case P:
			itr.t.c.Release(e.rsrc.pg)
		}
	}
	return nil
}

func (itr *forwardIterator) Next() error {
	for i := 0; ; i = 1 {
		if itr.s.IsEmpty() {
			return nil
		}
		e := itr.s.Peek().(*forwardElement)
		switch e.typ {
		case C:
			if i == 0 {
				itr.s.Pop()
			} else {
				return nil
			}
		case S:
			if i == 0 {
				e.itr.Next()
			}
			if e.itr.Valid() {
				return nil
			}
			itr.s.Pop()
		case R:
			if e.cnt == 256 {
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
			if e.cnt == 256 {
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

func (itr *forwardIterator) Valid() bool {
	return !itr.s.IsEmpty()
}

func (itr *forwardIterator) Key() []byte {
	if itr.s.IsEmpty() {
		return nil
	}
	e := itr.s.Peek().(*forwardElement)
	switch e.typ {
	case C:
		return e.pref
	case S:
		return append(e.pref, e.itr.Key()...)
	}
	return nil
}

func (itr *forwardIterator) Value() uint64 {
	if itr.s.IsEmpty() {
		return 0
	}
	e := itr.s.Peek().(*forwardElement)
	switch e.typ {
	case C:
		return e.val
	case S:
		return e.itr.Value()
	}
	return 0
}

func (itr *forwardIterator) seek() error {
	if e := itr.s.Peek().(*forwardElement); e.typ != S && e.typ != C {
		return itr.Next()
	}
	return nil
}

func (itr *forwardIterator) down(k byte, par cache.Page, e *forwardElement) error {
	if e.typ == R {
		e.cnt++
		root, err := itr.t.c.Get(RootPage)
		if err != nil {
			return err
		}
		le := itr.t.t.Get(uint64(RootPage) | uint64(k)<<constant.TypeOff)
		le.RLock()
		pn, _ := branch(k, root.Buffer())
		pg, err := itr.t.c.Get(pn)
		if err != nil {
			defer itr.t.c.Release(root)
			defer le.RUnlock()
			return err
		}
		itr.s.Push(&forwardElement{
			typ:  E,
			rsrc: &resource{le: le, pg: root},
		})
		itr.s.Push(&forwardElement{
			typ:  P,
			cnt:  0,
			pref: []byte{k},
			rsrc: &resource{pg: pg},
		})
		if v := binary.LittleEndian.Uint64(root.Buffer()[2048+int(k)*8:]); v != constant.Cancel {
			itr.s.Push(&forwardElement{
				typ:  C,
				val:  v,
				pref: []byte{k},
			})
		}
		return nil
	}
	pn, typ := branch(k, par.Buffer())
	pg, err := itr.t.c.Get(pn)
	if err != nil {
		return err
	}
	switch typ {
	case constant.PN:
		e.cnt++
		itr.s.Push(&forwardElement{
			typ:  P,
			cnt:  0,
			pref: append(e.pref, k),
			rsrc: &resource{pg: pg},
		})
		if v := binary.LittleEndian.Uint64(par.Buffer()[2048+int(k)*8:]); v != constant.Cancel {
			itr.s.Push(&forwardElement{
				typ:  C,
				val:  v,
				pref: append(e.pref, k),
			})
		}
	case constant.MS:
		var vs []uint64
		var ks [][]byte

		e.cnt = int(pg.Buffer()[3]) + 1
		for i, j := e.cnt-1, int(k); i >= j; i-- {
			if v := binary.LittleEndian.Uint64(par.Buffer()[2048+i*8:]); v != constant.Cancel {
				vs = append(vs, v)
				ks = append(ks, append(e.pref, byte(i)))
			}
		}
		if fItr := suffix.NewForwardIterator(ks, vs, nil, pg); fItr.Valid() {
			itr.s.Push(&forwardElement{
				typ:  S,
				itr:  fItr,
				pref: e.pref,
			})
		}
		e.cnt = int(pg.Buffer()[3]) + 1
		itr.t.c.Release(pg)
	case constant.SN:
		e.cnt++
		if fItr := suffix.NewForwardIterator(nil, nil, nil, pg); fItr.Valid() {
			itr.s.Push(&forwardElement{
				typ:  S,
				itr:  fItr,
				pref: append(e.pref, k),
			})
		}
		itr.t.c.Release(pg)
		if v := binary.LittleEndian.Uint64(par.Buffer()[2048+int(k)*8:]); v != constant.Cancel {
			itr.s.Push(&forwardElement{
				typ:  C,
				val:  v,
				pref: append(e.pref, k),
			})
		}
	default:
		e.cnt++
		itr.t.c.Release(pg)
		if v := binary.LittleEndian.Uint64(par.Buffer()[2048+int(k)*8:]); v != constant.Cancel {
			itr.s.Push(&forwardElement{
				typ:  C,
				val:  v,
				pref: append(e.pref, k),
			})
		}
	}
	return nil
}
