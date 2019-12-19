package prefix

import (
	"encoding/binary"

	"github.com/infinivision/gaeadb/cache"
	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/errmsg"
	"github.com/infinivision/gaeadb/locker"
	"github.com/infinivision/gaeadb/stack"
	"github.com/infinivision/gaeadb/suffix"
)

func New(c cache.Cache, t locker.Table) *tree {
	go t.Run()
	go c.Run()
	return &tree{c, t}
}

func (t *tree) Close() error {
	t.t.Stop()
	t.c.Stop()
	return nil
}

func (t *tree) Del(k []byte, w suffix.Writer) error {
	return t.Set(k, constant.Cancel, w)
}

func (t *tree) Get(k []byte) (uint64, error) {
	typ, pn, le, k, pg, err := t.down(k, false)
	if err != nil {
		return 0, err
	}
	defer t.c.Release(pg)
	defer le.RUnlock()
	switch typ {
	case constant.PN:
		if v := binary.LittleEndian.Uint64(pg.Buffer()[2048+int(k[0])*8:]); v == constant.Cancel {
			return 0, errmsg.NotExist
		} else {
			return v, nil
		}
	case constant.ES:
		return 0, nil
	case constant.MS:
		return t.find(k, pn)
	case constant.SN:
		return t.find(k[1:], pn)
	}
	return 0, nil
}

func (t *tree) Set(k []byte, v uint64, w suffix.Writer) error {
	typ, pn, le, k, pg, err := t.down(k, true)
	if err != nil {
		return err
	}
	defer t.c.Release(pg)
	defer le.Unlock()
	switch typ {
	case constant.PN:
		defer pg.Sync()
		binary.LittleEndian.PutUint64(pg.Buffer()[2048+int(k[0])*8:], v)
		return nil
	case constant.MS:
		return t.insert(k, v, w, pn, pg)
	case constant.SN:
		return t.insert(k[1:], v, w, pn, pg)
	case constant.ES:
		return t.insertByNewSuffix(k, v, w, pg)
	}
	return errmsg.UnknownError
}

func (t *tree) NewForwardIterator(pref []byte) (Iterator, error) {
	s := stack.New()
	switch {
	case len(pref) == 0:
		s.Push(&forwardElement{
			typ:  R,
			cnt:  0,
			pref: pref,
		})
	default:
		typ, pn, le, suff, pg, err := t.down(pref, false)
		if err != nil {
			return nil, err
		}
		if err := t.newForwardElement(s, typ, pn, &resource{pg, le}, pref[:len(pref)-len(suff)], suff); err != nil {
			return nil, err
		}
	}
	itr := &forwardIterator{t: t, s: s}
	if err := itr.seek(); err != nil {
		itr.Close()
		return nil, err
	}
	return itr, nil
}

func (t *tree) NewBackwardIterator(pref []byte) (Iterator, error) {
	s := stack.New()
	switch {
	case len(pref) == 0:
		s.Push(&backwardElement{
			typ:  R,
			cnt:  255,
			pref: pref,
		})
	default:
		typ, pn, le, suff, pg, err := t.down(pref, false)
		if err != nil {
			return nil, err
		}
		if err := t.newBackwardElement(s, typ, pn, &resource{pg, le}, pref[:len(pref)-len(suff)], suff); err != nil {
			return nil, err
		}
	}
	itr := &backwardIterator{t: t, s: s}
	if err := itr.seek(); err != nil {
		itr.Close()
		return nil, err
	}
	return itr, nil
}

func (t *tree) newForwardElement(s stack.Stack, typ int, pn int64, rsrc *resource, pref, suff []byte) error {
	for {
		switch typ {
		case constant.ES:
			s.Push(&forwardElement{
				typ:  E,
				rsrc: rsrc,
			})
			if len(suff) == 1 && binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]) != constant.Cancel {
				s.Push(&forwardElement{
					typ:  C,
					pref: append(pref, suff[0]),
					val:  binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]),
				})
			}
			return nil
		case constant.MS:
			pg, err := t.c.Get(pn)
			if err != nil {
				defer t.c.Release(rsrc.pg)
				defer rsrc.le.RUnlock()
				return err
			}
			defer t.c.Release(pg)
			s.Push(&forwardElement{
				typ:  E,
				rsrc: rsrc,
			})
			if fItr := suffix.NewForwardIterator(nil, nil, suff, pg); fItr.Valid() {
				s.Push(&forwardElement{
					typ:  S,
					itr:  fItr,
					pref: pref,
				})
			}
			if len(suff) == 1 && binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]) != constant.Cancel {
				s.Push(&forwardElement{
					typ:  C,
					pref: append(pref, suff[0]),
					val:  binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]),
				})
			}
			return nil
		case constant.SN:
			pg, err := t.c.Get(pn)
			if err != nil {
				defer t.c.Release(rsrc.pg)
				defer rsrc.le.RUnlock()
				return err
			}
			defer t.c.Release(pg)
			s.Push(&forwardElement{
				typ:  E,
				rsrc: rsrc,
			})
			if fItr := suffix.NewForwardIterator(nil, nil, suff[1:], pg); fItr.Valid() {
				s.Push(&forwardElement{
					typ:  S,
					itr:  fItr,
					pref: append(pref, suff[0]),
				})
			}
			if len(suff) == 1 && binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]) != constant.Cancel {
				s.Push(&forwardElement{
					typ:  C,
					pref: append(pref, suff[0]),
					val:  binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]),
				})
			}
			return nil
		case constant.PN:
			pn, typ = branch(suff[0], rsrc.pg.Buffer())
			if typ == constant.PN {
				pg, err := t.c.Get(pn)
				if err != nil {
					defer t.c.Release(rsrc.pg)
					defer rsrc.le.RUnlock()
					return err
				}
				s.Push(&forwardElement{
					typ:  E,
					rsrc: rsrc,
				})
				s.Push(&forwardElement{
					typ:  P,
					cnt:  0,
					rsrc: &resource{pg: pg},
					pref: append(pref, suff[0]),
				})
				if binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]) != constant.Cancel {
					s.Push(&forwardElement{
						typ:  C,
						pref: append(pref, suff[0]),
						val:  binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]),
					})
				}
				return nil
			}
		}
	}
	return errmsg.UnknownError
}

func (t *tree) newBackwardElement(s stack.Stack, typ int, pn int64, rsrc *resource, pref, suff []byte) error {
	for {
		switch typ {
		case constant.ES:
			s.Push(&backwardElement{
				typ:  E,
				rsrc: rsrc,
			})
			if len(suff) == 1 && binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]) != constant.Cancel {
				s.Push(&backwardElement{
					typ:  C,
					pref: append(pref, suff[0]),
					val:  binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]),
				})
			}
			return nil
		case constant.MS:
			pg, err := t.c.Get(pn)
			if err != nil {
				defer t.c.Release(rsrc.pg)
				defer rsrc.le.RUnlock()
				return err
			}
			defer t.c.Release(pg)
			s.Push(&backwardElement{
				typ:  E,
				rsrc: rsrc,
			})
			if len(suff) == 1 && binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]) != constant.Cancel {
				s.Push(&backwardElement{
					typ:  C,
					pref: append(pref, suff[0]),
					val:  binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]),
				})
			}
			if bItr := suffix.NewBackwardIterator(nil, nil, suff, pg); bItr.Valid() {
				s.Push(&backwardElement{
					typ:  S,
					itr:  bItr,
					pref: pref,
				})
			}
			return nil
		case constant.SN:
			pg, err := t.c.Get(pn)
			if err != nil {
				defer t.c.Release(rsrc.pg)
				defer rsrc.le.RUnlock()
				return err
			}
			defer t.c.Release(pg)
			s.Push(&backwardElement{
				typ:  E,
				rsrc: rsrc,
			})
			if len(suff) == 1 && binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]) != constant.Cancel {
				s.Push(&backwardElement{
					typ:  C,
					pref: append(pref, suff[0]),
					val:  binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]),
				})
			}
			if bItr := suffix.NewBackwardIterator(nil, nil, suff[1:], pg); bItr.Valid() {
				s.Push(&backwardElement{
					typ:  S,
					itr:  bItr,
					pref: append(pref, suff[0]),
				})
			}
			return nil
		case constant.PN:
			pn, typ = branch(suff[0], rsrc.pg.Buffer())
			if typ == constant.PN {
				pg, err := t.c.Get(pn)
				if err != nil {
					defer t.c.Release(rsrc.pg)
					defer rsrc.le.RUnlock()
					return err
				}
				s.Push(&backwardElement{
					typ:  E,
					rsrc: rsrc,
				})
				if binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]) != constant.Cancel {
					s.Push(&backwardElement{
						typ:  C,
						pref: append(pref, suff[0]),
						val:  binary.LittleEndian.Uint64(rsrc.pg.Buffer()[2048+int(suff[0])*8:]),
					})
				}
				s.Push(&backwardElement{
					typ:  P,
					cnt:  255,
					rsrc: &resource{pg: pg},
					pref: append(pref, suff[0]),
				})
				return nil
			}
		}
	}
	return errmsg.UnknownError
}

// return value
// 	typ - node's type
//  pn  - node's page number
//  le  - locker
//  k   - suffix
//  pg  - parent node
func (t *tree) down(k []byte, update bool) (int, int64, locker.Locker, []byte, cache.Page, error) {
	pn, typ := RootPage, constant.PN
	le := t.t.Get(uint64(pn) | uint64(k[0])<<constant.TypeOff)
	switch {
	case update:
		le.Lock()
	default:
		le.RLock()
	}
	pg, err := t.c.Get(pn)
	if err != nil {
		goto ERR
	}
	for {
		if len(k) == 1 {
			return typ, pn, le, k, pg, nil
		}
		pn, typ = branch(k[0], pg.Buffer())
		switch typ {
		case constant.PN:
			k = k[1:]
			if len(k) > 1 {
				if ok, rtyp, rpn, rpg, err := t.detect(k, pn); err != nil {
					goto ERR
				} else if ok {
					t.c.Release(pg)
					return rtyp, rpn, le, k, rpg, nil
				}
			}
			switch {
			case update:
				le.Unlock()
			default:
				le.RUnlock()
			}
			t.c.Release(pg)
			le = t.t.Get(uint64(pn) | uint64(k[0])<<constant.TypeOff)
			switch {
			case update:
				le.Lock()
			default:
				le.RLock()
			}
			pg, err = t.c.Get(pn)
			if err != nil {
				goto ERR
			}
		default:
			return typ, pn, le, k, pg, nil
		}
	}
ERR:
	switch {
	case update:
		le.Unlock()
	default:
		le.RUnlock()
	}
	return 0, 0, nil, nil, nil, err
}

func (t *tree) find(k []byte, pn int64) (uint64, error) {
	pg, err := t.c.Get(pn)
	if err != nil {
		return 0, err
	}
	defer t.c.Release(pg)
	return suffix.Find(k, pg.Buffer()), nil
}

func (t *tree) insert(k []byte, v uint64, w suffix.Writer, pn int64, par cache.Page) error {
	pg, err := t.c.Get(pn)
	if err != nil {
		return err
	}
	defer t.c.Release(pg)
	return suffix.Insert(k, v, w, t.c, pg, par)
}

func (t *tree) insertByNewSuffix(k []byte, v uint64, w suffix.Writer, par cache.Page) error {
	pg, err := t.c.Get(-1)
	if err != nil {
		return err
	}
	defer t.c.Release(pg)
	var start, end byte
	{
		buf := par.Buffer()
		for start = k[0]; start >= 0; start-- {
			if _, typ := branch(start, buf); typ != constant.ES {
				start++
				break
			}
			if start == 0 {
				break
			}
		}
		for end = k[0]; end <= 0xFF; end++ {
			if _, typ := branch(end, buf); typ != constant.ES {
				end--
				break
			}
			if end == 0xFF {
				break
			}
		}
		pn := uint64(pg.PageNumber())
		pg.Buffer()[2], pg.Buffer()[3] = start, end
		if start == end {
			pn |= constant.SN << constant.TypeOff
		} else {
			pn |= constant.MS << constant.TypeOff
		}
		for i := int(start); i <= int(end); i++ {
			binary.LittleEndian.PutUint64(buf[i*8:], pn)
		}
	}
	if err = w.NewSuffix(end, start, uint64(par.PageNumber()), uint64(pg.PageNumber())); err != nil {
		return err
	}
	par.Sync()
	if start == end {
		return suffix.Insert(k[1:], v, w, t.c, pg, par)
	}
	return suffix.Insert(k, v, w, t.c, pg, par)
}

func (t *tree) detect(k []byte, pn int64) (bool, int, int64, cache.Page, error) {
	pg, err := t.c.Get(pn)
	if err != nil {
		return false, 0, 0, nil, err
	}
	if pn, typ := branch(k[0], pg.Buffer()); typ == constant.MS || typ == constant.ES {
		return true, typ, pn, pg, nil
	}
	t.c.Release(pg)
	return false, 0, 0, nil, nil
}

func branch(k byte, buf []byte) (int64, int) {
	pn := binary.LittleEndian.Uint64(buf[int(k)*8:])
	return int64(pn & constant.Mask), int((pn >> constant.TypeOff) & constant.TypeMask)
}
