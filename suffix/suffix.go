package suffix

import (
	"bytes"
	"encoding/binary"
	"gaeadb/cache"
	"gaeadb/constant"
	"sort"
)

func Find(k []byte, buf []byte) uint64 {
	if o := find(k, buf); o > 0 {
		return binary.LittleEndian.Uint64(buf[o:])
	}
	return 0
}

func Insert(k []byte, v uint64, w Writer, c cache.Cache, pg, par cache.Page) error {
	if appendSuffix(k, v, pg) {
		return nil
	}
	return load(w, pg, true).insert(k, v, c, par)
}

func NewForwardIterator(ks [][]byte, vs []uint64, prefix []byte, pg cache.Page) Iterator {
	es := load(nil, pg, false).es
	for i := 0; i < len(ks); i++ {
		es = push(&element{vs[i], ks[i]}, es)
	}
	for len(es) > 0 {
		if bytes.HasPrefix(es[0].suff, prefix) {
			break
		}
		es = es[1:]
	}
	return &forwardIterator{prefix, es}
}

func NewBackwardIterator(ks [][]byte, vs []uint64, prefix []byte, pg cache.Page) Iterator {
	es := load(nil, pg, false).es
	for i := 0; i < len(ks); i++ {
		es = push(&element{vs[i], ks[i]}, es)
	}
	for len(es) > 0 {
		if bytes.HasPrefix(es[len(es)-1].suff, prefix) {
			break
		}
		es = es[:len(es)-1]
	}
	return &backwardIterator{prefix, es}
}

func (s *suffix) insert(k []byte, v uint64, c cache.Cache, par cache.Page) error {
	e := &element{v, k}
	if s.pg.Buffer()[2] == s.pg.Buffer()[3] {
		return s.insertBySN(e, c, par)
	}
	return s.insertByMS(e, c, par)
}

// all prefix node are sorted
func (s *suffix) insertBySN(e *element, c cache.Cache, par cache.Page) error {
	var os []uint16
	var vs []uint64
	var es []*element

	pg, err := c.Get(-1)
	if err != nil {
		return err
	}
	defer c.Release(pg)
	defer pg.Sync()
	defer par.Sync()
	for i, j := 0, len(s.es); i < j; i++ {
		switch {
		case len(s.es[i].suff) == 1:
			s.free += ElementHeaderSize + 1
			binary.LittleEndian.PutUint64(pg.Buffer()[2048+int(s.es[i].suff[0])*8:], s.es[i].off)
			vs = append(vs, s.es[i].off)
			os = append(os, uint16(s.es[i].suff[0]))
		default:
			es = append(es, s.es[i])
		}
	}
	if len(s.es) != len(es) {
		s.es = es
		pg.Sync()
		s.pg.Sync()
	}
	binary.LittleEndian.PutUint64(par.Buffer()[int(s.pg.Buffer()[2])*8:],
		uint64(pg.PageNumber())|constant.PN<<constant.TypeOff)
	pn := uint64(s.pg.PageNumber()) | constant.MS<<constant.TypeOff
	for i := 0; i <= 0xFF; i++ {
		binary.LittleEndian.PutUint64(pg.Buffer()[i*8:], pn)
		vs = append(vs, pn)
		os = append(os, uint16(i))
	}
	s.pg.Buffer()[2], s.pg.Buffer()[3] = 0, 0xFF
	if err = s.w.NewPrefix(uint64(par.PageNumber()), uint64(pg.PageNumber()), uint16(s.pg.Buffer()[2]), os, vs); err != nil {
		return err
	}
	if len(e.suff) == 1 {
		binary.LittleEndian.PutUint64(pg.Buffer()[2048+int(e.suff[0])*8:], e.off)
		return nil
	} else {
		return s.insertByMS(e, c, pg)
	}
}

// all suffix node are sorted
func (s *suffix) insertByMS(e *element, c cache.Cache, par cache.Page) error {
	buf := s.pg.Buffer()
	x := s.es[0].suff[0]
	y := s.es[len(s.es)-1].suff[0]
	switch {
	case e.suff[0] < x:
		pg, err := c.Get(-1)
		if err != nil {
			return err
		}
		defer c.Release(pg)
		return s.splitL(e, pg, par)
	case e.suff[0] > y:
		pg, err := c.Get(-1)
		if err != nil {
			return err
		}
		defer c.Release(pg)
		return s.splitR(e, pg, par)
	case x == y && x == e.suff[0]:
		var os []uint16
		var vs []uint64
		{
			for i, j := int(buf[2]), int(x); i < j; i++ {
				os = append(os, uint16(i))
				vs = append(vs, constant.ES<<constant.TypeOff)
				binary.LittleEndian.PutUint64(par.Buffer()[int(i)*8:], constant.ES<<constant.TypeOff)
			}
			for i, j := int(x)+1, int(buf[3])+1; i < j; i++ {
				os = append(os, uint16(i))
				vs = append(vs, constant.ES<<constant.TypeOff)
				binary.LittleEndian.PutUint64(par.Buffer()[int(i)*8:], constant.ES<<constant.TypeOff)
			}
		}
		buf[2], buf[3] = x, x
		s.reduce(par)
		e.suff = e.suff[1:]
		if err := flush(s.w, []*suffix{s}, par, os, vs); err != nil {
			return err
		}
		if s.append(e) {
			return s.writeBack()
		}
		return s.insertBySN(e, c, par)
	case x == e.suff[0]:
		pg, err := c.Get(-1)
		if err != nil {
			return err
		}
		defer c.Release(pg)
		var os []uint16
		var vs []uint64
		{
			for i := int(buf[2]); i < int(x); i++ {
				os = append(os, uint16(i))
				vs = append(vs, constant.ES<<constant.TypeOff)
				binary.LittleEndian.PutUint64(par.Buffer()[int(i)*8:], constant.ES<<constant.TypeOff)
			}
		}
		defer par.Sync()
		if err = s.splitRR(x, pg, par, os, vs); err != nil {
			return err
		}
		e.suff = e.suff[1:]
		if s.append(e) {
			s.writeBack()
			return nil
		}
		return s.insertBySN(e, c, par)
	case y == e.suff[0]:
		pg, err := c.Get(-1)
		if err != nil {
			return err
		}
		defer c.Release(pg)
		var os []uint16
		var vs []uint64
		{
			for i := int(y) + 1; i <= int(buf[3]); i++ {
				os = append(os, uint16(i))
				vs = append(vs, constant.ES<<constant.TypeOff)
				binary.LittleEndian.PutUint64(par.Buffer()[int(i)*8:], constant.ES<<constant.TypeOff)
			}
		}
		if err = s.splitLL(y, pg, par, os, vs); err != nil {
			return err
		}
		e.suff = e.suff[1:]
		if s.append(e) {
			s.writeBack()
			return nil
		}
		return s.insertBySN(e, c, par)
	default:
		mpg, err := c.Get(-1)
		if err != nil {
			return err
		}
		rpg, err := c.Get(-1)
		if err != nil {
			c.Release(mpg)
			return err
		}
		defer c.Release(rpg)
		defer c.Release(mpg)
		ms, err := s.splitM(e, mpg, rpg, par)
		if err != nil {
			return err
		}
		ms.w = s.w
		e.suff = e.suff[1:]
		if ms.append(e) {
			ms.writeBack()
			return nil
		}
		return ms.insertBySN(e, c, par)
	}
}

func (s *suffix) append(e *element) bool {
	if n := len(e.suff) + ElementHeaderSize; n <= s.free {
		s.free -= n
		s.es = push(e, s.es)
		return true
	}
	return false
}

func (s *suffix) writeBack() error {
	buf := s.pg.Buffer()
	binary.LittleEndian.PutUint16(buf, uint16(len(s.es)))
	o := HeaderSize
	for _, e := range s.es {
		binary.LittleEndian.PutUint16(buf[o:], uint16(len(e.suff)))
		binary.LittleEndian.PutUint64(buf[o+2:], e.off)
		copy(buf[o+ElementHeaderSize:o+ElementHeaderSize+len(e.suff)], e.suff)
		o += ElementHeaderSize + len(e.suff)
	}
	s.pg.Sync()
	return nil
}

func (s *suffix) splitL(e *element, pg, par cache.Page) error {
	k := s.es[0].suff[0]
	rs := &suffix{
		pg:   pg,
		es:   s.es,
		free: s.free,
	}
	s.es = []*element{e}
	s.free = constant.BlockSize - HeaderSize - ElementHeaderSize - len(e.suff)
	pg.Buffer()[2], pg.Buffer()[3] = k, s.pg.Buffer()[3] // rs.start = k, rs.end = end
	s.pg.Buffer()[3] = k - 1                             // ls.start = s.start, ls.end = k - 1
	s.reduce(par)
	rs.reduce(par)
	s.writeBack()
	rs.writeBack()
	return flush(s.w, []*suffix{s, rs}, par, []uint16{}, []uint64{})
}

func (s *suffix) splitR(e *element, pg, par cache.Page) error {
	k := s.es[len(s.es)-1].suff[0]
	rs := &suffix{
		pg:   pg,
		es:   []*element{e},
		free: constant.BlockSize - HeaderSize - ElementHeaderSize - len(e.suff),
	}
	pg.Buffer()[2], pg.Buffer()[3] = k+1, s.pg.Buffer()[3] // rs.start = k + 1, rs.end = s.end
	s.pg.Buffer()[3] = k                                   // s.end = k
	s.reduce(par)
	rs.reduce(par)
	s.writeBack()
	rs.writeBack()
	return flush(s.w, []*suffix{s, rs}, par, []uint16{}, []uint64{})
}

func (s *suffix) splitLL(k byte, pg, par cache.Page, os []uint16, vs []uint64) error {
	rs := &suffix{
		pg:   pg,
		es:   s.es,
		free: s.free,
	}
	s.es = []*element{}
	s.free = constant.BlockSize - HeaderSize
	for n, x, size := rs.cutL(); n > 0 && x < k; n, x, size = rs.cutL() {
		s.free -= size
		s.es = append(s.es, rs.es[:n]...)
		rs.free += size
		rs.es = rs.es[n:]
	}
	s.pg.Buffer()[3] = k - 1              // ls.end = k - 1
	pg.Buffer()[2], pg.Buffer()[3] = k, k // rs.start = rs.end = k
	{                                     // exchange
		s.pg, rs.pg = rs.pg, s.pg
		s.es, rs.es = rs.es, s.es
		s.free, rs.free = rs.free, s.free
	}
	s.reduce(par)
	rs.reduce(par)
	s.writeBack()
	rs.writeBack()
	return flush(s.w, []*suffix{s, rs}, par, os, vs)
}

func (s *suffix) splitRR(k byte, pg, par cache.Page, os []uint16, vs []uint64) error {
	rs := &suffix{
		pg:   pg,
		es:   []*element{},
		free: constant.BlockSize - HeaderSize,
	}
	pg.Buffer()[2], pg.Buffer()[3] = k+1, s.pg.Buffer()[3] // rs.start = k + 1, rs.end = s.end
	s.pg.Buffer()[2], s.pg.Buffer()[3] = k, k              // s.start = s.end = k
	for n, x, size := s.cutR(); n > 0 && x > k; n, x, size = s.cutR() {
		rs.free -= size
		for i, j := len(s.es)-n, len(s.es); i < j; i++ {
			rs.es = push(s.es[i], rs.es)
		}
		s.free += size
		s.es = s.es[:len(s.es)-n]
	}
	s.reduce(par)
	rs.reduce(par)
	s.writeBack()
	rs.writeBack()
	return flush(s.w, []*suffix{s, rs}, par, os, vs)
}

// s ms rs
func (s *suffix) splitM(e *element, mpg, rpg, par cache.Page) (*suffix, error) {
	rs := s.split(e.suff[0], rpg)
	ms := &suffix{
		pg:   mpg,
		es:   []*element{},
		free: constant.BlockSize - HeaderSize,
	}
	mpg.Buffer()[2], mpg.Buffer()[3] = e.suff[0], e.suff[0]
	if n, x, size := s.cutR(); x == e.suff[0] {
		ms.free -= size
		for i, j := len(s.es)-n, len(s.es); i < j; i++ {
			ms.es = push(s.es[i], ms.es)
		}
		s.free += size
		s.es = s.es[:len(s.es)-n]
	}
	s.reduce(par)
	ms.reduce(par)
	rs.reduce(par)
	s.writeBack()
	ms.writeBack()
	rs.writeBack()
	return ms, flush(s.w, []*suffix{s, ms, rs}, par, []uint16{}, []uint64{})
}

func (s *suffix) cutL() (int, byte, int) {
	switch len(s.es) {
	case 0:
		return 0, 0, 0
	case 1:
		return 1, s.es[0].suff[0], len(s.es[0].suff) + ElementHeaderSize
	default:
		k := s.es[0].suff[0]
		size := len(s.es[0].suff) + ElementHeaderSize
		for i, j := 1, len(s.es); i < j; i++ {
			if k != s.es[i].suff[0] {
				return i, k, size
			}
			size += len(s.es[i].suff) + ElementHeaderSize
		}
		return len(s.es), k, size
	}
}

func (s *suffix) cutR() (int, byte, int) {
	switch len(s.es) {
	case 0:
		return 0, 0, 0
	case 1:
		return 1, s.es[0].suff[0], len(s.es[0].suff) + ElementHeaderSize
	default:
		n := len(s.es)
		k := s.es[n-1].suff[0]
		size := len(s.es[n-1].suff) + ElementHeaderSize
		for i := n - 2; i >= 0; i-- {
			if k != s.es[i].suff[0] {
				return n - i - 1, k, size
			}
			size += len(s.es[i].suff) + ElementHeaderSize
		}
		return len(s.es), k, size
	}
}

// s - k - rs
func (s *suffix) split(k byte, pg cache.Page) *suffix {
	rs := &suffix{
		pg:   pg,
		es:   []*element{},
		free: constant.BlockSize - HeaderSize,
	}
	pg.Buffer()[2], pg.Buffer()[3] = k+1, s.pg.Buffer()[3]
	s.pg.Buffer()[3] = k - 1
	for n, x, size := s.cutR(); n > 0 && x > k; n, x, size = s.cutR() {
		s.free += size
		rs.free -= size
		for i, j := len(s.es)-n, len(s.es); i < j; i++ {
			rs.es = push(s.es[i], rs.es)
		}
		s.es = s.es[:len(s.es)-n]
	}
	return rs
}

func (s *suffix) reduce(pg cache.Page) {
	if s.pg.Buffer()[2] == s.pg.Buffer()[3] {
		for i, j := 0, len(s.es); i < j; i++ {
			s.free++
			s.es[i].suff = s.es[i].suff[1:]
		}
	}
}

func dup(xs []byte) []byte {
	return append([]byte{}, xs...)
}

func push(e *element, es []*element) []*element {
	i := sort.Search(len(es), func(i int) bool { return bytes.Compare(es[i].suff, e.suff) >= 0 })
	es = append(es, &element{})
	copy(es[i+1:], es[i:])
	es[i] = e
	return es
}

func find(k []byte, buf []byte) int {
	o := HeaderSize
	for i, j := 0, int(binary.LittleEndian.Uint16(buf)); i < j; i++ {
		n := int(binary.LittleEndian.Uint16(buf[o:]))
		if bytes.Compare(k, buf[ElementHeaderSize+o:ElementHeaderSize+n+o]) == 0 {
			return o + 2
		}
		o += ElementHeaderSize + n
	}
	return 0
}

func appendSuffix(k []byte, v uint64, pg cache.Page) bool {
	var i uint16

	o := HeaderSize
	buf := pg.Buffer()
	for j := binary.LittleEndian.Uint16(buf); i < j; i++ {
		n := int(binary.LittleEndian.Uint16(buf[o:]))
		if bytes.Compare(k, buf[ElementHeaderSize+o:ElementHeaderSize+n+o]) == 0 {
			binary.LittleEndian.PutUint64(buf[o+2:], v)
			pg.Sync()
			return true
		}
		o += ElementHeaderSize + n
	}
	if n := len(k) + ElementHeaderSize; n <= constant.BlockSize-o {
		binary.LittleEndian.PutUint16(buf[o:], uint16(len(k)))
		binary.LittleEndian.PutUint64(buf[o+2:], v)
		copy(buf[o+ElementHeaderSize:], k)
		binary.LittleEndian.PutUint16(buf, i+1)
		pg.Sync()
		return true
	}
	return false
}

func load(w Writer, pg cache.Page, update bool) *suffix {
	var s suffix

	s.w = w
	s.pg = pg
	o := HeaderSize
	buf := pg.Buffer()
	for i, j := 0, int(binary.LittleEndian.Uint16(buf)); i < j; i++ {
		n := int(binary.LittleEndian.Uint16(buf[o:]))
		e := &element{
			off: binary.LittleEndian.Uint64(buf[o+2:]),
		}
		if update {
			e.suff = dup(buf[ElementHeaderSize+o : ElementHeaderSize+n+o])
		} else {
			e.suff = buf[ElementHeaderSize+o : ElementHeaderSize+n+o]
		}
		s.es = append(s.es, e)
		o += ElementHeaderSize + n
	}
	sort.Sort(Elements(s.es))
	s.free = constant.BlockSize - o
	return &s
}

func flush(w Writer, xs []*suffix, pg cache.Page, os []uint16, vs []uint64) error {
	for _, s := range xs {
		start, end := s.pg.Buffer()[2], s.pg.Buffer()[3]
		pn := uint64(s.pg.PageNumber())
		switch {
		case start == end:
			pn |= constant.SN << constant.TypeOff
		default:
			pn |= constant.MS << constant.TypeOff
		}
		for i, j := int(start), int(end)+1; i < j; i++ {
			vs = append(vs, pn)
			os = append(os, uint16(i))
			binary.LittleEndian.PutUint64(pg.Buffer()[i*8:], pn)
		}
	}
	if len(os) > 0 {
		if err := w.ChgPrefix(uint64(pg.PageNumber()), os, vs); err != nil {
			return err
		}
	}
	pg.Sync()
	return nil
}
