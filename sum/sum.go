package sum

import (
	"hash"
)

func Sum(h hash.Hash32, data []byte) uint32 {
	h.Reset()
	h.Write(data)
	return h.Sum32()
}
