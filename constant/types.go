package constant

import "time"

var (
	CheckPointCycle = 5 * time.Second
)

const (
	RootPage    = int64(0)
	Preallocate = int64(257)
)

const (
	Cancel = iota // must be zero
	Delete
	Empty
	Cache
)

const (
	PreLoad = 100
)

const (
	MaxKeySize         = 4074
	MaxValueSize       = 1 << 16 // 64KB
	MaxTransactionSize = 1 << 26 // 64MB
	MaxDataFileSize    = 1 << 40 // 1TB
	MaxLoadDataSize    = 1 << 10 // 1KB
)

const (
	BlockSize = 4096 // 4k
)

const (
	PN = iota // prefix node
	SN        // suffix node
	MS        // mixed suffix node
	ES        // empty suffix node
)

const (
	TypeOff  = uint64(56)
	TypeMask = uint64(0xFF)
	Mask     = uint64(0xFFFFFFFFFFFFFF)
)
