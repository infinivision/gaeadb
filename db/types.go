package db

import (
	"io"
	"time"

	"github.com/infinivision/gaeadb/cache"
	"github.com/infinivision/gaeadb/data"
	"github.com/infinivision/gaeadb/mvcc"
	"github.com/infinivision/gaeadb/scheduler"
	"github.com/infinivision/gaeadb/transaction"
	"github.com/infinivision/gaeadb/wal"
	"github.com/nnsgmsone/damrey/logger"
)

/*
DB provides the various functions required to interact with gaeadb. DB is thread-safe.
*/
type DB interface {
	Close() error

	Del([]byte) error
	Set([]byte, []byte) error
	Get([]byte) ([]byte, error)

	NewTransaction() (transaction.Transaction, error)
}

type Config struct {
	CacheSize       int // cache size
	DirName         string
	LogWriter       io.Writer
	CheckPointCycle time.Duration
}

type db struct {
	d    data.Data
	m    mvcc.MVCC
	w    wal.Writer
	c    cache.Cache
	log  logger.Log
	schd scheduler.Scheduler
}
