package db

import (
	"gaeadb/cache"
	"gaeadb/data"
	"gaeadb/mvcc"
	"gaeadb/scheduler"
	"gaeadb/transaction"
	"gaeadb/wal"
	"io"
	"time"

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
