# gaeadb
gaeadb is a pure Go Database engine designed by nnsgmsone. 
The goal of the project is to project a database engine for sql.

## Table of Contents
 * [Getting Started](#getting-started)
    + [Opening a Database engine](#opening-a-database-engine)
    + [DB interface](#db-interface)
    + [Transaction interface](#transaction-interface)
  * [Benchmarks](#benchmarks)
  * [Caveats & Limitations](#caveats--limitations)


## Getting Started

### Opening a database
The top-level object in gaeadb is a `DB`. It represents multiple files on disk
in specific directories, which contain the data for a single database.

```go
package main

import (
	"log"

	"gaeadb/db"
)

func main() {
  // Open the gaeadb database located in the /tmp/gaea.db directory.
  // It will be created if it doesn't exist.
  cfg := db.DefaultConfig()
  cfg.DirName = "/tmp/gaea.db"
  db, err := db.Open(cfg)
  if err != nil {
	  log.Fatal(err)
  }
  defer db.Close()
  // Your code here…
}
```

### DB interface

```go
type DB interface {
	Close() error

	Del([]byte) error
	Set([]byte, []byte) error
	Get([]byte) ([]byte, error)

	NewTransaction() (Transaction, error)
}
```

### Transaction interface
```go
type Transaction interface {
	Commit() error
	Rollback() error
	Del([]byte) error
	Set([]byte, []byte) error
	Get([]byte) ([]byte, error)
	NewForwardIterator([]byte) (Iterator, error)
	NewBackwardIterator([]byte) (Iterator, error)
}

type Iterator interface {
	Close() error
	Next() error
	Valid() bool
	Key() []byte
	Value() ([]byte, error)
}

```

## Benchmarks

I have run comprehensive benchmarks against Bolt and Badger, The
benchmarking code, and the detailed logs for the benchmarks can be found in the
[gaeadbBench] repo.

## Caveats & Limitations

### Caveats
sync is always on and not allowed to close

### Limitations
The maximum value of key is 4074 and the maximum value of value is 64k.
