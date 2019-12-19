package db

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"syscall"

	"github.com/infinivision/gaeadb/cache"
	"github.com/infinivision/gaeadb/constant"
	"github.com/infinivision/gaeadb/data"
	"github.com/infinivision/gaeadb/disk"
	"github.com/infinivision/gaeadb/locker"
	"github.com/infinivision/gaeadb/mvcc"
	"github.com/infinivision/gaeadb/prefix"
	"github.com/infinivision/gaeadb/scheduler"
	"github.com/infinivision/gaeadb/transaction"
	"github.com/infinivision/gaeadb/wal"
	"github.com/nnsgmsone/damrey/logger"
)

func DefaultConfig() Config {
	return Config{
		CacheSize:       2000,
		DirName:         "gaea.db",
		LogWriter:       os.Stderr,
		CheckPointCycle: constant.CheckPointCycle,
	}
}

func Open(cfg Config) (*db, error) {
	if err := enlargelimit(); err != nil {
		return nil, err
	}
	if err := checkDir(cfg.DirName); err != nil {
		return nil, err
	}
	log := logger.New(cfg.LogWriter, "gaeadb")
	d, err := data.New(cfg.DirName)
	if err != nil {
		return nil, err
	}
	c, m, err := newMVCC(cfg, log)
	if err != nil {
		d.Close()
		return nil, err
	}
	ts, err := wal.Recover(cfg.DirName, d, m, c)
	if err != nil {
		d.Close()
		m.Close()
		return nil, err
	}
	w, err := wal.NewWriter(cfg.DirName)
	if err != nil {
		d.Close()
		return nil, err
	}
	constant.CheckPointCycle = cfg.CheckPointCycle
	schd := scheduler.New(ts, d, c, w)
	go schd.Run()
	return &db{d, m, w, c, log, schd}, nil
}

func (db *db) Close() error {
	db.schd.Stop()
	db.d.Close()
	db.w.Close()
	db.m.Close()
	return nil
}

func (db *db) Del(k []byte) error {
	tx := transaction.New(false, db.d, db.m, db.w, db.log, db.schd)
	defer tx.Rollback()
	if err := tx.Del(k); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *db) Set(k, v []byte) error {
	tx := transaction.New(false, db.d, db.m, db.w, db.log, db.schd)
	defer tx.Rollback()
	if err := tx.Set(k, v); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *db) Get(k []byte) ([]byte, error) {
	tx := transaction.New(true, db.d, db.m, db.w, db.log, db.schd)
	defer tx.Rollback()
	if v, err := tx.Get(k); err != nil {
		return nil, err
	} else {
		return v, tx.Rollback()
	}
}

func (db *db) NewTransaction(ro bool) (transaction.Transaction, error) {
	return transaction.New(ro, db.d, db.m, db.w, db.log, db.schd), nil
}

func checkDir(dir string) error {
	st, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return os.Mkdir(dir, os.FileMode(0775))
	}
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return errors.New("'%s' is not directory")
	}
	if st.Mode()&0700 != 0700 {
		return errors.New("permission denied")
	}
	return nil
}

func newMVCC(cfg Config, log logger.Log) (cache.Cache, mvcc.MVCC, error) {
	d, err := disk.New(fmt.Sprintf("%s%cIDX", cfg.DirName, os.PathSeparator))
	if err != nil {
		return nil, nil, err
	}
	c := cache.New(cfg.CacheSize, d, log)
	return c, mvcc.New(prefix.New(c, locker.New())), nil
}

func enlargelimit() error {
	var rlimit syscall.Rlimit

	runtime.GOMAXPROCS(runtime.NumCPU())
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		return err
	} else {
		rlimit.Cur = rlimit.Max
		return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlimit)
	}
	return nil
}
