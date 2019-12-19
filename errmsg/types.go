package errmsg

import "errors"

var (
	ScanEnd             = errors.New("scan end")
	NotExist            = errors.New("not exist")
	OpenFailed          = errors.New("open failed")
	ReadFailed          = errors.New("read failed")
	WriteFailed         = errors.New("write failed")
	KeyTooLong          = errors.New("key too long")
	KeyIsEmpty          = errors.New("key is empty")
	ValTooLong          = errors.New("value too long")
	OutOfSpace          = errors.New("out of space")
	UnknownError        = errors.New("unknown error")
	TransactionConflict = errors.New("transaction conflict")
	ReadOnlyTransaction = errors.New("read-only transaction")
)
