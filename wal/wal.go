package wal

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func NewWriter(dir string) (*walWriter, error) {
	return newWriter(dir, unix.O_RDWR|unix.O_DIRECT)
}

func fileName(idx int, dir string) string {
	return fmt.Sprintf("%s%c%v.LOG", dir, os.PathSeparator, idx)
}
