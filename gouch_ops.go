package gouchstore

import (
	"os"
)

// GouchOps an interface for plugging in differentl implementations
// of some common low-level operations
type GouchOps interface {
	OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error)
	ReadAt(f *os.File, b []byte, off int64) (n int, err error)
	WriteAt(f *os.File, b []byte, off int64) (n int, err error)
	GotoEOF(f *os.File) (ret int64, err error)
	Sync(f *os.File) error
	Close(f *os.File) error
}
