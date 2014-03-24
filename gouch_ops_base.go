package gouchstore

import (
	"os"
)

type BaseGouchOps struct{}

func NewBaseGouchOps() *BaseGouchOps {
	return &BaseGouchOps{}
}

func (g *BaseGouchOps) OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, flag, perm)
}

func (g *BaseGouchOps) ReadAt(f *os.File, b []byte, off int64) (n int, err error) {
	return f.ReadAt(b, off)
}

func (g *BaseGouchOps) WriteAt(f *os.File, b []byte, off int64) (n int, err error) {
	return f.WriteAt(b, off)
}

func (g *BaseGouchOps) GotoEOF(f *os.File) (ret int64, err error) {
	return f.Seek(0, os.SEEK_END)
}

func (g *BaseGouchOps) Sync(f *os.File) error {
	return f.Sync()
}

func (g *BaseGouchOps) Close(f *os.File) error {
	return f.Close()
}