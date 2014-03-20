package gouchstore

import (
	"log"
	"os"
)

type LogGouchOps struct {
	*BaseGouchOps
}

func NewLogGouchOps() *LogGouchOps {
	return &LogGouchOps{}
}

func (g *LogGouchOps) OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	log.Printf("GOUCHSTORE: Open - File: %s Flag: %d: Perm: %v", name, flag, perm)
	return g.BaseGouchOps.OpenFile(name, flag, perm)
}

func (g *LogGouchOps) ReadAt(f *os.File, b []byte, off int64) (n int, err error) {
	log.Printf("GOUCHSTORE: ReadAt - Offset: %d Size: %d", off, len(b))
	return g.BaseGouchOps.ReadAt(f, b, off)
}

func (g *LogGouchOps) WriteAt(f *os.File, b []byte, off int64) (n int, err error) {
	log.Printf("GOUCHSTORE: WriteAt - Offset: %d Bytes: % x", off, b)
	return g.BaseGouchOps.WriteAt(f, b, off)
}

func (g *LogGouchOps) GotoEOF(f *os.File) (ret int64, err error) {
	log.Printf("GOUCHSTORE: GotoEOF")
	return g.BaseGouchOps.GotoEOF(f)
}

func (g *LogGouchOps) Sync(f *os.File) error {
	log.Printf("GOUCHSTORE: Sync")
	return g.BaseGouchOps.Sync(f)
}

func (g *LogGouchOps) Close(f *os.File) error {
	log.Printf("GOUCHSTORE: Close")
	return g.BaseGouchOps.Close(f)
}
