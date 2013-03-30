package mdb

/*
#cgo LDFLAGS: -L/usr/local/lib -llmdb
#cgo CFLAGS: -I/usr/local

#include <stdlib.h>
#include <stdio.h>
#include <lmdb.h>
*/
import "C"

import (
	"unsafe"
)

func (cursor *Cursor) Close() error {
	if cursor._cursor == nil {
		return errors.New("Cursor already closed")
	}
	C.mdb_cursor_close(cursor._cursor)
	cursor._cursor = nil
	return nil
}

