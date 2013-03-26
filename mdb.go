package mdb

/*
#cgo LDFLAGS: -L/usr/local/lib -llmdb
#cgo CFLAGS: -I/usr/local

#include <stdio.h>
#include <lmdb.h>
*/
import "C"

/*
import (
	"unsafe"
)
*/

func Version() string {
	var major, minor, patch *C.int

	ver_str := C.mdb_version(major, minor, patch)
	// defer C.free(unsafe.Pointer(ver_str))
	return C.GoString(ver_str)
}
