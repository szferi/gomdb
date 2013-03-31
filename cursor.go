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
	"errors"
	"unsafe"
)

// MDB_cursor_op
const (
	FIRST = iota
	FIRST_DUP
	GET_BOTH
	GET_RANGE
	GET_CURRENT
	GET_MULTIPLE
	LAST
	LAST_DUP
	NEXT
	NEXT_DUP
	NEXT_MULTIPLE
	NEXT_NODUP
	PREV
	PREV_DUP
	PREV_NODUP
	SET
	SET_KEY
	SET_RANGE
)

func (cursor *Cursor) Close() error {
	if cursor._cursor == nil {
		return errors.New("Cursor already closed")
	}
	C.mdb_cursor_close(cursor._cursor)
	cursor._cursor = nil
	return nil
}

func (cursor *Cursor) Txn() *Txn {
	var _txn *C.MDB_txn
	_txn = C.mdb_cursor_txn(cursor._cursor)
	if _txn != nil {
		return &Txn{_txn}
	}
	return nil
}

func (cursor *Cursor) DBI() DBI {
	var _dbi C.MDB_dbi
	_dbi = C.mdb_cursor_dbi(cursor._cursor)
	return DBI(_dbi)
}

/*
func (cursor *Cursor) Get(op uint) (key []byte, val []byte, error) {
	var ckey *C.MDB_val
	ckey.mv_size = C.size_t(len(key))
	ckey.mv_data = unsafe.Pointer(&key[0])
	var cval *C.MDB_val
	ret := C.mdb_get(txn._txn, C.MDB_dbi(dbi), ckey, cval)
	if ret != SUCCESS {
		return nil, Errno(ret)
	}
	val := C.GoBytes(cval.mv_data, C.int(cval.mv_size))
	return val, nil
	return nil, nil, nil
}*/

func (cursor *Cursor) Put(key []byte, val []byte, flags uint) error {
	ckey := &C.MDB_val{mv_size: C.size_t(len(key)),
		mv_data: unsafe.Pointer(&key[0])}
	cval := &C.MDB_val{mv_size: C.size_t(len(val)),
		mv_data: unsafe.Pointer(&val[0])}
	ret := C.mdb_cursor_put(cursor._cursor, ckey, cval, C.uint(flags))
	if ret != SUCCESS {
		return Errno(ret)
	}
	return nil
}

func (cursor *Cursor) Del(flags uint) error {
	ret := C.mdb_cursor_del(cursor._cursor, C.uint(flags))
	if ret != SUCCESS {
		return Errno(ret)
	}
	return nil
}

func (cursor *Cursor) Count() (uint64, error) {
	var _size C.size_t
	ret := C.mdb_cursor_count(cursor._cursor, &_size)
	if ret != SUCCESS {
		return 0, Errno(ret)
	}
	return uint64(_size), nil
}
