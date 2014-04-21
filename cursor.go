package mdb

/*
#cgo CFLAGS: -pthread -W -Wall -Wno-unused-parameter -Wbad-function-cast -O2 -g
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import (
	// "fmt"
	"bytes"
	"encoding/gob"
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

// Retrieves the low-level MDB cursor.
func (cursor *Cursor) MdbCursor() *C.MDB_cursor {
	return cursor._cursor
}

func (cursor *Cursor) Get(set_key []byte, op uint) (key, val []byte, err error) {
	var ckey C.MDB_val
	var cval C.MDB_val
	if set_key != nil && (op == SET || op == SET_KEY || op == SET_RANGE) {
		ckey.mv_size = C.size_t(len(set_key))
		ckey.mv_data = unsafe.Pointer(&set_key[0])
	}
	ret := C.mdb_cursor_get(cursor._cursor, &ckey, &cval, C.MDB_cursor_op(op))
	if ret != SUCCESS {
		err = errno(ret)
		key = nil
		val = nil
		return
	}
	err = nil
	key = C.GoBytes(ckey.mv_data, C.int(ckey.mv_size))
	val = C.GoBytes(cval.mv_data, C.int(cval.mv_size))
	return
}

func (cursor *Cursor) GetGo(set_key interface{}, op uint, key, val interface{}) error {
	var err error
	var bset_key []byte
	if set_key != nil {
		var key_buffer bytes.Buffer
		encoder := gob.NewEncoder(&key_buffer)
		err = encoder.Encode(set_key)
		if err != nil {
			return err
		}
		bset_key = key_buffer.Bytes()
	}
	bkey, bval, err := cursor.Get(bset_key, op)
	if err != nil {
		return err
	}
	buf := bytes.NewReader(bkey)
	decoder := gob.NewDecoder(buf)
	err = decoder.Decode(key)
	if err != nil {
		return err
	}
	buf = bytes.NewReader(bval)
	decoder = gob.NewDecoder(buf)
	err = decoder.Decode(val)
	if err != nil {
		return err
	}
	return nil
}

func (cursor *Cursor) Put(key, val []byte, flags uint) error {
	ckey := &C.MDB_val{mv_size: C.size_t(len(key)),
		mv_data: unsafe.Pointer(&key[0])}
	cval := &C.MDB_val{mv_size: C.size_t(len(val)),
		mv_data: unsafe.Pointer(&val[0])}
	ret := C.mdb_cursor_put(cursor._cursor, ckey, cval, C.uint(flags))
	return errno(ret)
}

func (cursor *Cursor) PutGo(key, val interface{}, flags uint) error {
	var bkey bytes.Buffer
	encoder := gob.NewEncoder(&bkey)
	err := encoder.Encode(key)
	if err != nil {
		return err
	}
	var bval bytes.Buffer
	encoder = gob.NewEncoder(&bval)
	err = encoder.Encode(val)
	if err != nil {
		return err
	}
	return cursor.Put(bkey.Bytes(), bval.Bytes(), flags)
}

func (cursor *Cursor) Del(flags uint) error {
	ret := C.mdb_cursor_del(cursor._cursor, C.uint(flags))
	return errno(ret)
}

func (cursor *Cursor) Count() (uint64, error) {
	var _size C.size_t
	ret := C.mdb_cursor_count(cursor._cursor, &_size)
	if ret != SUCCESS {
		return 0, errno(ret)
	}
	return uint64(_size), nil
}
