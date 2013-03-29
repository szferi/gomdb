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

const (
	SUCCESS = 0
	// Env Open flags
	FIXEDMAP   = 0x01
	NOSUBDIR   = 0x4000
	NOSYNC     = 0x10000
	RDONLY     = 0x20000
	NOMETASYNC = 0x40000
	WRITEMAP   = 0x80000
	MAPASYNC   = 0x100000
)

type Errno int

func (e Errno) Error() string {
	return C.GoString(C.mdb_strerror(C.int(e)))
}

var (
	ErrKeyExist        error = Errno(-30799)
	ErrNotFound        error = Errno(-30798)
	ErrPageNotFound    error = Errno(-30797)
	ErrCorrupted       error = Errno(-30796)
	ErrPanic                 = Errno(-30795)
	ErrVersionMismatch       = Errno(-30794)
	ErrInvalid               = Errno(-30793)
	ErrMapFull               = Errno(-30792)
	ErrDbsFull               = Errno(-30791)
	ErrReadersFull           = Errno(-30790)
	ErrTlsFull               = Errno(-30789)
	ErrTxnFull               = Errno(-30788)
	ErrCursorFull            = Errno(-30787)
	ErrPageFull              = Errno(-30786)
	ErrMapResized            = Errno(-30785)
	ErrIncompatibile         = Errno(-30784)
)

func Version() string {
	var major, minor, patch *C.int

	ver_str := C.mdb_version(major, minor, patch)
	return C.GoString(ver_str)
}

type Env struct {
	_env *C.MDB_env
}

func NewEnv() (*Env, error) {
	var _env *C.MDB_env
	ret := C.mdb_env_create(&_env)
	if ret != SUCCESS {
		return nil, Errno(ret)
	}
	return &Env{_env}, nil
}

func (env *Env) Open(path string, flags uint, mode uint) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.mdb_env_open(env._env, cpath, C.uint(flags), C.mode_t(mode))
	if ret != SUCCESS {
		return Errno(ret)
	}
	return nil
}

func (env *Env) Close() error {
	if env._env == nil {
		return errors.New("Environment already closed")
	}
	C.mdb_env_close(env._env)
	env._env = nil
	return nil
}

func (env *Env) Copy(path string) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.mdb_env_copy(env._env, cpath)
	if ret != SUCCESS {
		return Errno(ret)
	}
	return nil
}

func (env *Env) Path() (string, error) {
	var path string
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.mdb_env_get_path(env._env, &cpath)
	if ret != SUCCESS {
		return "", Errno(ret)
	}
	return C.GoString(cpath), nil
}

type Stat struct {
	psize          uint
	depth          uint
	branch_pages   uint64
	leaf_pages     uint64
	owerflow_pages uint64
	entries        uint64
}

func (env *Env) Stat() (*Stat, error) {
	var _stat C.MDB_stat
	ret := C.mdb_env_stat(env._env, &_stat)
	if ret != SUCCESS {
		return nil, Errno(ret)
	}
	stat := Stat{psize: uint(_stat.ms_psize),
		depth:          uint(_stat.ms_depth),
		branch_pages:   uint64(_stat.ms_branch_pages),
		leaf_pages:     uint64(_stat.ms_leaf_pages),
		owerflow_pages: uint64(_stat.ms_overflow_pages),
		entries:        uint64(_stat.ms_entries)}
	return &stat, nil
}

/*
type Info struct {
}

func (env *Env)
*/
