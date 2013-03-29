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

type Txn struct {
	_txn *C.MDB_txn
}

func (env *Env) BeginTxn(parent *Txn, flags uint) (* Txn, error) {
	
}

func (txn *Txn) Commit() error {
}

func (txn *Txn) Abort() error {
}

func (txn *Txn) Renew() error {
}

func (txn *Txn) DBIOpen(flags uint) (DBI, error) {
}

func (txn *Txn) Stat(dbi DBI) (*Stat, error) {
}

func (txn *Txn) Drop(dbi DBI, int del) error {
}

