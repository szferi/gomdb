package mdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestTest1(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Fatalf("Cannot create environment: %s", err)
	}
	err = env.SetMapSize(10485760)
	if err != nil {
		t.Fatalf("Cannot set mapsize: %s", err)
	}
	path, err := ioutil.TempDir("/tmp", "mdb_test")
	if err != nil {
		t.Fatalf("Cannot create temporary directory")
	}
	t.Logf("Path: %s", path)
	err = os.MkdirAll(path, 0770)
	if err != nil {
		t.Fatalf("Cannot create directory: %s", path)
	}
	err = env.Open(path, FIXEDMAP, 0664)
	if err != nil {
		os.RemoveAll(path)
		t.Fatalf("Cannot open environment: %s", err)
	}
	var txn *Txn
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		env.Close()
		os.RemoveAll(path)
		t.Fatalf("Cannot begin transaction: %s", err)
	}
	var dbi DBI
	dbi, err = txn.DBIOpen(nil, 0)
	if err != nil {
		env.Close()
		os.RemoveAll(path)
		t.Fatalf("Cannot create DBI %s", err)
	}
	var data = map[string]string{}
	var key string
	var val string
	num_entries := 10
	for i := 0; i < num_entries; i++ {
		key = fmt.Sprintf("Key-%d", i)
		val = fmt.Sprintf("Val-%d", i)
		data[key] = val
		err = txn.Put(dbi, []byte(key), []byte(val), NOOVERWRITE)
		if err != nil {
			txn.Abort()
			env.DBIClose(dbi)
			env.Close()
			os.RemoveAll(path)
			t.Fatalf("Error during put: %s", err)
		}
	}
	err = txn.Commit()
	if err != nil {
		txn.Abort()
		env.DBIClose(dbi)
		env.Close()
		os.RemoveAll(path)
		t.Fatalf("Cannot commit %s", err)
	}
	stat, err := env.Stat()
	if err != nil {
		env.DBIClose(dbi)
		env.Close()
		t.Fatalf("Cannot get stat %s", err)
	}
	t.Logf("%+v", stat)
	if stat.Entries != uint64(num_entries) {
		t.Errorf("Less entry in the database than expected: %d <> %d", stat.Entries, num_entries)
	}
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		env.DBIClose(dbi)
		env.Close()
		os.RemoveAll(path)
		t.Fatalf("Cannot begin transaction: %s", err)
	}
	var cursor *Cursor
	cursor, err = txn.CursorOpen(dbi)
	if err != nil {
		txn.Abort()
		env.DBIClose(dbi)
		env.Close()
		os.RemoveAll(path)
		t.Fatalf("Error during cursor open %s", err)
	}
	/*
		bkey, bval, rc := cursor.Get(nil, NEXT)
		skey := string(bkey)
		t.Logf("Key: %s", skey)
		sval := string(bval)
		t.Logf("Val: %s", sval)
		t.Logf("Rc: %v", rc)

		bkey, bval, rc = cursor.Get(nil, NEXT)
		skey = string(bkey)
		t.Logf("Key: %s", skey)
		sval = string(bval)
		t.Logf("Val: %s", sval)
		t.Logf("Rc: %v", rc)
	*/
	var bkey, bval []byte
	var rc error
	for {
		bkey, bval, rc = cursor.Get(nil, NEXT)
		if rc != nil {
			break
		}
		skey := string(bkey)
		sval := string(bval)
		t.Logf("Val: %s", sval)
		t.Logf("Key: %s", skey)
		var d string
		var ok bool
		if d, ok = data[skey]; !ok {
			t.Errorf("Cannot found: %s", skey)
		}
		if d != sval {
			t.Errorf("Data missmatch: %s <> %s", sval, d)
		}
	}
	cursor.Close()
	txn.Abort()
	env.DBIClose(dbi)
	err = env.Close()
	if err != nil {
		t.Errorf("Error during close of environment: %s", err)
	}
	// clean up
	os.RemoveAll(path)
}
