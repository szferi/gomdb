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
		t.Fatalf("Cannot open environment: %s", err)
	}
	var txn *Txn
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		env.Close()
		t.Fatalf("Cannot begin transaction: %s", err)
	}
	var dbi DBI
	dbi, err = txn.DBIOpen(nil, 0)
	if err != nil {
		env.Close()
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
			t.Fatalf("Error during put: %s", err)
		}
	}
	err = txn.Commit()
	if err != nil {
		txn.Abort()
		env.DBIClose(dbi)
		env.Close()
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
	/*
		txn, err = env.BeginTxn(nil, 0)
		if err != nil {
			env.Close()
			t.Fatalf("Cannot begin transaction: %s", err)
		}*/
	err = env.Close()
	if err != nil {
		t.Errorf("Error during close of environment: %s", err)
	}
	// clean up
	os.RemoveAll(path)
}
