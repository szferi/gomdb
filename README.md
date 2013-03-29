gomdb
=====

Go wrapper for OpenLDAP Lightning Memory-Mapped Database (LMDB)

Install
=======

git clone -b mdb.master --single-branch git://git.openldap.org/openldap.git
make
make install

It will install to /usr/local

export LD_LIBRARY_PATH=/usr/local/lib
go test -v
