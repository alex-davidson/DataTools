# Changelog

## 1.1.1 (latest)

Fixes a (stupid) bug when seeking forwards through a stream.

### Known Limitations

* Command-line syntax is limited and the parser can be fussy.
* There is no option to append to tables at present. They will always be
  overwritten.
* The file format specifies 'segmented' buffer types in excess of 2GB. These
  are not currently implemented and the tool tends to buffer fields in memory,
  so particularly large field values (~1GB) might fail to export.
* The tool tries to use all the CPUs on the machine. There is no command-line
  option to control this.
* Indexes on indexed views are not properly re-enabled.
* Unseekable compressed streams aren't read properly.

## 1.1

Fixes import failures due to deadlocks, caused by indexed views upon on the
tables being imported.

* Indexed views will have all indexes disabled for the duration of the import
  and rebuilt afterwards.
* Inserts are performed in batches of 10000 rows, with table locks.

## 1.0

First useful version.

* Supports the majority of SQL Server data types.
* Supports GZip compression for import and export.
