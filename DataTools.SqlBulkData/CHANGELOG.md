# Changelog

## 1.0, latest

First useful version.

* Supports the majority of SQL Server data types.
* Supports GZip compression for import and export.

### Known Limitations

* Command-line syntax is limited and the parser can be fussy.
* There is no option to append to tables at present. They will always be
  overwritten.
* The file format specifies 'segmented' buffer types in excess of 2GB. These
  are not currently implemented and the tool tends to buffer fields in memory,
  so particularly large field values (~1GB) might fail to export.
* The tool tries to use all the CPUs on the machine. There is no command-line
  option to control this.
