# SQL Server Version-Agnostic Bulk Import/Export Tool, v1

Copies bulk data between files and SQL Server databases.

Original intended use cases:
* Copying entire databases to older versions of SQL Server.
* Version-agnostic bulk export of test databases.

Note that neither of these cases seem well-supported by `bcp` or similar.

Design Goals:
* File format must not be tied to SQL Server (any version).
* Import must be fast.
* Exported files should be small.

[File Format Specification, v1](FILEFORMAT.md)

## Usage

Currently only two modes are supported, with no options to tweak their
behaviour.

### Export

    DataTools.SqlBulkData.exe export [--server <instance>] --database <database> --files <dir-path>/

Dumps all tables in a given database to a directory, one bulk data file per
table.

### Import

    DataTools.SqlBulkData.exe import [--server <instance>] --database <database> --files <dir-path>/

Imports all bulk data files in a directory into a given database, overwriting
any existing data.

### Additional Options

    -v          Increases verbosity. May be supplied multiple times.
    --pause     Causes the tool to wait for a keypress before exiting, after
                a successful import or export. Intended mainly for debugging.

## Warnings

* Tables referenced by many foreign keys might sometimes fail to import with
  a deadlock. This appears to be random, so subsequent attempts will probably
  succeed. Reliability seems better when using Bulk-Logged mode for the
  database, but still not perfect.
* The filenames are based on the source table names, but are completely
  ignored during import. Each file contains information about the source table
  and schema, and this is used to identify the target table for import.
* There is no option to append to tables at present. They will always be
  overwritten.
* If an import fails halfway through, the database will likely be left with a
  lot of untrusted constraints and half-empty tables. Don't casually use this 
  on a production database!
* User-defined SQL Server data types are not supported. Nor are Variant types.
  The export process will bail out quickly if it encounters any of these.
* The file format specifies 'segmented' buffer types in excess of 2GB. These
  are not currently implemented and the tool tends to buffer fields in memory,
  so particularly large field values (~1GB) might fail to export.
* The tool tries to use all the CPUs on the machine. Running it on a
  production server is discouraged.

## Performance

This was developed on a Core i7-6700K machine running SQL Server 2008R2 and
some fast SSDs.

The original itch was a ~1GB/500-table test database which I typically need
to re-import a few dozen times per day. The existing approach involved
Powershell and sequential `bcp` invocations to load SQL Server native-format
bulk files.
(The application which uses this database needs to support 2008R2, hence the
need to test against that version and maintain compatible test data.)

This process usually takes a couple of minutes. The bulk files occupy about
530MB (uncompressed).

This tool takes ~35 seconds and the bulk files occupy about 440MB
uncompressed, but the built-in GZip compression brings that down to 85MB.

The speed-up is achieved by:
* handling all tables in one process, avoiding the need to create 500 new
  processes, and
* concurrent imports, up to one per logical CPU.

If the imports are done consecutively instead of concurrently, performance
is on a par with the `bcp`-based approach.
