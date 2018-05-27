# File Format, v1

All multibyte sequences are stored in little-endian order, unless otherwise
specified.

Provision is given for combining many tables' worth of data into a single
export file. The format defines unique IDs in some chunks, so if appropriate
Guid schemas are used it should be feasible to bundle multiple files into one
through fairly simple concatenation.

Note that user-defined data types are NOT SUPPORTED. If the source table or
tables contain columns of user-defined types, the tool will refuse to operate.

## Header

If a GZIP header is detected, unwrap it automatically.

Stream must begin with an 8-byte header:

    4 bytes: BTBL
    2 bytes: <version>
    2 bytes: - reserved, should be 0x0000 -

## Structure: Chunks

The header is followed by a series of chunks. Chunks may technically be in any
order, but tools may legitimately depend on a degree of sanity here. Bulk data
chunks should be ordered after the chunks required to import them for example.
As a general guideline, small chunks or chunks which describe later chunks
should appear first.

Each chunk follows the format:

    4 bytes: <chunk type>
    4 bytes: - reserved, should be 0x00000000 -
    8 bytes: <length in bytes>
    <length> bytes: <chunk-dependent>

Each chunk must be 8-byte-aligned. The length must not include padding, and
the reader must assume padding if a chunk does not end on an 8-byte boundary.

## Field types

### Null values

There is no general handling of null values. Fields defined by the file
format are always non-null. Where null needs to be supported this is done
explicitly, by eg. recording a boolean or bit to indicate null.

Bulk row data demonstrates this with the null field map.

### Fixed length

Fixed length fields are simply written as raw bytes, in little-endian order.

Subtypes:
* *Guid:* Consists of four fields: UInt32, UInt16, UInt16, byte[8]. The first
  three are multibyte primitives stored in little-endian format. The last is
  a raw byte array and is stored as such. Should be 8-byte-aligned.
* *DecimalFloatingPoint:* Represented as n-1 bytes of twos-complement integer
  significand, followed by a single twos-complement exponent byte. Should be
  4-byte aligned and the total length must always be a multiple of 4. The
  value may be calculated as: `significand * 10^exponent`
* *Time:* A little-endian Int64 value, representing a signed count of
  100-nanosecond ticks.
* *DateTime:* A little-endian UInt64 value. The lower 62 bits are a count of
  100-nanosecond ticks that have elapsed since 12:00:00 midnight at the start
  of January 1, 0001 (Gregorian). The type of DateTime is indicated by the
  top two bits.
* *DateTimeOffset:* A 12-byte field consisting of an 8-byte DateTime with
  Unspecified type, followed by a 4-byte timezone offset, stored as a
  little-endian Int32 count of minutes.

### Variable-length

All variable-length types are stored as one or more segments.
* Each segment must be 4-byte-aligned and can be up to (Int32.MaxValue - 3)
  bytes long.
* Each segment starts with a 4-byte field. The low 31 bits contain the segment
  length in little-endian order (minus the length field itself). The high bit
  is 0 if the segment is the last one, or 1 if more follow it.

Subtypes:
* *String:* Encoded as UTF-8.
* *VariableLengthBytes:* Arbitrary raw bytes.

## Table Info Chunk: TABL

    Guid:   Table ID
    String: Table name
    String: Table schema name

Since null values are not supported here, the schema name must be an empty
string if absent.

## Column Info Chunk: COLS

    Guid:   Table ID
    Int16:  Number of columns in this chunk
    Int16:  Length of each column record, excluding its name (should be 0x000C)
    Repeat, 4-byte-aligned:
        Int16:  Original column index
        Int16:  Bitfield: Column flags
        Int32:  Stored data type (see table B)
        Int32:  Column value length, if applicable, or -1 otherwise
        String: Original column name

The order of the columns in this chunk dictates the order in which their data
is serialised in a ROWD chunk. While any order is permissible as long as ROWD
matches, they should ideally be sorted as follows:

* Fixed length first, excluding char/nchar or binary, longest to shortest,
  aligned to 4 bytes or the column width, whichever is less.
* Then char and fixed length binary, padded to 4-byte alignment.
* Then variable-length and nchar, padded to 4-byte alignment.

Note that variable-length fields need to be 4-byte-aligned, so padding may be
necessary after the last fixed length field.

Note also that fixed length UTF-8 strings do not necessarily produce fixed 
length byte sequences, therefore nchar fields should be serialised as
variable-length strings, on the grounds that UTF-16 would usually only be
shorter than length-plus-UTF-8 for up to 2 characters (assuming predominately
single-byte characters). GZIP is assumed to fill the gaps, here.

The general idea here is to permit 4-byte-aligned reads wherever possible when
trying to scan a subset of the data.

Length may be included for eg. VarChar fields, even though the actual length
will vary per row, since this may be useful for schema validation. Note that
the meaning of the length may be type-specific, eg. for string types it may
be a character count rather than a byte count.
A negative length is valid only for variable-length fields and indicates
that the maximum length is unknown or unlimited.

Table A: Column flags

    0x0001  Nullable            The column may contain nulls, and must have a bit in the null field map.
    0x0003  Absent when null    The column's value is omitted entirely when it is null, rather than being empty.


Table B: Stored data types

    0   Invalid                 Sentinel value which should never be used.
    1   SignedInteger           A signed integer. The column's Length indicates the number of bytes.
    2   UnsignedInteger         An unsigned integer. The column's Length indicates the number of bytes.
    3   FloatingPoint           A floating point number. The column's Length indicates the number of bytes.
    4   String                  A variable-length UTF-8 string. The column's Length indicates the maximum number of characters.
    5   VariableLengthBytes     A variable-length sequence of bytes. The column's Length indicates the maximum number of bytes.
    6   FixedLengthString       A sequence of ASCII characters, one byte per character, the count of which is indicated by the column's Length.
    7   FixedLengthBytes        A sequence of bytes, the count of which is indicated by the column's Length.
    8   DecimalFloatingPoint    A twos-complement multibyte integer in little-endian order, followed by a single scale byte. The column's Length
                                indicates the total number of bytes, and it must always be multiple of 4.
    9   Guid                    A 16-byte Guid stored as little-endian UInt32, UInt16, UInt16, byte[8]. Should be 8-byte-aligned.
    10  Time                    A little-endian Int64 value, representing a signed count of 100-nanosecond ticks.
    11  DateTime                A little-endian UInt64 value. The lower 62 bits are a count of 100-nanosecond ticks that have elapsed since
                                12:00:00 midnight at the start of January 1, 0001 (Gregorian). The type of DateTime is indicated by the top two
                                bits, as described in Table C.
    12  DateTimeOffset          A 12-byte field consisting of an 8-byte DateTime (see above) with Unspecified type, followed by a 4-byte timezone
                                offset, stored as a little-endian Int32 count of minutes.


Table C: DateTime Kind

    0x00...                     Unspecified/Unknown
    0x40... Bit 62              UTC
    0x80... Bit 63              Local time
    0xC0... Both                Reserved. Do not use.


## Row Data Chunk: ROWD

    Guid:   Table ID
    Repeat until end of chunk:
        Byte:     Row header: 0x52
        Bitfield: Null field map
        <Columns>

Precise row format will depend on the columns.

The Table ID refers to a COLS chunk. The order of columns in that chunk is
the order which should be assumed when deserialising this one.

Due to alignment, the null field map always consists of at least 3 bytes. Bits
map only to nullable columns, so a schema with 16 columns of which only 7 are
nullable would use only a single byte of the null field map.

Rows are 4-byte-aligned, but are otherwise packed tightly. The data is
intended to be read forward-only and in bulk, so there are no indications of
row length.

If restart positions are desirable then multiple ROWD chunks should be used.

## Implementation Notes

Note that BinaryWriter *does not* appear to guarantee unbuffered operation, so
if you want to check the current position in the stream you should Flush() it
first. Since we check alignment quite frequently, this would result in a huge
number of otherwise-pointless flushes. Hence the reimplementation of a lot of
its basic functionality.
