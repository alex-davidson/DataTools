using System;

namespace DataTools.SqlBulkData.PersistedModel
{
    public enum ColumnDataType : int
    {
        Invalid = 0,
        /// <summary>
        /// A signed integer, of the size indicated by the column's Length.
        /// </summary>
        SignedInteger = 1,
        /// <summary>
        /// An unsigned integer, of the size indicated by the column's Length.
        /// </summary>
        UnsignedInteger = 2,
        /// <summary>
        /// A floating point number, of the size indicated by the column's Length.
        /// </summary>
        FloatingPoint = 3,
        /// <summary>
        /// A variable-length UTF-8 string.
        /// </summary>
        String = 4,
        /// <summary>
        /// A variable-length sequence of bytes.
        /// </summary>
        VariableLengthBytes = 5,
        /// <summary>
        /// A sequence of ASCII characters, one byte per character, the count of which is indicated by the column's Length.
        /// </summary>
        FixedLengthString = 6,
        /// <summary>
        /// A sequence of bytes, the count of which is indicated by the column's Length.
        /// </summary>
        FixedLengthBytes = 7,
        /// <summary>
        /// A twos-complement multibyte integer in little-endian order, followed by a single scale byte. The column's Length
        /// indicates the total number of bytes, and it must always be multiple of 4.
        /// </summary>
        DecimalFloatingPoint = 8,
        /// <summary>
        /// A 16-byte Guid stored as little-endian UInt32, UInt16, UInt16, byte[8]. Should be 8-byte-aligned.
        /// </summary>
        Guid = 9,
        /// <summary>
        /// A little-endian Int64 value, representing a signed count of 100-nanosecond ticks.
        /// </summary>
        Time = 10,
        /// <summary>
        /// A little-endian UInt64 value. The lower 62 bits are a count of 100-nanosecond ticks that have elapsed since
        /// 12:00:00 midnight at the start of January 1, 0001 (Gregorian). The type of DateTime is indicated by the top two
        /// bits.
        /// </summary>
        DateTime = 11,
        /// <summary>
        /// A 12-byte field consisting of an 8-byte DateTime (see above) with Unspecified type, followed by a 4-byte timezone
        /// offset, stored as a little-endian Int32 count of minutes.
        /// </summary>
        DateTimeOffset = 12
    }

    public static class ColumnDataTypeExtensions
    {
        public static ColumnDataTypeClassification Classify(this ColumnDataType dataType)
        {
            switch (dataType)
            {
                case ColumnDataType.SignedInteger:
                case ColumnDataType.UnsignedInteger:
                case ColumnDataType.FloatingPoint:
                case ColumnDataType.DecimalFloatingPoint:
                case ColumnDataType.Guid:
                case ColumnDataType.Time:
                case ColumnDataType.DateTime:
                case ColumnDataType.DateTimeOffset:
                    return ColumnDataTypeClassification.FixedLengthPrimitive;

                case ColumnDataType.FixedLengthString:
                case ColumnDataType.FixedLengthBytes:
                    return ColumnDataTypeClassification.FixedLengthBuffer;

                case ColumnDataType.String:
                case ColumnDataType.VariableLengthBytes:
                    return ColumnDataTypeClassification.VariableLengthBuffer;

                case ColumnDataType.Invalid:
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}
