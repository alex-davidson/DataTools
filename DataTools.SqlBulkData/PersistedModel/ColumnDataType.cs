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
