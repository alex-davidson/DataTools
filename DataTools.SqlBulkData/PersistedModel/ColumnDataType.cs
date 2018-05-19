﻿namespace DataTools.SqlBulkData.PersistedModel
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
    }
}
