namespace DataTools.SqlBulkData.PersistedModel
{
    /// <summary>
    /// Classifies column data types and specifies their ordering within a row.
    /// </summary>
    public enum ColumnDataTypeClassification
    {
        // Fixed-length primitives come first.
        FixedLengthPrimitive = 1,
        // Fixed-length byte buffers next.
        FixedLengthBuffer = 2,
        // Variable-length types last.
        VariableLengthBuffer = 3
    }
}
