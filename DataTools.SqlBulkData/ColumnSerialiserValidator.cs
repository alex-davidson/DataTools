using System;
using System.Linq;
using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;

namespace DataTools.SqlBulkData
{
    public class ColumnSerialiserValidator
    {
        public void Validate(IColumnSerialiser serialiser)
        {
            switch (serialiser.DataType.Classify())
            {
                case ColumnDataTypeClassification.FixedLengthPrimitive:
                case ColumnDataTypeClassification.FixedLengthBuffer:
                    if (serialiser.Length > 0) break;
                    throw new InvalidSerialiserException(serialiser, $"Serialiser does not define a length for type {serialiser.DataType}: {serialiser.GetType()}");

                case ColumnDataTypeClassification.VariableLengthBuffer: break;

                default:
                    throw new ArgumentOutOfRangeException();
            }
            var unknownFlags = serialiser.Flags.GetUnknownFlags();
            if (unknownFlags != 0)
            {
                throw new InvalidSerialiserException(serialiser, $"Serialiser specifies unknown flags {unknownFlags}: {serialiser.GetType()}");
            }
        }
    }
}
