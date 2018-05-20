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

        public void Validate(IColumnSerialiser serialiser, ColumnDescriptor descriptor)
        {
            Validate(serialiser);
            if (serialiser.Flags != descriptor.ColumnFlags) throw new InvalidSerialiserException(serialiser, $"Serialiser flags do not match descriptor: {serialiser.Flags} <> {descriptor.ColumnFlags}");
            if (serialiser.DataType != descriptor.StoredDataType) throw new InvalidSerialiserException(serialiser, $"Serialiser data type does not match descriptor: {serialiser.DataType} <> {descriptor.StoredDataType}");
            switch (serialiser.DataType.Classify())
            {
                case ColumnDataTypeClassification.FixedLengthPrimitive:
                case ColumnDataTypeClassification.FixedLengthBuffer:
                    if (serialiser.Length == descriptor.Length) break;
                    throw new InvalidSerialiserException(serialiser, $"Serialiser data length does not match descriptor: {serialiser.Length} <> {descriptor.Length}");

                case ColumnDataTypeClassification.VariableLengthBuffer:
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}
