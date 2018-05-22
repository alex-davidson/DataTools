using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Schema;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData
{
    public class SqlServerImportModelBuilder
    {
        public IFieldCorrelator Correlator { get; set; } = new StrictFieldNameCorrelator();

        public ImportModel Build(Table tableSchema, IList<ColumnDescriptor> sourceColumns)
        {
            var writableFields = tableSchema.Fields
                .Where(f => f.DataType.SqlDbType != SqlDbType.Timestamp)
                .Where(f => !f.IsComputed).ToArray();
            var fieldsModel = sourceColumns.Select(CreateImportField).ToArray();

            var unallocatedTargetFields = writableFields.Except(fieldsModel.SelectMany(f => f.TargetFields)).ToArray();
            if (unallocatedTargetFields.Any()) Correlator.OnUnallocatedTargetFields(unallocatedTargetFields);

            return new ImportModel {
                Table = tableSchema.Identify(),
                ColumnSerialisers = fieldsModel.Select(d => d.Serialiser).ToArray(),
                ColumnMetaInfos = fieldsModel.SelectMany(f => f.TargetFields.Select(t => CreateMetaInfo(t, f))).ToArray()
            };

            ImportField CreateImportField(ColumnDescriptor sourceColumn, int index)
            {
                var definition = InterpretDescriptor(sourceColumn);
                var serialiser = definition.GetSerialiser();
                new ColumnSerialiserValidator().Validate(serialiser, sourceColumn);
                return new ImportField {
                    Index = index,
                    TargetFields = Correlator.GetTargetFields(sourceColumn, writableFields),
                    Descriptor = sourceColumn,
                    Definition = definition,
                    Serialiser = serialiser
                };
            }
        }

        private ColumnMetaInfo CreateMetaInfo(Table.Field field, ImportField model)
        {
            return new ColumnMetaInfo(field.Name, model.Index) {
                DataTypeName = SelectCompatibleDataTypeName(model.Serialiser, field),
                FieldType = model.Serialiser.DotNetType
            };
        }

        private string SelectCompatibleDataTypeName(IColumnSerialiser serialiser, Table.Field field)
        {
            return serialiser.DotNetType.Name.ToLower();
        }

        private IColumnDefinition InterpretDescriptor(ColumnDescriptor descriptor)
        {
            switch (descriptor.StoredDataType)
            {
                case ColumnDataType.SignedInteger:
                    switch (descriptor.Length)
                    {
                        case 1: throw new NotSupportedException("SQL Server does not support single-byte signed integers.");
                        case 2: return new SqlServerSmallIntColumn { Flags = descriptor.ColumnFlags, Name = descriptor.OriginalName };
                        case 4: return new SqlServerIntColumn { Flags = descriptor.ColumnFlags, Name = descriptor.OriginalName };
                        case 8: return new SqlServerBigIntColumn { Flags = descriptor.ColumnFlags, Name = descriptor.OriginalName };
                    }
                    break;
                case ColumnDataType.UnsignedInteger:
                    if (descriptor.Length == 1) return new SqlServerTinyIntColumn { Flags = descriptor.ColumnFlags, Name = descriptor.OriginalName };
                    throw new NotSupportedException("SQL Server does not support unsigned integers longer than a byte.");
                case ColumnDataType.FloatingPoint:
                    if (descriptor.Length <= 0) break; 
                    if (descriptor.Length <= 4) return new SqlServerSinglePrecisionColumn() { Flags = descriptor.ColumnFlags, Name = descriptor.OriginalName };
                    if (descriptor.Length <= 8) return new SqlServerDoublePrecisionColumn() { Flags = descriptor.ColumnFlags, Name = descriptor.OriginalName };
                    break;
                case ColumnDataType.String:
                    return new SqlServerVariableLengthStringColumn { Flags = descriptor.ColumnFlags, Name = descriptor.OriginalName };
                case ColumnDataType.VariableLengthBytes:
                    break;
                case ColumnDataType.FixedLengthString:
                    return new SqlServerFixedLengthANSIStringColumn(descriptor.Length) { Flags = descriptor.ColumnFlags, Name = descriptor.OriginalName };

                case ColumnDataType.DecimalFloatingPoint:
                    return new SqlServerDecimalColumn(DecimalPacker.ForBufferSize(descriptor.Length)) { Flags = descriptor.ColumnFlags, Name = descriptor.OriginalName };

                case ColumnDataType.FixedLengthBytes:
                case ColumnDataType.Invalid:
                default:
                    throw new ArgumentOutOfRangeException();
            }
            throw new NotSupportedException($"Not supported: {descriptor.StoredDataType} with length {descriptor.Length}");
        }

        class ImportField
        {
            public int Index { get; set; }
            public Table.Field[] TargetFields { get; set; }
            public ColumnDescriptor Descriptor { get; set; }
            public IColumnDefinition Definition { get; set; }
            public IColumnSerialiser Serialiser { get; set; }
        }
    }
}
