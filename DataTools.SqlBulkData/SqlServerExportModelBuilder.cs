using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class SqlServerExportModelBuilder
    {
        public ITableGuidPolicy TableIdPolicy { get; set; } = new NameBasedTableGuidPolicy();

        public ExportModel Build(Table tableSchema)
        {
            if (tableSchema.Fields.Length > short.MaxValue) throw new ArgumentException($"Too many columns in schema: {tableSchema.Fields.Length}");
            var fieldsModel = tableSchema.Fields
                .Where(f => f.DataType.SqlDbType != SqlDbType.Timestamp)
                .Select(CreateExportField)
                .OrderBy(f => f, new AlignmentPackingComparer())
                .ToArray();

            var id = TableIdPolicy.GenerateGuid(tableSchema);
            return new ExportModel {
                Id = id,
                TableDescriptor = new TableDescriptor { Id = id, Name = tableSchema.Name, Schema = tableSchema.Schema ?? "" },
                ColumnDescriptors = fieldsModel.Select(CreateDescriptor).ToArray(),
                ColumnSerialisers = fieldsModel.Select(d => d.Serialiser).ToArray()
            };

            ExportField CreateExportField(Table.Field field, int index)
            {
                var definition = InterpretField(field);
                var serialiser = definition.GetSerialiser();
                new ColumnSerialiserValidator().Validate(serialiser);
                return new ExportField {
                    OriginalIndex = (short)index,
                    Field = field,
                    Serialiser = serialiser
                };
            }
        }

        private ColumnDescriptor CreateDescriptor(ExportField field)
        {
            return new ColumnDescriptor {
                OriginalName = field.Field.Name,
                ColumnFlags = field.Serialiser.Flags,
                StoredDataType = field.Serialiser.DataType,
                Length = GetLengthForDescriptor(field.Serialiser, field.Field.DataType.MaxLength),
                OriginalIndex = field.OriginalIndex
            };
        }

        private static int GetLengthForDescriptor(IColumnSerialiser serialiser, int dataTypeLength)
        {
            switch (serialiser.DataType.Classify())
            {
                case ColumnDataTypeClassification.FixedLengthPrimitive: return serialiser.Length;
                case ColumnDataTypeClassification.FixedLengthBuffer: return serialiser.Length;
                case ColumnDataTypeClassification.VariableLengthBuffer: return dataTypeLength;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public IColumnDefinition InterpretField(Table.Field field)
        {
            switch (field.DataType.SqlDbType)
            {
                case SqlDbType.BigInt:
                    return new SqlServerBigIntColumn() { Name = field.Name, Flags = field.IsNullable ? ColumnFlags.Nullable : ColumnFlags.None };
                case SqlDbType.Int:
                    return new SqlServerIntColumn() { Name = field.Name, Flags = field.IsNullable ? ColumnFlags.Nullable : ColumnFlags.None };
                case SqlDbType.SmallInt:
                    return new SqlServerSmallIntColumn() { Name = field.Name, Flags = field.IsNullable ? ColumnFlags.Nullable : ColumnFlags.None };
                case SqlDbType.TinyInt:
                    return new SqlServerTinyIntColumn() { Name = field.Name, Flags = field.IsNullable ? ColumnFlags.Nullable : ColumnFlags.None };

                case SqlDbType.Char:
                    return new SqlServerFixedLengthANSIStringColumn(field.DataType.MaxLength) { Name = field.Name, Flags = field.IsNullable ? ColumnFlags.AbsentWhenNull : ColumnFlags.None };
                case SqlDbType.Text:
                case SqlDbType.VarChar:
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                case SqlDbType.NChar:
                    return new SqlServerVariableLengthStringColumn() { Name = field.Name, Flags = field.IsNullable ? ColumnFlags.AbsentWhenNull : ColumnFlags.None };

                case SqlDbType.Xml:

                case SqlDbType.UniqueIdentifier:
                case SqlDbType.Bit:
                    break;

                case SqlDbType.Float:
                    if (field.DataType.MaxLength <= 0) break; 
                    if (field.DataType.MaxLength <= 4) return new SqlServerSinglePrecisionColumn() { Name = field.Name, Flags = field.IsNullable ? ColumnFlags.Nullable : ColumnFlags.None };
                    if (field.DataType.MaxLength <= 8) return new SqlServerDoublePrecisionColumn() { Name = field.Name, Flags = field.IsNullable ? ColumnFlags.Nullable : ColumnFlags.None };
                    break;
                case SqlDbType.Real:
                    return new SqlServerSinglePrecisionColumn() { Name = field.Name, Flags = field.IsNullable ? ColumnFlags.Nullable : ColumnFlags.None };
                case SqlDbType.VarBinary:
                case SqlDbType.Binary:
                case SqlDbType.Image:
                case SqlDbType.Timestamp:

                case SqlDbType.Decimal:
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:

                case SqlDbType.Date:
                case SqlDbType.Time:
                case SqlDbType.DateTime:
                case SqlDbType.DateTime2:
                case SqlDbType.SmallDateTime:
                case SqlDbType.DateTimeOffset:

                case SqlDbType.Variant:
                case SqlDbType.Udt:
                case SqlDbType.Structured:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            throw new NotSupportedException($"Not supported: {field.DataType.SqlDbType} with length {field.DataType.MaxLength}");
        }

        class ExportField
        {
            public short OriginalIndex { get; set; }
            public Table.Field Field { get; set; }
            public IColumnSerialiser Serialiser { get; set; }
        }

        class AlignmentPackingComparer : IComparer<ExportField>
        {
            public int Compare(ExportField x, ExportField y)
            {
                // Null values are not considered comparable.
                if (x == null) throw new ArgumentNullException(nameof(x));
                if (y == null) throw new ArgumentNullException(nameof(y));
                
                // Sort classification ascending.
                var xClassification = x.Serialiser.DataType.Classify();
                var yClassification = y.Serialiser.DataType.Classify();
                var classificationDifference = xClassification - yClassification;
                if (classificationDifference != 0) return classificationDifference;

                if (xClassification != ColumnDataTypeClassification.VariableLengthBuffer)
                {
                    var lengthDifference = x.Serialiser.Length - y.Serialiser.Length;
                    // Sort length descending, for fixed-length fields only.
                    if (lengthDifference != 0) return -lengthDifference;
                }

                // Sort original index ascending, ie. retain ordering of otherwise-same fields.
                return x.OriginalIndex - y.OriginalIndex;
            }
        }
    }
}
