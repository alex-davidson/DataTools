using System.Data;
using System.Linq;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Schema;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class SqlServerImportModelBuilderTests
    {
        [Test]
        public void OmitsComputedColumnsFromMetaInfo()
        {
            var sourceColumns = new []
            {
                new ColumnDescriptor { OriginalName = "Data", Length = 4, OriginalIndex = 0, StoredDataType = ColumnDataType.SignedInteger },
                new ColumnDescriptor { OriginalName = "Computed", Length = 4, OriginalIndex = 1, StoredDataType = ColumnDataType.SignedInteger }
            };
            var table = new Table
            {
                Name = "Table",
                Schema = "Schema",
                Fields = new [] {
                    new Table.Field {
                        Name = "Data",
                        IsComputed = false,
                        DataType = new Table.DataType { Name = "int", SqlDbType = SqlDbType.Int, MaxLength = 4 }
                    },
                    new Table.Field {
                        Name = "Computed",
                        IsComputed = true,
                        DataType = new Table.DataType { Name = "int", SqlDbType = SqlDbType.Int, MaxLength = 4 }
                    }
                }
            };
            var model = new SqlServerImportModelBuilder().Build(table, sourceColumns);

            Assert.That(model.ColumnMetaInfos, Has.Length.EqualTo(1));
            Assert.That(model.ColumnMetaInfos.Single().Name, Is.EqualTo("Data"));
        }

        [Test]
        public void IncludesSerialisersFromAllSourceColumnDescriptors()
        {
            var sourceColumns = new []
            {
                new ColumnDescriptor { OriginalName = "Data", Length = 4, OriginalIndex = 0, StoredDataType = ColumnDataType.SignedInteger },
                new ColumnDescriptor { OriginalName = "Computed", Length = 4, OriginalIndex = 1, StoredDataType = ColumnDataType.SignedInteger },
                new ColumnDescriptor { OriginalName = "Additional", Length = 4, OriginalIndex = 2, StoredDataType = ColumnDataType.SignedInteger }
            };
            var table = new Table
            {
                Name = "Table",
                Schema = "Schema",
                Fields = new [] {
                    new Table.Field {
                        Name = "Data",
                        IsComputed = false,
                        DataType = new Table.DataType { Name = "int", SqlDbType = SqlDbType.Int, MaxLength = 4 }
                    },
                    new Table.Field {
                        Name = "Computed",
                        IsComputed = true,
                        DataType = new Table.DataType { Name = "int", SqlDbType = SqlDbType.Int, MaxLength = 4 }
                    }
                }
            };
            var model = new SqlServerImportModelBuilder().Build(table, sourceColumns);

            Assert.That(model.ColumnSerialisers, Has.Length.EqualTo(3));
        }

        [Test]
        public void SerialiserOrderMatchesDescriptorOrder()
        {
            var sourceColumns = new []
            {
                new ColumnDescriptor { OriginalName = "A", Length = 8, OriginalIndex = 2, StoredDataType = ColumnDataType.SignedInteger },
                new ColumnDescriptor { OriginalName = "B", Length = 4, OriginalIndex = 3, StoredDataType = ColumnDataType.SignedInteger },
                new ColumnDescriptor { OriginalName = "C", Length = 2, OriginalIndex = 4, StoredDataType = ColumnDataType.SignedInteger },
                new ColumnDescriptor { OriginalName = "D", Length = 1, OriginalIndex = 1, StoredDataType = ColumnDataType.UnsignedInteger },
                new ColumnDescriptor { OriginalName = "E", Length = 10, OriginalIndex = 0, StoredDataType = ColumnDataType.FixedLengthString },
                new ColumnDescriptor { OriginalName = "F", Length = -1, OriginalIndex = 6, StoredDataType = ColumnDataType.String }
            };
            var table = new Table
            {
                Name = "Table",
                Schema = "Schema",
                Fields = new Table.Field[] {}
            };
            var model = new SqlServerImportModelBuilder().Build(table, sourceColumns);

            Assert.That(model.ColumnSerialisers.Select(s => new { s.DataType, s.Length }), Is.EqualTo(sourceColumns.Select(d => new { DataType = d.StoredDataType, d.Length })));
        }

        [Test]
        public void ColumnMetaInfosIncludeIndexOfCorrelatedColumnDescriptor()
        {
            var sourceColumns = new []
            {
                new ColumnDescriptor { OriginalName = "A", Length = 8, OriginalIndex = 2, StoredDataType = ColumnDataType.SignedInteger },
                new ColumnDescriptor { OriginalName = "B", Length = 4, OriginalIndex = 3, StoredDataType = ColumnDataType.SignedInteger },
                new ColumnDescriptor { OriginalName = "C", Length = 2, OriginalIndex = 4, StoredDataType = ColumnDataType.SignedInteger },
                new ColumnDescriptor { OriginalName = "D", Length = 1, OriginalIndex = 1, StoredDataType = ColumnDataType.UnsignedInteger },
                new ColumnDescriptor { OriginalName = "E", Length = 10, OriginalIndex = 0, StoredDataType = ColumnDataType.FixedLengthString },
                new ColumnDescriptor { OriginalName = "F", Length = -1, OriginalIndex = 6, StoredDataType = ColumnDataType.String }
            };
            var table = new Table
            {
                Name = "Table",
                Schema = "Schema",
                Fields = new [] {
                    new Table.Field { Name = "D", DataType = new Table.DataType { Name = "byte", SqlDbType = SqlDbType.TinyInt, MaxLength = 1 } },
                    new Table.Field { Name = "F", DataType = new Table.DataType { Name = "nvarchar", SqlDbType = SqlDbType.NVarChar, MaxLength = -1 } },
                    new Table.Field { Name = "A", DataType = new Table.DataType { Name = "bigint", SqlDbType = SqlDbType.BigInt, MaxLength = 8 } },
                    new Table.Field { Name = "B", DataType = new Table.DataType { Name = "int", SqlDbType = SqlDbType.Int, MaxLength = 4 } },
                    new Table.Field { Name = "C", DataType = new Table.DataType { Name = "smallint", SqlDbType = SqlDbType.SmallInt, MaxLength = 2 } },
                    new Table.Field { Name = "D", DataType = new Table.DataType { Name = "tinyint", SqlDbType = SqlDbType.TinyInt, MaxLength = 1 } },
                }
            };
            var model = new SqlServerImportModelBuilder().Build(table, sourceColumns);

            Assert.That(model.ColumnMetaInfos.Select(s => new { s.Name, s.SourceIndex }), Is.EquivalentTo(new [] {
                new { Name = "D", SourceIndex = 3 },
                new { Name = "F", SourceIndex = 5 },
                new { Name = "A", SourceIndex = 0 },
                new { Name = "B", SourceIndex = 1 },
                new { Name = "C", SourceIndex = 2 },
                new { Name = "D", SourceIndex = 3 },
            }));
        }
    }
}
