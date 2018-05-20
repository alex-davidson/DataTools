using System.Data;
using System.Linq;
using DataTools.SqlBulkData.Schema;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class SqlServerExportModelBuilderTests
    {
        [Test]
        public void IncludesComputedColumns()
        {
            var table = new Table
            {
                Name = "Table",
                Schema = "Schema",
                Fields = new [] {
                    new Table.Field {
                        Name = "Computed",
                        IsComputed = true,
                        DataType = new Table.DataType { Name = "int", SqlDbType = SqlDbType.Int, MaxLength = 4 }
                    }
                }
            };
            var model = new SqlServerExportModelBuilder().Build(table);

            Assert.That(model.ColumnSerialisers, Has.Length.EqualTo(1));
            Assert.That(model.ColumnDescriptors, Has.Length.EqualTo(1));
            Assert.That(model.ColumnDescriptors.Single().OriginalName, Is.EqualTo("Computed"));
        }

        [Test]
        public void SerialiserOrderMatchesDescriptorOrder()
        {
            var table = new Table
            {
                Name = "Table",
                Schema = "Schema",
                Fields = new [] {
                    new Table.Field { Name = "A", DataType = new Table.DataType { Name = "int", SqlDbType = SqlDbType.Int, MaxLength = 4 } },
                    new Table.Field { Name = "B", DataType = new Table.DataType { Name = "bigint", SqlDbType = SqlDbType.BigInt, MaxLength = 8 } },
                    new Table.Field { Name = "C", DataType = new Table.DataType { Name = "tinyint", SqlDbType = SqlDbType.TinyInt, MaxLength = 1 } },
                    new Table.Field { Name = "D", DataType = new Table.DataType { Name = "varchar", SqlDbType = SqlDbType.BigInt, MaxLength = 8 } },
                    new Table.Field { Name = "E", DataType = new Table.DataType { Name = "char", SqlDbType = SqlDbType.Char, MaxLength = 10 } },
                    new Table.Field { Name = "F", DataType = new Table.DataType { Name = "smallint", SqlDbType = SqlDbType.SmallInt, MaxLength = 2 } },
                }
            };
            var model = new SqlServerExportModelBuilder().Build(table);

            Assert.That(model.ColumnSerialisers.Select(s => new { s.DataType, s.Length }), Is.EqualTo(model.ColumnDescriptors.Select(d => new { DataType = d.StoredDataType, d.Length })));
        }

        [Test]
        public void PacksColumnsByAlignment()
        {
            var table = new Table
            {
                Name = "Table",
                Schema = "Schema",
                Fields = new [] {
                    new Table.Field { Name = "Int32", DataType = new Table.DataType { Name = "int", SqlDbType = SqlDbType.Int, MaxLength = 4 } },
                    new Table.Field { Name = "Int64", DataType = new Table.DataType { Name = "bigint", SqlDbType = SqlDbType.BigInt, MaxLength = 8 } },
                    new Table.Field { Name = "Byte", DataType = new Table.DataType { Name = "tinyint", SqlDbType = SqlDbType.TinyInt, MaxLength = 1 } },
                    new Table.Field { Name = "String", DataType = new Table.DataType { Name = "varchar", SqlDbType = SqlDbType.VarChar, MaxLength = 8 } },
                    new Table.Field { Name = "Fixed String", DataType = new Table.DataType { Name = "char", SqlDbType = SqlDbType.Char, MaxLength = 10 } },
                    new Table.Field { Name = "Int16", DataType = new Table.DataType { Name = "smallint", SqlDbType = SqlDbType.SmallInt, MaxLength = 2 } },
                }
            };
            var model = new SqlServerExportModelBuilder().Build(table);

            Assert.That(model.ColumnDescriptors.Select(s => s.OriginalName), Is.EqualTo(new [] {
                "Int64",
                "Int32",
                "Int16",
                "Byte",
                "Fixed String",
                "String"
            }));
        }
    }
}
