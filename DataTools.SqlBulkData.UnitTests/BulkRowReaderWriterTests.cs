using System;
using System.IO;
using System.Linq;
using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class BulkRowReaderWriterTests
    {
        [Test]
        public void Roundtrips()
        {
            var stream = new MemoryStream();
            var fieldNames = new [] { "Test" };
            var columns = new IColumnDefinition[] {
                new SqlServerBigIntColumn(),
                new SqlServerSmallIntColumn() { Flags = ColumnFlags.Nullable },
                new SqlServerIntColumn(),
                new SqlServerVariableLengthStringColumn() { Flags = ColumnFlags.AbsentWhenNull }
            };
            var rows = new [] {
                new object[] {  1,            2,    3,    DBNull.Value },
                new object[] {  2, DBNull.Value,    4,    "Second row" },
                new object[] { 42, DBNull.Value,    6,     "Third row" }
            };
            var writer = new BulkRowWriter(stream, columns.Select(c => c.GetSerialiser()).ToArray());
            foreach (var row in rows)
            {
                writer.Write(new MockDataRecord(fieldNames, row));
            }

            stream.Position = 0;

            var reader = new BulkRowReader(stream, columns.Select(c => c.GetSerialiser()).ToArray());
            for (var i = 0; reader.MoveNext(); i++)
            {
                Assert.That(reader.Current, Is.EqualTo(rows[i]));
            }
        }
    }
}
