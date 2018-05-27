using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class BulkTableFileReaderWriterTests
    {
        [Test]
        public void Roundtrips()
        {
            var id = Guid.NewGuid();
            var table = new TableDescriptor { Id = id, Name = "Table Name", Schema = "Schema" };
            var tableColumns = new TableColumns {
                TableId = id,
                Columns = new [] {
                    new ColumnDescriptor { OriginalName = "Column B", OriginalIndex = 1, ColumnFlags = ColumnFlags.None, StoredDataType = ColumnDataType.FloatingPoint, Length = 8 },
                    new ColumnDescriptor { OriginalName = "Column A", OriginalIndex = 0, ColumnFlags = ColumnFlags.None, StoredDataType = ColumnDataType.SignedInteger, Length = 4 },
                }
            };
            Assume.That(tableColumns.Columns, Is.EqualTo(tableColumns.Columns.ToList()).Using(ColumnDescriptor.EqualityComparer));

            var stream = new MemoryStream();
            WriteTestData(stream, table, tableColumns);

            stream.Position = 0;
            using (var reader = new BulkTableFileReader(stream, true))
            {
                Assert.That(reader.MoveNext(), Is.True);

                ReadAndVerifyTestData(reader, tableColumns);

                Assert.That(reader.MoveNext(), Is.False);
                Assert.That(reader.MoveNext(), Is.False);
            }
        }

        [Test]
        public void CanReadFromDetectedGZip()
        {
            var id = Guid.NewGuid();
            var table = new TableDescriptor { Id = id, Name = "Table Name", Schema = "Schema" };
            var tableColumns = new TableColumns {
                TableId = id,
                Columns = new [] {
                    new ColumnDescriptor { OriginalName = "Column B", OriginalIndex = 1, ColumnFlags = ColumnFlags.None, StoredDataType = ColumnDataType.FloatingPoint, Length = 8 },
                    new ColumnDescriptor { OriginalName = "Column A", OriginalIndex = 0, ColumnFlags = ColumnFlags.None, StoredDataType = ColumnDataType.SignedInteger, Length = 4 },
                }
            };
            Assume.That(tableColumns.Columns, Is.EqualTo(tableColumns.Columns.ToList()).Using(ColumnDescriptor.EqualityComparer));

            var stream = new MemoryStream();
            WriteTestData(stream, table, tableColumns);
            stream.Position = 0;

            var compressedStream = new MemoryStream();
            using (var compress = new GZipStream(compressedStream, CompressionMode.Compress, true))
            {
                stream.CopyTo(compress);
            }

            compressedStream.Position = 0;

            using (var reader = new BulkTableFileReader(compressedStream, true))
            {
                Assert.That(reader.MoveNext(), Is.True);

                ReadAndVerifyTestData(reader, tableColumns);

                Assert.That(reader.MoveNext(), Is.False);
                Assert.That(reader.MoveNext(), Is.False);
            }
        }

        [Test]
        public void CanReadFromProvidedGZipStream()
        {
            var id = Guid.NewGuid();
            var table = new TableDescriptor { Id = id, Name = "Table Name", Schema = "Schema" };
            var tableColumns = new TableColumns {
                TableId = id,
                Columns = new [] {
                    new ColumnDescriptor { OriginalName = "Column B", OriginalIndex = 1, ColumnFlags = ColumnFlags.None, StoredDataType = ColumnDataType.FloatingPoint, Length = 8 },
                    new ColumnDescriptor { OriginalName = "Column A", OriginalIndex = 0, ColumnFlags = ColumnFlags.None, StoredDataType = ColumnDataType.SignedInteger, Length = 4 },
                }
            };
            Assume.That(tableColumns.Columns, Is.EqualTo(tableColumns.Columns.ToList()).Using(ColumnDescriptor.EqualityComparer));

            var stream = new MemoryStream();
            WriteTestData(stream, table, tableColumns);
            stream.Position = 0;

            var compressedStream = new MemoryStream();
            using (var compress = new GZipStream(compressedStream, CompressionMode.Compress, true))
            {
                stream.CopyTo(compress);
            }

            compressedStream.Position = 0;

            using (var gzipStream = new GZipStream(compressedStream, CompressionMode.Decompress, true))
            using (var reader = new BulkTableFileReader(gzipStream, true))
            {
                Assert.That(reader.MoveNext(), Is.True);

                ReadAndVerifyTestData(reader, tableColumns);

                Assert.That(reader.MoveNext(), Is.False);
                Assert.That(reader.MoveNext(), Is.False);
            }
        }

        private static void ReadAndVerifyTestData(BulkTableFileReader reader, TableColumns tableColumns)
        {
            Assert.That(reader.Current.Table.Name, Is.EqualTo("Table Name"));
            Assert.That(reader.Current.Table.Schema, Is.EqualTo("Schema"));
            Assert.That(reader.Current.Columns, Is.EqualTo(tableColumns.Columns).Using(ColumnDescriptor.EqualityComparer));

            Assert.That(Serialiser.ReadByte(reader.Current.DataStream), Is.EqualTo(TypeIds.RowHeader));
            Serialiser.AlignRead(reader.Current.DataStream, 4);
            Assert.That(Serialiser.ReadDouble(reader.Current.DataStream), Is.EqualTo(42));
            Assert.That(Serialiser.ReadInt32(reader.Current.DataStream), Is.EqualTo(-42));
        }

        private static void WriteTestData(Stream stream, TableDescriptor table, TableColumns tableColumns)
        {
            using (var writer = new BulkTableFileWriter(stream, true))
            {
                writer.AddTable(table);
                writer.AddColumns(tableColumns);
                using (var rowData = writer.BeginAddRowData(table.Id))
                {
                    Serialiser.WriteByte(rowData.Stream, TypeIds.RowHeader);
                    Serialiser.AlignWrite(rowData.Stream, 4);
                    Serialiser.WriteDouble(rowData.Stream, 42);
                    Serialiser.WriteInt32(rowData.Stream, -42);
                }
            }
        }
    }
}
