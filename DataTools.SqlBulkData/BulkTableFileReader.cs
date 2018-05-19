using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData
{
    public class BulkTableFileReader : IDisposable
    {
        private readonly Stream stream;
        private readonly bool leaveOpen;
        private ChunkedFileHeader header;
        private ChunkedFileReader reader;
        private readonly Dictionary<Guid, TableDescriptor> tables = new Dictionary<Guid, TableDescriptor>();
        private readonly Dictionary<Guid, TableColumns> tableColumns = new Dictionary<Guid, TableColumns>();

        public BulkTableFileReader(Stream stream, bool leaveOpen = false)
        {
            this.stream = stream ?? throw new ArgumentNullException(nameof(stream));
            this.leaveOpen = leaveOpen;
        }

        private void EnsureReader()
        {
            if (reader != null) return;
            this.header = new ChunkedFileHeaderSerialiser().ReadHeader(stream);
            if (header.FileTypeId != TypeIds.FileHeader) throw new UnrecognisedFileFormatException("Did not find the expected header at the current stream position.");
            reader = new ChunkedFileReader(stream, header);
        }

        private static TableDescriptor ReadTableChunk(IChunkReader chunk)
        {
            Debug.Assert(chunk.TypeId == TypeIds.TableNameChunk);
            var tableId = Serialiser.ReadGuid(chunk.Stream);
            Serialiser.AlignRead(chunk.Stream, 4);
            var tableName = Serialiser.ReadString(chunk.Stream);
            Serialiser.AlignRead(chunk.Stream, 4);
            var tableSchema = Serialiser.ReadString(chunk.Stream);
            return new TableDescriptor { Id = tableId, Name = tableName, Schema = tableSchema };
        }

        private static TableColumns ReadColumnsChunk(IChunkReader chunk)
        {
            Debug.Assert(chunk.TypeId == TypeIds.ColumnsChunk);
            var tableId = Serialiser.ReadGuid(chunk.Stream);
            var count = Serialiser.ReadInt16(chunk.Stream);
            Serialiser.ReadInt16(chunk.Stream);
            var columns = new ColumnDescriptor[count];
            for (var i = 0; i < columns.Length; i++)
            {
                Serialiser.AlignRead(chunk.Stream, 4);
                var column = new ColumnDescriptor();
                column.OriginalIndex = Serialiser.ReadInt16(chunk.Stream);
                column.ColumnFlags = (ColumnFlags)Serialiser.ReadInt16(chunk.Stream);
                column.StoredDataType = (ColumnDataType)Serialiser.ReadInt32(chunk.Stream);
                column.Length = Serialiser.ReadInt32(chunk.Stream);
                column.OriginalName = Serialiser.ReadString(chunk.Stream);
                columns[i] = column;
            }
            return new TableColumns {
                TableId = tableId,
                Columns = columns
            };
        }

        private bool ReadUpToNextBulkData()
        {
            EnsureReader();
            while (reader.MoveNext())
            {
                if (reader.Current.TypeId == TypeIds.TableNameChunk)
                {
                    var table = ReadTableChunk(reader.Current);
                    tables.Add(table.Id, table);
                }
                else if (reader.Current.TypeId == TypeIds.ColumnsChunk)
                {
                    var columns = ReadColumnsChunk(reader.Current);
                    this.tableColumns.Add(columns.TableId, columns);
                }
                else if (reader.Current.TypeId == TypeIds.RowDataChunk)
                {
                    return true;
                }
            }
            return false;
        }

        private BulkTableData BeginReadBulkData(IChunkReader chunk)
        {
            if (chunk.TypeId != TypeIds.RowDataChunk) throw new InvalidOperationException("Current chunk is not bulk row data.");
            var tableId = Serialiser.ReadGuid(chunk.Stream);
            if (!tables.TryGetValue(tableId, out var table)) throw new NotSupportedException($"Row data was found before corresponding table descriptor: {tableId}");
            if (!tableColumns.TryGetValue(tableId, out var columns)) throw new NotSupportedException($"Row data was found before corresponding column list: {tableId}");
            return new BulkTableData(table, columns, chunk);
        }

        public bool MoveNext()
        {
            current = null;
            if (!ReadUpToNextBulkData()) return false;
            current = BeginReadBulkData(reader.Current);
            return true;
        }

        private BulkTableData current;
        public IBulkTableData Current => current;

        public void Dispose()
        {
            if (!leaveOpen)
            {
                stream?.Dispose();
            }
        }

        class BulkTableData : IBulkTableData
        {
            public BulkTableData(TableDescriptor table, TableColumns columns, IChunkReader reader)
            {
                Debug.Assert(table.Id == columns.TableId);
                Table = table;
                Columns = new ReadOnlyCollection<ColumnDescriptor>(columns.Columns);
                DataStream = reader.Stream.WindowRemaining();
            }

            public TableDescriptor Table { get; }
            public IList<ColumnDescriptor> Columns { get; }
            public Stream DataStream { get; }
        }
    }
}
