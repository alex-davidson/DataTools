using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
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
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            this.stream = PrepareReadOnlyStream(stream, leaveOpen);
            this.leaveOpen = leaveOpen;
        }

        /// <summary>
        /// Wrap the provided stream as necessary to track its position, which is required for
        /// eg. alignment checks. If the stream has a GZip header, unwrap it automatically.
        /// </summary>
        /// <remarks>
        /// For unseekable streams we have to assume we're starting at position 0. If the
        /// starting position is not 8-byte-aligned, neither can any reads.
        /// </remarks>
        private static Stream PrepareReadOnlyStream(Stream stream, bool leaveOpen)
        {
            if (!stream.CanSeek) return new PositionTrackingReadOnlyStream(stream, 0, leaveOpen);

            var position = stream.Position;
            var first = stream.ReadByte();
            var second = stream.ReadByte();
            stream.Position = position;

            if (first == 0x1f && second == 0x8b)    // GZip header.
            {
                var decompressed = new GZipStream(stream, CompressionMode.Decompress, leaveOpen);
                return new PositionTrackingReadOnlyStream(decompressed, 0, leaveOpen);
            }
            return new FastReadOnlyStream(stream);
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
