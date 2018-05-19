using System;
using System.Diagnostics;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData
{
    public class BulkTableFileWriter : IDisposable
    {
        private readonly Stream stream;
        private readonly bool leaveOpen;
        private ChunkedFileHeader header;
        private readonly ChunkedFileWriter writer;

        public BulkTableFileWriter(Stream stream, bool leaveOpen = false)
        {
            this.stream = stream ?? throw new ArgumentNullException(nameof(stream));
            this.leaveOpen = leaveOpen;
            this.header = new ChunkedFileHeaderSerialiser().WriteHeader(stream, TypeIds.FileHeader);
            writer = new ChunkedFileWriter(stream);
        }

        public void AddTable(TableDescriptor table)
        {
            using (var chunk = writer.BeginChunk(TypeIds.TableNameChunk))
            {
                Serialiser.WriteGuid(chunk.Stream, table.Id);
                Serialiser.AlignWrite(chunk.Stream, 4);
                Serialiser.WriteString(chunk.Stream, table.Name);
                Serialiser.AlignWrite(chunk.Stream, 4);
                Serialiser.WriteString(chunk.Stream, table.Schema);
            }
        }

        public void AddColumns(TableColumns columns)
        {
            if (columns.Columns.Length > short.MaxValue) throw new ArgumentOutOfRangeException(nameof(columns), $"Too many columns: {columns.Columns.Length} > {short.MaxValue}");
            const int columnDescriptorLengthMinusName = 12;
            using (var chunk = writer.BeginChunk(TypeIds.ColumnsChunk))
            {
                Serialiser.WriteGuid(chunk.Stream, columns.TableId);
                Serialiser.WriteInt16(chunk.Stream, (short)columns.Columns.Length);
                Serialiser.WriteInt16(chunk.Stream, columnDescriptorLengthMinusName);
                foreach (var column in columns.Columns)
                {
                    Serialiser.AlignWrite(chunk.Stream, 4);
                    var position = chunk.Stream.Position;
                    Serialiser.WriteInt16(chunk.Stream, column.OriginalIndex);
                    Serialiser.WriteInt16(chunk.Stream, (short)column.ColumnFlags);
                    Serialiser.WriteInt32(chunk.Stream, (int)column.StoredDataType);
                    Serialiser.WriteInt32(chunk.Stream, column.Length);
                    Debug.Assert(chunk.Stream.Position == columnDescriptorLengthMinusName + position);
                    Serialiser.WriteString(chunk.Stream, column.OriginalName);
                }
            }
        }

        public IChunkWriter BeginAddRowData(Guid tableId)
        {
            var chunk = writer.BeginChunk(TypeIds.RowDataChunk);
            Serialiser.WriteGuid(chunk.Stream, tableId);
            return chunk;
        }

        public void Dispose()
        {
            if (!leaveOpen)
            {
                stream?.Dispose();
            }
        }
    }
}
