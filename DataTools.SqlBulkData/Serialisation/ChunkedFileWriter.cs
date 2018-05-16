using System;
using System.Diagnostics;
using System.IO;

namespace DataTools.SqlBulkData.Serialisation
{
    public class ChunkedFileWriter
    {
        private readonly Stream stream;
        private IChunkWriter currentChunk;

        public ChunkedFileWriter(Stream stream)
        {
            if (!stream.CanWrite) throw new ArgumentException("Stream is not writable.", nameof(stream));
            if (!stream.CanSeek) throw new ArgumentException("Stream is not seekable.", nameof(stream));
            this.stream = stream;
        }

        public IChunkWriter BeginChunk(uint chunkTypeId)
        {
            if (currentChunk != null)
            {
                throw new InvalidOperationException("Cannot begin a new chunk while the previous one is still open.");
            }
            Serialiser.AlignWrite(stream, 8);
            currentChunk = new ChunkWriter(this, stream, chunkTypeId);
            return currentChunk;
        }

        class ChunkWriter : IChunkWriter
        {
            public uint TypeId { get; }
            public Stream Stream { get; }

            private readonly ChunkedFileWriter parent;
            /// <summary>
            /// The offset at which the content of this chunk starts.
            /// </summary>
            private readonly long currentStartOffset;
            private ReservedField<long> lengthField;

            public ChunkWriter(ChunkedFileWriter parent, Stream stream, uint typeId)
            {
                if (stream.Position % 8 != 0) throw new ArgumentException("Current position is not 8-byte-aligned.");
                this.parent = parent;
                this.Stream = stream;
                this.TypeId = typeId;

                Serialiser.WriteUInt32(stream, typeId);
                Serialiser.WriteUInt32(stream, 0);
                lengthField = new ReservedField<long>(stream, Serialiser.WriteInt64);
                currentStartOffset = stream.Position;
            }

            public void Dispose()
            {
                lengthField.Write(Stream.Position - currentStartOffset);
                parent.EndChunk(this);
            }
        }

        private void EndChunk(ChunkWriter chunkWriter)
        {
            if (!ReferenceEquals(chunkWriter, currentChunk))
            {
                Debug.Fail("Attempted to end a chunk which was not current.");
                return;
            }
            currentChunk = null;
        }
    }
}
