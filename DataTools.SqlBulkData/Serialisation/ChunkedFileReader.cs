using System;
using System.Diagnostics;
using System.IO;

namespace DataTools.SqlBulkData.Serialisation
{
    public class ChunkedFileReader
    {
        private readonly Stream stream;

        /// <summary>
        /// Parses a series of chunks out of a stream.
        /// </summary>
        /// <remarks>
        /// The stream's header should already have been read, and the stream's
        /// initial position should be at the end of the header (ie. the start
        /// of the first chunk).
        /// </remarks>
        public ChunkedFileReader(Stream stream, ChunkedFileHeader header)
        {
            Header = header;
            this.stream = stream;
        }

        private ChunkReader current;
        public IChunkReader Current => current;

        public IChunkReader SeekTo(ChunkBookmark bookmark)
        {
            if (!stream.CanSeek) throw new NotSupportedException("Stream is not seekable.");
            var originalPosition = stream.Position;
            try
            {
                stream.Position = bookmark.StartOffset;
                if (!TryReadChunkHeader(out var header)) throw new InvalidOperationException("No valid chunk header found.");
                if (header.TypeId != bookmark.TypeId) throw new InvalidOperationException("Bookmark chunk type does not match that found.");
                SetCurrent(header);
                return Current;
            }
            catch
            {
                stream.Position = originalPosition;
                throw;
            }
        }

        public ChunkedFileHeader Header { get; }

        public bool MoveNext()
        {
            // If we have a current chunk, seek to the end.
            if (current != null)
            {
                Serialiser.SeekReadForwards(stream, current.Header.EndOffset);
                if (!Serialiser.TryAlignRead(stream, 8)) return false;
            }
            if (!TryReadChunkHeader(out var header)) return false;
            SetCurrent(header);
            return true;
        }

        private void SetCurrent(ChunkHeader header)
        {
            var chunkDataStart = header.StartOffset + HeaderSizeBytes;
            Debug.Assert(stream.Position == chunkDataStart);
            var newChunkReader = new ChunkReader(header, new RangeStream(stream, chunkDataStart, header.Length));
            current?.Invalidate();
            current = newChunkReader;
        }

        const int HeaderSizeBytes = 16;

        private bool TryReadChunkHeader(out ChunkHeader header)
        {
            if (!Serialiser.IsAligned(stream, 8)) throw new InvalidOperationException("A chunk header cannot begin at this position because it is not 8-byte-aligned.");
            header = new ChunkHeader();

            if (stream.CanSeek)
            {
                var remainingBytes = stream.Length - stream.Position;
                if (remainingBytes < HeaderSizeBytes) return false;
            }

            header.StartOffset = stream.Position;
            try
            {
                header.TypeId = Serialiser.ReadUInt32(stream);
                Serialiser.ReadUInt32(stream);
                header.Length = Serialiser.ReadInt64(stream);
                header.EndOffset = stream.Position + header.Length;
            }
            catch (EndOfStreamException)
            {
                if (stream.Position == header.StartOffset) return false;
                throw;
            }
            if (header.Length < 0) throw new InvalidOperationException("Chunk length is invalid. Not a real chunk?");
            return true;
        }

        class ChunkReader : IChunkReader
        {
            private readonly RangeStream stream;

            public ChunkReader(ChunkHeader header, RangeStream stream)
            {
                Header = header;
                this.stream = stream;
            }

            public ChunkBookmark GetBookmark() => new ChunkBookmark(Header.TypeId, Header.StartOffset);

            public uint TypeId => Header.TypeId;
            public ChunkHeader Header { get; }
            public Stream Stream => stream;

            public void Invalidate()
            {
                stream.Dispose();
            }
        }

        struct ChunkHeader
        {
            /// <summary>
            /// Offset of the chunk's header.
            /// </summary>
            public long StartOffset { get; set; }
            /// <summary>
            /// Offset of the end of the chunk's data section.
            /// </summary>
            public long EndOffset { get; set; }

            /// <summary>
            /// Length of the chunk, minus the header.
            /// </summary>
            public long Length { get; set; }

            public uint TypeId { get; set; }
        }
    }
}
