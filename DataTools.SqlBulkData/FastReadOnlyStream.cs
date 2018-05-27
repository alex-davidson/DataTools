using System;
using System.IO;

namespace DataTools.SqlBulkData
{
    /// <summary>
    /// Caches length and position for fast alignment checks.
    /// </summary>
    class FastReadOnlyStream : Stream
    {
        private readonly Stream underlying;
        private long position;

        public FastReadOnlyStream(Stream underlying)
        {
            if (!underlying.CanRead) throw new ArgumentException("Not a readable stream.", nameof(underlying));
            this.underlying = underlying;
            Length = underlying.Length;
            position = underlying.Position;
        }

        public override void Flush() => underlying.Flush();

        public override long Seek(long offset, SeekOrigin origin)
        {
            position = underlying.Seek(offset, origin);
            return position;
        }

        public override void SetLength(long value) => throw new NotSupportedException("Read-only stream.");

        public override int Read(byte[] buffer, int offset, int count)
        {
            var readCount = underlying.Read(buffer, offset, count);
            position += readCount;
            return readCount;
        }

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException("Read-only stream.");

        public override bool CanRead => true;
        public override bool CanSeek => underlying.CanSeek;
        public override bool CanWrite => false;
        public override long Length { get; }

        public override long Position
        {
            get => position;
            set
            {
                underlying.Position = value;
                position = value;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) underlying.Dispose();
            base.Dispose(disposing);
        }
    }
}
