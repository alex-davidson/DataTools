using System;
using System.IO;

namespace DataTools.SqlBulkData
{
    /// <summary>
    /// Wrapper for reading an unseekable stream for which the initial position is known.
    /// </summary>
    class PositionTrackingReadOnlyStream : Stream
    {
        private readonly Stream underlying;
        private long position;
        private readonly bool leaveOpen;

        public PositionTrackingReadOnlyStream(Stream underlying, long position, bool leaveOpen)
        {
            if (!underlying.CanRead) throw new ArgumentException("Not a readable stream.", nameof(underlying));
            if (underlying.CanSeek) throw new ArgumentException("Overriding position and length of seekable streams is not allowed.", nameof(underlying));
            this.underlying = underlying;
            this.position = position;
            this.leaveOpen = leaveOpen;
        }

        public override void Flush() => underlying.Flush();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException("Unseekable stream.");
        public override void SetLength(long value) => throw new NotSupportedException("Read-only stream.");

        public override int Read(byte[] buffer, int offset, int count)
        {
            var readCount = underlying.Read(buffer, offset, count);
            position += readCount;
            return readCount;
        }

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException("Read-only stream.");

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException("Unseekable stream.");

        public override long Position
        {
            get => position;
            set => throw new NotSupportedException("Unseekable stream.");
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !leaveOpen) underlying.Dispose();
            base.Dispose(disposing);
        }
    }
}
