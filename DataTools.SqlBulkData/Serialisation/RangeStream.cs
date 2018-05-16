using System;
using System.IO;

namespace DataTools.SqlBulkData.Serialisation
{
    public class RangeStream  : Stream
    {
        private Stream stream;
        private readonly long startPosition;
        private readonly long length;

        public RangeStream(Stream stream, long startPosition, long length)
        {
            if (!stream.CanRead) throw new ArgumentException("Stream is not readable.", nameof(stream));
            ValidateRange(stream, startPosition, length);

            this.stream = stream;
            this.startPosition = startPosition;
            this.length = length;
        }

        private static void ValidateRange(Stream stream, long startPosition, long length)
        {
            if (startPosition < 0) throw new ArgumentOutOfRangeException(nameof(startPosition), "Offset cannot be negative.");
            if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), "Length cannot be negative.");

            var position = stream.Position - startPosition;
            if (position < 0) throw new InvalidOperationException("Range starts after current position in the underlying stream.");
            if (startPosition + length > stream.Length) throw new ArgumentOutOfRangeException(nameof(length), "Range extends beyond the end of the underlying stream.");
        }

        public override bool CanRead => stream.CanRead;
        public override bool CanSeek => stream.CanSeek;
        public override bool CanWrite => false;
        public override bool CanTimeout => stream.CanTimeout;

        public override long Length => length;

        public override long Position
        {
            get => stream.Position - startPosition;
            set
            {
                if (value < 0) throw new ArgumentOutOfRangeException(nameof(startPosition), "Position cannot be negative.");
                stream.Position = Math.Min(value, length) + startPosition;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    var clampedBeginOffset = Math.Max(Math.Min(length, offset), 0);
                    stream.Seek(startPosition + clampedBeginOffset, origin);
                    break;
                case SeekOrigin.End:
                    var clampedEndOffset = Math.Max(Math.Min(length, offset), 0);
                    stream.Seek(startPosition + length - clampedEndOffset, origin);
                    break;
                default:
                    var max = length - Position;
                    var min = -Position;
                    var clampedRelativeOffset = Math.Max(Math.Min(max, offset), min);
                    stream.Seek(clampedRelativeOffset, origin);
                    break;
            }
            return Position;
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var bytesAvailable = Length - Position;
            if (bytesAvailable <= 0) return 0;
            if (bytesAvailable < count)
            {
                return stream.Read(buffer, offset, Math.Min((int)bytesAvailable, count));
            }
            return stream.Read(buffer, offset, count);
        }

        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            var bytesAvailable = Length - Position;
            if (bytesAvailable < count)
            {
                return stream.BeginRead(buffer, offset, Math.Min((int)bytesAvailable, count), callback, state);
            }
            return stream.BeginRead(buffer, offset, count, callback, state);
        }

        public override int EndRead(IAsyncResult asyncResult) => stream.EndRead(asyncResult);

        protected sealed override void Dispose(bool disposing)
        {
            try
            {
                if (!disposing) return;
                stream = null;
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        public override int ReadTimeout
        {
            get => stream.ReadTimeout;
            set => throw new NotSupportedException("Cannot adjust ReadTimeout because the stream is a read-only range.");
        }

        public override int WriteTimeout
        {
            get => stream.ReadTimeout;
            set => throw new NotSupportedException("Cannot adjust WriteTimeout because the stream is a read-only range.");
        }

        public override void SetLength(long value) => throw new NotSupportedException("Stream is a read-only range.");

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException("Stream is a read-only range.");
        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state) => throw new NotSupportedException("Stream is a read-only range.");
        public override void EndWrite(IAsyncResult asyncResult) => throw new NotSupportedException("Stream is a read-only range.");
    }

    public static class RangeStreamExtensions
    {
        public static Stream Window(this Stream stream, long startOffset, long length)
        {
            return new RangeStream(stream, startOffset, length);
        }

        public static Stream WindowRemaining(this Stream stream)
        {
            return new RangeStream(stream, stream.Position, stream.Length - stream.Position);
        }
    }
}
