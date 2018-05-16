using System;
using System.IO;

namespace DataTools.SqlBulkData.Serialisation
{
    public struct ReservedField<T> where T : struct
    {
        private readonly Stream stream;
        private readonly Action<Stream, T> write;
        private readonly long mark;

        public ReservedField(Stream stream, Action<Stream, T> write)
        {
            if (!stream.CanWrite) throw new ArgumentException("Stream is not writable.", nameof(stream));
            if (!stream.CanSeek) throw new ArgumentException("Stream is not seekable.", nameof(stream));
            this.stream = stream;
            this.write = write;
            mark = stream.Position;
            write(stream, default(T));
        }

        public void Write(T value)
        {
            var currentPosition = stream.Position;
            stream.Seek(mark, SeekOrigin.Begin);
            write(stream, value);
            stream.Seek(currentPosition, SeekOrigin.Begin);
        }
    }
}
