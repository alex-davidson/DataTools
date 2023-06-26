using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

namespace DataTools.SqlBulkData.Serialisation
{
    /// <summary>
    /// Serialises primitive types in accordance with the rules in README.md. Asserts against alignment by default in DEBUG builds.
    /// </summary>
    public static class Serialiser
    {
        private const int BufferSize = 16;
        private const int MaximumAlignment = 16;
        private static readonly ThreadLocal<byte[]> sharedBuffer = new ThreadLocal<byte[]>(() => new byte[BufferSize]);
        private static readonly byte[] readAlignBuffer = new byte[MaximumAlignment];
        private static readonly byte[] writeAlignBuffer = new byte[MaximumAlignment];
        private static readonly byte[] largeSeekBuffer = new byte[4 * 1024 * 1024];

        public static bool IsAligned(Stream s, int alignment) => s.Position % alignment == 0;

        /// <summary>
        /// Seek forwards to the specified position. Use reads instead if the stream does
        /// not support seeking.
        /// </summary>
        public static void SeekReadForwards(Stream stream, long position)
        {
            if (stream.CanSeek)
            {
                stream.Seek(position, SeekOrigin.Begin);
                return;
            }
            // Unseekable streams will generally throw when we try to get Position, but if
            // this is one of our wrapper position-tracking classes it will work fine:
            var adjustment = position - stream.Position;
            if (adjustment < 0) throw new NotSupportedException("Specified position is before current position.");
            if (!SeekReadForwardsInternal(stream, largeSeekBuffer, adjustment)) throw new EndOfStreamException();
        }

        private static bool SeekReadForwardsInternal(Stream stream, byte[] scratchBuffer, long distance)
        {
            while (distance > scratchBuffer.Length)
            {
                var count = stream.Read(scratchBuffer, 0, scratchBuffer.Length);
                if (count <= 0) return false;
                distance -= count;
            }
            var lastCount = stream.Read(scratchBuffer, 0, (int)distance);
            return lastCount == distance;
        }

        /// <summary>
        /// Seek forwards as necessary to align the next read as specified.
        /// Throws EndOfStreamException if the seek would go beyond the end of the stream.
        /// </summary>
        public static void AlignRead(Stream stream, int alignment)
        {
            if (TryAlignRead(stream, alignment)) return;
            throw new EndOfStreamException();
        }

        /// <summary>
        /// Seek forwards as necessary to align the next read as specified.
        /// </summary>
        /// <returns>False if the seek would go beyond the end of the stream.</returns>
        public static bool TryAlignRead(Stream stream, int alignment)
        {
            if (!stream.CanRead) throw new NotSupportedException("Stream is not readable.");
            var adjustment = GetAlignmentAdjustment(stream, alignment);
            Debug.Assert(adjustment <= MaximumAlignment);
            return SeekReadForwardsInternal(stream, readAlignBuffer, adjustment);
        }

        /// <summary>
        /// Write empty bytes as necessary to align the next write as specified.
        /// </summary>
        public static void AlignWrite(Stream stream, int alignment)
        {
            if (!stream.CanWrite) throw new NotSupportedException("Stream is not readable.");
            var adjustment = GetAlignmentAdjustment(stream, alignment);
            if (adjustment == 0) return;

            Debug.Assert(adjustment <= MaximumAlignment);
            stream.Write(writeAlignBuffer, 0, adjustment);
        }

        public static bool IsEndOfStream(Stream stream) => stream.Position >= stream.Length;

        public static DateTime ReadDateTime(Stream stream) => DateTime.FromBinary(ReadInt64(stream));
        public static void WriteDateTime(Stream stream, DateTime value) => WriteInt64(stream, value.ToBinary());

        public static DateTimeOffset ReadDateTimeOffset(Stream stream)
        {
            var dateTime = ReadDateTime(stream);
            var offsetMinutes = ReadInt32(stream);
            return new DateTimeOffset(dateTime, TimeSpan.FromMinutes(offsetMinutes));
        }
        public static void WriteDateTimeOffset(Stream stream, DateTimeOffset value)
        {
            WriteDateTime(stream, value.DateTime);
            WriteInt32(stream, (int)value.Offset.TotalMinutes);
        }

        public static Guid ReadGuid(Stream stream) => new Guid(ReadFixedLengthBytesInternal(stream, 16, 8));
        public static void WriteGuid(Stream stream, Guid value) => WriteFixedLengthBytesInternal(stream, value.ToByteArray(), 8);

        public static float ReadSingle(Stream stream) => FromBytes(ReadFixedLengthBytesInternal(stream, 4), BitConverter.ToSingle);
        public static void WriteSingle(Stream stream, float value) => WriteFixedLengthBytesInternal(stream, ToBytes(value, BitConverter.GetBytes));
        public static double ReadDouble(Stream stream) => FromBytes(ReadFixedLengthBytesInternal(stream, 8, 4), BitConverter.ToDouble);
        public static void WriteDouble(Stream stream, double value) => WriteFixedLengthBytesInternal(stream, ToBytes(value, BitConverter.GetBytes), 4);

        public static long ReadInt64(Stream stream) => FromBytes(ReadFixedLengthBytesInternal(stream, 8, 4), BitConverter.ToInt64);
        public static void WriteInt64(Stream stream, long value) => WriteFixedLengthBytesInternal(stream, ToBytes(value, BitConverter.GetBytes), 4);
        public static ulong ReadUInt64(Stream stream) => FromBytes(ReadFixedLengthBytesInternal(stream, 8, 4), BitConverter.ToUInt64);
        public static void WriteUInt64(Stream stream, ulong value) => WriteFixedLengthBytesInternal(stream, ToBytes(value, BitConverter.GetBytes), 4);

        public static int ReadInt32(Stream stream) => FromBytes(ReadFixedLengthBytesInternal(stream, 4), BitConverter.ToInt32);
        public static void WriteInt32(Stream stream, int value) => WriteFixedLengthBytesInternal(stream, ToBytes(value, BitConverter.GetBytes));
        public static uint ReadUInt32(Stream stream) => FromBytes(ReadFixedLengthBytesInternal(stream, 4),  BitConverter.ToUInt32);
        public static void WriteUInt32(Stream stream, uint value) => WriteFixedLengthBytesInternal(stream, ToBytes(value, BitConverter.GetBytes));

        public static short ReadInt16(Stream stream) => FromBytes(ReadFixedLengthBytesInternal(stream, 2), BitConverter.ToInt16);
        public static void WriteInt16(Stream stream, short value) => WriteFixedLengthBytesInternal(stream, ToBytes(value, BitConverter.GetBytes));
        public static ushort ReadUInt16(Stream stream) => FromBytes(ReadFixedLengthBytesInternal(stream, 2), BitConverter.ToUInt16);
        public static void WriteUInt16(Stream stream, ushort value) => WriteFixedLengthBytesInternal(stream, ToBytes(value, BitConverter.GetBytes));

        public static byte ReadByte(Stream stream)
        {
            var value = stream.ReadByte();
            if (value < byte.MinValue || value > byte.MaxValue) throw new EndOfStreamException();
            return (byte)value;
        }
        public static void WriteByte(Stream stream, byte value) => stream.WriteByte(value);

        public static string ReadString(Stream stream)
        {
            var stringBuffer = ReadBytes(stream);
            return Encoding.UTF8.GetString(stringBuffer);
        }
        public static void WriteString(Stream stream, string value)
        {
            var stringBuffer = Encoding.UTF8.GetBytes(value);
            WriteBytes(stream, stringBuffer);
        }

        public static byte[] ReadFixedLengthBytes(Stream stream, byte[] buffer) => ReadFixedLengthBytesInternal(stream, buffer, 1);
        public static void WriteFixedLengthBytes(Stream stream, byte[] value) => WriteFixedLengthBytesInternal(stream, value, 1);

        public static byte[] ReadBytes(Stream stream) => ReadVariableLengthBuffer(stream);
        public static void WriteBytes(Stream stream, byte[] value) => WriteVariableLengthBuffer(stream, value);

        private static byte[] ReadFixedLengthBytesInternal(Stream stream, int count) => ReadFixedLengthBytesInternal(stream, count, count);
        private static byte[] ReadFixedLengthBytesInternal(Stream stream, int count, int alignment)
        {
            Debug.Assert(count <= BufferSize);
            CheckAlignment(stream, alignment);
            var buffer = sharedBuffer.Value;
            CheckedRead(stream, buffer, count);
            return buffer;
        }
        private static byte[] ReadFixedLengthBytesInternal(Stream stream, byte[] buffer, int alignment)
        {
            Debug.Assert(buffer != sharedBuffer.Value);
            CheckAlignment(stream, alignment);
            CheckedRead(stream, buffer, buffer.Length);
            return buffer;
        }

        private static void WriteFixedLengthBytesInternal(Stream stream, byte[] bytes) => WriteFixedLengthBytesInternal(stream, bytes, bytes.Length);
        private static void WriteFixedLengthBytesInternal(Stream stream, byte[] bytes, int alignment)
        {
            CheckAlignment(stream, alignment);
            stream.Write(bytes, 0, bytes.Length);
        }

        private static byte[] ReadVariableLengthBuffer(Stream stream)
        {
            var length = ReadInt32(stream);
            var buffer = new byte[length];
            CheckedRead(stream, buffer, buffer.Length);
            return buffer;
        }

        private static void WriteVariableLengthBuffer(Stream stream, byte[] buffer)
        {
            WriteInt32(stream, buffer.Length);
            stream.Write(buffer, 0, buffer.Length);
        }

        private static T FromBytes<T>(byte[] bytes, Func<byte[], int, T> convert)
        {
            if (!BitConverter.IsLittleEndian) Array.Reverse(bytes);
            return convert(bytes, 0);
        }

        private static byte[] ToBytes<T>(T value, Func<T, byte[]> convert)
        {
            var bytes = convert(value);
            if (!BitConverter.IsLittleEndian) Array.Reverse(bytes);
            return bytes;
        }

        private static void CheckedRead(Stream stream, byte[] buffer, int count)
        {
            Debug.Assert(count <= buffer.Length);
            var read = stream.Read(buffer, 0, count);
            if (read < count) throw new EndOfStreamException();
        }

        private static int GetAlignmentAdjustment(Stream stream, int alignment)
        {
            if (alignment <= 0) throw new ArgumentException("Alignment must be positive.", nameof(alignment));
            if (alignment > MaximumAlignment) throw new ArgumentException($"Alignment cannot exceed {MaximumAlignment}.", nameof(alignment));

            var misalignment = stream.Position % alignment;
            if (misalignment == 0) return 0;
            var adjustment = alignment - misalignment;
            Debug.Assert(adjustment <= MaximumAlignment);
            return (int)adjustment;
        }

        public static bool EnableParanoidAlignmentChecks { get; set; } = false;

        private static void CheckAlignment(Stream s, int alignment)
        {
            if (!EnableParanoidAlignmentChecks) return;
            if (alignment <= 1) return; // No alignment check required. Avoid expensive call to Position.
            if (!IsAligned(s, alignment)) throw new InvalidOperationException($"Stream position is not aligned with a {alignment}-byte boundary.");
        }

        static Serialiser()
        {
            EnableParanoidAlignmentChecksInDebugBuilds();
        }

        [Conditional("DEBUG")]
        private static void EnableParanoidAlignmentChecksInDebugBuilds()
        {
            EnableParanoidAlignmentChecks = true;
        }
    }
}
