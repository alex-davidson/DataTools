using System;
using System.Data.SqlTypes;
using System.Diagnostics;

namespace DataTools.SqlBulkData.Serialisation
{
    public class DecimalPacker
    {
        private const int SqlDecimalByteCount = 16;
        private const int SqlDecimalPrecisionLimit = 38;

        private readonly int digitCount;
        private readonly int significandBytes;
        private readonly int bufferSize;
        private readonly int[] intBuffer = new int[SqlDecimalByteCount / 4];

        static DecimalPacker()
        {
            if (SqlDecimal.MaxValue.BinData.Length != SqlDecimalByteCount) throw new ApplicationException("VIOLATED ASSUMPTION: SqlDecimal buffer is not 16 bytes in length.");
            if (SqlDecimal.MaxValue.Precision != SqlDecimalPrecisionLimit) throw new ApplicationException("VIOLATED ASSUMPTION: SqlDecimal precision limit is not 38.");
        }

        public static DecimalPacker ForDigitCount(int digitCount) => new DecimalPacker(digitCount, GetByteBufferSize(digitCount));
        public static DecimalPacker ForBufferSize(int bufferSizeBytes) => new DecimalPacker(GetDigitCount(bufferSizeBytes), bufferSizeBytes);

        private DecimalPacker(int digitCount, int bufferSize)
        {
            if (bufferSize % 4 != 0) throw new ArgumentException("Buffer size must be a multiple of 4 bytes.");
            this.digitCount = digitCount;
            this.bufferSize = bufferSize;
            significandBytes = bufferSize - 1;
        }

        public byte[] CreateBuffer() => new byte[bufferSize];
        public int ByteCount => bufferSize;
        public int DigitCount => digitCount;

        /// <summary>
        /// Pack an SqlDecimal into the specified buffer as a floating-point decimal.
        /// </summary>
        public void Pack(SqlDecimal fromDecimal, byte[] toBytes)
        {
            var fromBytes = fromDecimal.BinData;
            Debug.Assert(fromBytes.Length == SqlDecimalByteCount);
            if (toBytes.Length != bufferSize) throw new ArgumentException($"Incorrect buffer size. Expected {bufferSize}, got {toBytes.Length}.");

            for (var i = significandBytes; i < SqlDecimalByteCount; i++)
            {
                if (fromBytes[i] != 0) throw BufferTooSmallException(fromDecimal);
            }

            var count = Math.Min(bufferSize, SqlDecimalByteCount);
            var signMask = fromDecimal.IsPositive ? 0 : -1;
            BufferUtils.ApplySignToUnsigned(fromBytes, toBytes, count, signMask);
            toBytes[significandBytes] = (byte)(1 + ~fromDecimal.Scale);
        }

        /// <summary>
        /// Unpack a floating-point decimal from the specified buffer. NOTE: Modifies the buffer!
        /// </summary>
        public SqlDecimal Unpack(byte[] fromBytes)
        {
            if (fromBytes.Length != bufferSize) throw new ArgumentException($"Incorrect buffer size. Expected {bufferSize}, got {fromBytes.Length}.");

            var scale = (byte)(1 + ~fromBytes[significandBytes]);
            var signMask = (fromBytes[significandBytes - 1] & 0x80) == 0 ? 0: -1;
            for (var i = SqlDecimalByteCount; i < significandBytes; i++)
            {
                if (fromBytes[i] != (byte)signMask) throw SqlDecimalTooSmallException(fromBytes);
            }
            fromBytes[significandBytes] = (byte)signMask;

            var count = Math.Min(bufferSize, SqlDecimalByteCount);
            BufferUtils.UnapplySignFromSigned(fromBytes, intBuffer, count, signMask);

            var precision = (byte)Math.Max(scale, Math.Min(digitCount, SqlDecimalPrecisionLimit));
            return new SqlDecimal(precision, scale, signMask == 0, intBuffer);
        }

        private InvalidCastException BufferTooSmallException(SqlDecimal fromDecimal) =>
            new InvalidCastException($"The SqlDecimal value has precision {fromDecimal.Precision} but this DecimalPacker expects at most {digitCount}: {fromDecimal}");

        private InvalidCastException SqlDecimalTooSmallException(byte[] fromBytes) =>
            new InvalidCastException($"The SqlDecimal value can support at most 38 digits (128 bits) but the buffer provided contains up to {(fromBytes.Length - 1)/ 8} bits.");

        private static readonly float log2Of10 = (float)(Math.Log(10) / Math.Log(2));
        private static readonly float log10Of2 = (float)(Math.Log(2) / Math.Log(10));

        public static int GetNumberOfBitsForSignedIntegersOfLength(int digitCount) => GetNumberOfBitsForUnsignedIntegersOfLength(digitCount) + 1;
        public static int GetNumberOfBitsForUnsignedIntegersOfLength(int digitCount) => (int)Math.Ceiling(digitCount * log2Of10);
        public static int GetNumberOfDigitsForBufferOfLength(int byteCount) => GetDigitCount(byteCount);

        private static int GetByteBufferSize(int digitCount)
        {
            var significandBits = GetNumberOfBitsForSignedIntegersOfLength(digitCount);
            const int exponentBits = 8;
            return 4 * (1 + (significandBits + exponentBits - 1) / 32);
        }

        private static int GetDigitCount(int bufferSize)
        {
            const int exponentBits = 8;
            var significandBits = (bufferSize * 8) - exponentBits;
            var unsignedSignificandBits = significandBits - 1;
            return (int)Math.Ceiling((unsignedSignificandBits - 1) * log10Of2);
        }
    }
}
