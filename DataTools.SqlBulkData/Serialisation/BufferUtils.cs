using System.Diagnostics;

namespace DataTools.SqlBulkData.Serialisation
{
    internal class BufferUtils
    {
        internal static unsafe void ApplySignToUnsigned(byte[] source, byte[] target, int byteCount, int signMask)
        {
            Debug.Assert(byteCount % 4 == 0);
            Debug.Assert(target.Length % 4 == 0);
            fixed (byte* sourceBuffer = source)
            fixed (byte* targetBuffer = target)
            {
                ApplySignMultiplierToTwosComplement((int*)sourceBuffer, (int*)targetBuffer, byteCount / 4, signMask);
                for (var i = byteCount; i < target.Length; i++) targetBuffer[i] = (byte)signMask;
            }
        }

        private static unsafe void ApplySignMultiplierToTwosComplement(int* source, int* target, int count, int signMask)
        {
            var carry = 0;
            for (var i = 0; i < count; i++)
            {
                var twosComplement = (source[i] ^ signMask) - signMask;
                target[i] = twosComplement + carry;
                carry = twosComplement == 0 ? carry : signMask;
            }
        }

        internal static unsafe void UnapplySignFromSigned(byte[] source, int[] target, int byteCount, int signMask)
        {
            Debug.Assert(byteCount % 4 == 0);
            fixed (byte* sourceBuffer = source)
            fixed (int* targetBuffer = target)
            {
                ApplySignMultiplierToTwosComplement((int*)sourceBuffer, targetBuffer, byteCount / 4, signMask);
                for (var i = byteCount / 4; i < target.Length; i++) target[i] = 0;
            }
        }
    }
}
