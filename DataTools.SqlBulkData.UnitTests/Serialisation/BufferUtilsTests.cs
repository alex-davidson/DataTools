using System;
using DataTools.SqlBulkData.Serialisation;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests.Serialisation
{
    public class BufferUtilsTests
    {
        [TestCase(9UL, 9L)]
        [TestCase(9UL, -9L)]
        [TestCase(999UL, 999L)]
        [TestCase(999UL, -999L)]
        [TestCase(9999999UL, 9999999L)]
        [TestCase(9999999UL, -9999999L)]
        [TestCase(999999999999UL, 999999999999L)]
        [TestCase(999999999999UL, -999999999999L)]
        [TestCase(9999999999999999UL, 9999999999999999L)]
        [TestCase(9999999999999999UL, -9999999999999999L)]
        [TestCase(494336UL, -494336L)]
        public void ApplySignToUnsigned(ulong unsigned, long signed)
        {
            var unsignedBuffer = BitConverter.GetBytes(unsigned);
            var signedBuffer = new byte[8];
            BufferUtils.ApplySignToUnsigned(unsignedBuffer, signedBuffer, 8, signed < 0 ? -1 : 0);
            Assert.That(signedBuffer, Is.EqualTo(BitConverter.GetBytes(signed)));
        }

        [TestCase(9UL, 9L)]
        [TestCase(9UL, -9L)]
        [TestCase(999UL, 999L)]
        [TestCase(999UL, -999L)]
        [TestCase(9999999UL, 9999999L)]
        [TestCase(9999999UL, -9999999L)]
        [TestCase(999999999999UL, 999999999999L)]
        [TestCase(999999999999UL, -999999999999L)]
        [TestCase(9999999999999999UL, 9999999999999999L)]
        [TestCase(9999999999999999UL, -9999999999999999L)]
        [TestCase(494336UL, -494336L)]
        public void UnapplySignFromSigned(ulong unsigned, long signed)
        {
            var signedBuffer = BitConverter.GetBytes(signed);
            var unsignedBuffer = new int[2];
            BufferUtils.UnapplySignFromSigned(signedBuffer, unsignedBuffer, 8, signed < 0 ? -1 : 0);
            var combinedUnsignedBuffer = new byte[8];
            Array.Copy(BitConverter.GetBytes(unsignedBuffer[0]), 0, combinedUnsignedBuffer, 0, 4);
            Array.Copy(BitConverter.GetBytes(unsignedBuffer[1]), 0, combinedUnsignedBuffer, 4, 4);
            Assert.That(combinedUnsignedBuffer, Is.EqualTo(BitConverter.GetBytes(unsigned)));
        }
    }
}
