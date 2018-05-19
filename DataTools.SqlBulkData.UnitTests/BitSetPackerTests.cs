using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class BitSetPackerTests
    {
        private static int[] TestIndexes => new [] {
             0,  1,      3,
                 5,  6,
                 9, 10,
            12, 13,     15,
            16,     18,
        };
        private static bool[] GetTestBits() => new [] {
            true, false, true, false,
            false, true, true, false,
            true, false, true, false,
            true, true, false, true,
            true, true, true, false,
        };

        [Test]
        public void CanPackNoIndexes()
        {
            var packer = new BitSetPacker(new int[0]);
            var bytes = new byte[packer.PackedByteCount];

            packer.Pack(GetTestBits(), bytes);
            Assert.That(bytes, Is.Empty);
        }

        [Test]
        public void CanUnpackNoIndexes()
        {
            var packer = new BitSetPacker(new int[0]);
            var bytes = new byte[] { 0b11011001, 0b00001111 };

            var bits = new bool[GetTestBits().Length];
            packer.Unpack(bytes, bits);
            Assert.That(bits, Is.All.EqualTo(false));
        }

        [Test]
        public void PacksSpecifiedIndexes()
        {
            var packer = new BitSetPacker(TestIndexes);
            var bytes = new byte[packer.PackedByteCount];

            packer.Pack(GetTestBits(), bytes);

            Assert.That(bytes, Is.EqualTo(new [] { 0b11011001, 0b00001111 }));
        }

        [Test]
        public void UnpacksSpecifiedIndexes()
        {
            var packer = new BitSetPacker(TestIndexes);
            var bytes = new byte[] { 0b11011001, 0b00001111 };

            var bits = new bool[GetTestBits().Length];
            packer.Unpack(bytes, bits);

            Assert.That(bits, Is.EqualTo(new [] {
                true, false, false, false,
                false, true, true, false,
                false, false, true, false,
                true, true, false, true,
                true, false, true, false
            }));
        }

        [Test]
        public void DoesNotChangeAbsentIndexes()
        {
            var packer = new BitSetPacker(TestIndexes);
            var bytes = new byte[] { 0b11011001, 0b00001111 };

            var bits = GetTestBits();
            packer.Unpack(bytes, bits);

            Assert.That(bits, Is.EqualTo(GetTestBits()));
        }
    }
}
