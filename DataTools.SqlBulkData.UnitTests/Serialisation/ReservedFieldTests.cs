using System.IO;
using DataTools.SqlBulkData.Serialisation;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests.Serialisation
{
    [TestFixture]
    public class ReservedFieldTests
    {
        [Test]
        public void ReservesSpaceForField()
        {
            var stream = new MemoryStream();
            Serialiser.WriteByte(stream, 0x01);
            new ReservedField<byte>(stream, Serialiser.WriteByte);
            Serialiser.WriteByte(stream, 0x03);

            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x01, 0x00, 0x03 }));
        }

        [Test]
        public void WritesValueIntoReservedSpace()
        {
            var stream = new MemoryStream();
            Serialiser.WriteByte(stream, 0x01);
            var field = new ReservedField<byte>(stream, Serialiser.WriteByte);
            Serialiser.WriteByte(stream, 0x03);
            field.Write(0x02);

            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x01, 0x02, 0x03 }));
        }

        [Test]
        public void RestoresPositionAfterWritingValue()
        {
            var stream = new MemoryStream();
            Serialiser.WriteByte(stream, 0x01);
            var field = new ReservedField<byte>(stream, Serialiser.WriteByte);
            Serialiser.WriteByte(stream, 0x03);

            Assume.That(stream.Position, Is.EqualTo(3));

            field.Write(0x02);

            Assert.That(stream.Position, Is.EqualTo(3));
        }
    }
}
