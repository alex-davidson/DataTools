using System;
using System.IO;
using System.Linq;
using DataTools.SqlBulkData.Serialisation;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests.Serialisation
{
    [TestFixture]
    public class SerialiserTests
    {
        [Test]
        public void RoundTripsGuid()
        {
            var stream = new MemoryStream();
            var guid = new Guid("{00112233-4455-6677-8899-AABBCCDDEEFF}");
            Serialiser.WriteGuid(stream, guid);
            stream.Position = 0;

            Assert.That(Serialiser.ReadGuid(stream), Is.EqualTo(guid));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] {
                0x33, 0x22, 0x11, 0x00,
                0x55, 0x44,
                0x77, 0x66, 
                0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF
            }));
        }

        [Test]
        public void RoundTripsSinglePrecision()
        {
            var stream = new MemoryStream();
            Serialiser.WriteSingle(stream, (float)Math.E);
            stream.Position = 0;

            Assert.That(Serialiser.ReadSingle(stream), Is.EqualTo((float)Math.E));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x54, 0xF8, 0x2D, 0x40 }));
        }

        [Test]
        public void RoundTripsDoublePrecision()
        {
            var stream = new MemoryStream();
            Serialiser.WriteDouble(stream, Math.PI);
            stream.Position = 0;

            Assert.That(Serialiser.ReadDouble(stream), Is.EqualTo(Math.PI));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x18, 0x2D, 0x44, 0x54, 0xFB, 0x21, 0x09, 0x40 }));
        }

        [Test]
        public void RoundTripsInt64()
        {
            var stream = new MemoryStream();
            Serialiser.WriteInt64(stream, -7757820597154693433);  // 0x9456AF450362BEC7
            stream.Position = 0;

            Assert.That(Serialiser.ReadInt64(stream), Is.EqualTo(-7757820597154693433));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0xC7, 0xBE, 0x62, 0x03, 0x45, 0xAF, 0x56, 0x94 }));
        }

        [Test]
        public void RoundTripsUInt64()
        {
            var stream = new MemoryStream();
            Serialiser.WriteUInt64(stream, 0x9456AF450362BEC7);
            stream.Position = 0;

            Assert.That(Serialiser.ReadUInt64(stream), Is.EqualTo(0x9456AF450362BEC7));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0xC7, 0xBE, 0x62, 0x03, 0x45, 0xAF, 0x56, 0x94 }));
        }

        [Test]
        public void RoundTripsInt32()
        {
            var stream = new MemoryStream();
            Serialiser.WriteInt32(stream, -1806258363);  // 0x9456AF45
            stream.Position = 0;

            Assert.That(Serialiser.ReadInt32(stream), Is.EqualTo(-1806258363));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x45, 0xAF, 0x56, 0x94 }));
        }

        [Test]
        public void RoundTripsUInt32()
        {
            var stream = new MemoryStream();
            Serialiser.WriteUInt32(stream, 0x9456AF45);
            stream.Position = 0;

            Assert.That(Serialiser.ReadUInt32(stream), Is.EqualTo(0x9456AF45));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x45, 0xAF, 0x56, 0x94 }));
        }

        [Test]
        public void RoundTripsInt16()
        {
            var stream = new MemoryStream();
            Serialiser.WriteInt16(stream, -27562);  // 0x9456
            stream.Position = 0;

            Assert.That(Serialiser.ReadInt16(stream), Is.EqualTo(-27562));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x56, 0x94 }));
        }

        [Test]
        public void RoundTripsUInt16()
        {
            var stream = new MemoryStream();
            Serialiser.WriteUInt16(stream, 0x9456);
            stream.Position = 0;

            Assert.That(Serialiser.ReadUInt16(stream), Is.EqualTo(0x9456));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x56, 0x94 }));
        }

        [Test]
        public void RoundTripsVariableLengthString()
        {
            var stream = new MemoryStream();
            Serialiser.WriteString(stream, "Hello");
            stream.Position = 0;

            Assert.That(Serialiser.ReadString(stream), Is.EqualTo("Hello"));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x05, 0x00, 0x00, 0x00, 0x48, 0x65, 0x6C, 0x6C, 0x6F }));
        }

        [Test]
        public void RoundTripsFixedLengthBytes()
        {
            var stream = new MemoryStream();
            var bytes = new byte[] { 0xC7, 0xBE, 0x62, 0x03, 0x45, 0xAF };
            Serialiser.WriteFixedLengthBytes(stream, bytes);
            stream.Position = 0;

            Assert.That(Serialiser.ReadFixedLengthBytes(stream, new byte[6]), Is.EqualTo(bytes));
            Assert.That(stream.ToArray(), Is.EqualTo(bytes));
        }

        [Test]
        public void RoundTripsVariableLengthBytes()
        {
            var stream = new MemoryStream();
            var bytes = new byte[] { 0xC7, 0xBE, 0x62, 0x03, 0x45, 0xAF };
            Serialiser.WriteBytes(stream, bytes);
            stream.Position = 0;

            Assert.That(Serialiser.ReadBytes(stream), Is.EqualTo(bytes));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x06, 0x00, 0x00, 0x00 }.Concat(bytes)));
        }

        [Test]
        public void RoundTripsDateTime()
        {
            var dateTime = new DateTime(2018, 05, 26, 17, 50, 33, 120, DateTimeKind.Utc);
            var stream = new MemoryStream();
            Serialiser.WriteDateTime(stream, dateTime);
            stream.Position = 0;

            Assert.That(Serialiser.ReadDateTime(stream), Is.EqualTo(dateTime));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x00, 0x8A, 0xC0, 0x2D, 0x31, 0xC3, 0xD5, 0x48 }));
        }

        [Test]
        public void RoundTripsDateTimeOffset()
        {
            var dateTimeOffset = new DateTimeOffset(2018, 05, 26, 17, 50, 33, 120, TimeSpan.FromHours(-5));
            var stream = new MemoryStream();
            Serialiser.WriteDateTimeOffset(stream, dateTimeOffset);
            stream.Position = 0;

            Assert.That(Serialiser.ReadDateTimeOffset(stream), Is.EqualTo(dateTimeOffset));
            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] { 0x00, 0x8A, 0xC0, 0x2D, 0x31, 0xC3, 0xD5, 0x08, 0xD4, 0xFE, 0xFF, 0xFF }));
        }

        [Test]
        public void AlignReadDoesNotModifyAlreadyAlignedPosition()
        {
            var stream = new MemoryStream(new byte[8]);
            stream.Position = 4;

            Assert.That(Serialiser.TryAlignRead(stream, 4), Is.True);
            Assert.That(stream.Position, Is.EqualTo(4));
        }

        [Test]
        public void AlignReadAdvancesToAlignedPosition()
        {
            var stream = new MemoryStream(new byte[8]);
            stream.Position = 5;

            Assert.That(Serialiser.TryAlignRead(stream, 4), Is.True);
            Assert.That(stream.Position, Is.EqualTo(8));
        }

        [Test]
        public void AlignReadReturnsFalseIfEndOfReadOnlyStreamWouldBePassed()
        {
            var stream = new MemoryStream(new byte[5], false);
            stream.Position = 5;

            Assert.That(Serialiser.TryAlignRead(stream, 4), Is.False);
            Assert.That(stream.Position, Is.EqualTo(5));
        }

        [Test]
        public void AlignWriteDoesNotModifyAlreadyAlignedPosition()
        {
            var stream = new MemoryStream(new byte[8]);
            stream.Position = 4;

            Serialiser.AlignWrite(stream, 4);
            Assert.That(stream.Position, Is.EqualTo(4));
        }

        [Test]
        public void AlignWriteAdvancesToAlignedPosition()
        {
            var stream = new MemoryStream(new byte[8]);
            stream.Position = 5;

            Serialiser.AlignWrite(stream, 4);
            Assert.That(stream.Position, Is.EqualTo(8));
        }

        [Test]
        public void AlignWriteExtendsStreamIfEndOfExpandableStreamWouldBePassed()
        {
            var stream = new MemoryStream();
            stream.Position = 5;

            Serialiser.AlignWrite(stream, 4);
            Assert.That(stream.Position, Is.EqualTo(8));
            Assert.That(stream.Length, Is.EqualTo(8));
        }
    }
}
