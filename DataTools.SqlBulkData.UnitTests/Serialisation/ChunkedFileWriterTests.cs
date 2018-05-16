using System;
using System.IO;
using DataTools.SqlBulkData.Serialisation;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests.Serialisation
{
    [TestFixture]
    public class ChunkedFileWriterTests
    {
        [Test]
        public void WritesChunkLengthWhenClosed()
        {
            var stream = new MemoryStream();
            var builder = new ChunkedFileWriter(stream);
            using (var chunk = builder.BeginChunk(1))
            {
                Serialiser.WriteByte(chunk.Stream, 0x42);
                Assert.That(stream.ToArray(), Is.EqualTo(new byte[] {
                    0x01, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x42
                }));
            }

            Assert.That(stream.ToArray(), Is.EqualTo(new byte[] {
                0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x42
            }));
        }

        [Test]
        public void LeavesPositionAfterChunkWhenClosed()
        {
            var stream = new MemoryStream();
            var builder = new ChunkedFileWriter(stream);
            using (var chunk = builder.BeginChunk(1))
            {
                Serialiser.WriteByte(chunk.Stream, 0x42);
                Assume.That(stream.Length, Is.EqualTo(17));
                Assume.That(stream.Position, Is.EqualTo(17));
            }

            Assert.That(stream.Position, Is.EqualTo(17));
        }

        [Test]
        public void CannotOpenChunkBeforeClosingPreviousChunk()
        {
            var stream = new MemoryStream();
            var builder = new ChunkedFileWriter(stream);
            using (builder.BeginChunk(1))
            {
                Assert.Throws<InvalidOperationException>(() => builder.BeginChunk(2));
            }
        }
    }
}
