using System;
using System.IO;
using DataTools.SqlBulkData.Serialisation;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests.Serialisation
{
    [TestFixture]
    public class ChunkedFileReaderTests
    {
        [Test]
        public void RestrictsChunkStreamToChunkDataSectionLength()
        {
            var stream = new MemoryStream(new byte[] {
                0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x42
            });
            var reader = new ChunkedFileReader(stream, new ChunkedFileHeader());

            Assume.That(reader.MoveNext(), Is.True);

            var buffer = new byte[8];
            var count = reader.Current.Stream.Read(buffer, 0, buffer.Length);

            Assert.That(reader.Current.Stream.Length, Is.EqualTo(1));
            Assert.That(count, Is.EqualTo(1));
            Assert.That(buffer, Is.EqualTo(new byte[] { 0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }));
        }

        [Test]
        public void ChunkStreamPositionIsRelativeToChunkDataSection()
        {
            var stream = new MemoryStream(new byte[] {
                0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x42, 0x43
            });
            var reader = new ChunkedFileReader(stream, new ChunkedFileHeader());

            Assume.That(reader.MoveNext(), Is.True);

            reader.Current.Stream.Position = 1;

            Assert.That(Serialiser.ReadByte(reader.Current.Stream), Is.EqualTo(0x43));
        }

        [Test]
        public void CanSeekToEarlierChunk()
        {
            var stream = new MemoryStream(new byte[] {
                0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x42, 0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
            });
            var reader = new ChunkedFileReader(stream, new ChunkedFileHeader());

            Assume.That(reader.MoveNext(), Is.True);
            Assume.That(reader.Current.TypeId, Is.EqualTo(0x00000001));
            var firstChunk = reader.Current.GetBookmark();

            Assume.That(reader.MoveNext(), Is.True);
            Assume.That(reader.Current.TypeId, Is.EqualTo(0x00000002));

            reader.SeekTo(firstChunk);

            Assert.That(reader.Current.TypeId, Is.EqualTo(0x00000001));
            Assert.That(reader.Current.Stream.Length, Is.EqualTo(2));
            Assert.That(Serialiser.ReadByte(reader.Current.Stream), Is.EqualTo(0x42));
        }

        [Test]
        public void SeekingToInvalidBookmarkThrowsException()
        {
            var stream = new MemoryStream(new byte[] {
                0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x42, 0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
            });
            var reader = new ChunkedFileReader(stream, new ChunkedFileHeader());

            Assume.That(reader.MoveNext(), Is.True);
            Assume.That(reader.MoveNext(), Is.True);

            Assert.Throws<InvalidOperationException>(() => reader.SeekTo(new ChunkBookmark(0x00000001, 8)));
        }

        [Test]
        public void MoveNextReturnsFalseAfterTheLastChunk()
        {
            var stream = new MemoryStream(new byte[] {
                0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x42
            });
            var reader = new ChunkedFileReader(stream, new ChunkedFileHeader());

            Assume.That(reader.MoveNext(), Is.True);
            Assume.That(reader.MoveNext(), Is.False);
        }
    }
}
