using System;
using System.Diagnostics;
using System.IO;

namespace DataTools.SqlBulkData.Serialisation
{
    public class ChunkedFileHeaderSerialiser
    {
        public ChunkedFileHeader WriteHeader(Stream stream, uint fileTypeId)
        {
            if (!Serialiser.IsAligned(stream, 8)) throw new InvalidOperationException("Stream position is not 8-byte-aligned.");
            var position = stream.Position;
            var header = new ChunkedFileHeader
            {
                FileTypeId = fileTypeId,
                Version = 1,
                HeaderSizeBytes = 8
            };
            Serialiser.WriteUInt32(stream, header.FileTypeId);
            Serialiser.WriteInt16(stream, header.Version);
            Serialiser.WriteUInt16(stream, 0);
            Debug.Assert(position + header.HeaderSizeBytes == stream.Position);
            return header;
        }

        public ChunkedFileHeader ReadHeader(Stream stream)
        {
            if (!Serialiser.IsAligned(stream, 8)) throw new InvalidOperationException("Stream position is not 8-byte-aligned.");
            var position = stream.Position;
            var header = new ChunkedFileHeader { HeaderSizeBytes = 8 };
            header.FileTypeId = Serialiser.ReadUInt32(stream);
            header.Version = Serialiser.ReadInt16(stream);
            Serialiser.ReadUInt16(stream);
            Debug.Assert(position + header.HeaderSizeBytes == stream.Position);
            return header;
        }
    }
}
