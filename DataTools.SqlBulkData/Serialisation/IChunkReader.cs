using System.IO;

namespace DataTools.SqlBulkData.Serialisation
{
    public interface IChunkReader
    {
        uint TypeId { get; }
        ChunkBookmark GetBookmark();
        Stream Stream { get; }
    }
}
