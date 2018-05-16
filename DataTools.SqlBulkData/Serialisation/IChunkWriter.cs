using System;
using System.IO;

namespace DataTools.SqlBulkData.Serialisation
{
    public interface IChunkWriter : IDisposable
    {
        uint TypeId { get; }
        Stream Stream { get; }
    }
}
