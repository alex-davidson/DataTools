namespace DataTools.SqlBulkData.Serialisation
{
    public struct ChunkBookmark
    {
        /// <summary>
        /// Offset of the chunk's header.
        /// </summary>
        public long StartOffset { get; }

        public uint TypeId { get; }

        public ChunkBookmark(uint typeId, long startOffset)
        {
            TypeId = typeId;
            StartOffset = startOffset;
        }
    }
}
