namespace DataTools.SqlBulkData.Serialisation
{
    public struct ChunkedFileHeader
    {
        public uint FileTypeId { get; set; }
        public short Version { get; set; }
        public int HeaderSizeBytes { get; set; }
    }
}
