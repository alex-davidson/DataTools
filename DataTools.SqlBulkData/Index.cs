using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class Index
    {
        public string Name { get; set; }
        public TableIdentifier Owner { get; set; }
        public IndexType Type { get; set; }
        public bool Unique { get; set; }

        public enum IndexType
        {
            Heap = 0,
            Clustered = 1,
            Nonclustered = 2
        }
    }
}
