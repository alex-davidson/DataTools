using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;

namespace DataTools.SqlBulkData
{
    public class ImportModel
    {
        public TableIdentifier Table { get; set;}
        public IColumnSerialiser[] ColumnSerialisers { get; set; }
        public ColumnMetaInfo[] ColumnMetaInfos { get; set; }
    }
}
