using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class ImportModel
    {
        public TableIdentifier Table { get; set;}
        public IColumnSerialiser[] ColumnSerialisers { get; set; }
        public ColumnMetaInfo[] ColumnMetaInfos { get; set; }
    }
}
