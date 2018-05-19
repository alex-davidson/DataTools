using System;

namespace DataTools.SqlBulkData.PersistedModel
{
    public class TableColumns
    {
        public Guid TableId { get; set; }
        public ColumnDescriptor[] Columns { get; set; }
    }
}
