using System;
using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;

namespace DataTools.SqlBulkData
{
    public class ExportModel
    {
        public Guid Id { get; set; }
        public IColumnSerialiser[] ColumnSerialisers { get; set; }
        public TableDescriptor TableDescriptor { get; set; }
        public ColumnDescriptor[] ColumnDescriptors { get; set; }
    }
}
