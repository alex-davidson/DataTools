using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public interface IFieldCorrelator
    {
        Table.Field[] GetTargetFields(ColumnDescriptor sourceColumn, Table.Field[] targetFields);
        void OnUnallocatedTargetFields(Table.Field[] unallocatedTargetFields);
    }
}
