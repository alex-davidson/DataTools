using System.Collections.Generic;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;

namespace DataTools.SqlBulkData
{
    public interface IBulkTableData
    {
        TableDescriptor Table { get; }
        IList<ColumnDescriptor> Columns { get; }
        Stream DataStream { get; }
    }
}
