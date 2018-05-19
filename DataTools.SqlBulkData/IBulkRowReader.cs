using System.Collections.Generic;
using DataTools.SqlBulkData.Columns;

namespace DataTools.SqlBulkData
{
    public interface IBulkRowReader
    {
        IList<IColumnSerialiser> Columns { get; }
        object[] Current { get; }

        bool MoveNext();
    }
}
