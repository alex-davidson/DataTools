using System;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public interface ITableGuidPolicy
    {
        Guid GenerateGuid(Table table);
    }
}
