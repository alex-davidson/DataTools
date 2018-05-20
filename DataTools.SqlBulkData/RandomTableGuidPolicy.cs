using System;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    /// <summary>
    /// Generates a random Guid for each table.
    /// </summary>
    public class RandomTableGuidPolicy : ITableGuidPolicy
    {
        public Guid GenerateGuid(Table table) => Guid.NewGuid();
    }
}
