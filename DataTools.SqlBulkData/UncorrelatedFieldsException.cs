using System;
using System.Linq;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class UncorrelatedFieldsException : ApplicationException
    {
        public Table.Field[] Targets { get; }

        public UncorrelatedFieldsException(Table.Field[] targets) : base($"Unable to select source fields for {targets.Length} fields: {String.Join(", ", targets.Select(t => t.Name))}")
        {
            this.Targets = targets;
        }
    }
}
