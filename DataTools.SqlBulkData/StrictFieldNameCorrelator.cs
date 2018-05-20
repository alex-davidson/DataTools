using System;
using System.Collections.Generic;
using System.Linq;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class StrictFieldNameCorrelator : IFieldCorrelator
    {
        public IEqualityComparer<string> FieldNameEqualityComparer { get; set; } = StringComparer.OrdinalIgnoreCase;

        public Table.Field[] GetTargetFields(ColumnDescriptor sourceColumn, Table.Field[] targetFields)
        {
            return targetFields.Where(f => FieldNameEqualityComparer.Equals(f.Name, sourceColumn.OriginalName)).ToArray();
        }

        public void OnUnallocatedTargetFields(Table.Field[] unallocatedTargetFields)
        {
            if (!unallocatedTargetFields.Any()) return;
            throw new UncorrelatedFieldsException(unallocatedTargetFields);
        }
    }
}
