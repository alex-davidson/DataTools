using System;
using System.Collections.Generic;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData.PersistedModel
{
    public class TableDescriptor
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public string Schema { get; set; }

        public override string ToString() => $"{Schema}.{Name}".Trim('.');
        public TableIdentifier Identify() => new TableIdentifier(Schema, Name);

        public static IEqualityComparer<TableDescriptor> EqualityComparer { get; } = new TableDescriptorEqualityComparer();

        private sealed class TableDescriptorEqualityComparer : IEqualityComparer<TableDescriptor>
        {
            public bool Equals(TableDescriptor x, TableDescriptor y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (ReferenceEquals(x, null)) return false;
                if (ReferenceEquals(y, null)) return false;
                if (x.GetType() != y.GetType()) return false;
                return x.Id.Equals(y.Id)
                       && string.Equals(x.Name, y.Name)
                       && string.Equals(x.Schema, y.Schema);
            }

            public int GetHashCode(TableDescriptor obj)
            {
                unchecked
                {
                    var hashCode = obj.Id.GetHashCode();
                    hashCode = (hashCode * 397) ^ (obj.Name != null ? obj.Name.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (obj.Schema != null ? obj.Schema.GetHashCode() : 0);
                    return hashCode;
                }
            }
        }
    }
}
