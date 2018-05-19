using System;

namespace DataTools.SqlBulkData.PersistedModel
{
    public struct TableIdentifier : IEquatable<TableIdentifier>
    {
        public TableIdentifier(string schema, string name)
        {
            this.Schema = schema ?? "";
            this.Name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public string Name { get; }
        public string Schema { get; }

        public override string ToString() => $"{Schema}.{Name}".Trim('.');

        public bool Equals(TableIdentifier other)
        {
            return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(Schema, other.Schema, StringComparison.OrdinalIgnoreCase);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is TableIdentifier && Equals((TableIdentifier) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Name) : 0) * 397) ^ (Schema != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Schema) : 0);
            }
        }
    }
}
