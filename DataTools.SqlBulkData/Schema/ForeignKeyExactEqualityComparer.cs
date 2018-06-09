using System;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.SqlBulkData.Schema
{
    /// <summary>
    /// Foreign key equality comparison based on all members of the constraint.
    /// </summary>
    /// <remarks>
    /// Under this equality comparison every aspect of the constraints are compared, including functionally-irrelevant
    /// details such as Description.
    /// Hashcode is derived from the constraint name and the participating tables only.
    /// </remarks>
    public class ForeignKeyExactEqualityComparer : IEqualityComparer<ForeignKey>
    {
        private readonly StringComparer symbolComparer = StringComparer.OrdinalIgnoreCase;

        public bool Equals(ForeignKey x, ForeignKey y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x == null || y == null) return false;
            if (!symbolComparer.Equals(x.Name, y.Name)) return false;
            if (!Equals(x.ForeignTable, y.ForeignTable)) return false;
            if (!Equals(x.PrimaryTable, y.PrimaryTable)) return false;
            if (x.ForeignColumns.Length != y.ForeignColumns.Length) return false;
            if (x.PrimaryColumns.Length != y.PrimaryColumns.Length) return false;
            if (!x.ForeignColumns.SequenceEqual(y.ForeignColumns, symbolComparer)) return false;
            if (!x.PrimaryColumns.SequenceEqual(y.PrimaryColumns, symbolComparer)) return false;
            if (x.EnforceConstraint != y.EnforceConstraint) return false;
            if (x.EnforceForReplication != y.EnforceForReplication) return false;
            if (x.UpdateRule != y.UpdateRule) return false;
            if (x.DeleteRule != y.DeleteRule) return false;
            return true;
        }

        public int GetHashCode(ForeignKey obj)
        {
            unchecked
            {
                var hashCode = obj.Name != null ? symbolComparer.GetHashCode(obj.Name) : 0;
                hashCode = (hashCode * 397) ^ obj.ForeignTable.GetHashCode();
                hashCode = (hashCode * 397) ^ obj.PrimaryTable.GetHashCode();
                return hashCode;
            }
        }
    }
}
