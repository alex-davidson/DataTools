using System.Collections.Generic;

namespace DataTools.SqlBulkData.PersistedModel
{
    public class ColumnDescriptor
    {
        public string OriginalName { get; set; }
        public ColumnFlags ColumnFlags { get; set; }
        public short OriginalIndex { get; set; }
        public ColumnDataType StoredDataType { get; set; }
        public int Length { get; set; }

        public static IEqualityComparer<ColumnDescriptor> EqualityComparer { get; } = new ColumnDescriptorEqualityComparer();

        private sealed class ColumnDescriptorEqualityComparer : IEqualityComparer<ColumnDescriptor>
        {
            public bool Equals(ColumnDescriptor x, ColumnDescriptor y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (ReferenceEquals(x, null)) return false;
                if (ReferenceEquals(y, null)) return false;
                if (x.GetType() != y.GetType()) return false;
                return string.Equals(x.OriginalName, y.OriginalName)
                       && x.ColumnFlags == y.ColumnFlags
                       && x.OriginalIndex == y.OriginalIndex
                       && x.StoredDataType == y.StoredDataType
                       && x.Length == y.Length;
            }

            public int GetHashCode(ColumnDescriptor obj)
            {
                unchecked
                {
                    var hashCode = (obj.OriginalName != null ? obj.OriginalName.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (int) obj.ColumnFlags;
                    hashCode = (hashCode * 397) ^ obj.OriginalIndex.GetHashCode();
                    hashCode = (hashCode * 397) ^ (int) obj.StoredDataType;
                    hashCode = (hashCode * 397) ^ obj.Length;
                    return hashCode;
                }
            }
        }
    }
}
