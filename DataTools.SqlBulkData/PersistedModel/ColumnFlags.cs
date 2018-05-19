using System;
using System.Linq;

namespace DataTools.SqlBulkData.PersistedModel
{
    [Flags]
    public enum ColumnFlags : short
    {
        None = 0,
        Nullable = 0x0001,
        AbsentWhenNull = 0x0003
    }

    public static class ColumnFlagsExtensions
    {
        public static bool IsNullable(this ColumnFlags flags) => InternalHasFlag(flags, ColumnFlags.Nullable);
        public static bool OmitNulls(this ColumnFlags flags) => InternalHasFlag(flags, ColumnFlags.AbsentWhenNull);
        public static ColumnFlags GetUnknownFlags(this ColumnFlags flags) => flags & ~allColumnFlags;
        public static ColumnFlags GetKnownFlags(this ColumnFlags flags) => flags & allColumnFlags;

        private static bool InternalHasFlag(ColumnFlags flags, ColumnFlags flag) => (flags & flag) == flag;

        private static readonly ColumnFlags allColumnFlags = Enum.GetValues(typeof(ColumnFlags)).Cast<ColumnFlags>().Aggregate((fs, f) => fs | f);
    }
}
