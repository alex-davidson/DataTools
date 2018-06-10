using System;
using System.Collections.Generic;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    /// <summary>
    /// Tests for equivalence of server instance and database name. Case-insensitive.
    /// </summary>
    public sealed class SqlServerDatabaseEqualityComparer : IEqualityComparer<SqlServerDatabase>
    {
        public bool Equals(SqlServerDatabase x, SqlServerDatabase y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (ReferenceEquals(x, null)) return false;
            if (ReferenceEquals(y, null)) return false;
            if (x.GetType() != y.GetType()) return false;
            return StringComparer.OrdinalIgnoreCase.Equals(NormaliseServer(x.Server), NormaliseServer(y.Server))
                   && SqlServerSymbolComparer.Instance.Equals(x.Name, y.Name);
        }

        private static string NormaliseServer(string server)
        {
            if (server.Equals("(local)", StringComparison.OrdinalIgnoreCase)) return "localhost";
            if (server.StartsWith("(local)\\", StringComparison.OrdinalIgnoreCase)) return "localhost" + server.Substring("(local)".Length);
            return server;
        }

        public int GetHashCode(SqlServerDatabase obj)
        {
            unchecked
            {
                var hashCode = StringComparer.OrdinalIgnoreCase.GetHashCode(NormaliseServer(obj.Server));
                hashCode = (hashCode * 397) ^ SqlServerSymbolComparer.Instance.GetHashCode(obj.Name);
                return hashCode;
            }
        }
    }
}
