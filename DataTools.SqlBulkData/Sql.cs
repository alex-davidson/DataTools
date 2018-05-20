using System;
using System.Data;
using System.Linq;

namespace DataTools.SqlBulkData
{
    public static class Sql
    {
        public static IDbCommand CreateQuery(IDbConnection cn, string query)
        {
            var cmd = cn.CreateCommand();
            cmd.CommandText = query;
            cmd.CommandType = CommandType.Text;
            return cmd;
        }

        public static string Escape(params string[] symbols)
        {
            var result = String.Join(".", symbols.Where(s => !String.IsNullOrEmpty(s)).Select(EscapeSingle));
            if (result.Length == 0) throw new ArgumentException("No non-empty symbols specified.", nameof(symbols));
            return result;
        }

        private static string EscapeSingle(string symbol) => String.Concat("[", symbol, "]");
    }
}
