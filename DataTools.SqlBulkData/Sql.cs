using System;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace DataTools.SqlBulkData
{
    public static class Sql
    {
        public static DbCommand CreateQuery(IDbConnection cn, string query)
        {
            var cmd = (DbCommand)cn.CreateCommand();
            cmd.CommandText = query;
            cmd.CommandType = CommandType.Text;
            return cmd;
        }

        public static DbCommand CreateQuery(IDbConnection cn, string query, TimeSpan timeout)
        {
            var cmd = CreateQuery(cn, query);
            cmd.CommandTimeout = (int)timeout.TotalSeconds;
            return cmd;
        }

        public static string Statement(params string[] parts) => String.Join(" ", parts.Where(s => !String.IsNullOrEmpty(s)));

        public static string EscapeColumnList(params string[] columns)
        {
            var result = String.Join(", ", columns.Where(s => !String.IsNullOrEmpty(s)).Select(EscapeSingle));
            if (result.Length == 0) throw new ArgumentException("No columns specified.", nameof(columns));
            return result;
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
