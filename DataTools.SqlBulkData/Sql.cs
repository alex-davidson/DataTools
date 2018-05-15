using System.Data;

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
    }
}
