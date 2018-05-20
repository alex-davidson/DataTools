using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class DisableConstraintsStatement
    {
        public void Execute(SqlServerDatabase database, Table table)
        {
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, $"alter table {Sql.Escape(table.Schema, table.Name)} nocheck constraint all"))
            {
                cmd.ExecuteNonQuery();
            }
        }
    }
}
