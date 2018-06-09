using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class DeleteForeignKeyStatement
    {
        public void Execute(SqlServerDatabase database, ForeignKey key)
        {
            var dropSql = Sql.Statement(
                $"alter table {Sql.Escape(key.ForeignTable.Schema, key.ForeignTable.Name)}",
                $"drop constraint {Sql.Escape(key.Name)}"
            );
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, dropSql, database.DefaultTimeout))
            {
                cmd.ExecuteNonQuery();
            }
        }
    }
}
