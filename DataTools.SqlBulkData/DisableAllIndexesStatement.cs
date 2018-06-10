using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class DisableAllIndexesStatement
    {
        public void Execute(SqlServerDatabase database, TableIdentifier tableOrView)
        {
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, $"alter index all on {Sql.Escape(tableOrView.Schema, tableOrView.Name)} disable", database.DefaultTimeout))
            {
                cmd.ExecuteNonQuery();
            }
        }
    }
}
