using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class EnableAllIndexesStatement
    {
        public void Execute(SqlServerDatabase database, TableIdentifier tableOrView)
        {
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, $"alter index all on {Sql.Escape(tableOrView.Schema, tableOrView.Name)} rebuild", database.DefaultTimeout))
            {
                cmd.ExecuteNonQuery();
            }
        }
    }
}
