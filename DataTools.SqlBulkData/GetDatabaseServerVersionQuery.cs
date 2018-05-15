namespace DataTools.SqlBulkData
{
    public class GetDatabaseServerVersionQuery
    {
        public string Execute(SqlServerDatabase database)
        {
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, "select @@version"))
            using (var reader = cmd.ExecuteReader())
            if (reader.Read())
            {
                return reader.GetString(0);
            }
            return null;
        }
    }
}
