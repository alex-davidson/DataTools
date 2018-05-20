namespace DataTools.SqlBulkData
{
    public class CheckConstraintsStatement
    {
        public void Execute(SqlServerDatabase database)
        {
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, "dbcc checkconstraints with all_constraints;"))
            using (var reader = cmd.ExecuteReader())
            {
                var count = 0;
                while (reader.Read()) count++;
                if (count == 0) return;
                throw new ViolatedConstraintsException($"{count} constraints are currently violated.");
            }
        }
    }
}
