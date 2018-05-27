using System.Data.SqlClient;
using System.Threading;
using DataTools.SqlBulkData.Schema;
using log4net;

namespace DataTools.SqlBulkData
{
    public class TruncateTableStatement
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(TruncateTableStatement));

        public void Execute(SqlServerDatabase database, Table table)
        {
            try
            {
                ExecuteWithRetryDeadlocks(database,  $"truncate table {Sql.Escape(table.Schema, table.Name)}");
            }
            catch (SqlException ex) when (ex.Number == 0x1268)
            {
                // Table is referenced by a foreign key.
                // Fall back to using `delete from`.
                ExecuteWithRetryDeadlocks(database, $"delete from {Sql.Escape(table.Schema, table.Name)}");
            }
            catch (SqlException ex) when (ex.Number == 0x0e91)
            {
                // Table is referenced by a schema-bound object, possibly an indexed view.
                // Fall back to using `delete from`.
                ExecuteWithRetryDeadlocks(database, $"delete from {Sql.Escape(table.Schema, table.Name)}");
            }
        }

        private static void ExecuteWithRetryDeadlocks(SqlServerDatabase database, string sql)
        {
            while (true)
            {
                try
                {
                    using (var cn = database.OpenConnection())
                    {
                        using (var cmd = Sql.CreateQuery(cn, "set deadlock_priority -10"))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (var cmd = Sql.CreateQuery(cn, sql, database.DefaultTimeout))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }
                    return;
                }
                catch (SqlException ex) when (ex.Number == 0x04B5)
                {
                    log.Debug($"Deadlock detected, retrying statement: {sql}");
                    Thread.Yield();
                }
            }
        }
    }
}
