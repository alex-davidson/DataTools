using System;
using System.Collections.Generic;
using System.Linq;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData.UnitTests.IntegrationTesting
{
    class TemporaryTables : IDisposable
    {
        private readonly SqlServerDatabase database;
        private readonly Queue<string> tableNames = new Queue<string>();

        public TemporaryTables(SqlServerDatabase database)
        {
            this.database = database;
        }

        public void Create(Table table)
        {
            var sql = $@"create table {Sql.Escape(table.Name)}(
    id int identity primary key not null,
    {String.Join(",\n    ", table.Fields.Select(FormatColumnDefinition))}
)";
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, sql))
            {
                cmd.ExecuteNonQuery();
                tableNames.Enqueue(table.Name);
            }
        }

        private string FormatColumnDefinition(Table.Field column) => String.Join(" ", Sql.Escape(column.Name), column.DataType.Name, column.IsNullable ? "null" : "not null");

        private void Drop(string name)
        {
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, $"drop table {name}"))
            {
                cmd.ExecuteNonQuery();
            }
        }

        public void DropAll()
        {
            // Handle ordering problems caused by constraints by retrying each table until either
            // a droppable one cannot be found or they're all gone.
            var consecutiveFailures = 0;
            while (tableNames.Count > consecutiveFailures)
            {
                var name = tableNames.Dequeue();
                try
                {
                    Drop(name);
                    consecutiveFailures = 0;
                }
                catch
                {
                    tableNames.Enqueue(name);
                    consecutiveFailures++;
                }
            }
        }

        public void Dispose()
        {
            DropAll();
        }
    }
}