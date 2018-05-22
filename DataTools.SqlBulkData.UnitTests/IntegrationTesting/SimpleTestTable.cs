using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData.UnitTests.IntegrationTesting
{
    /// <summary>
    /// Simple table containing an identity primary key and two columns of a specified type,
    /// one of which is nullable.
    /// </summary>
    public class SimpleTestTable : IDisposable
    {
        private readonly SqlServerDatabase database;
        private readonly string name;
        private readonly string dbType;
        private bool isCreated = false;
        private SqlDbType? sqlDbType;

        public SimpleTestTable(SqlServerDatabase database, string dbType, SqlDbType? sqlDbType = null)
        {
            this.database = database;
            this.name = $"test_table_{Guid.NewGuid():N}";
            this.dbType = dbType;
            this.sqlDbType = sqlDbType;
        }

        public void Create()
        {
            var sql = $@"create table {Sql.Escape(name)}(
    id int identity primary key not null,
    column_notnull {dbType} not null,
    column_null {dbType} null
)";
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, sql))
            {
                cmd.ExecuteNonQuery();
            }
            isCreated = true;
        }

        public void AddRow(object value, object nullableValue)
        {
            if (value == null || value == DBNull.Value) throw new ArgumentNullException(nameof(value));
            if (!isCreated) throw new InvalidOperationException("Table has not yet been created.");

            var sql = $"insert into {Sql.Escape(name)}(column_notnull, column_null) values (@value_notnull, @value_null)";
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, sql))
            {
                cmd.Parameters.Add(CreateParameter("value_notnull", value));
                cmd.Parameters.Add(CreateParameter("value_null", nullableValue ?? DBNull.Value));
                cmd.ExecuteNonQuery();
            }
        }

        private SqlParameter CreateParameter(string paramName, object paramValue)
        {
            if (paramValue == null) throw new ArgumentNullException(nameof(paramValue));
            var parameter = new SqlParameter(paramName, paramValue);
            if (sqlDbType != null) parameter.SqlDbType = sqlDbType.Value;
            return parameter;
        }

        public object[][] ReadRows(Type expectedType)
        {
            if (!isCreated) throw new InvalidOperationException("Table has not yet been created.");
            var sql = $"select column_notnull, column_null from {Sql.Escape(name)}";
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, sql))
            using (var reader = cmd.ExecuteReader())
            {
                var rows = new List<object[]>();
                while (reader.Read())
                {
                    rows.Add(new [] {
                        ReadField(reader, 0, expectedType),
                        ReadField(reader, 1, expectedType)
                    });
                }
                return rows.ToArray();
            }
        }

        private static object ReadField(IDataReader record, int ordinal, Type expectedType)
        {
            if (record.IsDBNull(ordinal)) return null;
            var value = record.GetValue(ordinal);
            if (expectedType.IsInstanceOfType(value)) return value;
            // Maybe convert, or look for SQL Server types?
            return value;
        }

        public Table ReadSchema()
        {
            return new GetAllTablesQuery().List(database).Single(t => t.Name == name);
        }

        public void Dispose()
        {
            if (!isCreated) return;
            try
            {
                using (var cn = database.OpenConnection())
                using (var cmd = Sql.CreateQuery(cn, $"drop table {Sql.Escape(name)}"))
                {
                    cmd.ExecuteNonQuery();
                }
                isCreated = false;
            }
            catch { }
        }
    }
}
