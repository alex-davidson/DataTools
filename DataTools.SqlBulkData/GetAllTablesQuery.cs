using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class GetAllTablesQuery
    {
        public IList<Table> List(SqlServerDatabase database)
        {
            const string sql = @"
select
    T.object_id as table_id,
    T.name as table_name,
    S.name as schema_name,
    C.column_id as column_id,
    C.name as column_name,
    C.is_computed as column_is_computed,
    C.is_nullable as column_is_nullable,
    C.precision as column_precision,
    C.max_length as column_max_length,
    D.name as column_data_type
from sys.tables T
inner join sys.schemas S on S.schema_id = T.schema_id
inner join sys.columns C on C.object_id = T.object_id
left join sys.systypes D on D.xtype = C.system_type_id and D.xusertype = C.user_type_id
order by T.name, C.column_id
";
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, sql))
            using (var reader = cmd.ExecuteReader())
            {
                var rows = GetAllTablesRow.ReadAll(reader);
                return rows
                    .GroupBy(r => new { r.table_name, r.schema_name })
                    .Select(rs => new Table {
                        Name = rs.Key.table_name,
                        Schema = rs.Key.schema_name,
                        Fields = rs
                            .OrderBy(r => r.column_id)
                            .Select(r => new Table.Field {
                                Name = r.column_name,
                                IsComputed = r.column_is_computed,
                                IsNullable = r.column_is_nullable,
                                DataType = new Table.DataType {
                                    Name = r.column_data_type,
                                    SqlDbType = new SqlDbTypeInterpreter().Interpret(r.column_data_type),
                                    Precision = r.column_precision,
                                    MaxLength = r.column_max_length
                                }
                            })
                            .ToArray()
                    })
                    .ToList();
            }
        }

        private class GetAllTablesRow
        {
            public static IEnumerable<GetAllTablesRow> ReadAll(IDataReader reader)
            {
                while (reader.Read())
                {
                    yield return new GetAllTablesRow {
                        table_id = Get<int>("table_id"),
                        table_name = Get<string>("table_name"),
                        schema_name = Get<string>("schema_name"),
                        column_id = Get<int>("column_id"),
                        column_name =  Get<string>("column_name"),
                        column_is_computed = Get<bool>("column_is_computed"),
                        column_is_nullable = Get<bool>("column_is_nullable"),
                        column_precision = Get<int>("column_precision"),
                        column_max_length = Get<int>("column_max_length"),
                        column_data_type = Get<string>("column_data_type")
                    };
                }

                T Get<T>(string name) => (T)Convert.ChangeType(reader[name], typeof(T));
            }

            public int table_id { get; set; }
            public string table_name { get; set; }
            public string schema_name { get; set; }
            public int column_id { get; set; }
            public string column_name { get; set; }
            public bool column_is_computed { get; set; }
            public bool column_is_nullable { get; set; }
            public int column_precision { get; set; }
            public int column_max_length { get; set; }
            public string column_data_type { get; set; }
        }
    }
}
