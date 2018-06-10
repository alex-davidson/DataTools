using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class GetAllIndexesOnViewsQuery
    {
        public IList<Index> List(SqlServerDatabase database)
        {
            const string sql = @"
select
    I.name as index_name,
    V.name as view_name,
    S.name as view_schema,
    I.type as index_type,
    I.is_unique
from sys.indexes I 
inner join sys.views V ON I.object_id = V.object_id and V.is_ms_shipped = 0
inner join sys.schemas S on S.schema_id = V.schema_id
";
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, sql, database.DefaultTimeout))
            using (var reader = cmd.ExecuteReader())
            {
                var rows = GetAllIndexesOnViewsRow.ReadAll(reader);
                return rows
                    .Select(r => new Index {
                        Name = r.index_name,
                        Owner = new TableIdentifier(r.view_schema, r.view_name),
                        Type = InterpretIndexType(r.index_type),
                        Unique = r.is_unique
                    })
                    .ToList();
            }
        }

        private static Index.IndexType InterpretIndexType(int value)
        {
            switch (value)
            {
                case 0: return Index.IndexType.Heap;
                case 1: return Index.IndexType.Clustered;
                case 2: return Index.IndexType.Nonclustered;
                default:
                    throw new ArgumentOutOfRangeException(nameof(value), value, null);
            }
        }

        private class GetAllIndexesOnViewsRow
        {
            public static IEnumerable<GetAllIndexesOnViewsRow> ReadAll(IDataReader reader)
            {
                while (reader.Read())
                {
                    yield return new GetAllIndexesOnViewsRow {
                        index_name = Get<string>("index_name"),
                        view_name = Get<string>("view_name"),
                        view_schema = Get<string>("view_schema"),
                        index_type = Get<int>("index_type"),
                        is_unique =  Get<bool>("is_unique")
                    };
                }

                T Get<T>(string name) => (T)Convert.ChangeType(reader[name], typeof(T));
            }

            public string index_name { get; set; }
            public string view_name { get; set; }
            public string view_schema { get; set; }
            public int index_type { get; set; }
            public bool is_unique { get; set; }
        }
    }
}