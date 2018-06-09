using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class GetAllForeignKeysQuery
    {
        public IList<ForeignKey> List(SqlServerDatabase database)
        {
            const string sql = @"
select
    fkn.name as foreign_key_name,
    ep_desc.value as foreign_key_description,
    fkn.object_id as foreign_key_id,
    object_name(fk.referenced_object_id) as primary_table_name,
    schema_name (cast(objectpropertyex(fk.referenced_object_id,N'SchemaId') as int)) as primary_table_schema,

    object_name(fk.parent_object_id) as foreign_table_name,
    schema_name (cast(objectpropertyex(fk.parent_object_id,N'SchemaId') as int)) as foreign_table_schema,

    fk.constraint_column_id as column_index,
    clm2.name as primary_column,
    clm1.name as foreign_column,
    
    fkn.is_not_for_replication,
    fkn.is_disabled,
    fkn.update_referential_action,
    fkn.delete_referential_action,
    fkn.is_not_trusted
from sys.foreign_keys fkn
inner join sys.foreign_key_columns fk on fkn.object_id = fk.constraint_object_id
inner join sys.columns clm1 on fk.parent_column_id = clm1.column_id and fk.parent_object_id = clm1.object_id
inner join sys.columns clm2 on fk.referenced_column_id = clm2.column_id and fk.referenced_object_id = clm2.object_id
left join sys.extended_properties ep_desc on ep_desc.major_id = fkn.object_id and ep_desc.name = 'MS_Description'

";
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, sql, database.DefaultTimeout))
            using (var reader = cmd.ExecuteReader())
            {
                var rows = GetAllForeignKeysRow.ReadAll(reader);
                return rows
                    .GroupBy(r => r.constraint, r => r.columns)
                    .Select(rs => new ForeignKey {
                        Name = rs.Key.foreign_key_name,
                        Description = rs.Key.foreign_key_description,
                        PrimaryTable = new TableIdentifier(rs.Key.primary_table_schema, rs.Key.primary_table_name),
                        PrimaryColumns = rs.OrderBy(r => r.column_index).Select(r => r.primary_column).ToArray(),
                        ForeignTable = new TableIdentifier(rs.Key.foreign_table_schema, rs.Key.foreign_table_name),
                        ForeignColumns = rs.OrderBy(r => r.column_index).Select(r => r.foreign_column).ToArray(),
                        EnforceConstraint = !rs.Key.is_disabled,
                        EnforceForReplication = !rs.Key.is_not_for_replication,
                        UpdateRule = InterpretRule(rs.Key.update_referential_action),
                        DeleteRule = InterpretRule(rs.Key.delete_referential_action)
                    })
                    .ToList();
            }
        }

        private static ForeignKey.Rule InterpretRule(int value)
        {
            switch (value)
            {
                case 0: return ForeignKey.Rule.NoAction;
                case 1: return ForeignKey.Rule.Cascade;
                case 2: return ForeignKey.Rule.SetNull;
                case 3: return ForeignKey.Rule.SetDefault;
                default:
                    throw new ArgumentOutOfRangeException(nameof(value), value, null);
            }
        }

        private class GetAllForeignKeysRow
        {
            public static IEnumerable<GetAllForeignKeysRow> ReadAll(IDataReader reader)
            {
                while (reader.Read())
                {
                    yield return new GetAllForeignKeysRow {
                        constraint = new ConstraintDef {
                            foreign_key_name = Get<string>("foreign_key_name"),
                            foreign_key_description = Get<string>("foreign_key_description"),
                            foreign_key_id = Get<int>("foreign_key_id"),
                            primary_table_name = Get<string>("primary_table_name"),
                            primary_table_schema = Get<string>("primary_table_schema"),
                            foreign_table_name = Get<string>("foreign_table_name"),
                            foreign_table_schema = Get<string>("foreign_table_schema"),
                            is_not_for_replication = Get<bool>("is_not_for_replication"),
                            is_disabled = Get<bool>("is_not_for_replication"),
                            update_referential_action = Get<int>("update_referential_action"),
                            delete_referential_action = Get<int>("delete_referential_action"),
                            is_not_trusted = Get<bool>("is_not_trusted"),
                        },
                        columns = new ColumnDefPair {
                            column_index = Get<int>("column_index"),
                            primary_column = Get<string>("primary_column"),
                            foreign_column = Get<string>("foreign_column")
                        }
                    };
                }

                T Get<T>(string name) => (T)Convert.ChangeType(reader[name], typeof(T));
            }

            public ConstraintDef constraint { get; set; }
            public ColumnDefPair columns { get; set; }

            public struct ConstraintDef
            {
                public string foreign_key_name { get; set; }
                public string foreign_key_description { get; set; }
                public int foreign_key_id { get; set; }
                public string primary_table_name { get; set; }
                public string primary_table_schema { get; set; }
                public string foreign_table_name { get; set; }
                public string foreign_table_schema { get; set; }

                public bool is_not_for_replication { get; set; }
                public bool is_disabled { get; set; }
                public int update_referential_action { get; set; }
                public int delete_referential_action { get; set; }
                public bool is_not_trusted { get; set; }
            }
            public struct ColumnDefPair
            {
                public int column_index { get; set; }
                public string primary_column { get; set; }
                public string foreign_column { get; set; }
            }
        }
    }
}
