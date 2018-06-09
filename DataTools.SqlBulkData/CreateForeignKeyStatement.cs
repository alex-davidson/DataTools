using System;
using System.Data.SqlClient;
using DataTools.SqlBulkData.Schema;

namespace DataTools.SqlBulkData
{
    public class CreateForeignKeyStatement
    {
        public bool WithNoCheck { get; set; }

        public void Execute(SqlServerDatabase database, ForeignKey key)
        {
            var check = key.EnforceConstraint && !WithNoCheck;

            using (var cn = database.OpenConnection())
            {
                var createSql = Sql.Statement(
                    $"alter table {Sql.Escape(key.ForeignTable.Schema, key.ForeignTable.Name)}",
                    check ? "with check" : "with nocheck",
                    $"add constraint {Sql.Escape(key.Name)}",
                    $"foreign key ({Sql.EscapeColumnList(key.ForeignColumns)})",
                    $"references {Sql.Escape(key.PrimaryTable.Schema, key.PrimaryTable.Name)}({Sql.EscapeColumnList(key.PrimaryColumns)})",
                    FormatRule("update", key.UpdateRule),
                    FormatRule("delete", key.DeleteRule),
                    key.EnforceForReplication ? "" : "not for replication"
                );
                using (var cmd = Sql.CreateQuery(cn, createSql, database.DefaultTimeout))
                {
                    cmd.ExecuteNonQuery();
                }

                var checkSql = Sql.Statement(
                    $"alter table {Sql.Escape(key.ForeignTable.Schema, key.ForeignTable.Name)}",
                    check ? "check" : "nocheck",
                    $"constraint {Sql.Escape(key.Name)}"
                );
                using (var cmd = Sql.CreateQuery(cn, checkSql, database.DefaultTimeout))
                {
                    cmd.ExecuteNonQuery();
                }

                if (!String.IsNullOrEmpty(key.Description))
                {
                    var descriptionSql = Sql.Statement(
                        "exec sys.sp_addextendedproperty",
                        "@name=N'MS_Description', @value=@description,",
                        "@level0type=N'SCHEMA', @level0name=@foreignTableSchema,",
                        "@level1type=N'TABLE', @level1name=@foreignTableName,",
                        "@level2type=N'CONSTRAINT',@level2name=@constraintName"
                    );
                    using (var cmd = Sql.CreateQuery(cn, descriptionSql, database.DefaultTimeout))
                    {
                        cmd.Parameters.Add(CreateParameter("description", key.Description));
                        cmd.Parameters.Add(CreateParameter("foreignTableSchema", key.ForeignTable.Schema));
                        cmd.Parameters.Add(CreateParameter("foreignTableName", key.ForeignTable.Name));
                        cmd.Parameters.Add(CreateParameter("constraintName", key.Name));
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        private SqlParameter CreateParameter(string paramName, object paramValue)
        {
            if (paramValue == null) throw new ArgumentNullException(nameof(paramValue));
            var parameter = new SqlParameter(paramName, paramValue);
            return parameter;
        }

        private string FormatRule(string operation, ForeignKey.Rule rule)
        {
            switch (rule)
            {
                case ForeignKey.Rule.NoAction: return "";
                case ForeignKey.Rule.Cascade: return $"on {operation} cascade";
                case ForeignKey.Rule.SetNull: return $"on {operation} set null";
                case ForeignKey.Rule.SetDefault: return $"on {operation} set default";
                default:
                    throw new ArgumentOutOfRangeException(nameof(rule), rule, null);
            }
        }
    }
}
