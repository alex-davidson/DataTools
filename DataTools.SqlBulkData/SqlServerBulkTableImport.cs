﻿using System;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DataTools.SqlBulkData
{
    public class SqlServerBulkTableImport
    {
        private readonly SqlServerDatabase database;
        public SqlServerImportModelBuilder ModelBuilder { get; set; } = new SqlServerImportModelBuilder();
        public TimeSpan Timeout { get; set; } = TimeSpan.FromHours(8);

        public SqlServerBulkTableImport(SqlServerDatabase database)
        {
            this.database = database;
        }

        public async Task Execute(ImportModel model, Stream bulkDataStream, CancellationToken token)
        {
            using (var cn = database.OpenConnection())
            using (var bulkCopy = new SqlBulkCopy(cn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.UseInternalTransaction, null))
            {
                PrepareSqlBulkCopy(bulkCopy, model);

                var rowReader = new BulkRowReader(bulkDataStream, model.ColumnSerialisers);
                using (var rowDataReader = new BulkRowDataReader(rowReader, model.ColumnMetaInfos))
                {
                    await bulkCopy.WriteToServerAsync(rowDataReader, token);
                }
            }
        }

        private void PrepareSqlBulkCopy(SqlBulkCopy bulkCopy, ImportModel model)
        {
            bulkCopy.EnableStreaming = true;
            bulkCopy.BulkCopyTimeout = (int)Timeout.TotalSeconds;
            bulkCopy.DestinationTableName = Sql.Escape(model.Table.Schema, model.Table.Name);
            bulkCopy.ColumnMappings.Clear();
            foreach (var field in model.ColumnMetaInfos)
            {
                bulkCopy.ColumnMappings.Add(field.Name, field.Name);
            }
        }
    }
}
