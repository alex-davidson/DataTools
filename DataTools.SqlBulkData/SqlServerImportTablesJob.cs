using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Schema;
using log4net;

namespace DataTools.SqlBulkData
{
    /// <summary>
    /// For every file in the source directory, attempt to locate a corresponding table in the
    /// database. If found, import the contents of the file into that table.
    /// </summary>
    public class SqlServerImportTablesJob
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(SqlServerImportTablesJob));

        public int ConcurrencyLevel { get; set; } = Environment.ProcessorCount;
        public TableFileNamingRule TableFileNamingRule { get; set; } = new TableFileNamingRule();
        public BulkFileStreamFactory BulkFileStreamFactory { get; set; } = new BulkFileStreamFactory();
        public SqlServerImportModelBuilder ModelBuilder { get; set; } = new SqlServerImportModelBuilder();
        public bool TruncateTableBeforeImport { get; set; } = true;

        public async Task Execute(SqlServerDatabase sqlServerDatabase, string bulkFilesPath, CancellationToken token)
        {
            var workerLimit = new SemaphoreSlim(ConcurrencyLevel);
            var bulkImporter = new SqlServerBulkTableImport(sqlServerDatabase);

            var tablesImported = 0;
            var filesImported = 0;

            var files = TableFileNamingRule.ListTableFiles(bulkFilesPath);
            log.Debug($"Found {files.Length} files.");
            var tables = new GetAllTablesQuery().List(sqlServerDatabase);
            log.Debug($"Found {tables.Count} tables.");
            var tableLookup = tables.ToLookup(t => t.Identify());

            var disabledConstraintTables = new List<Table>();
            var clearedTables = new HashSet<TableIdentifier>();
            try
            {
                DisableAllConstraints(sqlServerDatabase, tables, disabledConstraintTables);
                if (token.IsCancellationRequested) return;
                if (token.IsCancellationRequested) return;

                var tasks = files
                    .Select(f => Task.Run(() => ImportFromFile(f), token))
                    .ToArray();

                await Task.WhenAll(tasks);
                token.ThrowIfCancellationRequested();
                log.Info($"Imported {tablesImported} tables from {filesImported} files in {bulkFilesPath}");

                new CheckConstraintsStatement().Execute(sqlServerDatabase);
            }
            finally
            {
                EnableAllConstraints(sqlServerDatabase, disabledConstraintTables);
            }
            log.Info("Constraints are restored and verified.");

            void MaybeTruncateTable(Table table)
            {
                if (!TruncateTableBeforeImport) return;
                lock (table)
                {
                    lock (clearedTables)
                    {
                        if (!clearedTables.Add(table.Identify())) return;
                    }

                    log.Info($"Clearing table: {table}");
                    new TruncateTableStatement().Execute(sqlServerDatabase, table);
                }
            }

            async Task ImportFromFile(string filePath)
            {
                try
                {
                    using (var stream = BulkFileStreamFactory.OpenForImport(filePath))
                    using (var fileReader = new BulkTableFileReader(stream, true))
                    while (fileReader.MoveNext())
                    {
                        var candidateTables = tableLookup[fileReader.Current.Table.Identify()].ToArray();
                        if (!candidateTables.Any())
                        {
                            log.Warn($"Found no target table in database, for table {fileReader.Current.Table} in file {filePath}.");
                            continue;
                        }
                        if (candidateTables.Length > 1)
                        {
                            throw new NotSupportedException($"Found {candidateTables.Length} target tables in database, for table {fileReader.Current.Table} in file {filePath}.");
                        }

                        var targetTable = candidateTables.Single();
                        var model = ModelBuilder.Build(targetTable, fileReader.Current.Columns);
                        using (var lease = await workerLimit.WaitForLeaseAsync(token))
                        {
                            log.Info($"Starting: {filePath} -> {model.Table.Name}");
                            try
                            {
                                MaybeTruncateTable(targetTable);
                                await bulkImporter.Execute(model, fileReader.Current.DataStream, token);
                                lease.Dispose();
                                log.Info($"Finished: {filePath} -> {model.Table.Name}");
                            }
                            catch (SqlException)
                            {
                                log.Warn($"Failed: {filePath} -> {model.Table.Name}");
                                throw;
                            }
                        }
                        Interlocked.Increment(ref tablesImported);
                    }
                    Interlocked.Increment(ref filesImported);
                }
                catch (OperationCanceledException)
                {
                    log.Debug($"Cancelled: {filePath}");
                    if (token.IsCancellationRequested) return;
                    throw;
                }
                catch (UnrecognisedFileFormatException ex)
                {
                    throw new UnrecognisedFileFormatException($"File {filePath} did not appear to be a valid bulk file.", ex);
                }
            }
        }

        private static void DisableAllConstraints(SqlServerDatabase database, IList<Table> allTables, IList<Table> disabled)
        {
            foreach (var table in allTables)
            {
                disabled.Add(table);
                try
                {
                    new DisableConstraintsStatement().Execute(database, table);
                }
                catch
                {
                    log.Error($"Failed to disable constraints on table: {table}");
                    // Bail out and let the caller re-enable constraints on those we did already, if possible.
                    throw;
                }
            }
        }

        private static void EnableAllConstraints(SqlServerDatabase database, IList<Table> allTables)
        {
            foreach (var table in allTables)
            {
                try
                {
                    new EnableConstraintsStatement().Execute(database, table);
                }
                catch (Exception ex)
                {
                    log.Error($"Failed to enable constraints on table: {table}");
                    log.Debug(ex);
                    // Should still re-enable constraints on all the other tables, so carry on.
                }
            }
        }
    }
}
