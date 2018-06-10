using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
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

            var foreignKeys = new GetAllForeignKeysQuery().List(sqlServerDatabase);
            log.Debug($"Found {foreignKeys.Count} foreign keys.");

            var disabledConstraintTables = new List<Table>();
            var deletedForeignKeys = new List<ForeignKey>();
            var clearedTables = new Dictionary<TableIdentifier, Task>();

            try
            {
                DisableAllConstraints(sqlServerDatabase, tables, disabledConstraintTables);
                if (token.IsCancellationRequested) return;
                if (token.IsCancellationRequested) return;

                try
                {
                    // I would rather only drop foreign keys relevant to the tables being
                    // imported, but this seems to cause occasional import failures
                    // complaining of a 'schema change'. I suspect this happens when:
                    // * an indexed view touches tables A and B,
                    // * a bulk import against table A is in progress,
                    // * a foreign key is dropped from table B.
                    // It's easier and safer to just drop all of them up-front.
                    DropAllForeignKeys(sqlServerDatabase, foreignKeys, deletedForeignKeys);

                    var tasks = files
                        // Simple heuristic for better parallelisation: start with large files.
                        .OrderByDescending(f => new FileInfo(f).Length)
                        .Select(f => Task.Run(() => ImportFromFile(f), token))
                        .ToArray();

                    await Task.WhenAll(tasks);
                    token.ThrowIfCancellationRequested();
                    log.Info($"Imported {tablesImported} tables from {filesImported} files in {bulkFilesPath}");
                }
                finally
                {
                    RecreateForeignKeys(sqlServerDatabase, deletedForeignKeys);
                }
                new CheckConstraintsStatement().Execute(sqlServerDatabase);
            }
            finally
            {
                EnableAllConstraints(sqlServerDatabase, disabledConstraintTables);
            }
            log.Info("Constraints are restored and verified.");

            Task MaybeTruncateTable(Table table)
            {
                if (!TruncateTableBeforeImport) return Task.CompletedTask;
                lock (clearedTables)
                {
                    if (clearedTables.TryGetValue(table.Identify(), out var task)) return task;
                    var newTask = Task.Run(() => {
                        log.Info($"Clearing table: {table}");
                        return new TruncateTableStatement().ExecuteAsync(sqlServerDatabase, table, token);
                    }, token);
                    clearedTables.Add(table.Identify(), newTask);
                    return newTask;
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
                                await MaybeTruncateTable(targetTable);
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

        private static void DropAllForeignKeys(SqlServerDatabase database, IList<ForeignKey> allForeignKeys, IList<ForeignKey> deleted)
        {
            foreach (var key in allForeignKeys)
            {
                try
                {
                    new DeleteForeignKeyStatement().Execute(database, key);
                    deleted.Add(key);
                }
                catch
                {
                    log.Error($"Failed to delete foreign key: {key}");
                    // Bail out and let the caller recreate constraints which we already deleted, if possible.
                    throw;
                }
            }
        }

        private static void RecreateForeignKeys(SqlServerDatabase database, ICollection<ForeignKey> foreignKeys)
        {
            foreach (var key in foreignKeys)
            {
                try
                {
                    // By recreating the foreign keys 'disabled' we can recreate all of them, restoring
                    // the schema to its pre-import state before we worry about constraint violations.
                    // This makes it easier for the user to recover from failures, because their database
                    // does at least contain all its original objects and they just need to resolve data
                    // issues.
                    new CreateForeignKeyStatement { WithNoCheck = true }.Execute(database, key);
                }
                catch (Exception ex)
                {
                    log.Error($"Failed to recreate foreign key: {key}");
                    log.Debug(ex);
                    // Should still re-create all other foreign keys, so carry on.
                }
            }
        }
    }
}
