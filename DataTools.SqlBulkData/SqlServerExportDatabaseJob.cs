using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace DataTools.SqlBulkData
{
    /// <summary>
    /// Export each table in the specified database to a separate file in the target directory.
    /// </summary>
    public class SqlServerExportDatabaseJob
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(SqlServerExportDatabaseJob));

        public int ConcurrencyLevel { get; set; } = Environment.ProcessorCount;
        public TableFileNamingRule TableFileNamingRule { get; set; } = new TableFileNamingRule();
        public BulkFileStreamFactory BulkFileStreamFactory { get; set; } = new BulkFileStreamFactory();
        public SqlServerExportModelBuilder ModelBuilder { get; set; } = new SqlServerExportModelBuilder();

        public async Task Execute(SqlServerDatabase sqlServerDatabase, string bulkFilesPath, CancellationToken token)
        {
            var workerLimit = new SemaphoreSlim(ConcurrencyLevel);
            var bulkExporter = new SqlServerBulkTableExport(sqlServerDatabase);
            Directory.CreateDirectory(bulkFilesPath);

            var tables = new GetAllTablesQuery().List(sqlServerDatabase);
            var models = tables.Select(ModelBuilder.Build).ToArray();

            var tasks = models
                .Select(m => {
                    var fileName = TableFileNamingRule.GetFileNameForTable(m.TableDescriptor);
                    var filePath = Path.Combine(bulkFilesPath, fileName);
                    return Task.Run(() => ExportToPath(m, filePath), token);
                })
                .ToArray();

            await Task.WhenAll(tasks);
            token.ThrowIfCancellationRequested();
            log.Info($"Exported {tables.Count} tables to {bulkFilesPath}");

            async Task ExportToPath(ExportModel model, string filePath)
            {
                try
                {
                    using (var lease = await workerLimit.WaitForLeaseAsync(token))
                    {
                        log.Info($"Starting: {model.TableDescriptor} -> {filePath}");
                        using (var stream = BulkFileStreamFactory.OpenForExport(filePath))
                        {
                            using (var fileWriter = new BulkTableFileWriter(stream, true))
                            {
                                await bulkExporter.Execute(model, fileWriter, token);
                            }
                            lease.Dispose();
                            await stream.FlushAsync(token);
                        }
                        log.Info($"Finished: {model.TableDescriptor} -> {filePath}");
                    }
                }
                catch (OperationCanceledException)
                {
                    log.Debug($"Cancelled: {filePath}");
                    if (token.IsCancellationRequested) return;
                    throw;
                }
            }
        }
    }
}
