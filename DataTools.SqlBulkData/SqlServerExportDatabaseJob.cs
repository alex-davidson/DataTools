using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
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

        public bool Compress { get; set; } = true;
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
                    var uncompressedFilePath = Path.Combine(bulkFilesPath, fileName);
                    var compressedFilePath = Compress ? $"{uncompressedFilePath}.gz" : null;
                    return Task.Run(() => ExportToPath(m, uncompressedFilePath, compressedFilePath), token);
                })
                .ToArray();

            await Task.WhenAll(tasks);
            token.ThrowIfCancellationRequested();
            log.Info($"Exported {tables.Count} tables to {bulkFilesPath}");

            async Task ExportToPath(ExportModel model, string uncompressedFilePath, string compressedFilePath = null)
            {
                var filesToCleanUp = new HashSet<string>();
                try
                {
                    using (var lease = await workerLimit.WaitForLeaseAsync(token))
                    {
                        log.Info($"Starting: {model.TableDescriptor} -> {compressedFilePath ?? uncompressedFilePath}");
                        try
                        {
                            using (var stream = BulkFileStreamFactory.OpenForExport(uncompressedFilePath))
                            {
                                filesToCleanUp.Add(uncompressedFilePath);
                                using (var fileWriter = new BulkTableFileWriter(stream, true))
                                {
                                    await bulkExporter.Execute(model, fileWriter, token);
                                }
                                if (compressedFilePath == null)
                                {
                                    lease.Dispose();
                                    await stream.FlushAsync(token);
                                    filesToCleanUp.Remove(uncompressedFilePath);
                                }
                                else
                                {
                                    stream.Position = 0;
                                    using (var compressedStream = BulkFileStreamFactory.OpenForExport(compressedFilePath))
                                    {
                                        filesToCleanUp.Add(compressedFilePath);
                                        using (var gzipStream = new GZipStream(compressedStream, CompressionLevel.Optimal, true))
                                        {
                                            await stream.CopyToAsync(gzipStream);
                                        }
                                        lease.Dispose();
                                        await compressedStream.FlushAsync(token);
                                    }
                                    filesToCleanUp.Remove(compressedFilePath);
                                }
                            }
                            log.Info($"Finished: {model.TableDescriptor} -> {compressedFilePath ?? uncompressedFilePath}");
                        }
                        catch (SqlException)
                        {
                            log.Warn($"Failed: {model.TableDescriptor} -> {compressedFilePath ?? uncompressedFilePath}");
                            throw;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    log.Debug($"Cancelled: {compressedFilePath ?? uncompressedFilePath}");
                    if (token.IsCancellationRequested) return;
                    throw;
                }
                finally
                {
                    foreach (var file in filesToCleanUp)
                    {
                        File.Delete(file);
                    }
                }
            }
        }
    }
}
