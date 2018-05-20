using System;
using System.Diagnostics;
using System.Threading.Tasks;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;

namespace DataTools.SqlBulkData
{
    public class Program
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Program));

        static int Main(string[] args)
        {
            var sw = Stopwatch.StartNew();
            var program = new Program();
            try
            {
                new ArgumentParser().Parse(args, program);
                var exitCode = program.Run().GetAwaiter().GetResult();
                sw.Stop();
                if (program.PauseBeforeExit)
                {
                    Console.Error.WriteLine("Press any key to exit...");
                    Console.ReadKey();
                }
                return exitCode;
            }
            catch (InvalidArgumentsException ex)
            {
                Console.Error.WriteLine(ex.Message);
                new ArgumentParser().WriteUsage(Console.Error);
                return 1;
            }
            catch (ApplicationException ex)
            {
                if (log.IsDebugEnabled)
                {
                    log.Error(ex);
                }
                else
                {
                    log.Error(ex.Message);
                }
                return 3;
            }
            catch (OperationCanceledException)
            {
                log.Warn("Cancelled.");
                return 255;
            }
            catch (Exception ex)
            {
                log.Error(ex);
                return 4;
            }
            finally
            {
                log.Debug($"Execution time: {sw.Elapsed}");
            }
        }

        public ProgramMode Mode { get; set; }
        public string BulkFilesPath { get; set; }
        public ProgramSubjectDatabase SubjectDatabase { get; set; } = new ProgramSubjectDatabase();
        public LoggingVerbosity LoggingVerbosity { get; } = new LoggingVerbosity();
        public bool PauseBeforeExit { get; set; }

        public Task<int> Run()
        {
            BasicConfigurator.Configure(new ConsoleAppender {
                Name = "Console.STDERR",
                Layout = new PatternLayout("%message%newline"),
                Target = "Console.Error",
                Threshold = LoggingVerbosity.Current
            });
            var cancelMonitor = new CancelKeyMonitor();
            cancelMonitor.LogRequestsTo(log);

            var sqlServerDatabase = new SqlServerDatabase(SubjectDatabase.GetConnectionString());
            var versionString = new GetDatabaseServerVersionQuery().Execute(sqlServerDatabase);
            if (versionString == null)
            {
                log.Error("Database did not return a version string.");
                return Task.FromResult(2);
            }
            log.Info($"{sqlServerDatabase.Server}: {versionString}");
            return Task.FromResult(0);
        }
    }
}
