using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace DataTools.SqlBulkData
{
    public class Program
    {
        static int Main(string[] args)
        {
            try
            {
                var program = new Program();
                new ArgumentParser().Parse(args, program);
                return program.Run().GetAwaiter().GetResult();
            }
            catch (InvalidArgumentsException ex)
            {
                Console.Error.WriteLine(ex.Message);
                new ArgumentParser().WriteUsage(Console.Error);
                return 1;
            }
        }

        public ProgramMode Mode { get; set; }
        public string BulkFilesPath { get; set; }
        public ProgramSubjectDatabase SubjectDatabase { get; set; } = new ProgramSubjectDatabase();

        public Task<int> Run()
        {
            var sqlServerDatabase = new SqlServerDatabase(SubjectDatabase.GetConnectionString());
            var versionString = new GetDatabaseServerVersionQuery().Execute(sqlServerDatabase);
            if (versionString == null)
            {
                Console.Error.WriteLine("Database did not return a version string.");
                return Task.FromResult(2);
            }
            Console.Error.WriteLine($"{sqlServerDatabase.Server}: {versionString}");
            return Task.FromResult(0);
        }
    }
}
