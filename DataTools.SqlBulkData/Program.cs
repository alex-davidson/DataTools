using System;
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
            return Task.FromResult(0);
        }
    }
}
