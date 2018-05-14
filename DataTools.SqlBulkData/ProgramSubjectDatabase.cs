namespace DataTools.SqlBulkData
{
    public class ProgramSubjectDatabase
    {
        public string Server { get; set; } = "(local)";
        public string Database { get; set; } = "tempdb";

        public string GetConnectionString()
        {
            return $"data source={Server};initial catalog={Database};Integrated Security=SSPI";
        }
    }
}
