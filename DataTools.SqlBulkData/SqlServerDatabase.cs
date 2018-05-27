using System;
using System.Data.SqlClient;

namespace DataTools.SqlBulkData
{
    public class SqlServerDatabase
    {
        private readonly SqlConnectionStringBuilder connectionString;

        public SqlServerDatabase(string connectionString)
        {
            this.connectionString = new SqlConnectionStringBuilder(connectionString);
        }

        public string Name => connectionString.InitialCatalog;
        public string Server => connectionString.DataSource;
        public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromHours(8);

        public override string ToString() => $"{Name} on {Server}";

        public SqlConnection OpenConnection()
        {
            var cn = new SqlConnection(connectionString.ConnectionString);
            cn.Open();
            return cn;
        }
    }
}
