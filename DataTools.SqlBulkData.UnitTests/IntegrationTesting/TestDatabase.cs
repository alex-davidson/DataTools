using System.Collections.Generic;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests.IntegrationTesting
{
    public class TestDatabase
    {
        public static TestDatabase LocalTempDb => UseFirstAvailable(
            "data source=(local);initial catalog=tempdb;Integrated Security=SSPI;Connect Timeout = 1",
            "data source=(local)\\SQLExpress;initial catalog=tempdb;Integrated Security=SSPI;Connect Timeout = 1"
            );

        private readonly SqlServerDatabase instance;

        public TestDatabase(string connectionString)
        {
            instance = new SqlServerDatabase(connectionString);
        }

        public bool IsAvailable => CheckAvailability(instance);

        public SqlServerDatabase Get()
        {
            // We want to rapidly report 'inconclusive' if the database is unavailable.
            if (!IsAvailable) Assert.Ignore($"Database unavailable: {instance}");
            return instance;
        }

        private static TestDatabase UseFirstAvailable(string firstConnectionString, params string[] fallbackConnectionStrings)
        {
            var first = new TestDatabase(firstConnectionString);
            if (!first.IsAvailable)
            {
                foreach (var connectionString in fallbackConnectionStrings)
                {
                    var fallback = new TestDatabase(connectionString);
                    if (fallback.IsAvailable) return fallback;
                }
            }
            return first;
        }

        private static readonly Dictionary<SqlServerDatabase, bool> availability = new Dictionary<SqlServerDatabase, bool>(new SqlServerDatabaseEqualityComparer());

        private static bool CheckAvailability(SqlServerDatabase instance)
        {
            lock (availability)
            {
                if (availability.TryGetValue(instance, out var isAvailable)) return isAvailable;
                try
                {
                    using (instance.OpenConnection()) { }
                    availability[instance] = true;
                    return true;
                }
                catch
                {
                    availability[instance] = false;
                    return false;
                }
            }
        }
    }
}
