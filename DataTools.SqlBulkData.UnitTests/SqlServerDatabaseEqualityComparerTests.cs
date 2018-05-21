using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class SqlServerDatabaseEqualityComparerTests
    {
        [TestCase("data source=(local);initial catalog=tempdb", "data source=(local);initial catalog=tempdb")]
        [TestCase("data source=(local);initial catalog=tempdb", "data source=localhost;initial catalog=TEMPDB")]
        [TestCase("data source=(local)\\SQLExpress;initial catalog=tempdb", "data source=(local)\\sqlexpress;initial catalog=tempdb")]
        [TestCase("data source=localhost\\SQLExpress;initial catalog=tempdb", "data source=(local)\\sqlexpress;initial catalog=tempdb")]
        public void AreEqual(string x, string y)
        {
            Assert.That(new SqlServerDatabase(x), Is.EqualTo(new SqlServerDatabase(y)).Using(new SqlServerDatabaseEqualityComparer()));
        }

        [TestCase("data source=(local);initial catalog=tempdb", "data source=(local)\\SQLExpress;initial catalog=tempdb")]
        [TestCase("data source=remote;initial catalog=tempdb", "data source=(local);initial catalog=tempdb")]
        [TestCase("data source=remote;initial catalog=tempdb", "data source=remote\\instance);initial catalog=TEMPdb")]
        [TestCase("data source=remote;initial catalog=tempdb", "data source=remote;initial catalog=master")]
        public void AreNotEqual(string x, string y)
        {
            Assert.That(new SqlServerDatabase(x), Is.Not.EqualTo(new SqlServerDatabase(y)).Using(new SqlServerDatabaseEqualityComparer()));
        }
    }
}
