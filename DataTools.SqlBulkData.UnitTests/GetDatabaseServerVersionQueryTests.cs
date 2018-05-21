using DataTools.SqlBulkData.UnitTests.IntegrationTesting;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class GetDatabaseServerVersionQueryTests
    {
        [Test]
        public void CanExecute()
        {
            new GetDatabaseServerVersionQuery().Execute(TestDatabase.LocalTempDb.Get());
        }
    }
}
