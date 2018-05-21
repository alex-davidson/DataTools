using DataTools.SqlBulkData.UnitTests.IntegrationTesting;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class GetAllTablesQueryTests
    {
        [Test]
        public void CanExecute()
        {
            new GetAllTablesQuery().List(TestDatabase.LocalTempDb.Get());
        }
    }
}
