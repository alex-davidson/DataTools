using DataTools.SqlBulkData.UnitTests.IntegrationTesting;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class GetAllIndexesOnViewsQueryTests
    {
        [Test]
        public void CanExecute()
        {
            new GetAllIndexesOnViewsQuery().List(TestDatabase.LocalTempDb.Get());
        }
    }
}
