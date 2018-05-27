using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class TableFileNamingRuleTests
    {
        [Test]
        public void IdentifiesFileWithExtension()
        {
            var rule = new TableFileNamingRule { Extension = "test" };

            Assert.That(rule.IsTableFilePath(@"c:\data\testfile.test"), Is.True);
        }
        [Test]
        public void IdentifiesGzippedFileWithExtension()
        {
            var rule = new TableFileNamingRule { Extension = "test" };

            Assert.That(rule.IsTableFilePath(@"c:\data\testfile.test.gz"), Is.True);
        }

        [Test]
        public void IdentifiesFileWithDefaultExtension()
        {
            var rule = new TableFileNamingRule();

            Assert.That(rule.IsTableFilePath(@"c:\data\testfile.bulktable"), Is.True);
        }
    }
}
