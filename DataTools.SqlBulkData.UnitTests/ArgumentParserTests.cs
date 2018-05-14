using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class ArgumentParserTests
    {
        [Test]
        public void ParsesValidMode()
        {
            var program = Parse(new [] { "import" });
            Assert.That(program.Mode, Is.EqualTo(ProgramMode.Import));
        }

        [Test]
        public void ParsesServer()
        {
            var program = Parse(new [] { "import", "-s", "localhost" });
            Assert.That(program.Mode, Is.EqualTo(ProgramMode.Import));
            Assert.That(program.SubjectDatabase.Server, Is.EqualTo("localhost"));
        }

        [TestCase(Description = "No mode")]
        [TestCase("-s", "localhost", Description = "Server but no mode")]
        [TestCase("import", "-s", Description = "Missing server name")]
        public void InvalidArguments(params string[] args)
        {
            Assert.Throws<InvalidArgumentsException>(() => Parse(args));
        }

        private Program Parse(string[] args)
        {
            var program = new Program();
            new ArgumentParser().Parse(args, program);
            return program;
        }
    }
}
