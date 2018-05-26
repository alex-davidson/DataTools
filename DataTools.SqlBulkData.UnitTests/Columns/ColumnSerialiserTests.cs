using System;
using System.IO;
using System.Linq;
using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests.Columns
{
    [TestFixture]
    public class ColumnSerialiserTests
    {
        public static ICase[] Cases = {
            new Case<long> { Column = f => new SqlServerBigIntColumn { Flags = f }, TestValue = -26872773602773242L },
            new Case<int> { Column = f => new SqlServerIntColumn { Flags = f }, TestValue = -42 },
            new Case<short> { Column = f => new SqlServerSmallIntColumn { Flags = f }, TestValue = -42 },
            new Case<byte> { Column = f => new SqlServerTinyIntColumn { Flags = f }, TestValue = 42 },
            new Case<string> { Column = f => new SqlServerVariableLengthStringColumn { Flags = f }, TestValue = "ASCII string" },
            new Case<string> { Column = f => new SqlServerFixedLengthANSIStringColumn(12) { Flags = f }, TestValue = "ASCII string" },
            new Case<string> { Column = f => new SqlServerVariableLengthStringColumn { Flags = f }, TestValue = "Ṳṅḯḉоɖέ string" },
            new Case<float> { Column = f => new SqlServerSinglePrecisionColumn { Flags = f }, TestValue = (float)Math.E },
            new Case<double> { Column = f => new SqlServerDoublePrecisionColumn { Flags = f }, TestValue = Math.PI },
            new Case<decimal> { Column = f => new SqlServerDecimalColumn(DecimalPacker.ForBufferSize(16)) { Flags = f }, TestValue = new decimal(Math.PI) },
            new Case<Guid> { Column = f => new SqlServerUniqueIdentifierColumn { Flags = f }, TestValue = Guid.NewGuid() },
            new Case<byte[]> { Column = f => new SqlServerFixedLengthBytesColumn(5) { Flags = f }, TestValue = new byte[] { 0x03, 0x3F, 0x94, 0xF3, 0x92 } },
            new Case<byte[]> { Column = f => new SqlServerVariableLengthBytesColumn { Flags = f }, TestValue = new byte[] { 0x03, 0x3F, 0x94, 0xF3, 0x92 } },
            new Case<TimeSpan> { Column = f => new SqlServerTimeColumn { Flags = f }, TestValue = new TimeSpan(-5, 04, 59, 23, 192) },
            new Case<DateTime> { Column = f => new SqlServerDateTimeColumn { Flags = f }, TestValue = new DateTime(2016, 05, 26, 18, 49, 13, 750, DateTimeKind.Utc) },
            new Case<DateTimeOffset> { Column = f => new SqlServerDateTimeOffsetColumn { Flags = f }, TestValue = new DateTimeOffset(new DateTime(2016, 05, 26, 18, 49, 13, 750), TimeSpan.FromHours(-7)) },
        };

        [Test]
        public void AssertThatValueRoundtrips([ValueSource(nameof(Cases))] ICase testCase, [Values] ColumnFlags flags)
        {
            var column = testCase.Column(flags);
            var serialiser = column.GetSerialiser();
            new ColumnSerialiserValidator().Validate(serialiser);
            Assert.That(testCase.TestValue.GetType(), Is.EqualTo(serialiser.DotNetType));

            var roundtripped = Roundtrip(column, testCase.TestValue);
            Assert.That(roundtripped, Is.EqualTo(testCase.TestValue));
        }

        [Test]
        public void AssertThatNullValueRoundtripsThroughNullableSerialiser([ValueSource(nameof(Cases))] ICase testCase)
        {
            var column = testCase.Column(ColumnFlags.Nullable);
            new ColumnSerialiserValidator().Validate(column.GetSerialiser());

            Roundtrip(column, null);
        }

        [Test, Description("This test is included for completeness, but the serialiser should not be called for null columns when AbsentIfNull is set.")]
        public void AssertThatNullValueRoundtripsThroughAbsentWhenNullSerialiser([ValueSource(nameof(Cases))] ICase testCase)
        {
            var column = testCase.Column(ColumnFlags.AbsentWhenNull);
            new ColumnSerialiserValidator().Validate(column.GetSerialiser());

            Roundtrip(column, null);
        }

        [Test, Description("This test is included for completeness, but we really don't care how nulls are roundtripped (or not) for not-null columns.")]
        public void AssertThatNullValueDoesNotRoundtripThroughNotNullableSerialiser([ValueSource(nameof(Cases))] ICase testCase)
        {
            var column = testCase.Column(ColumnFlags.None);
            new ColumnSerialiserValidator().Validate(column.GetSerialiser());

            try
            {
                Assert.IsNotNull(Roundtrip(column, null));
            }
            catch (InvalidDataException) { Assert.Pass(); }
        }

        [Test]
        public void AllColumnSerialisersAreTested()
        {
            var allTypes = typeof(IColumnSerialiser).Assembly.GetTypes()
                .Where(c => c.IsClass && !c.IsAbstract)
                .Where(typeof(IColumnDefinition).IsAssignableFrom)
                .ToArray();
            var testedTypes = Cases.Select(c => c.Column(ColumnFlags.None).GetType());
            Assert.That(allTypes.Except(testedTypes).ToArray(), Is.Empty);

            // Sanity check. Shouldn't be testing classes which don't exist.
            Assert.That(testedTypes.Except(allTypes).ToArray(), Is.Empty);
        }

        private object Roundtrip(IColumnDefinition column, object value)
        {
            var nullMap = new [] { value == null && column.GetSerialiser().Flags.IsNullable() };
            var stream = new MemoryStream();
            // Verify alignment symmetry by starting off misaligned.
            stream.WriteByte(0);

            column.GetSerialiser().Write(stream, new MockDataRecord(new [] { "Field" }, new [] { value ?? DBNull.Value }), 0);
            var end = stream.Position;

            stream.Position = 1;

            var roundtripped = column.GetSerialiser().Read(stream, 0, nullMap);
            Assert.That(stream.Position, Is.EqualTo(end));
            return roundtripped;
        }

        public interface ICase
        {
            Func<ColumnFlags, IColumnDefinition> Column { get; }
            object TestValue { get; }
        }

        public class Case<T> : ICase
        {
            public Func<ColumnFlags, IColumnDefinition> Column { get; set; }
            public T TestValue { get; set; }
            object ICase.TestValue => TestValue;

            public override string ToString() => $"{Column(ColumnFlags.None).GetType().Name}: {TestValue}";
        }
    }
}
