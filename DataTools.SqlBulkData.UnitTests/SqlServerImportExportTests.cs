using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DataTools.SqlBulkData.Columns;
using DataTools.SqlBulkData.Schema;
using DataTools.SqlBulkData.UnitTests.IntegrationTesting;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class SqlServerImportExportTests
    {
        public static Case[] SimpleCases = {
            new Case(new Field<long>("bigint", -26872773602773242L)),
            new Case(new Field<int>("int", -42)),
            new Case(new Field<short>("smallint", -42)),
            new Case(new Field<byte>("tinyint", 42)),
            new Case(new Field<string>("varchar(256)", "ASCII string")),
            new Case(new Field<string>("char(12)", "ASCII string")),
            new Case(new Field<string>("nvarchar(256)", "Ṳṅḯḉоɖέ string")),
            new Case(new Field<string>("nchar(14)", "Ṳṅḯḉоɖέ string")),
            new Case(new Field<float>("real", (float)Math.E)),
            new Case(new Field<float>("float(24)", (float)Math.E)),
            new Case(new Field<double>("float(53)", Math.PI)),
            new Case(new Field<decimal>("decimal(38, 14)", SqlDecimal.ConvertToPrecScale(new SqlDecimal(Math.PI), 38, 14).Value)),
            new Case(new Field<decimal>("money", 26872929752907.2978m)),
            new Case(new Field<decimal>("smallmoney", 2907.2978m)),
            new Case(new Field<Guid>("uniqueidentifier", Guid.NewGuid())),
            new Case(new Field<byte[]>("binary(5)", new byte[] { 0x03, 0x3F, 0x94, 0xF3, 0x92 }) { SqlDbType = SqlDbType.Binary } ),
            new Case(new Field<byte[]>("varbinary(12)", new byte[] { 0x03, 0x3F, 0x94, 0xF3, 0x92 }) { SqlDbType = SqlDbType.Binary } ),
            new Case(new Field<byte[]>("image", new byte[] { 0x03, 0x3F, 0x94, 0xF3, 0x92 }) { SqlDbType = SqlDbType.Binary } ),
        };

        public static Case[] CompatibleColumnTypeCases = {
            new Case(new Field<int>("int", -42)) { Target = new Field<long>("bigint", -42) },
            new Case(new Field<short>("smallint", -42)) { Target = new Field<int>("int", -42) },
            new Case(new Field<byte>("tinyint", 42)) { Target = new Field<short>("smallint", 42) },
            new Case(new Field<string>("varchar(256)", "ASCII string")) { Target = new Field<string>("char(12)", "ASCII string") },
            new Case(new Field<string>("varchar(256)", "ASCII string")) { Target = new Field<string>("nchar(12)", "ASCII string") },
            new Case(new Field<string>("char(12)", "ASCII string")) { Target = new Field<string>("varchar(256)", "ASCII string") },
            new Case(new Field<string>("nvarchar(256)", "Ṳṅḯḉоɖέ string")) { Target = new Field<string>("varchar(256)", "??????? string") },
            new Case(new Field<string>("nchar(14)", "Ṳṅḯḉоɖέ string")) { Target = new Field<string>("nvarchar(256)", "Ṳṅḯḉоɖέ string") },
            new Case(new Field<float>("real", (float)Math.E)) { Target = new Field<double>("float(53)", (float)Math.E) },
            new Case(new Field<float>("float(24)", (float)Math.E)) { Target = new Field<float>("real", (float)Math.E) },
            new Case(new Field<double>("float(53)", Math.PI)) { Target = new Field<string>("varchar(max)", Math.PI.ToString()) },
            new Case(new Field<decimal>("decimal(38, 16)", SqlDecimal.ConvertToPrecScale(new SqlDecimal(Math.PI), 38, 16).Value)) { Target = new Field<string>("varchar(max)", new SqlDecimal(Math.PI).Value.ToString()) },
            new Case(new Field<decimal>("decimal(38, 14)", 26872929752907.2978m)) { Target = new Field<decimal>("money", 26872929752907.2978m) },
            new Case(new Field<decimal>("money", 26872929752907.2978m)) { Target = new Field<decimal>("decimal(20, 4)", 26872929752907.2978m) },
            new Case(new Field<Guid>("uniqueidentifier", new Guid("00112233-4455-6677-8899-AABBCCDDEEFF"))) { Target = new Field<string>("varchar(max)", "00112233-4455-6677-8899-aabbccddeeff") },
            new Case(new Field<byte[]>("image", new byte[] { 0x03, 0x3F, 0x94, 0xF3, 0x92 }) { SqlDbType = SqlDbType.Binary }) { Target = new Field<byte[]>("binary(6)", new byte[] { 0x03, 0x3F, 0x94, 0xF3, 0x92, 0x00 }) { SqlDbType = SqlDbType.Binary } },
        };

        [TestCaseSource(nameof(SimpleCases))]
        [TestCaseSource(nameof(CompatibleColumnTypeCases))]
        public void Roundtrips(Case testCase)
        {
            var firstExportStream = new MemoryStream();
            var secondExportStream = new MemoryStream();

            Assert.DoesNotThrowAsync(() => Roundtrip(testCase, firstExportStream, secondExportStream));

            if (testCase.Source.DbType == testCase.Target.DbType)
            {
                // If the types are the same, we expect the second export's ROWD chunk to be identical in every way.
                Assert.That(GetSingleDataStreamOnly(firstExportStream), Is.EqualTo(GetSingleDataStreamOnly(secondExportStream)));
            }
        }

        private static async Task Roundtrip(Case testCase, Stream firstExportStream, Stream secondExportStream)
        {
            var database = TestDatabase.LocalTempDb.Get();

            using (var source = new SimpleTestTable(database, testCase.Source.DbType, testCase.Source.SqlDbType))
            using (var writer = new BulkTableFileWriter(firstExportStream, true))
            {
                // Create source table, populate some rows, then export to firstExportStream.
                source.Create();
                source.AddRow(testCase.Source.Value, null);
                source.AddRow(testCase.Source.Value, testCase.Source.Value);
                var exportModel = new SqlServerExportModelBuilder().Build(source.ReadSchema());
                AssertExpectedDotNetTypes(testCase.Source, exportModel.ColumnSerialisers);
                await new SqlServerBulkTableExport(database).Execute(exportModel, writer, CancellationToken.None);
            }

            firstExportStream.Position = 0;

            using (var target = new SimpleTestTable(database, testCase.Target.DbType, testCase.Target.SqlDbType))
            {
                // Create target table, then import from firstExportStream.
                using (var reader = new BulkTableFileReader(firstExportStream, true))
                {
                    Assume.That(reader.MoveNext(), Is.True);
                    target.Create();
                    var importModel = new SqlServerImportModelBuilder().Build(target.ReadSchema(), reader.Current.Columns);
                    await new SqlServerBulkTableImport(database).Execute(importModel, reader.Current.DataStream, CancellationToken.None);
                    Assume.That(reader.MoveNext(), Is.False);
                }

                Assert.That(target.ReadRows(testCase.Target.DotNetType), Is.EqualTo(new [] {
                    new [] { testCase.Target.Value, null },
                    new [] { testCase.Target.Value, testCase.Target.Value }
                }));

                // Export target table to secondExportStream.
                using (var writer = new BulkTableFileWriter(secondExportStream, true))
                {
                    var exportModel = new SqlServerExportModelBuilder().Build(target.ReadSchema());
                    AssertExpectedDotNetTypes(testCase.Target, exportModel.ColumnSerialisers);
                    await new SqlServerBulkTableExport(database).Execute(exportModel, writer, CancellationToken.None);
                }
            }

            // Leave streams ready for reading from the start.
            firstExportStream.Position = 0;
            secondExportStream.Position = 0;
        }

        private byte[] GetSingleDataStreamOnly(Stream stream)
        {
            using (var reader = new BulkTableFileReader(stream, true))
            {
                Assume.That(reader.MoveNext(), Is.True);
                var bytes = new byte[reader.Current.DataStream.Length];
                reader.Current.DataStream.Read(bytes, 0, bytes.Length);
                Assume.That(reader.MoveNext(), Is.False);
                return bytes;
            }
        }

        private static void AssertExpectedDotNetTypes(IField field, IList<IColumnSerialiser> columns)
        {
            if (field.DotNetType == null) return;
            Assert.That(columns.Select(s => s.DotNetType), Is.EquivalentTo(new [] {
                typeof(int),    // Primary key of SimpleTestTable.
                field.DotNetType,
                field.DotNetType
            }));
        }

        public interface IField
        {
            string DbType { get; }
            SqlDbType? SqlDbType { get; }
            Type DotNetType { get; }
            object Value { get; }
        }

        class Field<T> : IField
        {
            public Field(string dbType, T value)
            {
                DbType = dbType;
                Value = value;
            }

            public string DbType { get; }
            public SqlDbType? SqlDbType { get; set; }
            public Type DotNetType => typeof(T);
            public object Value { get; }

            public override string ToString() => $"{DbType}:{Value}";
        }

        public struct Case
        {
            public Case(IField field)
            {
                Source = field;
                Target = field;
            }

            public IField Source { get; set; }
            public IField Target { get; set; }

            public override string ToString() => $"{Source} -> {Target}";
        }
    }
}
