using System;
using System.Data.SqlClient;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Schema;
using DataTools.SqlBulkData.UnitTests.IntegrationTesting;
using NUnit.Framework;

namespace DataTools.SqlBulkData.UnitTests
{
    [TestFixture]
    public class ForeignKeyStatementTests
    {
        private static string RandomiseSymbolName(string prefix) => $"{prefix}_{Guid.NewGuid():N}";

        private Table DefinePrimaryTable() =>
            new Table {
                Name = RandomiseSymbolName("primary_table"),
                Schema = "dbo",
                Fields = new [] {
                    new Table.Field { Name = "p_first", DataType = new Table.DataType { Name = "int" } },
                    new Table.Field { Name = "p_second", DataType = new Table.DataType { Name = "int" } }
                }
            };

        private Table DefineForeignTable() =>
            new Table {
                Name = RandomiseSymbolName("foreign_table"),
                Schema = "dbo",
                Fields = new [] {
                    new Table.Field { Name = "f_first", DataType = new Table.DataType { Name = "int" } },
                    new Table.Field { Name = "f_second", DataType = new Table.DataType { Name = "int" } }
                }
            };

        [Test]
        public void CanCreateMultiColumnForeignKey()
        {
            var database = TestDatabase.LocalTempDb.Get();
            using (var tempTables = new TemporaryTables(database))
            {
                var primaryTable = DefinePrimaryTable();
                tempTables.Create(primaryTable);
                var foreignTable = DefineForeignTable();
                tempTables.Create(foreignTable);
                CreateUniqueIndex(database, primaryTable.Identify(), "testindex", new [] { "p_first", "p_second" });

                var key = new ForeignKey {
                    Name = RandomiseSymbolName("foreign_key"),
                    Description = "description test",
                    PrimaryTable = primaryTable.Identify(),
                    PrimaryColumns = new [] { "p_first", "p_second" },
                    ForeignTable = foreignTable.Identify(),
                    ForeignColumns = new [] { "f_first", "f_second" }
                };
                new CreateForeignKeyStatement().Execute(database, key);
            }
        }

        [Test]
        public void CanGetMultiColumnForeignKey()
        {
            var database = TestDatabase.LocalTempDb.Get();
            using (var tempTables = new TemporaryTables(database))
            {
                var primaryTable = DefinePrimaryTable();
                tempTables.Create(primaryTable);
                var foreignTable = DefineForeignTable();
                tempTables.Create(foreignTable);
                CreateUniqueIndex(database, primaryTable.Identify(), "testindex", new [] { "p_first", "p_second" });

                var key = new ForeignKey {
                    Name = RandomiseSymbolName("foreign_key"),
                    PrimaryTable = primaryTable.Identify(),
                    PrimaryColumns = new [] { "p_first", "p_second" },
                    ForeignTable = foreignTable.Identify(),
                    ForeignColumns = new [] { "f_first", "f_second" }
                };
                new CreateForeignKeyStatement().Execute(database, key);

                var keys = new GetAllForeignKeysQuery().List(database);
                Assert.That(keys, Has.Member(key).Using(new ForeignKeyExactEqualityComparer()));
            }
        }

        [Test]
        public void CanDeleteMultiColumnForeignKey()
        {
            var database = TestDatabase.LocalTempDb.Get();
            using (var tempTables = new TemporaryTables(database))
            {
                var primaryTable = DefinePrimaryTable();
                tempTables.Create(primaryTable);
                var foreignTable = DefineForeignTable();
                tempTables.Create(foreignTable);
                CreateUniqueIndex(database, primaryTable.Identify(), "testindex", new [] { "p_first", "p_second" });

                var key = new ForeignKey {
                    Name = RandomiseSymbolName("foreign_key"),
                    PrimaryTable = primaryTable.Identify(),
                    PrimaryColumns = new [] { "p_first", "p_second" },
                    ForeignTable = foreignTable.Identify(),
                    ForeignColumns = new [] { "f_first", "f_second" }
                };
                new CreateForeignKeyStatement().Execute(database, key);
                new DeleteForeignKeyStatement().Execute(database, key);
            }
        }

        [Test]
        public void CannotCreateEnforcedInitiallyInvalidForeignKey()
        {
            var database = TestDatabase.LocalTempDb.Get();
            using (var tempTables = new TemporaryTables(database))
            {
                var primaryTable = DefinePrimaryTable();
                tempTables.Create(primaryTable);
                var foreignTable = DefineForeignTable();
                tempTables.Create(foreignTable);
                CreateUniqueIndex(database, primaryTable.Identify(), "testindex", new [] { "p_first", "p_second" });

                var sql = $"insert into {Sql.Escape(foreignTable.Schema, foreignTable.Name)}(f_first, f_second) values (1, 2)";
                using (var cn = database.OpenConnection())
                using (var cmd = Sql.CreateQuery(cn, sql))
                {
                    cmd.ExecuteNonQuery();
                }

                var key = new ForeignKey {
                    Name = RandomiseSymbolName("foreign_key"),
                    PrimaryTable = primaryTable.Identify(),
                    PrimaryColumns = new [] { "p_first", "p_second" },
                    ForeignTable = foreignTable.Identify(),
                    ForeignColumns = new [] { "f_first", "f_second" }
                };
                Assert.Throws<SqlException>(() => new CreateForeignKeyStatement().Execute(database, key));
            }
        }

        [Test]
        public void CanCreateNotEnforcedInitiallyInvalidForeignKey()
        {
            var database = TestDatabase.LocalTempDb.Get();
            using (var tempTables = new TemporaryTables(database))
            {
                var primaryTable = DefinePrimaryTable();
                tempTables.Create(primaryTable);
                var foreignTable = DefineForeignTable();
                tempTables.Create(foreignTable);
                CreateUniqueIndex(database, primaryTable.Identify(), "testindex", new [] { "p_first", "p_second" });

                var sql = $"insert into {Sql.Escape(foreignTable.Schema, foreignTable.Name)}(f_first, f_second) values (1, 2)";
                using (var cn = database.OpenConnection())
                using (var cmd = Sql.CreateQuery(cn, sql))
                {
                    cmd.ExecuteNonQuery();
                }

                var key = new ForeignKey {
                    Name = RandomiseSymbolName("foreign_key"),
                    PrimaryTable = primaryTable.Identify(),
                    PrimaryColumns = new [] { "p_first", "p_second" },
                    ForeignTable = foreignTable.Identify(),
                    ForeignColumns = new [] { "f_first", "f_second" }
                };
                new CreateForeignKeyStatement { WithNoCheck = true }.Execute(database, key);
            }
        }

        [Test]
        public void CannotEnableInvalidForeignKey()
        {
            var database = TestDatabase.LocalTempDb.Get();
            using (var tempTables = new TemporaryTables(database))
            {
                var primaryTable = DefinePrimaryTable();
                tempTables.Create(primaryTable);
                var foreignTable = DefineForeignTable();
                tempTables.Create(foreignTable);
                CreateUniqueIndex(database, primaryTable.Identify(), "testindex", new [] { "p_first", "p_second" });

                var sql = $"insert into {Sql.Escape(foreignTable.Schema, foreignTable.Name)}(f_first, f_second) values (1, 2)";
                using (var cn = database.OpenConnection())
                using (var cmd = Sql.CreateQuery(cn, sql))
                {
                    cmd.ExecuteNonQuery();
                }

                var key = new ForeignKey {
                    Name = RandomiseSymbolName("foreign_key"),
                    PrimaryTable = primaryTable.Identify(),
                    PrimaryColumns = new [] { "p_first", "p_second" },
                    ForeignTable = foreignTable.Identify(),
                    ForeignColumns = new [] { "f_first", "f_second" }
                };
                new CreateForeignKeyStatement { WithNoCheck = true }.Execute(database, key);

                Assert.Throws<SqlException>(() => new EnableConstraintsStatement().Execute(database, foreignTable));
            }
        }

        [Test]
        public void CanEnableValidForeignKey()
        {
            var database = TestDatabase.LocalTempDb.Get();
            using (var tempTables = new TemporaryTables(database))
            {
                var primaryTable = DefinePrimaryTable();
                tempTables.Create(primaryTable);
                var foreignTable = DefineForeignTable();
                tempTables.Create(foreignTable);
                CreateUniqueIndex(database, primaryTable.Identify(), "testindex", new [] { "p_first", "p_second" });

                var key = new ForeignKey {
                    Name = RandomiseSymbolName("foreign_key"),
                    PrimaryTable = primaryTable.Identify(),
                    PrimaryColumns = new [] { "p_first", "p_second" },
                    ForeignTable = foreignTable.Identify(),
                    ForeignColumns = new [] { "f_first", "f_second" }
                };
                new CreateForeignKeyStatement { WithNoCheck = true }.Execute(database, key);

                new EnableConstraintsStatement().Execute(database, foreignTable);
            }
        }

        private void CreateUniqueIndex(SqlServerDatabase database, TableIdentifier table, string indexName, string[] columns)
        {
            var sql = $"create unique nonclustered index {Sql.Escape(indexName)} on {Sql.Escape(table.Schema, table.Name)}({Sql.EscapeColumnList(columns)})";
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, sql, database.DefaultTimeout))
            {
                cmd.ExecuteNonQuery();
            }
        }
    }
}
