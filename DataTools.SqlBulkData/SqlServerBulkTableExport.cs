using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DataTools.SqlBulkData.PersistedModel;

namespace DataTools.SqlBulkData
{
    public class SqlServerBulkTableExport
    {
        private readonly SqlServerDatabase database;

        public SqlServerBulkTableExport(SqlServerDatabase database)
        {
            this.database = database;
        }

        public Task Execute(ExportModel model, BulkTableFileWriter fileWriter, CancellationToken token)
        {
            using (var cn = database.OpenConnection())
            using (var cmd = Sql.CreateQuery(cn, BuildSelectStatement(model), database.DefaultTimeout))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    token.ThrowIfCancellationRequested();
                    fileWriter.AddTable(model.TableDescriptor);
                    fileWriter.AddColumns(new TableColumns { TableId = model.Id, Columns = model.ColumnDescriptors });
                    using (var rowData = fileWriter.BeginAddRowData(model.Id))
                    {
                        var rowWriter = new BulkRowWriter(rowData.Stream, model.ColumnSerialisers);
                        while (reader.Read())
                        {
                            token.ThrowIfCancellationRequested();
                            rowWriter.Write(reader);
                        }
                    }
                }
            }
            return Task.CompletedTask;
        }

        private static string BuildSelectStatement(ExportModel model)
        {
            return $@"select
    {String.Join(",\n    ", model.ColumnDescriptors.Select(c => Sql.Escape(c.OriginalName)))}
from {Sql.Escape(model.TableDescriptor.Schema, model.TableDescriptor.Name)}";
        }
    }
}
