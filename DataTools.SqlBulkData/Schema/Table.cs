using System.Data;
using DataTools.SqlBulkData.PersistedModel;

namespace DataTools.SqlBulkData.Schema
{
    public class Table
    {
        public string Name { get; set; }
        public string Schema { get; set; }
        public Field[] Fields { get; set; }

        public class Field
        {
            public string Name { get; set; }
            public DataType DataType { get; set; }
            public bool IsNullable { get; set; }
            public bool IsComputed { get; set; }

            public override string ToString() => Name;
        }

        public struct DataType
        {
            public string Name { get; set; }
            public SqlDbType SqlDbType { get; set; }
            public int MaxLength { get; set; }
        }

        public override string ToString() => $"{Schema}.{Name}".Trim('.');
        public TableIdentifier Identify() => new TableIdentifier(Schema, Name);
    }
}
