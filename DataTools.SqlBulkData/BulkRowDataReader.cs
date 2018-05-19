using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace DataTools.SqlBulkData
{
    public class BulkRowDataReader : DataReaderBase
    {
        private readonly IBulkRowReader underlying;
        private readonly ColumnMetaInfo[] columnMetaInfo;

        public BulkRowDataReader(IBulkRowReader underlying, ColumnMetaInfo[] columnMetaInfo)
        {
            foreach (var column in columnMetaInfo)
            {
                if (column.SourceIndex >= underlying.Columns.Count) throw new ArgumentException($"Column {column.Name} specifies SourceIndex {column.SourceIndex}, but only {underlying.Columns.Count} columns exist in the data source.");
            }
            this.underlying = underlying;
            this.columnMetaInfo = columnMetaInfo;
        }

        public override string GetName(int ordinal) => columnMetaInfo[ordinal].Name;
        public override int GetOrdinal(string name) => Array.FindIndex(columnMetaInfo, c => c.Name == name);
        public override object GetValue(int ordinal) => underlying.Current[columnMetaInfo[ordinal].SourceIndex];
        public override bool IsDBNull(int ordinal) => this[ordinal] == DBNull.Value;
        
        public override Type GetFieldType(int ordinal) => columnMetaInfo[ordinal].FieldType;
        public override string GetDataTypeName(int ordinal) => columnMetaInfo[ordinal].DataTypeName;
        public override int FieldCount => columnMetaInfo.Length;

        public override bool Read() => underlying.MoveNext();

        private bool isClosed;
        public override bool IsClosed => isClosed;
        public override bool HasRows => true;

        public override void Close()
        {
            isClosed = true;
            base.Close();
        }
    }
}
