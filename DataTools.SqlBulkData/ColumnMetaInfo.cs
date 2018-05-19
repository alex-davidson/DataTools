using System;

namespace DataTools.SqlBulkData
{
    public class ColumnMetaInfo
    {
        public ColumnMetaInfo(string name, int sourceIndex)
        {
            if (sourceIndex < 0) throw new ArgumentOutOfRangeException(nameof(sourceIndex));
            Name = name;
            SourceIndex = sourceIndex;
        }

        public string Name { get; }
        public int SourceIndex { get; }
        public Type FieldType { get; set; }
        public string DataTypeName { get; set; }
    }
}
