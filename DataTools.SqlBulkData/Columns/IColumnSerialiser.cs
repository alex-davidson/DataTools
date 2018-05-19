using System;
using System.Data;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;

namespace DataTools.SqlBulkData.Columns
{
    public interface IColumnSerialiser
    {
        Type DotNetType { get; }
        ColumnDataType DataType { get; }
        ColumnFlags Flags { get; }
        int Length { get; }

        void Write(Stream stream, IDataRecord record, int i);
        object Read(Stream stream, int i, bool[] nullMap);
    }
}
