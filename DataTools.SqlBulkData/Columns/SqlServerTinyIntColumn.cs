using System;
using System.Data;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerTinyIntColumn : IColumnDefinition
    {
        public string Name { get; set; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(Flags);

        class Impl : IColumnSerialiser
        {
            private const byte NullPlaceholder = 0;

            public Type DotNetType => typeof(byte);
            public ColumnDataType DataType => ColumnDataType.UnsignedInteger;
            public ColumnFlags Flags { get; }
            public int Length => 1;

            public Impl(ColumnFlags flags)
            {
                Flags = flags & ColumnFlags.AbsentWhenNull;
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                Serialiser.WriteByte(stream, record.IsDBNull(i) ? NullPlaceholder : record.GetByte(i));
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                return Serialiser.ReadByte(stream);
            }
        }
    }
}
