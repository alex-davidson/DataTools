using System;
using System.Data;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerBigIntColumn : IColumnDefinition
    {
        public string Name { get; set; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(Flags);

        class Impl : IColumnSerialiser
        {
            private const byte NullPlaceholder = 0;

            public Type DotNetType => typeof(long);
            public ColumnDataType DataType => ColumnDataType.SignedInteger;
            public ColumnFlags Flags { get; }
            public int Length => 8;

            public Impl(ColumnFlags flags)
            {
                Flags = flags & ColumnFlags.AbsentWhenNull;
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                Serialiser.AlignWrite(stream, 4);
                Serialiser.WriteInt64(stream, record.IsDBNull(i) ? NullPlaceholder : record.GetInt64(i));
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                Serialiser.AlignRead(stream, 4);
                return Serialiser.ReadInt64(stream);
            }
        }
    }
}
