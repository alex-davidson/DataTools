using System;
using System.Data;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerSmallIntColumn : IColumnDefinition
    {
        public string Name { get; set; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(Flags);

        class Impl : IColumnSerialiser
        {
            private const short NullPlaceholder = 0;

            public Type DotNetType => typeof(short);
            public ColumnDataType DataType => ColumnDataType.SignedInteger;
            public ColumnFlags Flags { get; }
            public int Length => 2;

            public Impl(ColumnFlags flags)
            {
                Flags = flags & ColumnFlags.AbsentWhenNull;
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                Serialiser.AlignWrite(stream, 2);
                Serialiser.WriteInt16(stream, record.IsDBNull(i) ? NullPlaceholder : record.GetInt16(i));
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                Serialiser.AlignRead(stream, 2);
                return Serialiser.ReadInt16(stream);
            }
        }
    }
}
