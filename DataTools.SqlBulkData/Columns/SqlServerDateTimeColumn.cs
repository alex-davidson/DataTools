using System;
using System.Data;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerDateTimeColumn : IColumnDefinition
    {
        public string Name { get; set; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(Flags);

        class Impl : IColumnSerialiser
        {
            private readonly DateTime NullPlaceholder = DateTime.MinValue;

            public Type DotNetType => typeof(DateTime);
            public ColumnDataType DataType => ColumnDataType.DateTime;
            public ColumnFlags Flags { get; }
            public int Length => 8;

            public Impl(ColumnFlags flags)
            {
                Flags = flags & ColumnFlags.AbsentWhenNull;
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                Serialiser.AlignWrite(stream, 4);
                Serialiser.WriteDateTime(stream, record.IsDBNull(i) ? NullPlaceholder : record.GetDateTime(i));
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                Serialiser.AlignRead(stream, 4);
                return Serialiser.ReadDateTime(stream);
            }
        }
    }
}
