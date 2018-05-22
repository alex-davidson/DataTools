using System;
using System.Data;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerUniqueIdentifierColumn : IColumnDefinition
    {
        public string Name { get; set; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(Flags);

        class Impl : IColumnSerialiser
        {
            private readonly Guid NullPlaceholder = Guid.Empty;

            public Type DotNetType => typeof(Guid);
            public ColumnDataType DataType => ColumnDataType.Guid;
            public ColumnFlags Flags { get; }
            public int Length => 16;

            public Impl(ColumnFlags flags)
            {
                Flags = flags & ColumnFlags.AbsentWhenNull;
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                Serialiser.AlignWrite(stream, 8);
                Serialiser.WriteGuid(stream, record.IsDBNull(i) ? NullPlaceholder : record.GetGuid(i));
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                Serialiser.AlignRead(stream, 8);
                return Serialiser.ReadGuid(stream);
            }
        }
    }
}
