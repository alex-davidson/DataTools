using System;
using System.Data;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerSinglePrecisionColumn : IColumnDefinition
    {
        public string Name { get; set; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(Flags);

        class Impl : IColumnSerialiser
        {
            private const float NullPlaceholder = 0;

            public Type DotNetType => typeof(float);
            public ColumnDataType DataType => ColumnDataType.FloatingPoint;
            public ColumnFlags Flags { get; }
            public int Length => 4;

            public Impl(ColumnFlags flags)
            {
                Flags = flags & ColumnFlags.AbsentWhenNull;
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                Serialiser.AlignWrite(stream, 4);
                Serialiser.WriteSingle(stream, record.IsDBNull(i) ? NullPlaceholder : record.GetFloat(i));
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                Serialiser.AlignRead(stream, 4);
                return Serialiser.ReadSingle(stream);
            }
        }
    }
}
