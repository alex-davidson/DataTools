using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using DataTools.SqlBulkData.PersistedModel;
using DataTools.SqlBulkData.Serialisation;
using Microsoft.SqlServer.Server;

namespace DataTools.SqlBulkData.Columns
{
    public class SqlServerTimeColumn : IColumnDefinition
    {
        public string Name { get; set; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(Flags);

        class Impl : IColumnSerialiser
        {
            private static readonly TimeSpan NullPlaceholder = TimeSpan.Zero;

            public Type DotNetType => typeof(TimeSpan);
            public ColumnDataType DataType => ColumnDataType.Time;
            public ColumnFlags Flags { get; }
            public int Length => 8;

            public Impl(ColumnFlags flags)
            {
                Flags = flags & ColumnFlags.AbsentWhenNull;
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                Serialiser.AlignWrite(stream, 4);
                Serialiser.WriteInt64(stream, GetTimeSpanValue(record, i).Ticks);
            }

            private static TimeSpan GetTimeSpanValue(IDataRecord record, int i)
            {
                if (record.IsDBNull(i)) return NullPlaceholder;
                if (record is SqlDataReader sqlDataReader) return sqlDataReader.GetTimeSpan(i);
                if (record is SqlDataRecord sqlDataRecord) return sqlDataRecord.GetTimeSpan(i);
                return (TimeSpan)record.GetValue(i);
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                Serialiser.AlignRead(stream, 4);
                return new TimeSpan(Serialiser.ReadInt64(stream));
            }
        }
    }
}
