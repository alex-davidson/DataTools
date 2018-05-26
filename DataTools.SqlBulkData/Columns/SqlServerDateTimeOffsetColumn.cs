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
    public class SqlServerDateTimeOffsetColumn : IColumnDefinition
    {
        public string Name { get; set; }
        public ColumnFlags Flags { get; set; }

        public IColumnSerialiser GetSerialiser() => new Impl(Flags);

        class Impl : IColumnSerialiser
        {
            private static readonly DateTimeOffset NullPlaceholder = DateTimeOffset.MinValue;

            public Type DotNetType => typeof(DateTimeOffset);
            public ColumnDataType DataType => ColumnDataType.DateTimeOffset;
            public ColumnFlags Flags { get; }
            public int Length => 12;

            public Impl(ColumnFlags flags)
            {
                Flags = flags & ColumnFlags.AbsentWhenNull;
            }

            void IColumnSerialiser.Write(Stream stream, IDataRecord record, int i)
            {
                Serialiser.AlignWrite(stream, 4);
                Serialiser.WriteDateTimeOffset(stream, GetDateTimeOffsetValue(record, i));
            }

            private static DateTimeOffset GetDateTimeOffsetValue(IDataRecord record, int i)
            {
                if (record.IsDBNull(i)) return NullPlaceholder;
                if (record is SqlDataReader sqlDataReader) return sqlDataReader.GetDateTimeOffset(i);
                if (record is SqlDataRecord sqlDataRecord) return sqlDataRecord.GetDateTimeOffset(i);
                var rawValue = record.GetValue(i);
                if (rawValue is DateTimeOffset sql) return sql;
                return new DateTimeOffset(record.GetDateTime(i));
            }

            object IColumnSerialiser.Read(Stream stream, int i, bool[] nullMap)
            {
                Serialiser.AlignRead(stream, 4);
                return Serialiser.ReadDateTimeOffset(stream);
            }
        }
    }
}
